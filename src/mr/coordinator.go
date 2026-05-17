package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"

type MapTask struct {
	state string // "idle" "inProgress" "completed"
	input_filename string
	start_time time.Time
	completed_by int
	assigned_to int
}

type ReduceTask struct {
	state string
	start_time time.Time
	assigned_to int
}

type Coordinator struct {
	// Your definitions here.
	mtx sync.Mutex
	cv *sync.Cond
	map_tasks []MapTask
	reduce_tasks []ReduceTask
	num_map_finished int
	num_reduce_finished int
	num_reduce_tasks int
	num_map_tasks int
	phase int // 0 for map phase, 1 for reduce phase
}

// Your code here -- RPC handlers for the worker to call.
// potentially problem - how do i poll for stragglers?
// atomically pulls a task. checks if the next task is a map task (can start immediately)
// or eles it's a reduce/exit task, which must wait for all map tasks to finish
func (c *Coordinator) AtomicPullTask(args *Args, reply *Reply) error {
	
	c.cv.L.Lock()
	for c.num_map_finished != c.num_map_tasks { // keep worker pool alive while tasks are running
		for i := 0; i < c.num_map_tasks; i++ {
			now := time.Now()
			if c.map_tasks[i].state == "idle" {
				c.map_tasks[i].state = "inProgress"
				c.map_tasks[i].start_time = now
				c.map_tasks[i].assigned_to = args.ID
				reply.Filename = c.map_tasks[i].input_filename
				reply.NumMap = c.num_map_tasks
				reply.NumReduce = c.num_reduce_tasks
				reply.T = "map"
				reply.TaskIndex = i
				c.cv.L.Unlock()
				return nil
			}
		}
		c.cv.Wait()
	}

	c.phase = 1

	for c.num_reduce_finished != c.num_reduce_tasks {
		for i := 0; i < c.num_reduce_tasks; i++ {
			now := time.Now()
			if c.reduce_tasks[i].state == "idle" {
				c.reduce_tasks[i].state = "inProgress"
				c.reduce_tasks[i].start_time = now
				c.reduce_tasks[i].assigned_to = args.ID
				reply.NumMap = c.num_map_tasks
				reply.NumReduce = c.num_reduce_tasks
				reply.T = "reduce"
				reply.TaskIndex = i
				c.cv.L.Unlock()
				return nil
			}
		}
		c.cv.Wait()
	}

	reply.Exit = 1
	
	c.cv.L.Unlock()
	return nil
}

func (c *Coordinator) FinishMapTask(args *Args, reply *Reply) error {
	c.cv.L.Lock()
	if c.map_tasks[args.TaskIndex].state == "inProgress" {
		c.num_map_finished++
		c.map_tasks[args.TaskIndex].completed_by = args.ID
		c.map_tasks[args.TaskIndex].state = "completed"
		c.cv.Broadcast()
	}
	c.cv.L.Unlock()
	return nil
}

func (c *Coordinator) FinishReduceTask(args *Args, reply *Reply) error {
	c.cv.L.Lock()
	if c.reduce_tasks[args.TaskIndex].state == "inProgress" {
		c.num_reduce_finished++
		c.reduce_tasks[args.TaskIndex].state = "completed"
		c.cv.Broadcast()
	}
	c.cv.L.Unlock()
	return nil
}


func (c *Coordinator) monitor() {
	for {
		time.Sleep(1 * time.Second)

		c.cv.L.Lock()
		
		if c.phase == 0 {
			for i:=0; i < c.num_map_tasks; i++ {
				now := time.Now()
				if (c.map_tasks[i].state == "inProgress" && (now.Sub(c.map_tasks[i].start_time)) > 10 * time.Second) {
					c.map_tasks[i].state = "idle"
					for j := 0; j < len(c.map_tasks); j++ {
						if c.map_tasks[j].state == "completed" && c.map_tasks[j].completed_by == c.map_tasks[i].assigned_to {
							c.map_tasks[j].completed_by = -1
							c.map_tasks[j].assigned_to = -1
							c.map_tasks[j].state = "idle"
							c.num_map_finished--
						}
					}
					c.map_tasks[i].assigned_to = -1
					c.cv.Broadcast()
				}
			}
		} else {
			for i:=0; i < c.num_reduce_tasks; i++ {
				now := time.Now()
				if c.reduce_tasks[i].state == "inProgress" && (now.Sub(c.reduce_tasks[i].start_time)) > 10 * time.Second {
					c.reduce_tasks[i].state = "idle"
					c.reduce_tasks[i].assigned_to = -1
					c.cv.Broadcast()
				}
			}
		}
		

		c.cv.L.Unlock()
	}
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	done := false
	c.cv.L.Lock()
	if c.num_reduce_finished == c.num_reduce_tasks {
		done = true
	}
	c.cv.L.Unlock()
	return done
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.cv = sync.NewCond(&c.mtx)
	c.num_map_finished = 0
	c.num_reduce_finished = 0
	c.num_map_tasks = len(files)
	c.num_reduce_tasks = nReduce

	for i := 0; i < len(files); i++ {
		task := MapTask{}
		task.assigned_to = -1
		task.completed_by = -1
		task.input_filename	= files[i]
		task.state = "idle"
		c.map_tasks = append(c.map_tasks, task)
	}

	for i := 0; i < nReduce; i++ {
		task := ReduceTask{}
		task.assigned_to = -1
		task.state = "idle"
		c.reduce_tasks = append(c.reduce_tasks, task)
	}

	// Your code here.
	go c.monitor()
	c.server()
	return &c
}
