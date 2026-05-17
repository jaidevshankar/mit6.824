package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
)




//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	workerID := os.Getpid()

	for {
		args := Args{}
		args.ID = workerID
		args.TaskIndex = -1
		reply := Reply{}

		ok := call("Coordinator.AtomicPullTask", &args, &reply)
		if !ok {
			break
		}

		if reply.Exit == 1 {
			break
		}

		if reply.T == "map" {
			content, err := os.ReadFile(reply.Filename)
			if err != nil {
				log.Fatalf("cannot read %v", reply.Filename)
			}
			kva := mapf(reply.Filename, string(content))

			tempFiles := [](*os.File){}
			encoders := []*json.Encoder{}
			for i := 0; i < reply.NumReduce; i++ {
				tempFile, _ := os.CreateTemp("", fmt.Sprintf("mr-tmp-%d", i))
				encoder := json.NewEncoder(tempFile)
				tempFiles = append(tempFiles, tempFile)
				encoders = append(encoders, encoder)
			}

			for _, kv := range kva {
				bucket := ihash(kv.Key) % reply.NumReduce
				err := encoders[bucket].Encode(&kv)
				if err != nil {
					log.Fatalf("Cannot encode")
				}
			}

			for i := 0; i < reply.NumReduce; i++ {
				tempFiles[i].Close()
				name := fmt.Sprintf("mr-%d-%d", reply.TaskIndex, i)
				err := os.Rename(tempFiles[i].Name(), name)
				if err != nil {
					log.Fatalf("Cannot rename")
				}
			}
			args.TaskIndex = reply.TaskIndex
			reply = Reply{}
			ok := call("Coordinator.FinishMapTask", &args, &reply)
			if !ok {
				break
			}

		} else {
			kva := []KeyValue{}
			for i := 0; i < reply.NumMap; i++ {
				filename := fmt.Sprintf("mr-%d-%d", i, reply.TaskIndex)

				file, err := os.Open(filename)
				if err != nil {
					continue
				}

				decoder := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := decoder.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}

				file.Close()
			}

			sort.Sort(ByKey(kva))

			tempName := fmt.Sprintf("mr-out-tmp-%d", reply.TaskIndex)
			tempFile, _ := os.CreateTemp("", tempName)

			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(tempFile, "%v %v\n", kva[i].Key, output)

				i = j
			}
			oname := fmt.Sprintf("mr-out-%d", reply.TaskIndex)
			tempFile.Close()
			err := os.Rename(tempFile.Name(), oname)
			if err != nil {
				log.Fatalf("Cannot rename")
			}
			args.TaskIndex = reply.TaskIndex
			reply = Reply{}
			ok := call("Coordinator.FinishReduceTask", &args, &reply)

			if !ok {
				break
			}
		}

	}


}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
