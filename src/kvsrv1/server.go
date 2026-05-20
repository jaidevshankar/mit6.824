package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Entry struct {
	version rpc.Tversion
	value string
}

type KVServer struct {
	mu sync.Mutex
	data map[string]Entry
	// Your definitions here.
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// Your code here.
	kv.data = make(map[string]Entry)
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	key := args.Key

	kv.mu.Lock()
	entry, ok := kv.data[key]
	kv.mu.Unlock()
	if ok {
		reply.Err = rpc.OK
		reply.Value = entry.value
		reply.Version = entry.version
	} else {
		reply.Err = rpc.ErrNoKey
	}
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	key := args.Key
	value := args.Value
	version := args.Version

	kv.mu.Lock()
	entry, ok := kv.data[key]
	if ok && entry.version != version { // key exists but version doesn't match
		reply.Err = rpc.ErrVersion
	} else if !ok && version != 0 { // key doesn't exist yet but version is not 0
		reply.Err = rpc.ErrNoKey
	} else { // ok
		newEntry := Entry{}
		newEntry.value = value
		newEntry.version = version + 1
		kv.data[key] = newEntry
		reply.Err = rpc.OK
	}
	kv.mu.Unlock()
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}


// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
