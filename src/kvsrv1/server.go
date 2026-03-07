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

type Data struct {
	value   string
	version rpc.Tversion
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	data map[string]Data
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// Your code here.
	kv.data = make(map[string]Data)
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if data, ok := kv.data[args.Key]; ok {
		reply.Err = rpc.OK
		reply.Value = data.value
		reply.Version = data.version
		return
	}

	reply.Err = rpc.ErrNoKey
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _, ok := kv.data[args.Key]; !ok {
		if args.Version == 0 {
			kv.data[args.Key] = Data{value: args.Value, version: 1}
			reply.Err = rpc.OK
			return
		}
		reply.Err = rpc.ErrNoKey
		return
	}

	data := kv.data[args.Key]
	if args.Version == data.version {
		kv.data[args.Key] = Data{value: args.Value, version: args.Version + 1}
		reply.Err = rpc.OK
		return
	}

	reply.Err = rpc.ErrVersion
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
