package mr

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(
	mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	for {
		r := CallGetTask()
		switch r.Task.TaskType {
		case WaitTask:
			time.Sleep(3 * time.Second)
		case MapTask:
			doMap(r.Task, mapf)
			CallReportDone(r.Task)
		case ReduceTask:
			doReduce(r.Task, reducef)
			CallReportDone(r.Task)
		case DoneTask:
			return
		}
	}

}

func doMap(task Task, mapf func(string, string) []KeyValue) {
	filename := task.FileName

	b, err := os.ReadFile(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}

	contents := string(b)
	kva := mapf(filename, contents)

	tmpFiles := make([]*os.File, task.NReduce)
	encoders := make([]*json.Encoder, task.NReduce)

	for i := 0; i < task.NReduce; i++ {
		tmpFiles[i], _ = os.CreateTemp("", "mr-map-tmp-*")
		encoders[i] = json.NewEncoder(tmpFiles[i])
	}

	for _, kv := range kva {
		bucket := ihash(kv.Key) % task.NReduce
		encoders[bucket].Encode(kv)
	}

	for i := 0; i < len(tmpFiles); i++ {
		tmpFiles[i].Close()
		finalName := fmt.Sprintf("mr-%d-%d", task.TaskId, i)
		os.Rename(tmpFiles[i].Name(), finalName)
	}
}

func doReduce(task Task, reducef func(string, []string) string) {
	var kva []KeyValue
	for i := 0; i < task.NMap; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, task.TaskId)
		f, _ := os.Open(filename)
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break // EOF
			}

			kva = append(kva, kv)
		}

		f.Close()
	}

	sort.Sort(ByKey(kva))

	tmpFile, _ := os.CreateTemp("", "mr-reduce-tmp-*")
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

		fmt.Fprintf(tmpFile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	tmpFile.Close()
	os.Rename(tmpFile.Name(), fmt.Sprintf("mr-out-%d", task.TaskId))
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

func CallGetTask() TaskReply {
	args := TaskArgs{}
	reply := TaskReply{}

	if ok := call("Coordinator.GetTask", &args, &reply); !ok {
		fmt.Printf("call failed!\n")
	}

	return reply
}

func CallReportDone(task Task) {
	args := DoneArgs{
		TaskType: task.TaskType,
		TaskId:   task.TaskId,
	}

	reply := DoneReply{}

	if ok := call("Coordinator.ReportDone", &args, &reply); !ok {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
