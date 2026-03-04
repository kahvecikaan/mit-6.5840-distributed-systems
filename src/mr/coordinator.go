package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// holds a task that's currently being executed, along with when it was assigned
type inProgressInfo struct {
	task      Task
	startTime time.Time
}

type Coordinator struct {
	mapTasks    chan Task
	reduceTasks chan Task

	done chan struct{} // closed when entire job is complete

	mu               sync.Mutex
	mapInProgress    map[int]inProgressInfo
	reduceInProgress map[int]inProgressInfo

	nMapDone    int // only increments when a worker calls ReportDone
	nReduceDone int // only increments when a worker calls ReportDone
	nMap        int
	nReduce     int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapTasks:         make(chan Task, len(files)),
		reduceTasks:      make(chan Task, nReduce),
		done:             make(chan struct{}),
		mapInProgress:    make(map[int]inProgressInfo),
		reduceInProgress: make(map[int]inProgressInfo),
		nMap:             len(files),
		nReduce:          nReduce,
	}

	for i, file := range files {
		c.mapTasks <- Task{
			TaskType: MapTask,
			TaskId:   i,
			FileName: file,
			NReduce:  nReduce,
			NMap:     len(files),
		}
	}

	go c.timeoutChecker()
	c.server()
	return &c
}

func (c *Coordinator) timeoutChecker() {
	for {
		time.Sleep(1 * time.Second)

		var timedOutMap []Task
		var timedOutReduce []Task

		c.mu.Lock()
		for id, info := range c.mapInProgress {
			if time.Since(info.startTime) > 10*time.Second {
				timedOutMap = append(timedOutMap, info.task)
				delete(c.mapInProgress, id)
			}
		}

		for id, info := range c.reduceInProgress {
			if time.Since(info.startTime) > 10*time.Second {
				timedOutReduce = append(timedOutReduce, info.task)
				delete(c.reduceInProgress, id)
			}
		}
		c.mu.Unlock()

		for _, task := range timedOutMap {
			c.mapTasks <- task
		}

		for _, task := range timedOutReduce {
			c.reduceTasks <- task
		}
	}
}

func (c *Coordinator) GetTask(args *TaskArgs, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	select {
	case task := <-c.mapTasks:
		c.mapInProgress[task.TaskId] = inProgressInfo{
			task:      task,
			startTime: time.Now(),
		}

		reply.Task = task
		return nil
	default:
		// channel is empty, non-blocking
	}

	if c.nMapDone < c.nMap {
		// all the map tasks are being ran by some workers, but one of them might fail so check back soon
		reply.Task = Task{TaskType: WaitTask}
		return nil
	}

	select {
	case task := <-c.reduceTasks:
		c.reduceInProgress[task.TaskId] = inProgressInfo{
			task:      task,
			startTime: time.Now(),
		}

		reply.Task = task
		return nil
	default:
	}

	if c.nReduceDone < c.nReduce {
		reply.Task = Task{TaskType: WaitTask}
		return nil
	}

	reply.Task = Task{TaskType: DoneTask}
	return nil
}

func (c *Coordinator) ReportDone(args *DoneArgs, reply *DoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch args.TaskType {
	case MapTask:
		if _, ok := c.mapInProgress[args.TaskId]; !ok {
			return nil
		}

		delete(c.mapInProgress, args.TaskId)
		c.nMapDone++

		if c.nMapDone == c.nMap {
			for i := 0; i < c.nReduce; i++ {
				c.reduceTasks <- Task{
					TaskType: ReduceTask,
					TaskId:   i,
					NReduce:  c.nReduce,
					NMap:     c.nMap,
				}
			}
		}

	case ReduceTask:
		if _, ok := c.reduceInProgress[args.TaskId]; !ok {
			return nil
		}

		delete(c.reduceInProgress, args.TaskId)
		c.nReduceDone++

		if c.nReduceDone == c.nReduce {
			close(c.done)
		}
	default:
	}

	return nil
}
