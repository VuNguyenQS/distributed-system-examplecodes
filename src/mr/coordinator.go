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

type Coordinator struct {
	// Your definitions here.
	done    bool
	nMap    int
	stage   string
	nReduce int
	mtask   maptask
	rtask   reducetask
}

type maptask struct {
	mu        sync.Mutex
	doneMap   int
	idleMap   []int
	files     []string
	isDoneMap map[int]bool
}

type reducetask struct {
	mu           sync.Mutex
	doneRed      int
	idleReduce   []int
	isDoneReduce map[int]bool
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) mapReply(reply *TaskReply) {
	//check idle map
	c.mtask.mu.Lock()
	defer c.mtask.mu.Unlock()

	if len(c.mtask.idleMap) > 0 {
		nth := c.mtask.idleMap[0]
		c.mtask.idleMap = c.mtask.idleMap[1:]
		if c.mtask.isDoneMap[nth] {
			return
		}
		reply.Task = "map"
		reply.Filename = c.mtask.files[nth]
		reply.NReduce = c.nReduce
		reply.NMap = nth
		go func(nth int) {
			time.Sleep(time.Second * 10)
			c.mtask.mu.Lock()
			defer c.mtask.mu.Unlock()
			if !c.mtask.isDoneMap[nth] {
				c.mtask.idleMap = append(c.mtask.idleMap, nth)
			}
		}(nth)
	}
}

func (c *Coordinator) reduceReply(reply *TaskReply) {
	//tasks := c.rtask
	c.rtask.mu.Lock()
	defer c.rtask.mu.Unlock()
	if len(c.rtask.idleReduce) > 0 {
		nth := c.rtask.idleReduce[0]
		c.rtask.idleReduce = c.rtask.idleReduce[1:]
		if c.rtask.isDoneReduce[nth] {
			return
		}
		reply.Task = "reduce"
		reply.NMap = c.nMap
		reply.NReduce = nth
		go func(nth int) {
			time.Sleep(time.Second * 10)
			c.rtask.mu.Lock()
			defer c.rtask.mu.Unlock()
			if !c.rtask.isDoneReduce[nth] {
				c.rtask.idleReduce = append(c.rtask.idleReduce, nth)
			}
		}(nth)
	}
}

// ask a task
func (c *Coordinator) AskTask(args *TaskArgs, reply *TaskReply) error {
	switch c.stage {
	case "map":
		c.mapReply(reply)
	case "reduce":
		c.reduceReply(reply)
	default:
		//do nothing
		reply.Task = "done"
	}
	return nil
}

// done a task
func (c *Coordinator) DoneMaptask(args *DoneArgs, reply *TaskReply) error {
	c.mtask.mu.Lock()
	c.mtask.isDoneMap[args.Nth] = true
	c.mtask.doneMap++
	c.mtask.mu.Unlock()
	if c.mtask.doneMap == c.nMap {
		c.stage = "reduce"
		c.reduceReply(reply)
	} else {
		c.mapReply(reply)
	}
	return nil
}

func (c *Coordinator) DoneReducetask(args *DoneArgs, reply *TaskReply) error {
	c.rtask.mu.Lock()
	c.rtask.isDoneReduce[args.Nth] = true
	c.rtask.doneRed++
	c.rtask.mu.Unlock()
	if c.rtask.doneRed == c.nReduce {
		c.stage = "done"
		reply.Task = c.stage
		c.done = true
	} else {
		c.reduceReply(reply)
	}
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

	return c.done
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.nMap = len(files)
	c.nReduce = nReduce
	c.stage = "map"
	c.mtask.files = files
	for i := 0; i < c.nMap; i++ {
		c.mtask.idleMap = append(c.mtask.idleMap, i)
	}
	c.mtask.isDoneMap = make(map[int]bool)

	for i := 0; i < nReduce; i++ {
		c.rtask.idleReduce = append(c.rtask.idleReduce, i)
	}
	c.rtask.isDoneReduce = make(map[int]bool)

	// Your code here.

	c.server()
	return &c
}
