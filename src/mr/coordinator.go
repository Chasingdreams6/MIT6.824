package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	nReduce                 int
	isDone                  bool
	workersNum              int // [0, ...workerNum)
	fileNames               []string
	mapAllocated            []bool
	mapSucceed              []bool
	reduceAllocated         []bool
	reduceSucceed           []bool
	lastMapAllocatedTime    map[int]time.Time
	lastReduceAllocatedTime map[int]time.Time
	bigLock                 sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// server got a request from worker
// may reply three jobs:
// NO_JOB
// MAP_JOB
// REDUCE_JOB
func (c *Coordinator) RequestHandler(args *RequestArgs, reply *Job) error {
	c.bigLock.Lock()
	canReduce := true
	for k, fileName := range c.fileNames {
		if c.mapAllocated[k] == true {
			continue
		} else {
			reply.FileName = fileName
			reply.Index = k
			reply.Kind = MAP_JOB
			c.mapAllocated[k] = true
			c.lastMapAllocatedTime[k] = time.Now()
			goto release
		}
	}
	for k, _ := range c.fileNames {
		if c.mapSucceed[k] == true {
			continue
		}
		canReduce = false
		break
	}
	if canReduce == false {
		reply.Kind = NO_JOB
		goto release
	}
	for i := 0; i < c.nReduce; i++ {
		if c.reduceAllocated[i] == true {
			continue
		} else {
			reply.Kind = REDUCE_JOB
			reply.Index = i
			c.reduceAllocated[i] = true
			c.lastReduceAllocatedTime[i] = time.Now()
			goto release
		}
	}
	reply.Kind = NO_JOB
release:
	c.bigLock.Unlock()
	return nil
}

func (c *Coordinator) FinishJobHandler(args *Job, reply *ExampleReply) error {
	c.bigLock.Lock()
	if args.Kind == MAP_JOB {
		c.mapSucceed[args.Index] = true
	}
	if args.Kind == REDUCE_JOB {
		c.reduceSucceed[args.Index] = true
	}
	c.bigLock.Unlock()
	return nil
}

func (c *Coordinator) RegisterHandler(args *RequestArgs, reply *RegisterReply) error {
	c.bigLock.Lock()
	reply.Id = c.workersNum
	reply.NReduce = c.nReduce
	//fmt.Println(reply.nReduce)
	c.workersNum++
	c.bigLock.Unlock()
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
	return c.isDone
}

// started by a go routine
// check all jobs's state
func (c *Coordinator) Checker() {
	for {
		doneFlag := true
		c.bigLock.Lock()
		// checkmap
		for k, _ := range c.fileNames {
			if c.mapSucceed[k] {
				continue
			}
			doneFlag = false
			if c.mapAllocated[k] {
				dur := time.Now().Sub(c.lastMapAllocatedTime[k])
				if dur > 10*time.Second { // fail, re alloc
					c.mapAllocated[k] = false
				}
			}
		}
		// checkReduce
		for i := 0; i < c.nReduce; i++ {
			if c.reduceSucceed[i] {
				continue
			}
			doneFlag = false
			if c.reduceAllocated[i] {
				dur := time.Now().Sub(c.lastReduceAllocatedTime[i])
				if dur > 10*time.Second {
					c.reduceAllocated[i] = false
				}
			}
		}
		if doneFlag {
			c.isDone = true
		}
		c.bigLock.Unlock()
		time.Sleep(5 * time.Second)
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.isDone = false
	c.workersNum = 0
	c.nReduce = nReduce
	// Your code here.
	for _, filename := range files {
		c.fileNames = append(c.fileNames, filename)
		c.mapAllocated = append(c.mapAllocated, false)
		c.mapSucceed = append(c.mapSucceed, false)
	}
	for i := 0; i < nReduce; i++ {
		c.reduceAllocated = append(c.reduceAllocated, false)
		c.reduceSucceed = append(c.reduceSucceed, false)
	}
	c.lastReduceAllocatedTime = make(map[int]time.Time)
	c.lastMapAllocatedTime = make(map[int]time.Time)
	c.server()
	go c.Checker()
	return &c
}
