package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type RequestArgs struct {
	Id int // request worker's id
}

type RegisterReply struct {
	NReduce int
	Id      int
}

type Job struct {
	Kind     string
	FileName string
	Index    int
}

const NO_JOB string = "NO_JOB"
const MAP_JOB string = "MAP_JOB"
const REDUCE_JOB string = "REDUCE_JOB"

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
