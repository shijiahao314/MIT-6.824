package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type RpcFinArgs struct {
	WorkerId int
	JobId    int
}

type RpcArgs struct {
	X int
}

type RpcReply struct {
	Y int
}

type RegReply struct {
	WorkerId int
}

// Add your RPC definitions here.
// Job.JobType
type JobType int

const (
	MapJob = iota
	ReduceJob
)

// Job.JobStatus
type JobStatus int

const (
	JobWaiting = iota
	JobWorking
	JobDone
)

// Coordinator.CoordinatorCondition
type Phase int

const (
	MapPhase = iota
	ReducePhase
	AllDone
)

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
