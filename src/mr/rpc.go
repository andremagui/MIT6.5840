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

type WorkerState int

const (
	IDLE WorkerState = iota
	RUNNING
)

const (
	MAP = iota
	REDUCE
)

type WorkerUnit struct {
	WorkerJob              int    // type of job
	Filename               string // input file name
	IntermediateFilesIndex int    // index of generated intermediate file
	MapFilesCount          int    // Count of map files
	ReduceFilesCount       int    // Count of reduce files
}

// Arguments and reply needed when the Worker is communicating with the coordinator and
// asking for a task.

// The args each worker should exchange with the coordinator when requesting a Task. We
// send to the coordinator the WorkerID that is communicating.
type WorkerArgs struct {
	WorkerID int
}

// The reply each worker should exchange with the coordinator when requesting a Task. We
// send if there is a task assigned to that worker, the worker type and the attempt for
// this specific job executed by the Worker.
type WorkerReply struct {
	TaskAssigned bool
	Worker       WorkerUnit
	Attempt      int
}

// Arguments and reply needed when the Worker is communicating with the coordinator and
// reporting the result for a task.

// The args each worker should exchange with the coordinator when reporting a Task.
// We send if there is the worker type and the attempt for this specific job executed by
// the Worker.
type ReportArgs struct {
	Worker  WorkerUnit
	Attempt int
}

// The reply each worker should exchange with the coordinator when reporting a Task. We
// send if the task was successful.
type ReportReply struct {
	Success bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
