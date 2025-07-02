package mr

import (
	"log"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const SUCCESS = math.MaxInt32

type TaskStatus struct {
	WorkerId int
	Status   string
}
type WorkerStatus struct {
	Status    string
	StartedAt int64
}
type Coordinator struct {
	mapQueue     chan WorkerUnit
	reduceQueue  chan WorkerUnit
	reportChan   chan ReportArgs
	retryChan    chan WorkerUnit
	doneChan     chan WorkerUnit
	wg           sync.WaitGroup
	taskStatus   map[int]int
	mu           sync.Mutex
	attempts     []int
	nReduce      int
	nMap         int
	mapPhaseDone bool
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) runCoordinator() {
	for {
		select {
		case worker := <-c.retryChan:
			c.mu.Lock()
			if !c.mapPhaseDone {
				c.mapQueue <- worker
			} else {
				c.reduceQueue <- worker
			}
			c.mu.Unlock()
		case report := <-c.reportChan:
			c.mu.Lock()
			idx := report.Worker.IntermediateFilesIndex
			current_attempt := c.taskStatus[idx]
			if current_attempt == report.Attempt {
				c.taskStatus[idx] = SUCCESS
				c.wg.Done()
			}
			c.mu.Unlock()
		case <-c.doneChan:
			return
		}
	}
}

func (c *Coordinator) CallGetWorker(args *WorkerArgs, reply *WorkerReply) error {
	c.mu.Lock()
	mapDone := c.mapPhaseDone
	c.mu.Unlock()

	select {
	case task := <-c.mapQueue:
		c.mu.Lock()
		attempt := c.taskStatus[task.IntermediateFilesIndex]
		c.mu.Unlock()

		reply.TaskAssigned = true
		reply.Worker = task
		reply.Attempt = attempt

		go c.retryAfterTimeout(task, false)
	case task := <-c.reduceQueue:
		if !mapDone {
			reply.TaskAssigned = false
			return nil
		}
		c.mu.Lock()
		attempt := c.taskStatus[task.IntermediateFilesIndex]
		c.mu.Unlock()

		reply.TaskAssigned = true
		reply.Worker = task
		reply.Attempt = attempt

		go c.retryAfterTimeout(task, false)
	default:
		reply.TaskAssigned = false
	}
	return nil
}

func (c *Coordinator) retryAfterTimeout(task WorkerUnit, isReduce bool) {
	time.Sleep(10 * time.Second)

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.taskStatus[task.IntermediateFilesIndex] != SUCCESS {
		c.taskStatus[task.IntermediateFilesIndex]++
		if isReduce {
			c.reduceQueue <- task
		} else {
			c.mapQueue <- task
		}
	}
}

func (c *Coordinator) CallReport(args *ReportArgs, reply *ReportReply) error {
	c.reportChan <- *args
	reply.Success = true
	return nil
}

func (c *Coordinator) startReducePhase() {
	c.mu.Lock()
	c.mapPhaseDone = true
	c.taskStatus = make(map[int]int)
	c.attempts = make([]int, c.nReduce)
	for i := range c.nReduce {
		c.taskStatus[i] = 0
		c.attempts[i] = 0
		c.wg.Add(1)
		c.reduceQueue <- WorkerUnit{
			WorkerJob:              REDUCE,
			IntermediateFilesIndex: i,
			MapFilesCount:          c.nMap,
			ReduceFilesCount:       c.nReduce,
		}
	}
	c.mu.Unlock()
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
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.mapPhaseDone {
		for _, status := range c.taskStatus {
			if status != SUCCESS {
				return false
			}
		}
		return true
	}
	return false
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapQueue:     make(chan WorkerUnit, len(files)),
		reduceQueue:  make(chan WorkerUnit, nReduce),
		reportChan:   make(chan ReportArgs, len(files)+nReduce),
		retryChan:    make(chan WorkerUnit, len(files)+nReduce),
		doneChan:     make(chan WorkerUnit, 1),
		wg:           sync.WaitGroup{},
		taskStatus:   make(map[int]int),
		mu:           sync.Mutex{},
		attempts:     make([]int, nReduce),
		nReduce:      nReduce,
		nMap:         len(files),
		mapPhaseDone: false,
	}

	for i, file := range files {
		task := WorkerUnit{
			WorkerJob:              MAP,
			Filename:               file,
			IntermediateFilesIndex: i,
			MapFilesCount:          len(files),
			ReduceFilesCount:       nReduce,
		}
		c.taskStatus[i] = 0
		c.attempts[i] = 0

		c.mapQueue <- task
		c.wg.Add(1)
	}

	go func() {
		c.wg.Wait()
		c.startReducePhase()
	}()

	go c.runCoordinator()
	c.server()
	return &c
}
