package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type WorkerRunner struct {
	ID         int
	mapFunc    func(string, string) []KeyValue
	reduceFunc func(string, []string) string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func (wr *WorkerRunner) ReportCompletion(worker WorkerUnit, attempt int) {
	args := ReportArgs{Worker: worker, Attempt: attempt}
	reply := ReportReply{}
	ok := call("Coordinator.CallReport", &args, &reply)
	if !ok {
		fmt.Printf("CallReport failed!\n")
	}
}

func (wr *WorkerRunner) handleMap(worker WorkerUnit, attempt int) {
	filename := worker.Filename
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v: %v", filename, err)
	}

	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v: %v", filename, err)
	}

	file.Close()

	kva := wr.mapFunc(filename, string(content))

	// This will create the number of intermediate files = nReduce
	for i := 0; i < worker.ReduceFilesCount; i++ {
		interFilename := fmt.Sprintf("mr-%d-%d", worker.IntermediateFilesIndex, i)
		tmpFile, err := os.CreateTemp(".", interFilename)
		if err != nil {
			log.Fatalf("Cannot create temp file: %v", tmpFile)
		}
		enc := json.NewEncoder(tmpFile)
		for _, kv := range kva {
			if ihash(kv.Key)%worker.ReduceFilesCount == i {
				_ = enc.Encode(&kv)
			}
		}
		tmpFile.Close()
		os.Rename(tmpFile.Name(), interFilename)
	}
	wr.ReportCompletion(worker, attempt)
}

func (wr *WorkerRunner) handleReduce(worker WorkerUnit, attempt int) {
	var intermediate []KeyValue
	for i := 0; i < worker.MapFilesCount; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, worker.IntermediateFilesIndex)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("Cannot open: %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(intermediate))

	oName := fmt.Sprintf("mr-out-%d", worker.IntermediateFilesIndex)
	oFile, _ := os.CreateTemp(".", oName)
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := wr.reduceFunc(intermediate[i].Key, values)
		fmt.Fprintf(oFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	oFile.Close()
	os.Rename(oFile.Name(), oName)
	wr.ReportCompletion(worker, attempt)
}

func (wr *WorkerRunner) requestTask() (WorkerReply, bool) {
	args := WorkerArgs{WorkerID: wr.ID}
	reply := WorkerReply{}
	ok := call("Coordinator.CallGetWorker", &args, &reply)

	if !ok {
		fmt.Printf("CallGetWorker failed!\n")
	}
	return reply, ok
}

func (wr *WorkerRunner) Run() {
	for {
		reply, ok := wr.requestTask()
		if !ok {
			return
		}
		if !reply.TaskAssigned {
			time.Sleep(3 * time.Second)
			continue
		}
		switch reply.Worker.WorkerJob {
		case MAP:
			wr.handleMap(reply.Worker, reply.Attempt)
		case REDUCE:
			wr.handleReduce(reply.Worker, reply.Attempt)
		}
	}
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Worker crashed with:", r)
		}
	}()
	runner := &WorkerRunner{
		ID:         os.Getpid(),
		mapFunc:    mapf,
		reduceFunc: reducef,
	}

	runner.Run()
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		fmt.Println("RPC Dial error:", err)
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err != nil {
		fmt.Println("RPC Call error:", err)
		return false
	}

	return true
}
