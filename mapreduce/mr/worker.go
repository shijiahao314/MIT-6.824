package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

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

func RegisterWorker() int {
	args := &RpcArgs{}
	reply := &RegReply{}

	log.Printf("worker send register request\n")
	call("Coordinator.RegisterWorker", &args, &reply)
	log.Printf("worker [%d] success register\n", reply.WorkerId)
	return reply.WorkerId
}

func RequestJob(workerId int) *Job {
	reply := Job{}

	log.Printf("worker [%d] request job\n", workerId)
	call("Coordinator.DistributeJob", workerId, &reply)
	log.Printf("worker [%d] get job [%d]\n", workerId, reply.JobId)

	return &reply
}

// 发送完成Job
func FinishJob(workerId int, jobId int) {
	args := RpcFinArgs{
		WorkerId: workerId,
		JobId:    jobId,
	}
	reply := &RpcReply{}
	call("Coordinator.FinishJob", args, &reply)
}

// Worker更新状态
func UpdateWorker(workerId int) {
	reply := &RpcReply{}
	call("Coordinator.UpdateWorker", workerId, &reply)
}

// DoMap
func DoMap(mapf func(string, string) []KeyValue, mapJob *Job) {
	var intermediate []KeyValue
	filename := mapJob.InputFile[0]

	// 打开文件
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open file %v", filename)
	}
	// 读取文件内容
	con, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read file %v", filename)
	}
	//
	file.Close()
	// 使用mapf计算
	intermediate = mapf(filename, string(con))
	rn := mapJob.ReducerNum
	HashedKV := make([][]KeyValue, rn)

	for _, kv := range intermediate {
		HashedKV[ihash(kv.Key)%rn] = append(HashedKV[ihash(kv.Key)%rn], kv)
	}
	for i := 0; i < rn; i++ {
		oname := "mr-tmp-" + strconv.Itoa(mapJob.JobId) + "-" + strconv.Itoa(i)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range HashedKV[i] {
			enc.Encode(kv)
		}
		ofile.Close()
	}
}

// 读取文件
func readFromLocalFile(files []string) []KeyValue {
	var kva []KeyValue
	for _, filepath := range files {
		file, _ := os.Open(filepath)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	return kva
}

type ByKey []KeyValue

func (b ByKey) Len() int {
	return len(b)
}
func (b ByKey) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}
func (b ByKey) Less(i, j int) bool {
	return b[i].Key < b[j].Key
}

// DoReduce
func DoReduce(reducef func(string, []string) string, response *Job) {
	reduceFileNum := response.JobId
	intermediate := readFromLocalFile(response.InputFile)
	sort.Sort(ByKey(intermediate))
	dir, _ := os.Getwd()
	// 使用ioutil.TempFile(dir, "mr-tmp-*")文件已经存在时，会在文件名后随机添加几个字符确保不重
	//tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}
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
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	tempFile.Close()
	oname := fmt.Sprintf("mr-out-%d", reduceFileNum)
	os.Rename(tempFile.Name(), oname)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// set workerId: string
	workerId := RegisterWorker()
	alive := true
	go func() {
		for alive {
			// 定期发送心跳
			UpdateWorker(workerId)
			time.Sleep(time.Second)
		}
	}()

	for attempt := 0; alive; attempt++ {
		log.Printf("worker [%d] attempt times [%d]\n", workerId, attempt)
		job := RequestJob(workerId)
		switch job.JobStatus {
		case JobWorking:
			if job.JobType == MapJob {
				log.Printf("worker [%d] do map job [%d]\n", workerId, job.JobId)
				DoMap(mapf, job)
			} else {
				// ReduceJob
				log.Printf("worker [%d] do reduce job [%d]\n", workerId, job.JobId)
				DoReduce(reducef, job)
			}
			FinishJob(workerId, job.JobId)
		case JobWaiting:
			log.Printf("worker [%d] waiting\n", workerId)
			time.Sleep(time.Second)
		case JobDone:
			log.Printf("worker [%d] kill\n", workerId)
			alive = false
		default:
			panic("[ERROR] Unknown JobType")
		}
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

	log.Println(err)
	return false
}
