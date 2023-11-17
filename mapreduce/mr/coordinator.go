package mr

import (
	"errors"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Job
type Job struct {
	JobId        int
	JobType      JobType
	JobStatus    JobStatus
	WorkerStatus *WorkerStatus
	ReducerNum   int
	InputFile    []string
}

// 记录Worker状态
type WorkerStatus struct {
	WorkerId   int
	LastOnline time.Time
}

// ConcurrentMap
// 1.sync.RWMutex: mu.Lock()/Unlock(), mu.RLock()/RUnlock()
// 2.sync.map: 在读多写少的场景下效果好
// 考虑到项目复杂性，这里使用方案1

// 记录Job相关信息
type JobMetaInfo struct {
	// MetaMap map[int]*Job
	MetaMap RWMap[*Job]
	// Workers map[int]*WorkerStatus
	Workers RWMap[*WorkerStatus]
}

// 向MetaMap中放入Job
func (j *JobMetaInfo) putJob(job *Job) bool {
	jobId := job.JobId
	// meta := j.MetaMap[jobId]
	meta := j.MetaMap.Get(jobId)
	if meta != nil {
		log.Printf("MetaMap already contains job [%d]\n", jobId)
		return false
	} else {
		// j.MetaMap[jobId] = job
		j.MetaMap.Set(jobId, job)
	}
	return true
}

// 检查Job是否均已完成
func (j *JobMetaInfo) checkJobDone(phase Phase) bool {
	//
	mapDoneNum := 0
	mapUndoneNum := 0
	reduceDoneNum := 0
	reduceUndoneNum := 0
	for _, job := range j.MetaMap.MSet() {
		if job.JobType == MapJob {
			if job.JobStatus == JobDone {
				mapDoneNum++
			} else {
				mapUndoneNum++
			}
		} else {
			// ReduceJob
			if job.JobStatus == JobDone {
				reduceDoneNum++
			} else {
				reduceUndoneNum++
			}
		}
	}
	log.Printf("[%d / %d] map jobs are done, [%d / %d] reduce jobs are done\n",
		mapDoneNum, mapDoneNum+mapUndoneNum, reduceDoneNum, reduceDoneNum+reduceUndoneNum)
	switch phase {
	case MapPhase:
		return mapUndoneNum == 0
	case ReducePhase:
		return reduceUndoneNum == 0
	default:
		panic("Unexpetced phase")
	}
	return false
}

type Coordinator struct {
	// Your definitions here.
	MapJobChan    chan *Job
	ReduceJobChan chan *Job
	MapNum        int
	ReducerNum    int
	Phase         Phase
	jobMetaInfo   JobMetaInfo
	jobUID        int
	workerUID     int
}

var jobIdLock sync.Mutex

// 生成JobId
func (c *Coordinator) generateJobId() int {
	jobIdLock.Lock()
	defer jobIdLock.Unlock()
	id := c.jobUID
	c.jobUID++
	return id
}

var workerIdLock sync.Mutex

// 生成WorkerId
func (c *Coordinator) generateWorkerId() int {
	workerIdLock.Lock()
	defer workerIdLock.Unlock()
	id := c.workerUID
	c.workerUID++
	return id
}

// Coordinator进入下一个阶段
func (c *Coordinator) nextPhase() {
	if c.Phase == MapPhase {
		// 在Map阶段，进入Reduce阶段
		c.Phase = ReducePhase
		c.makeReduceJobs()
	} else if c.Phase == ReducePhase {
		// 在Reduce阶段，进入结束阶段
		c.Phase = AllDone
	} else {
		panic("In [AllDone] phase, but nextPhase")
	}
}

// 根据files生成MapJob
func (c *Coordinator) makeMapJobs(files []string) {
	for _, v := range files {
		id := c.generateJobId()
		job := Job{
			JobId:      id,
			JobType:    MapJob,
			JobStatus:  JobWaiting,
			ReducerNum: c.ReducerNum,
			InputFile:  []string{v},
		}
		c.jobMetaInfo.putJob(&job)
		// 输出job
		log.Printf("making map job [%v]\n", job)
		c.MapJobChan <- &job
	}
	log.Println("done making map jobs")
}

// 文件管理器
func FileAssigneHelper(reduceIndex int, directoryName string) []string {
	var res []string
	path, _ := os.Getwd()
	rd, _ := ioutil.ReadDir(path)
	for _, file := range rd {
		if strings.HasPrefix(file.Name(), "mr-tmp") &&
			strings.HasSuffix(file.Name(), strconv.Itoa(reduceIndex)) {
			res = append(res, file.Name())
		}
	}
	return res
}

// 生成ReduceJob
func (c *Coordinator) makeReduceJobs() {
	for i := 0; i < c.ReducerNum; i++ {
		id := c.generateJobId()
		job := Job{
			JobId:      id,
			JobType:    ReduceJob,
			JobStatus:  JobWaiting,
			ReducerNum: c.ReducerNum,
			InputFile:  FileAssigneHelper(i, "main/mr-tmp"),
		}
		c.jobMetaInfo.putJob(&job)
		// 输出job
		log.Println("making reduce job =", job)
		c.ReduceJobChan <- &job
	}
	log.Println("done making reduce jobs")
}

// Worker注册
func (c *Coordinator) RegisterWorker(args *RpcArgs, reply *RegReply) error {
	// 生成ID
	id := c.generateWorkerId()
	// meta := c.jobMetaInfo.Workers[id]
	meta := c.jobMetaInfo.Workers.Get(id)
	if meta != nil {
		log.Fatalf("worker [%d] already registered!", id)
		return errors.New("Failed")
	}
	// 注册Worker
	workerStatus := WorkerStatus{
		WorkerId:   id,
		LastOnline: time.Now(),
	}
	// c.jobMetaInfo.Workers[id] = &workerStatus
	c.jobMetaInfo.Workers.Set(id, &workerStatus)
	reply.WorkerId = id
	return nil
}

var mu sync.Mutex

// 分发任务
func (c *Coordinator) DistributeJob(workerId int, reply *Job) error {
	mu.Lock()
	defer mu.Unlock()
	log.Printf("[Coordinator] get a request from worker [%d]\n", workerId)
	switch c.Phase {
	case MapPhase:
		// Map阶段
		if len(c.MapJobChan) > 0 {
			// 还有空闲的Map任务
			job := <-c.MapJobChan
			job.JobStatus = JobWorking
			// job.WorkerStatus = c.jobMetaInfo.Workers[workerId]
			job.WorkerStatus = c.jobMetaInfo.Workers.Get(workerId)
			*reply = *job
		} else {
			// 没有空闲的Map任务
			reply.JobId = -1
			reply.JobStatus = JobWaiting
			if c.jobMetaInfo.checkJobDone(c.Phase) {
				c.nextPhase()
			}
		}
	case ReducePhase:
		// Reduce阶段
		if len(c.ReduceJobChan) > 0 {
			// 还有空闲的Reduce任务
			job := <-c.ReduceJobChan
			job.JobStatus = JobWorking
			// job.WorkerStatus = c.jobMetaInfo.Workers[workerId]
			job.WorkerStatus = c.jobMetaInfo.Workers.Get(workerId)
			*reply = *job
		} else {
			// 没有空闲的Reduce任务
			reply.JobId = -2
			reply.JobStatus = JobWaiting
			if c.jobMetaInfo.checkJobDone(c.Phase) {
				c.nextPhase()
			}
		}
	case AllDone:
		// 完成阶段
		reply.JobId = -3
		reply.JobStatus = JobDone
	default:
		panic("[ERROR] Unknown Phase")
	}
	return nil
}

// 完成Job
func (c *Coordinator) FinishJob(args RpcFinArgs, reply *RpcReply) error {
	mu.Lock()
	defer mu.Unlock()
	workerId := args.WorkerId
	jobId := args.JobId
	// meta := c.jobMetaInfo.MetaMap[jobId]
	meta := c.jobMetaInfo.MetaMap.Get(jobId)
	if meta == nil {
		log.Printf("MetaMap doesn't contains job [%d]\n", jobId)
		return errors.New("Failed")
	}
	if meta.JobStatus == JobWorking {
		log.Printf("[Coordinator] worker [%d] finish job [%d]\n", workerId, jobId)
		meta.JobStatus = JobDone
	} else {
		// 已经完成
		log.Printf("[duplicated] job [%d] already done\n", jobId)
	}
	return nil
}

// Worker更新自己的状态
func (c *Coordinator) UpdateWorker(workerId int, reply *RpcReply) error {
	// workerStatus := c.jobMetaInfo.Workers[workerId]
	workerStatus := c.jobMetaInfo.Workers.Get(workerId)
	if workerStatus == nil {
		// 未注册
		log.Fatalf("worker [%d] not registered", workerId)
		return errors.New("Failed")
	}
	// 更新
	workerStatus.LastOnline = time.Now()
	return nil
}

// 轮询查看Worker存活情况
func (c *Coordinator) WorkerMonitor() {
	// 轮询
	for c.Phase != AllDone {
		// for _, job := range c.jobMetaInfo.MetaMap {
		for _, job := range c.jobMetaInfo.MetaMap.MSet() {
			if job.JobStatus == JobWorking {
				// goroutine
				go func(job *Job) {
					if job.WorkerStatus != nil && time.Since(job.WorkerStatus.LastOnline) > 3*time.Second {
						// 已经大于3秒，认为Worker死亡，重新发布任务
						log.Printf("time %v\n", time.Since(job.WorkerStatus.LastOnline))
						log.Printf("job [%d] delete worker [%d]\n", job.JobId, job.WorkerStatus.WorkerId)
						mu.Lock()
						defer mu.Unlock()
						// 解除注册
						// delete(c.jobMetaInfo.Workers, job.WorkerStatus.WorkerId)
						c.jobMetaInfo.Workers.Delete(job.WorkerStatus.WorkerId)
						job.JobStatus = JobWaiting
						job.WorkerStatus = nil
						// 重新加入工作队列
						if job.JobType == MapJob {
							log.Printf("job [%d] enter MapJobChan\n", job.JobId)
							c.MapJobChan <- job
						} else {
							// ReduceJob
							log.Printf("job [%d] enter ReduceJobChan\n", job.JobId)
							c.ReduceJobChan <- job
						}
					}
				}(job)
			}
		}
		time.Sleep(time.Second)
	}
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
	mu.Lock()
	defer mu.Unlock()
	return c.Phase == AllDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		MapJobChan:    make(chan *Job, len(files)),
		ReduceJobChan: make(chan *Job, nReduce),
		jobMetaInfo: JobMetaInfo{
			// MetaMap: make(map[int]*Job, len(files)+nReduce),
			MetaMap: *NewRWMap[*Job](),
			Workers: *NewRWMap[*WorkerStatus](),
		},
		Phase:      MapPhase,
		MapNum:     len(files),
		ReducerNum: nReduce,
		jobUID:     0,
		workerUID:  0,
	}

	// Your code here.
	c.makeMapJobs(files)
	go c.WorkerMonitor()

	c.server()
	return &c
}
