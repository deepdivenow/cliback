package workerpool

import (
	"log"
	"sync"
	"time"
)

type TaskElem interface{}
type Task interface {
	Run(element interface{}) (interface{}, error)
}

type TaskFunc func(interface{}) (interface{}, error)

func (f TaskFunc) Run(x interface{}) (interface{}, error) {
	return f(x)
}

type WorkerPool struct {
	task        Task
	jobsChan    chan TaskElem
	resultsChan chan TaskElem
	numWorkers  int
	numRetry    int
	needRetry   bool
	wg          sync.WaitGroup
}

func MakeWorkerPool(t Task, numWorkers, numRetry, chanLen int) *WorkerPool {
	if numWorkers < 1 {
		numWorkers = 8
	}
	if numRetry < 1 {
		numRetry = 3
	}
	if chanLen < numWorkers {
		chanLen = numWorkers * 2
	}
	return &WorkerPool{
		task:        t,
		jobsChan:    make(chan TaskElem, chanLen),
		resultsChan: make(chan TaskElem, chanLen),
		numWorkers:  numWorkers,
		numRetry:    numRetry,
		wg:          sync.WaitGroup{},
	}
}

func (wp *WorkerPool) GetJobsChan() chan<- TaskElem {
	return wp.jobsChan
}
func (wp *WorkerPool) GetResultsChan() <-chan TaskElem {
	return wp.resultsChan
}

func (wp *WorkerPool) workerFunc(id int) {
	defer wp.wg.Done()
	for job := range wp.jobsChan {
		jobComplete := false
		var jobResult TaskElem
		var err error
		for !jobComplete {
			jobResult, err = wp.task.Run(job)
			if wp.needRetry && err != nil {
				log.Print("Job is fail", err)
				time.Sleep(2 * time.Second)
				continue
			}
			jobComplete = true
		}
		wp.resultsChan <- jobResult
	}
}

func (wp *WorkerPool) waitFunc() {
	wp.wg.Wait()
	close(wp.resultsChan)
}

func (wp *WorkerPool) Start() {
	for w := 1; w <= wp.numWorkers; w++ {
		wp.wg.Add(1)
		go wp.workerFunc(w)
	}
	go wp.waitFunc()
}

//func RunJobs(files []CliFile, jobsChan chan<- CliFile){
//	for _,file := range (files){
//		jobsChan <- file
//	}
//	close(jobsChan)
//}

//func main() {
//	const numJobs = 5
//	const numRetry = 3
//	jobs := make(chan CliFile, numJobs)
//	results := make(chan CliFile, numJobs)
//	wg := sync.WaitGroup{}
//	var wp = WorkerPool{jobs,results,numJobs,numRetry, wg}
//	var files []CliFile
//
//	for i := 0; i<1000; i++{
//		files=append(files, CliFile{name:"a",count:i,result:0})
//	}
//	wp.Start()
//	go RunJobs(files,jobs)
//
//	for job := range(results){
//		fmt.Println(job.worker_id,job.count,job.result)
//	}
//}
