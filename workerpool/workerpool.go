package workerpool

import (
	"fmt"
	"sync"
)
type TaskElem interface {}
type Task interface {
	Run(element interface{}) (interface{}, error)
}

type TaskFunc func(interface{}) (interface{}, error)

func (f TaskFunc) Run(x interface{}) (interface{}, error) {
	return f(x)
}

type WorkerPool struct{
	task 		 Task
	jobs_chan    chan TaskElem
	results_chan chan TaskElem
	num_workers  int
	num_retry    int
	wg           sync.WaitGroup
}

func MakeWorkerPool(t Task,num_workers,num_retry,chan_len int) (*WorkerPool) {
	return &WorkerPool{
		task: t,
		jobs_chan:    make(chan TaskElem, chan_len),
		results_chan: make(chan TaskElem, chan_len),
		num_workers:  num_workers,
		num_retry:    num_retry,
		wg:           sync.WaitGroup{},
	}
}

func (wp *WorkerPool)Get_Jobs_Chan() (chan<- TaskElem) {
	return wp.jobs_chan
}
func (wp *WorkerPool)Get_Results_Chan() (<-chan TaskElem) {
	return wp.results_chan
}

func (wp *WorkerPool) workerFunc (id int) {
	defer wp.wg.Done()
	for job := range wp.jobs_chan {
		job,err:=wp.task.Run(job)
		if err != nil {
			fmt.Print("Job is fail")
		}
		wp.results_chan <- job
	}
}

func (wp *WorkerPool) waitFunc()  {
	wp.wg.Wait()
	close(wp.results_chan)
}

func (wp *WorkerPool) Start () {
	for w := 1; w <= wp.num_workers; w++ {
		wp.wg.Add(1)
		go wp.workerFunc(w)
	}
	go wp.waitFunc()
}


//func RunJobs(files []CliFile, jobs_chan chan<- CliFile){
//	for _,file := range (files){
//		jobs_chan <- file
//	}
//	close(jobs_chan)
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
