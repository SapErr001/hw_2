package main

import (
	"sort"
	"strconv"
	"strings"
	"sync"
)

var globIn, globOut chan interface{}
var muMd5 sync.Mutex

func newJob(job job, in chan interface{}, out chan interface{}, wg *sync.WaitGroup) {
	defer wg.Done()
	defer close(out)
	job(in , out)
}

func ExecutePipeline (freeFlowJobs... job) {
	globIn = make(chan interface{})
	wg := &sync.WaitGroup{}
	for _, curJob := range freeFlowJobs {
		wg.Add(1)
		globOut = make(chan interface{})
		go newJob(curJob, globIn, globOut, wg)
		globIn = globOut
	}

	wg.Wait()
}

func wrapperCrc32(data string,	 res * string, wg * sync.WaitGroup)  {
	defer wg.Done()
	*res = DataSignerCrc32(data)
}

func wrapperMd5(finish chan interface{}, data string, res * string, wg * sync.WaitGroup)  {
	defer wg.Done()
	defer func() { finish <- 1 }()
	muMd5.Lock()
	*res = DataSignerMd5(data)
	muMd5.Unlock()
}

func singleHashOneThread(data string, out chan interface{}, wgoOuter * sync.WaitGroup)  {
	defer wgoOuter.Done()
	wg := &sync.WaitGroup{}
	wg.Add(3)
	end := make(chan interface{})

	var step0 string
	go wrapperMd5(end, data, &step0, wg)

	var step1 string
	go wrapperCrc32(data , &step1, wg)

	<-end
	var step3 string
	go wrapperCrc32(step0, &step3, wg)

	wg.Wait()
	res := step1 + "~" + step3
	out <- res
}

func SingleHash(in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	for data := range in {
		wg.Add(1)
		go singleHashOneThread(strconv.Itoa(data.(int)), out, wg)
	}
	wg.Wait()
}

func multiHashOneThread(data string, out chan interface{}, wgoOuter * sync.WaitGroup) {
	defer wgoOuter.Done()
	result := make([]string, 6)
	wg := &sync.WaitGroup{}
	wg.Add(6)
	for i := 0; i < 6; i++ {
		go wrapperCrc32(strconv.Itoa(i) + data, &result[i], wg)
	}
	wg.Wait()
	out <- strings.Join(result, "")
}

func MultiHash(in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	for data := range in {
		wg.Add(1)
		go multiHashOneThread(data.(string), out, wg)
	}
	wg.Wait()
}

func CombineResults(in, out chan interface{}) {
	var result []string
	for data := range in {
		result = append(result, data.(string))
	}
	sort.Strings(result)
	out <- strings.Join(result, "_")
}


