package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
)

var globIn, globOut chan interface{}

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

func SingleHash(in, out chan interface{}) {
	for data := range in {
		//step1 :=  DataSignerCrc32(strconv.Itoa(data.(int)))
		res := DataSignerCrc32(strconv.Itoa(data.(int))) + "~" +
			DataSignerCrc32(DataSignerMd5(strconv.Itoa(data.(int))))
		fmt.Println("SingleHash result ", res)
		out <- res
	}
}

func MultiHash(in, out chan interface{}) {
	for data := range in {
		var result string
		for i := 0; i < 6; i++ {
			result += DataSignerCrc32(strconv.Itoa(i) + data.(string))
		}
		fmt.Println("MultiHash", result)
		out <- result
	}
}

func CombineResults(in, out chan interface{}) {
	var result []string
	for data := range in {
		result = append(result, data.(string))
	}
	sort.Strings(result)
	out <- strings.Join(result, "_")
}


