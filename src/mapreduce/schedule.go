package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	var waitGroup sync.WaitGroup
	var taskChan = make(chan int, ntasks)
	var closeChan = make(chan int)
	var resultChan = make(chan int, ntasks)

	go func (closeChan chan int) {
		LOOP:
		for {
			select {
			case addr := <-registerChan:
				waitGroup.Add(1)
				go func (addr string, phase jobPhase, taskChan chan int) {
					defer waitGroup.Done()
					var res = struct{}{}

					for task := range taskChan {
						var arg DoTaskArgs
						switch phase {
						case mapPhase:
							arg = DoTaskArgs{
								JobName: jobName,
								File: mapFiles[task],
								Phase: mapPhase,
								TaskNumber: task,
								NumOtherPhase: n_other,
							}
						case reducePhase:
							arg = DoTaskArgs{
								JobName: jobName,
								File: "",
								Phase: reducePhase,
								TaskNumber: task,
								NumOtherPhase: n_other,
							}
						}
						success := call(addr, "Worker.DoTask", arg, &res)
						if success {
							resultChan <- task
						} else {
							taskChan <- task
						}
					}
				}(addr, phase, taskChan)
			case <-closeChan:
				break LOOP
			}
		}
	}(closeChan)

	for i := 0; i < ntasks; i++ {
		taskChan <- i
	}

	var num int = 0;
	for _ = range resultChan {
		num++
		if num == ntasks {
			break
		}
	}
	close(taskChan)

	waitGroup.Wait()
	close(closeChan)

	fmt.Printf("Schedule: %v phase done\n", phase)
}
