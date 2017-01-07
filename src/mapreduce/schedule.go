package mapreduce

import "fmt"

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	finish:=make(chan bool,ntasks)
	for i:=0;i<ntasks;i++ {
		index:=i
		res:=false
		go func() {
			for !res {
				worker:=<-mr.registerChannel
				arg:=DoTaskArgs{mr.jobName,mr.files[index],phase,index,nios}
				res=call(worker,"Worker.DoTask",&arg,new(struct{}))
				if res {
					//必须先把true添加到finish这个channel中，现在下面程序会卡在下一句。
					//因为registerChannel是一个只有一个空间的channel，相当于一个锁，最后一个worker执行完最后一个task会卡在这里。
					//我在想这是不是这个mapreduce框架的一个bug
					finish<- true
					mr.registerChannel<-worker
				} else {
					fmt.Printf("phase %s,task %d failed",phase,index)
				}
			}
		}()
	}

	for count:=0;count<ntasks;count++ {
		<-finish
		fmt.Printf("%d task finished",count+1)
	}



	fmt.Printf("Schedule: %v phase done\n", phase)
}
