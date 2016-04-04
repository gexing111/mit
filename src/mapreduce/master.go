package mapreduce

import "container/list"
import "fmt"
import "log"


type WorkerInfo struct {
	address string
	// You can add definitions here.
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func input_chan(info string, c chan string) {
	c <- info
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here

	log.Print("gexing, run master,begin for map")
	for i := 0; i < mr.nMap; i++ {
		idle_worker := <- mr.registerChannel
		log.Print("gexing dec mr.register chan from idle_work,", idle_worker)

		var args DoJobArgs
		args.File = mr.file
		args.JobNumber = i
		args.NumOtherPhase = mr.nReduce
		args.Operation = Map

		var reply DoJobReply

		ok := call(idle_worker, "Worker.DoJob", &args, &reply)
		if ok == false {
			fmt.Printf("call: RPC %s idle_worker map error\n", idle_worker)
		}

		log.Print("gexing add mr.register chan from idle_work,", idle_worker)
		go input_chan(idle_worker, mr.registerChannel)

	}

	log.Print("gexing, run master,begin for reduce")
	for i := 0; i < mr.nReduce; i++ {
		idle_worker := <- mr.registerChannel

		var args DoJobArgs
		args.File = mr.file
		args.JobNumber = i
		args.NumOtherPhase = mr.nMap
		args.Operation = Reduce

		var reply DoJobReply

		ok := call(idle_worker, "Worker.DoJob", &args, &reply)
		if ok == false {
			fmt.Printf("call: RPC %s idle_worker reduce error\n", idle_worker)
		}

		go input_chan(idle_worker, mr.registerChannel)
	}

	return mr.KillWorkers()
}
