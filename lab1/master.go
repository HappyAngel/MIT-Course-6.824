package mapreduce

import "container/list"
import "fmt"


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

func (mr *MapReduce) RunMaster() *list.List {   
    // check registered workers
    go func(){
        for {
            registeredWorker := <-mr.registerChannel
            fmt.Printf("RunMaster: new registered work %s arrive\n", registeredWorker)
            
            workerInfo := WorkerInfo{
                address : registeredWorker,
            }
            
            mr.AvailableWorkersChannel <- workerInfo
        }
    }()
    
    fmt.Printf("RunMaster: start to run jobs for Map\n")
    // Map
    for i:=0; i < mr.nMap; i++{
       // copy in order to avoid race condition
       mapIndex := i   
       
       workerInfo := <-mr.AvailableWorkersChannel
       registeredWorker := workerInfo.address
       mr.Workers[registeredWorker] = &WorkerInfo{address:registeredWorker}    
       fmt.Printf("RunMaster: fetch Worker %s from Channel for Map job Num.%d\n", registeredWorker, mapIndex)
                    
       go func(){
           fmt.Printf("RunMaster: jobNum to pass %d\n", mapIndex)
           reply := mr.sendJobToWoker(registeredWorker, Map, mapIndex, mr.nReduce)
           mr.AvailableWorkersChannel <- WorkerInfo{address:registeredWorker,}
           fmt.Printf("RunMaster: return Worker %s to Channel from Map\n", registeredWorker)
                  
           if reply == nil{
            return
           }
       
           if reply.OK == false{
             fmt.Printf("RunMaster: Worker %s Job %n DoJob run failed for Map\n", registeredWorker, mapIndex)
          }
       }()
    }   
    fmt.Printf("RunMaster: Map job done...\n")
        
    fmt.Printf("RunMaster: start to run jobs for Reduce\n")
    // Reduce
    for i:=0; i < mr.nReduce; i++{
        // copy to avoid race condition
        reduceIndex := i

        workerInfo := <-mr.AvailableWorkersChannel
        registeredWorker := workerInfo.address
        mr.Workers[registeredWorker] = &WorkerInfo{address:registeredWorker} 
        fmt.Printf("RunMaster: fetch Worker %s from Channel for Reduce for job Num.%d\n", registeredWorker, reduceIndex)  
        
        go func(){
            reply := mr.sendJobToWoker(registeredWorker, Reduce, reduceIndex, mr.nMap)
            mr.AvailableWorkersChannel <- WorkerInfo{address:registeredWorker,}
            fmt.Printf("RunMaster: return Worker %s to Channel from Reduce\n", registeredWorker)
            
            if reply == nil{
              return
            }
        
            if reply.OK == false{
              fmt.Printf("RunMaster: Worker %s Job %n DoJob run failed for Reduce\n", registeredWorker, reduceIndex)
            }
         }()
    }
    
	return mr.KillWorkers()
}

func (mr *MapReduce) sendJobToWoker(registeredWorker string, op JobType, jobNum int, numOfPhrase int) *DoJobReply{        
       args := &DoJobArgs{
        File : mr.file,
	    Operation : op,
	    JobNumber : jobNum,
    	NumOtherPhase : numOfPhrase,
       }
                 
	   var reply DoJobReply
	   ok := call(registeredWorker, "Worker.DoJob", args, &reply)
	   
       if ok == false {
		  fmt.Printf("RunMaster: Worker %n RPC %s DoJob error\n", jobNum ,registeredWorker)
          return nil
	   }
       
       return &reply
}