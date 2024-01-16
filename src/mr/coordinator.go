package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	nReduce        int
	nMap           int
	files          []string
	mapfinished    int
	maptasklog     []int // 0 未分配；1 已分配；2 已完成
	reducefinished int
	reducetasklog  []int
	mu             sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (m *Coordinator) ReceiveFinishedMap(arg *WorkerArgs, reply *WorkerReply) error {
	m.mu.Lock()
	m.mapfinished++
	m.maptasklog[arg.MapTaskNumber] = 2
	m.mu.Unlock()
	return nil
}

func (m *Coordinator) ReceiveFinishedReduce(arg *WorkerArgs, reply *WorkerReply) error {
	m.mu.Lock()
	m.reducefinished++
	m.reducetasklog[arg.ReduceTaskNumber] = 2
	m.mu.Unlock()
	return nil
}

func (m *Coordinator) AllocateTask(args *WorkerArgs, reply *WorkerReply) error {
	m.mu.Lock()
	if m.mapfinished < m.nMap {
		allocate := -1
		for i := 0; i < m.nMap; i++ {
			if m.maptasklog[i] == 0 {
				allocate = i
				break
			}
		}
		if allocate == -1 {
			reply.Tasktype = 2
			m.mu.Unlock()
		} else {
			reply.NReduce = m.nReduce
			reply.Tasktype = 0
			reply.MapTaskNumber = allocate
			reply.Filename = m.files[allocate]
			m.maptasklog[allocate] = 1
			m.mu.Unlock()
			go func() {
				time.Sleep(time.Duration(10) * time.Second)
				m.mu.Lock()
				if m.maptasklog[allocate] == 1 {
					m.maptasklog[allocate] = 0
				}
				m.mu.Unlock()
			}()
		}
	} else if m.mapfinished == m.nMap && m.reducefinished < m.nReduce {
		allocate := -1
		for i := 0; i < m.nReduce; i++ {
			if m.reducetasklog[i] == 0 {
				allocate = i
				break
			}
		}
		if allocate == -1 {
			reply.Tasktype = 2
			m.mu.Unlock()
		} else {
			reply.NMap = m.nMap
			reply.Tasktype = 1
			reply.ReduceTaskNumber = allocate
			m.reducetasklog[allocate] = 1
			m.mu.Unlock()
			go func() {
				time.Sleep(time.Duration(10) * time.Second)
				m.mu.Lock()
				if m.reducetasklog[allocate] == 1 {
					m.reducetasklog[allocate] = 0
				}
				m.mu.Unlock()
			}()
		}
	} else {
		reply.Tasktype = 3
		m.mu.Unlock()
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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
	// ret := false
	ret := c.reducefinished == c.nReduce

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.files = files
	c.nMap = len(files)
	c.nReduce = nReduce
	c.maptasklog = make([]int, c.nMap)
	c.reducetasklog = make([]int, c.nReduce)

	c.server()
	return &c
}
