package core

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

type Task struct {
	Status    Status
	StartTime time.Time
}

type Master struct {
	sync.Mutex

	files            []string
	nReduce          int
	mapTaskRemain    int
	reduceTaskRemain int
	mapTasks         []Task
	reduceTasks      []Task
}

func (m *Master) serve() {
	rpc.Register(m)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (m *Master) SubmitTask(arg *Argument, _ *Reply) error {
	m.Lock()
	defer m.Unlock()

	if arg.TaskType == Map {
		fmt.Printf("map task %d fininshed\n", arg.Id)
		m.mapTasks[arg.Id].Status = Completed
		m.mapTaskRemain--
		fmt.Printf("map task remain: %d\n", m.mapTaskRemain)
		fmt.Printf("reduce task remain: %d\n", m.reduceTaskRemain)
	} else if arg.TaskType == Reduce {
		fmt.Printf("reduce task %d fininshed\n", arg.Id)
		m.reduceTasks[arg.Id].Status = Completed
		m.reduceTaskRemain--
		fmt.Printf("map task remain: %d\n", m.mapTaskRemain)
		fmt.Printf("reduce task remain: %d\n", m.reduceTaskRemain)
	}

	// ignore other cases

	return nil
}

func (m *Master) AssignTask(_ *Argument, reply *Reply) error {
	m.Lock()
	defer m.Unlock()

	// Todo
	// use function to remove redundant code
	if m.mapTaskRemain > 0 {
		// Map phase

		// assign map task
		taskId := -1
		for i := 0; i < len(m.mapTasks); i++ {
			if m.mapTasks[i].Status == Initial || (m.mapTasks[i].Status == InProgress && m.mapTasks[i].StartTime.Before(time.Now().Add(-10*time.Second))) {
				taskId = i
				break
			}
		}
		if taskId == -1 {
			reply.TaskType = Wait
		} else {
			reply.Id = taskId
			reply.TaskType = Map
			reply.NReduce = m.nReduce
			reply.Filename = m.files[taskId]
			m.mapTasks[taskId].Status = InProgress
			m.mapTasks[taskId].StartTime = time.Now()
		}

	} else if m.mapTaskRemain == 0 && m.reduceTaskRemain > 0 {
		// Reduce Phase

		// assign reduce task
		taskId := -1
		for i := 0; i < len(m.reduceTasks); i++ {
			if m.reduceTasks[i].Status == Initial {
				taskId = i
				break
			}
			if m.reduceTasks[i].Status == InProgress && m.reduceTasks[i].StartTime.Before(time.Now().Add(-10*time.Second)) {
				taskId = i
				break
			}
		}
		if taskId == -1 {
			reply.TaskType = Wait
		} else {
			reply.Id = taskId
			reply.TaskType = Reduce
			reply.NReduce = m.nReduce
			reply.NMap = len(m.mapTasks)
			m.reduceTasks[taskId].Status = InProgress
			m.reduceTasks[taskId].StartTime = time.Now()
		}
	} else {
		// Exit Phase
		reply.TaskType = Exit
	}
	return nil
}

func (m *Master) Done() bool {
	// shall we use lock here?
	m.Lock()
	defer m.Unlock()
	return m.mapTaskRemain == 0 && m.reduceTaskRemain == 0
}

func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	m.files = files
	m.mapTaskRemain = len(files)
	m.nReduce = nReduce
	m.reduceTaskRemain = nReduce
	m.mapTasks = make([]Task, len(files))
	m.reduceTasks = make([]Task, nReduce)

	for i, file := range files {
		m.files[i] = file
	}

	for i := 0; i < len(files); i++ {
		m.mapTasks[i].Status = Initial
	}
	for i := 0; i < nReduce; i++ {
		m.reduceTasks[i].Status = Initial
	}

	m.serve()
	return &m
}
