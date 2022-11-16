package core

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"time"
)

type Worker struct {
	MapF          func(string, string) []KeyValue
	ReduceF       func(string, []string) string
	MasterAddress string
}

func MakeWorker(mapf func(string, string) []KeyValue, reducef func(string, []string) string, masterAddress string) *Worker {
	return &Worker{
		MapF:          mapf,
		ReduceF:       reducef,
		MasterAddress: masterAddress,
	}
}

func (w *Worker) Run() {
	for {
		reply := Reply{}
		call(w.MasterAddress, "Master.AssignTask", &struct{}{}, &reply)

		switch reply.TaskType {
		case Map:
			doMap(w.MasterAddress, w.MapF, &reply)
		case Reduce:
			doReduce(w.MasterAddress, w.ReduceF, &reply)
		case Wait:
			time.Sleep(500 * time.Millisecond)
		case Exit:
			return
		}
	}
}

func doMap(masterAddress string, mapf func(string, string) []KeyValue, reply *Reply) {
	file, err := os.Open(reply.Filename)
	defer file.Close()
	if err != nil {
		log.Fatal(err)
	}

	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatal(err)
	}

	intermediate := mapf(reply.Filename, string(content))

	buffer := make(map[int][]KeyValue)

	for _, kv := range intermediate {
		idx := ihash(kv.Key) % reply.NReduce
		buffer[idx] = append(buffer[idx], kv)
	}

	for id, kvs := range buffer {
		oname := fmt.Sprintf("mr-%d-%d", reply.Id, id)
		tempFile, err := os.CreateTemp(".", oname+"-tmp")
		if err != nil {
			log.Fatal(err)
		}

		encoder := json.NewEncoder(tempFile)
		for _, kv := range kvs {
			err := encoder.Encode(&kv)
			if err != nil {
				log.Fatal(err)
			}
		}
		os.Rename(tempFile.Name(), oname)
		tempFile.Close()
	}

	arg := Argument{Id: reply.Id, TaskType: Map}
	call(masterAddress, "Master.SubmitTask", &arg, &struct{}{})
}

func doReduce(masterAddress string, reducef func(string, []string) string, reply *Reply) {
	var intermediate []KeyValue
	for i := 0; i < reply.NMap; i++ {
		file, err := os.Open(fmt.Sprintf("mr-%d-%d", i, reply.Id))
		if err != nil {
			log.Fatal(err)
		}
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d", reply.Id)
	tempFile, err := os.CreateTemp(".", oname+"-tmp")
	if err != nil {
		log.Fatal(err)
	}

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	os.Rename(tempFile.Name(), oname)
	tempFile.Close()

	arg := Argument{Id: reply.Id, TaskType: Reduce}
	call(masterAddress, "Master.SubmitTask", &arg, &struct{}{})
}
