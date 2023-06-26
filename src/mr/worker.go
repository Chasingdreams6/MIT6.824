package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

var Id int
var NReduce int

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	Register() // Get worker id and NReduce
	for {
		args := RequestArgs{}
		args.Id = Id
		reply := Job{}
		ok := call("Coordinator.RequestHandler", &args, &reply)
		if ok {
			switch reply.Kind {
			case NO_JOB:
				{
					fmt.Println(Id, " Get No job")
					break
				}
			case MAP_JOB:
				{
					doMap(&reply, mapf)
					break
				}
			case REDUCE_JOB:
				{
					doReduce(&reply, reducef)
					break
				}
			}
		} else {
			fmt.Println(Id, " Get Job Failed")
		}
		time.Sleep(1 * time.Second)
	}
}

func doMap(job *Job, mapf func(string, string) []KeyValue) {
	fmt.Println(strconv.Itoa(Id) + " Got map job " + job.FileName)
	filename := job.FileName
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	sort.Sort(ByKey(kva))
	//oName := "tmp-" + strconv.Itoa(job.Index)
	//ofile, _ := os.Create(oName)
	tmp := map[int][]KeyValue{}
	for _, v := range kva {
		hashedKey := ihash(v.Key) % NReduce
		tmp[hashedKey] = append(tmp[hashedKey], v)
	}
	for i := 0; i < NReduce; i++ {
		oName := "tmp-" + strconv.Itoa(Id) + "-" + strconv.Itoa(i)
		ofile, _ := os.OpenFile(oName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		enc := json.NewEncoder(ofile)
		for _, v := range tmp[i] {
			enc.Encode(v)
			//fmt.Fprintf(ofile, "%v %v\n", v.Key, v.Value)
		}
		ofile.Close()
	}
	// send finish
	_ = call("Coordinator.FinishJobHandler", job, &ExampleReply{})
}

func doReduce(job *Job, reducef func(string, []string) string) {
	fmt.Println(strconv.Itoa(Id) + " Got reduce job " + strconv.Itoa(job.Index))
	pwd, _ := os.Getwd()
	entries, _ := ioutil.ReadDir(pwd)
	var intermediate []KeyValue
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		} else {
			filename := entry.Name()
			if strings.HasPrefix(filename, "tmp") {
				strs := strings.Split(filename, "-")
				if strs[2] == strconv.Itoa(job.Index) {
					rfile, _ := os.Open(filename)
					dec := json.NewDecoder(rfile)
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							break
						}
						intermediate = append(intermediate, kv)
					}
					rfile.Close()
				}
			}
		}
	}

	sort.Sort(ByKey(intermediate))

	oname := "mr-out-" + strconv.Itoa(job.Index)
	ofile, _ := os.Create(oname)
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()

	// call back
	_ = call("Coordinator.FinishJobHandler", job, &ExampleReply{})
}

func Register() {
	reply := RegisterReply{}
	args := RequestArgs{}
	ok := call("Coordinator.RegisterHandler", &args, &reply)
	if ok {
		fmt.Printf("Id:%d nReduce:%d\n", reply.Id, reply.NReduce)
		Id = reply.Id
		NReduce = reply.NReduce
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
