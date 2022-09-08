package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type SortedKey []KeyValue

// Len 重写len,swap,less才能排序
func (k SortedKey) Len() int           { return len(k) }
func (k SortedKey) Swap(i, j int)      { k[i], k[j] = k[j], k[i] }
func (k SortedKey) Less(i, j int) bool { return k[i].Key < k[j].Key }

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
// send an RPC request to the coordinator, wait for the response.
//  传一个空的PRC也就是一个空的Task给coordinator，让它定义Task内容并返回
// usually returns true.returns false if something goes wrong.
// 同时通过本身返回true和false来判断work和coordinator是否建立连接
// 没有连接说明要么连接出错，要么任务结束可以关掉work
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

// GetTask 获取任务信息（需要知道是Map任务，还是Reduce）
func GetTask() Task {

	//wMu.Lock()
	args := TaskArgs{}
	reply := Task{}
	// 用call调用coordinator中的分发任务并赋值给reply
	ok := call("Coordinator.PollTask", &args, &reply)
	//wMu.Unlock()
	if ok {
		//fmt.Println("worker get ", reply.TaskType, "task :Id[", reply.TaskId, "]")
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply
}

// 这两个函数作为参数传入，具体传入什么样的函数在../mrapps/wc.go里可以查看，传入的时候是编译好的wc.so文件
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	//CallExample()
	keepFlag := true
	for keepFlag {

		task := GetTask()
		switch task.TaskType {
		case MapTask:
			{
				DoMapTask(mapf, &task)
				//fmt.Print("*****************Map****************")
				callDone(&task)
			}

		case WaittingTask:
			{
				time.Sleep(time.Second * 5)
				//fmt.Println("All tasks are in progress, please wait...")
			}

		case ReduceTask:
			{
				DoReduceTask(reducef, &task)
				//fmt.Print("*****************Reduce*************")
				callDone(&task)
			}

		case ExitTask:
			{
				time.Sleep(time.Second)
				fmt.Println("All tasks are Done ,will be exiting...")
				keepFlag = false
			}

		}
	}

	time.Sleep(time.Second)
	// uncomment to send the Example RPC to the coordinator.

}

func DoMapTask(mapf func(string, string) []KeyValue, response *Task) {
	var intermediate []KeyValue
	filename := response.FileSlice[0]

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	// 通过io工具包获取conten,作为mapf的参数
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	// map返回一组KV结构体数组
	intermediate = mapf(filename, string(content))

	//initialize and loop over []KeyValue
	rn := response.ReducerNum
	// 创建一个长度为nReduce的二维切片
	HashedKV := make([][]KeyValue, rn)

	for _, kv := range intermediate {
		HashedKV[ihash(kv.Key)%rn] = append(HashedKV[ihash(kv.Key)%rn], kv)
	}
	for i := 0; i < rn; i++ {
		oname := "mr-tmp-" + strconv.Itoa(response.TaskId) + "-" + strconv.Itoa(i)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range HashedKV[i] {
			err := enc.Encode(kv)
			if err != nil {
				return
			}
		}
		ofile.Close()
	}

}

func DoReduceTask(reducef func(string, []string) string, response *Task) {
	reduceFileNum := response.TaskId
	intermediate := shuffle(response.FileSlice)
	dir, _ := os.Getwd()
	// 创建临时文件，这么做是为了确保在出现崩溃时没有人观察到部分写入的文件
	//tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	tempFile, err := os.CreateTemp(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
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
	tempFile.Close()

	// 在完全写入后进行重命名
	fn := fmt.Sprintf("mr-out-%d", reduceFileNum)
	os.Rename(tempFile.Name(), fn)
}

// 排序并得到一组排序好的kv数组
func shuffle(files []string) []KeyValue {
	var kva []KeyValue
	for _, filepath := range files {
		file, _ := os.Open(filepath)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	sort.Sort(SortedKey(kva))
	return kva
}

// callDone Call RPC to mark the task as completed
// 类似下面callexample的作用，用来给赋值并接收错误
//
func callDone(f *Task) {

	args := f
	reply := Task{}
	// 这个string说明的函数会被调用来判断是否结束
	// 这里用到的其实是args，这个reply没什么用，只是用来适应call的格式
	ok := call("Coordinator.MarkFinished", &args, &reply)

	if ok {
		//fmt.Println("worker finish :taskId[", args.TaskId, "]")
	} else {
		fmt.Printf("call failed!\n")
	}
}

// ------------------------------------------------------------------------------------
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
