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
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
// 方法名大写可以被外包引用
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
	// func DialHTTP(network, address string) (*Client, error)
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	// 如果发生错误，打印错误
	fmt.Println(err)
	return false
}

// callDone Call RPC to mark the task as completed
func callDone(f *Task) Task {

	args := f
	reply := Task{}
	// 这里有个Coordinator.MarkFinished
	ok := call("Coordinator.MarkFinished", &args, &reply)

	if ok {
		//fmt.Println("worker finish :taskId[", args.TaskId, "]")
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply
}

// -----------------------------------------------------------------------------------------------
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	// Your worker implementation here.
	keepFlag := true
	for keepFlag {
		task := GetTask()
		switch task.TaskType {
		case MapTask:
			{
				DoMapTask(mapf, &task)
				callDone(&task) // map完成
			}
		case WaitingTask:
			{
				fmt.Println("All tasks are in progress, please wait...")
				time.Sleep(time.Second)
			}
		case ExitTask:
			{
				fmt.Println("Task about :[", task.TaskId, "] is terminated...")
				keepFlag = false
			}
		case ReduceTask:
			{
				DoReduceTask(reducef, &task)
				callDone(&task)
			}
		}
	}
}

// GetTask 获取任务（需要知道是Map任务，还是Reduce）
func GetTask() Task {

	args := TaskArgs{}
	reply := Task{}
	ok := call("Coordinator.PollTask", &args, &reply) // 获取Task并且传给reply

	if ok {
		fmt.Println(reply)
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply
}

// mapf传入的参数一个文件名，一个文件内容，返回一组kv结构体 []KeyValue, 也就是文件中所有单词作为key，1作为value
func DoMapTask(mapf func(string, string) []KeyValue, response *Task) {
	var intermediate []KeyValue
	filename := response.FileSlice[0]

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		// log.Fatalln("cannot open", filename)  // 等同于这一句
	}
	// 通过io工具包获取content,作为mapf的参数
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close() // 立即关掉不用defer
	// map返回一组KV结构体数组
	intermediate = mapf(filename, string(content))

	//initialize and loop over []KeyValue
	rn := response.ReducerNum
	// 创建一个长度为nReduce的二维切片，始化rn行二维切片，存储元素是KeyValue
	HashedKV := make([][]KeyValue, rn)

	for _, kv := range intermediate {
		HashedKV[ihash(kv.Key)%rn] = append(HashedKV[ihash(kv.Key)%rn], kv)
	}

	// 把HashedKV写成json文件
	for i := 0; i < rn; i++ {
		oname := "mr-tmp-" + strconv.Itoa(response.TaskId) + "-" + strconv.Itoa(i)
		ofile, _ := os.Create(oname)  // 忽略掉返回的erro
		enc := json.NewEncoder(ofile) // 创建json编码器
		for _, kv := range HashedKV[i] {
			err := enc.Encode(kv)
			if err != nil {
				return
			}
		}
		ofile.Close()
	}
}

// reducef 传入的两个参数key，key对应的value切片组
func DoReduceTask(reducef func(string, []string) string, response *Task) {
	reduceFileNum := response.TaskId
	intermediate := shuffle(response.FileSlice) // []KeyValue
	dir, _ := os.Getwd()                        // 得到相对路径位置
	//tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	// 从go1.16开始，ioutil包已被声明废弃，应换用io包以及os包中的函数代替。
	// tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	tempFile, err := os.CreateTemp(dir, "mr-tmp-*") // go 1.6之后用os，io替代ioutils包
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}
	i := 0
	for i < len(intermediate) {
		// values存储所有相同的intermediate[i]的key对应的value
		var values []string
		values = append(values, intermediate[i].Value)
		j := i + 1 // j的用处是用来帮i跳转到已经算过的key之后
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			values = append(values, intermediate[j].Value)
			j++
		}
		output := reducef(intermediate[i].Key, values)                // var reducef func(string, []string) string
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output) // 写到上面创建的文件中，这里的output已经合并过了
		i = j
	}
	tempFile.Close()

	// 在完全写入后进行重命名， Sprinf格式化字符串并赋给新值
	fn := fmt.Sprintf("mr-out-%d", reduceFileNum)
	os.Rename(tempFile.Name(), fn)
}

// 洗牌方法，得到一组排序好的kv数组
func shuffle(files []string) []KeyValue {
	// 将[]string 存储到 []KeyValue
	var kva []KeyValue
	for _, filepath := range files {
		file, _ := os.Open(filepath)
		dec := json.NewDecoder(file) // 解码器，读json
		for {
			var kv KeyValue
			err := dec.Decode(&kv) // 解码并读到kv
			if err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	sort.Sort(SortedKey(kva)) // 这里对[]KeyValue排序
	return kva
}
