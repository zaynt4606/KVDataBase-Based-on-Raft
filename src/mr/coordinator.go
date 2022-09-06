package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// 定义为全局，worker之间访问coordinator时加锁
var (
	mu sync.Mutex
)

type Coordinator struct {
	// Your definitions here.
	ReducerNum        int            // 传入的参数决定需要多少个reducer
	TaskId            int            // 用于生成task的特殊id
	DistPhase         Phase          // 目前整个框架应该处于什么任务阶段
	ReduceTaskChannel chan *Task     // 使用chan保证并发安全
	MapTaskChannel    chan *Task     // 使用chan保证并发安全
	taskMetaHolder    TaskMetaHolder // 存着task
	files             []string       // 传入的文件数组
}

// TaskMetaHolder 保存全部任务的元数据
type TaskMetaHolder struct {
	MetaMap map[int]*TaskMetaInfo // 通过下标hash快速定位
}

// TaskMetaInfo 保存任务的元数据
type TaskMetaInfo struct {
	state     State     // 任务的状态
	StartTime time.Time // 任务开始的时间
	TaskAdr   *Task     // 传入任务的指针,为的是这个任务从通道中取出来后，还能通过地址标记这个任务已经完成
}

// //
// // Your code here -- RPC handlers for the worker to call.
// // an example RPC handler.
// // the RPC argument and reply types are defined in rpc.go.
// //
// func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
// 	reply.Y = args.X + 1
// 	return nil
// }

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// Your code here.
	mu.Lock()
	defer mu.Unlock()
	if c.DistPhase == AllDone { // 已经全部完成
		fmt.Println("all task hava finished! the coordinator will exit!")
		return true
	}
	return false
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// 初始化一个Coordinator
	c := Coordinator{
		ReducerNum:        nReduce,
		files:             files,
		DistPhase:         MapPhase,
		ReduceTaskChannel: make(chan *Task, nReduce), // chan是关键字定义channel
		MapTaskChannel:    make(chan *Task, len(files)),
		taskMetaHolder: TaskMetaHolder{
			MetaMap: make(map[int]*TaskMetaInfo, len(files)+nReduce),
		},
	}
	c.server()
	return &c
}

// TaskMetaHolder的几个成员函数
// ---------------------------------------------------------------------------
// 补充几个TaskMetaHolder的成员函数
// 检查并接受一个TaskMetaInfo到TaskMetaHolder
func (t *TaskMetaHolder) acceptMeta(TaskInfo *TaskMetaInfo) bool {
	tid := TaskInfo.TaskAdr.TaskId
	_, err := t.MetaMap[tid]
	if !err {
		return false
	} else {
		t.MetaMap[tid] = TaskInfo
	}
	return true
}

// 统计多少任务完成，是否有还在工作的map或者reduce
func (t *TaskMetaHolder) checkTaskDone() bool {
	var (
		mapDoneNum      = 0
		mapUnDoneNum    = 0
		reduceDoneNum   = 0
		reduceUnDoneNum = 0
	)

	for _, v := range t.MetaMap {
		if v.TaskAdr.TaskType == MapTask {
			if v.state == Done {
				mapDoneNum++
			} else {
				mapUnDoneNum++
			}
		} else if v.TaskAdr.TaskType == ReduceTask {
			if v.state == Done {
				reduceDoneNum++
			} else {
				reduceUnDoneNum++
			}
		}
	}
	// map和reduce都完成了或者只有reduce完成了
	if (mapDoneNum > 0 && mapUnDoneNum == 0) && (reduceDoneNum > 0 && reduceUnDoneNum == 0) {
		return true
	} else if reduceDoneNum > 0 && reduceUnDoneNum == 0 {
		return true
	}
	return false
}

// 将在waiting状态的任务改成working，判断并修改成功返回true
func (t *TaskMetaHolder) judgeState(taskId int) bool {
	taskinfo, err := t.MetaMap[taskId]
	// 正在Working或者已经Done就直接返回false
	// err发生错误时为false
	if !err || taskinfo.state != Waiting {
		return false
	}
	taskinfo.state = Working
	taskinfo.StartTime = time.Now()
	return true
}

//
// 一些Coordinate的成员函数
// -----------------------------------------------------------------------
// worker崩溃检测
func (c *Coordinator) CrashDectector() {
	for {
		// 先暂停工作并上锁
		time.Sleep(2 * time.Second)
		mu.Lock()

		// 跳出循环条件，任务全部完成
		if c.DistPhase == AllDone {
			mu.Unlock()
			break
		}

		for _, v := range c.taskMetaHolder.MetaMap {
			// lab说明 10s 没有结束woker可以看作崩溃
			if v.state == Working && time.Now().Sub(v.StartTime) > 10*time.Second {
				// 打印一下崩溃信息
				fmt.Printf("the task[ %d ] is crash,take [%d] s\n", v.TaskAdr.TaskId, time.Since(v.StartTime))
				switch v.TaskAdr.TaskType {
				case MapTask:
					c.MapTaskChannel <- v.TaskAdr
					v.state = Waiting
				case ReduceTask:
					c.ReduceTaskChannel <- v.TaskAdr
					v.state = Waiting
				}
			}
		}
		mu.Unlock()
	}
}

//
// 分配一个map任务
//
func (c *Coordinator) makeMapTasks(files []string) {
	for _, v := range files {
		c.TaskId++
		id := c.TaskId
		task := Task{
			TaskType:   MapTask,
			TaskId:     id,
			ReducerNum: c.ReducerNum,
			FileSlice:  []string{v}, // 这里的FileSlice需要的是文件的切片
		}
		taskMetaInfo := TaskMetaInfo{
			// 这里的time不赋值，因为还没有开始working
			state:   Waiting,
			TaskAdr: &task, // 要引用，因为会对task进行修改，不能拷贝
		}

		c.taskMetaHolder.acceptMeta(&taskMetaInfo) // 同样要引用，会修改
		c.MapTaskChannel <- &task                  // Waitng状态的要加入channel
	}
}

// 找到同文件目录下的所有map临时文件以mr-tmp开头
// 返回文件名的[]string
// 分配reduce任务用来创建reduce要处理的task，makeReduceTask
func selectReduceName(reduceNum int) []string {
	path, _ := os.Getwd()
	files, _ := os.ReadDir(path)
	var s []string
	for _, file := range files {
		if strings.HasPrefix(file.Name(), "mr-tmp") &&
			strings.HasSuffix(file.Name(), strconv.Itoa(reduceNum)) {
			s = append(s, file.Name())
		}
	}
	return s
}

//
// 分配一个reduce任务
//
func (c *Coordinator) makeReduceTasks() {
	for i := 0; i < c.ReducerNum; i++ {
		c.TaskId++
		id := c.TaskId
		task := Task{
			TaskType:  ReduceTask,
			TaskId:    id,                  // 一样整一个新的
			FileSlice: selectReduceName(i), // 这里的FileSlice需要的是文件的切片
		}
		taskMetaInfo := TaskMetaInfo{
			// 这里的time不赋值，因为还没有开始working
			state:   Waiting,
			TaskAdr: &task, // 要引用，因为会对task进行修改，不能拷贝
		}

		c.taskMetaHolder.acceptMeta(&taskMetaInfo) // 同样要引用，会修改
		c.MapTaskChannel <- &task                  // Waitng状态的要加入channel
	}
}

//
// 在分发任务阶段使用，满足了checkTaskDone
// 就可以让Coodinator进入下一个阶段
//
func (c *Coordinator) toNextPhase() {
	if c.DistPhase == MapPhase {
		c.makeReduceTasks()
		c.DistPhase = ReducePhase
	} else if c.DistPhase == ReducePhase {
		c.DistPhase = AllDone
	}
}

//
// 分发任务
//
func (c *Coordinator) PollTask(args *TaskArgs, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()

	switch c.DistPhase {
	// map阶段的
	case MapPhase:
		{
			if len(c.MapTaskChannel) > 0 {
				*reply = *<-c.MapTaskChannel // chan是一个一个的传递的
				// 修改状态，waiting就跳过，working表示正在working，出现问题
				if !c.taskMetaHolder.judgeState(reply.TaskId) {
					fmt.Printf("Map-taskid[ %d ] is running\n", reply.TaskId)
				}

			} else { // map已经分发完了但是没有完成
				reply.TaskType = WaitingTask
				if c.taskMetaHolder.checkTaskDone() {
					c.toNextPhase()
				}
				return nil
			}
		}

	case ReducePhase:
		{
			if len(c.ReduceTaskChannel) > 0 {
				*reply = *<-c.ReduceTaskChannel // chan是一个一个的传递的
				// 修改状态，waiting就跳过，working表示正在working，出现问题
				if !c.taskMetaHolder.judgeState(reply.TaskId) {
					fmt.Printf("Reduce-taskid[ %d ] is running\n", reply.TaskId)
				}
			} else { // 分发完还没结束就改状态在waiting
				reply.TaskType = WaitingTask
				if c.taskMetaHolder.checkTaskDone() {
					c.toNextPhase()
				}
				return nil
			}

		}

	case AllDone:
		{
			reply.TaskType = ExitTask
		}

	default:
		fmt.Println("cannot find the phase!")
	}
	return nil
}

//
// 在worker中会作为call的参数函数被调用，检查有没有什么错误并返回
//
func (c *Coordinator) MarkFinished(args *Task, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	switch args.TaskType {
	case MapTask:
		meta, ok := c.taskMetaHolder.MetaMap[args.TaskId]

		//prevent a duplicated work which returned from another worker
		if ok && meta.state == Working {
			meta.state = Done
			//fmt.Printf("Map task Id[%d] is finished.\n", args.TaskId)
		} else {
			fmt.Printf("Map task Id[%d] is finished,already ! ! !\n", args.TaskId)
		}

	case ReduceTask:
		meta, ok := c.taskMetaHolder.MetaMap[args.TaskId]

		//prevent a duplicated work which returned from another worker
		if ok && meta.state == Working {
			meta.state = Done
			//fmt.Printf("Reduce task Id[%d] is finished.\n", args.TaskId)
		} else {
			fmt.Printf("Reduce task Id[%d] is finished,already ! ! !\n", args.TaskId)
		}

	default:
		panic("The task type undefined ! ! !")
	}
	return nil

}
