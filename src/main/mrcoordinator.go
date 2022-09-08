package main

//
// 启动一个 coordinator/协调器 进程
// start the coordinator process, which is implemented
// in ../mr/coordinator.go
//
// go run mrcoordinator.go pg*.txt
//
// Please do not change this file.
//

import (
	"fmt"
	"os"
	"time"

	"6.824/mr"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		os.Exit(1)
	}
	// 第二个参数是nReduce
	// 第一个参数是file []string
	// 启动一个coordinator，传入要操作的文件和reduce的数量
	m := mr.MakeCoordinator(os.Args[1:], 10)
	// 全部任务完成才跳出循环结束，每秒判断一次
	for m.Done() == false {
		time.Sleep(time.Second)
	}
	time.Sleep(time.Second)
}
