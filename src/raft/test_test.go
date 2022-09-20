package raft

//
// Raft tests.
//
// we will use the original test_test.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The tester generously allows solutions to complete elections in one second
// (much more than the paper's range of timeouts).
const RaftElectionTimeout = 1000 * time.Millisecond

func TestInitialElection2A(t *testing.T) {
	// 测试最开始的leader状态，只能存在一个leader，并且所有server对此是共识
	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup() // 最后用完全部kill掉

	cfg.begin("Test (2A): initial election")

	// is a leader elected? 确认是不是只有一个server认为自己是leader
	cfg.checkOneLeader()

	// sleep a bit to avoid racing with followers learning of the
	// election, then check that all peers agree on the term.
	time.Sleep(50 * time.Millisecond)
	term1 := cfg.checkTerms() // 所有的raft的term都是一致的，并且返回其term值
	if term1 < 1 {
		t.Fatalf("term is %v, but should be at least 1", term1)
	}

	// does the leader+term stay the same if there is no network failure?
	time.Sleep(2 * RaftElectionTimeout)
	// time.Sleep(50 * time.Millisecond)
	term2 := cfg.checkTerms()
	if term1 != term2 {
		fmt.Println("term1 = ", term1)
		fmt.Println("term2 = ", term2)
		// 只做了2A的话可能因为没有实现leader定期发送heartbeat会出现这种情况
		// 完成了
		fmt.Println("warning: term changed even though there were no failures")
	}

	// there should still be a leader.
	cfg.checkOneLeader()

	cfg.end()
}

func TestReElection2A(t *testing.T) {
	// 通过下线server再上线server来测试raft的select 机制
	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2A): election after network failure")

	leader1 := cfg.checkOneLeader()

	// if the leader disconnects, a new one should be elected.
	cfg.disconnect(leader1)
	cfg.checkOneLeader()

	// if the old leader rejoins, that shouldn't
	// disturb the new leader. and the old leader
	// should switch to follower.
	cfg.connect(leader1)
	leader2 := cfg.checkOneLeader()

	// if there's no quorum, no new leader should
	// be elected.
	// 掉线了两个，一共三个，选不出leader
	cfg.disconnect(leader2)
	cfg.disconnect((leader2 + 1) % servers)
	time.Sleep(2 * RaftElectionTimeout)

	// check that the one connected server
	// does not think it is the leader.
	cfg.checkNoLeader()

	// if a quorum arises, it should elect a leader.
	// 再上线一个server，人数够，能选出来了
	cfg.connect((leader2 + 1) % servers)
	cfg.checkOneLeader()

	// re-join of last node shouldn't prevent leader from existing.
	// 上线最后一个，不能妨碍已经存在的leader
	cfg.connect(leader2)
	cfg.checkOneLeader()

	cfg.end()
}

func TestManyElections2A(t *testing.T) {
	// 多个server同时掉线并开始选举来测试并发选举
	servers := 7
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2A): multiple elections")

	cfg.checkOneLeader()

	iters := 10
	for ii := 1; ii < iters; ii++ {
		// disconnect three nodes
		i1 := rand.Int() % servers
		i2 := rand.Int() % servers
		i3 := rand.Int() % servers
		cfg.disconnect(i1)
		cfg.disconnect(i2)
		cfg.disconnect(i3)

		// either the current leader should still be alive,
		// or the remaining four should elect a new one.
		cfg.checkOneLeader()

		cfg.connect(i1)
		cfg.connect(i2)
		cfg.connect(i3)
	}

	cfg.checkOneLeader()

	cfg.end()
}

func TestBasicAgree2B(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2B): basic agreement")

	iters := 3
	for index := 1; index < iters+1; index++ {
		// 还没有start，应该检测不到新的index
		nd, _ := cfg.nCommitted(index)
		if nd > 0 {
			t.Fatalf("some have committed before Start()")
		}
		// one方法中才开始调用start
		xindex := cfg.one(index*100, servers, false)
		if xindex != index { // 得到的index和期望的不符合
			t.Fatalf("got index %v but expected %v", xindex, index)
		}
	}

	cfg.end()
}

//
// check, based on counting bytes of RPCs, that
// each command is sent to each peer just once.
//
func TestRPCBytes2B(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2B): RPC byte count")
	// 一开始是填充了一个log，所以这里的新log的index是1
	cfg.one(99, servers, false) // start一次，
	bytes0 := cfg.bytesTotal()  // 发送的总共的bytes

	iters := 10
	var sent int64 = 0 // 统计发送的总共的字符数
	// 前面发送了一次，index从2开始了
	for index := 2; index < iters+2; index++ {
		cmd := randstring(5000)
		xindex := cfg.one(cmd, servers, false)
		if xindex != index {
			t.Fatalf("got index %v but expected %v", xindex, index)
		}
		sent += int64(len(cmd))
	}

	bytes1 := cfg.bytesTotal()
	got := bytes1 - bytes0            // 两次bytes之差
	expected := int64(servers) * sent // sent的字符*servers数量
	if got > expected+50000 {         // 实际bytes差大于预计发送的总字符数
		t.Fatalf("too many RPC bytes; got %v, expected %v", got, expected)
	}

	cfg.end()
}

//
// test just failure of followers.
//
func For2023TestFollowerFailure2B(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2B): test progressive failure of followers")

	cfg.one(101, servers, false) // start一次 ，index = 1

	// disconnect one follower from the network.
	leader1 := cfg.checkOneLeader()
	cfg.disconnect((leader1 + 1) % servers)

	// the leader and remaining follower should be
	// able to agree despite the disconnected follower.
	cfg.one(102, servers-1, false) // 再start一次， index = 2
	time.Sleep(RaftElectionTimeout)
	cfg.one(103, servers-1, false) // 掉线一个server后start两次， index = 3
	// 这俩index都能提交，有一个leader一个server在线，大于定义时候的3个
	// disconnect the remaining follower
	leader2 := cfg.checkOneLeader()         // 检查是否还是一个leader
	cfg.disconnect((leader2 + 1) % servers) // 掉线两个，不包括leader，包括已经掉线的那个
	cfg.disconnect((leader2 + 2) % servers)

	// submit a command.
	index, _, ok := cfg.rafts[leader2].Start(104) // expected index = 4
	if ok != true {
		t.Fatalf("leader rejected Start()")
	}
	if index != 4 {
		t.Fatalf("expected index 4, got %v", index)
	}

	time.Sleep(2 * RaftElectionTimeout)

	// check that command 104 did not commit.
	// 这个command只有leader一个人，只有一个在线的leader，不能提交
	n, _ := cfg.nCommitted(index)
	if n > 0 {
		t.Fatalf("%v committed but no majority", n)
	}

	cfg.end()
}

//
// test just failure of leaders.
//
func For2023TestLeaderFailure2B(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2B): test failure of leaders")

	cfg.one(101, servers, false) // start一次，index = 1

	// disconnect the first leader. 掉线一个leader
	leader1 := cfg.checkOneLeader()
	cfg.disconnect(leader1)

	// the remaining followers should elect a new leader.
	cfg.one(102, servers-1, false) // 再start一次，index = 2
	time.Sleep(RaftElectionTimeout)
	cfg.one(103, servers-1, false) // 再start一次，index = 3

	// disconnect the new leader. 掉线新leader
	leader2 := cfg.checkOneLeader()
	cfg.disconnect(leader2)

	// submit a command to each server.
	// 也就是剩下的那个start一次，剩下的这个肯定不能选举为leader，所以要这样start
	for i := 0; i < servers; i++ {
		cfg.rafts[i].Start(104)
	}

	time.Sleep(2 * RaftElectionTimeout) // 选举时间也不能选举

	// check that command 104 did not commit.
	n, _ := cfg.nCommitted(4)
	if n > 0 {
		t.Fatalf("%v committed but no majority", n)
	}

	cfg.end()
}

//
// test that a follower participates after
// disconnect and re-connect.
//
func TestFailAgree2B(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2B): agreement after follower reconnects")

	cfg.one(101, servers, false) // start一次 index = 1

	// disconnect one follower from the network.下线一个follower
	leader := cfg.checkOneLeader()
	cfg.disconnect((leader + 1) % servers)

	// the leader and remaining follower should be
	// able to agree despite the disconnected follower.
	cfg.one(102, servers-1, false) // start一次，index = 2
	cfg.one(103, servers-1, false) // start一次，index = 3
	time.Sleep(RaftElectionTimeout)
	cfg.one(104, servers-1, false) // start一次，index = 4
	cfg.one(105, servers-1, false) // start一次，index = 5

	// re-connect
	cfg.connect((leader + 1) % servers) // 下线的follower重新上线

	// the full set of servers should preserve
	// previous agreements, and be able to agree on new commands.
	cfg.one(106, servers, true) // 重复start，直到新上线的server可以agree新的command，index = 6
	time.Sleep(RaftElectionTimeout)
	cfg.one(107, servers, true) // 再重复start，index = 7
	// 这个重复start的过程中不会报错，10s内成功就不会报错，不然会超时未完成报错
	cfg.end()
}

func TestFailNoAgree2B(t *testing.T) {
	servers := 5
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2B): no agreement if too many followers disconnect")

	cfg.one(10, servers, false) // start一次，index = 1

	// 3 of 5 followers disconnect，下线3个server，一共5个，再上线前start不会成功
	leader := cfg.checkOneLeader()
	cfg.disconnect((leader + 1) % servers)
	cfg.disconnect((leader + 2) % servers)
	cfg.disconnect((leader + 3) % servers)

	index, _, ok := cfg.rafts[leader].Start(20) // 可以start，index = 2，ok也是true，但是不会commit
	if ok != true {
		t.Fatalf("leader rejected Start()")
	}
	if index != 2 {
		t.Fatalf("expected index 2, got %v", index)
	}

	time.Sleep(2 * RaftElectionTimeout)

	n, _ := cfg.nCommitted(index) // index = 2这个部分不能commit
	if n > 0 {
		t.Fatalf("%v committed but no majority", n)
	}

	// repair，重新上线下线的server
	cfg.connect((leader + 1) % servers)
	cfg.connect((leader + 2) % servers)
	cfg.connect((leader + 3) % servers)

	// the disconnected majority may have chosen a leader from
	// among their own ranks, forgetting index 2.
	// 掉线的server可能自己单独选了一个leader，所以后面重新start的index可能是2或者3
	leader2 := cfg.checkOneLeader()
	index2, _, ok2 := cfg.rafts[leader2].Start(30) // index是2，说明选了一个新的leader，index是3说明leader没有变
	if ok2 == false {
		t.Fatalf("leader2 rejected Start()")
	}
	if index2 < 2 || index2 > 3 { // index在2，3之外就出错了
		t.Fatalf("unexpected index %v", index2)
	}

	cfg.one(1000, servers, true)

	cfg.end()
}

func TestConcurrentStarts2B(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2B): concurrent Start()s")

	var success bool
loop: // 做个标记，goto，continue用的，没别的意思
	for try := 0; try < 5; try++ {
		if try > 0 {
			// give solution some time to settle
			time.Sleep(3 * time.Second)
		}

		leader := cfg.checkOneLeader()
		_, term, ok := cfg.rafts[leader].Start(1) // start一个
		if !ok {                                  // 不是leader就重新loop
			// leader moved on really quickly
			continue
		}

		iters := 5
		var wg sync.WaitGroup
		is := make(chan int, iters)
		for ii := 0; ii < iters; ii++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				i, term1, ok := cfg.rafts[leader].Start(100 + i)
				if term1 != term {
					return
				}
				if ok != true {
					return
				}
				is <- i // index到is的chan中
			}(ii)
		}

		wg.Wait()
		close(is)

		for j := 0; j < servers; j++ {
			if t, _ := cfg.rafts[j].GetState(); t != term {
				// term changed -- can't expect low RPC counts
				continue loop
			}
		}

		failed := false
		cmds := []int{}
		for index := range is {
			cmd := cfg.wait(index, servers, term)
			if ix, ok := cmd.(int); ok {
				if ix == -1 {
					// peers have moved on to later terms
					// so we can't expect all Start()s to
					// have succeeded
					failed = true
					break
				}
				cmds = append(cmds, ix)
			} else {
				t.Fatalf("value %v is not an int", cmd)
			}
		}

		if failed {
			// avoid leaking goroutines
			go func() {
				for range is {
				}
			}()
			continue
		}

		// 遍历查找commend来确认是不是commit成功
		for ii := 0; ii < iters; ii++ {
			x := 100 + ii
			ok := false
			for j := 0; j < len(cmds); j++ {
				if cmds[j] == x {
					ok = true
				}
			}
			if ok == false { // 丢失了一个commend
				t.Fatalf("cmd %v missing in %v", x, cmds)
			}
		}

		success = true
		break
	}

	if !success {
		t.Fatalf("term changed too often")
	}

	cfg.end()
}

func TestRejoin2B(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2B): rejoin of partitioned leader")

	cfg.one(101, servers, true) // index = 1

	// leader network failure，下线leader
	leader1 := cfg.checkOneLeader()
	cfg.disconnect(leader1)

	// make old leader try to agree on some entries
	cfg.rafts[leader1].Start(102)
	cfg.rafts[leader1].Start(103)
	cfg.rafts[leader1].Start(104)

	// new leader commits, also for index=2
	cfg.one(103, 2, true) // index = 2， 循环是true

	// new leader network failure， 下线第二个leader
	leader2 := cfg.checkOneLeader()
	cfg.disconnect(leader2)

	// old leader connected again，恢复第一个leader
	cfg.connect(leader1)

	cfg.one(104, 2, true) // index = 3， 循环是true

	// all together now， 全部恢复
	cfg.connect(leader2)

	cfg.one(105, servers, true) // index = 4，循环是true

	cfg.end()
}

// 只是测试流程能不能走通，上线的server数量和commit的联系
func TestBackup2B(t *testing.T) {
	servers := 5
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2B): leader backs up quickly over incorrect follower logs")

	cfg.one(rand.Int(), servers, true) // start，循环，index = 1

	// put leader and one follower in a partition，下线三个server
	leader1 := cfg.checkOneLeader()
	cfg.disconnect((leader1 + 2) % servers)
	cfg.disconnect((leader1 + 3) % servers)
	cfg.disconnect((leader1 + 4) % servers)

	// submit lots of commands that won't commit
	for i := 0; i < 50; i++ { // 加50个不会commit的log，index顺延了
		cfg.rafts[leader1].Start(rand.Int())
	}

	time.Sleep(RaftElectionTimeout / 2)

	cfg.disconnect((leader1 + 0) % servers) // 剩下俩也下线
	cfg.disconnect((leader1 + 1) % servers)

	// allow other partition to recover ， 上线之前下线的三个
	cfg.connect((leader1 + 2) % servers)
	cfg.connect((leader1 + 3) % servers)
	cfg.connect((leader1 + 4) % servers)

	// lots of successful commands to new group.
	for i := 0; i < 50; i++ { // 这三个可以选出有效的新的leader，这些可以提交
		cfg.one(rand.Int(), 3, true)
	}

	// now another partitioned leader and one follower
	leader2 := cfg.checkOneLeader() // 下线一个上线的非leader的server
	other := (leader1 + 2) % servers
	if leader2 == other {
		other = (leader2 + 1) % servers
	}
	cfg.disconnect(other)

	// lots more commands that won't commit，线上只有两个，这些不会commit
	for i := 0; i < 50; i++ {
		cfg.rafts[leader2].Start(rand.Int())
	}

	time.Sleep(RaftElectionTimeout / 2)

	// bring original leader back to life, 全部下线
	for i := 0; i < servers; i++ {
		cfg.disconnect(i)
	}
	cfg.connect((leader1 + 0) % servers) // 重新上线最开始的几个server
	cfg.connect((leader1 + 1) % servers)
	cfg.connect(other)

	// lots of successful commands to new group. 满3个server，可以commit
	for i := 0; i < 50; i++ {
		cfg.one(rand.Int(), 3, true)
	}

	// now everyone
	for i := 0; i < servers; i++ { // 全部上线
		cfg.connect(i)
	}
	cfg.one(rand.Int(), servers, true) // 循环start，不用管index了

	cfg.end()
}

func TestCount2B(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2B): RPC counts aren't too high")

	rpcs := func() (n int) { // 得到所有的servers传入RPC的数量
		for j := 0; j < servers; j++ {
			// get a server's count of incoming RPCs.
			n += cfg.rpcCount(j)
		}
		return
	}

	leader := cfg.checkOneLeader()

	total1 := rpcs() // 先统计一下当下所有servers的RPC的数量

	if total1 > 30 || total1 < 1 {
		t.Fatalf("too many or few RPCs (%v) to elect initial leader\n", total1)
	}

	var total2 int
	var success bool
loop:
	for try := 0; try < 5; try++ {
		if try > 0 {
			// give solution some time to settle
			time.Sleep(3 * time.Second)
		}

		leader = cfg.checkOneLeader()
		total1 = rpcs()

		iters := 10
		starti, term, ok := cfg.rafts[leader].Start(1)
		if !ok {
			// leader moved on really quickly
			continue
		}
		cmds := []int{}
		for i := 1; i < iters+2; i++ {
			x := int(rand.Int31())
			cmds = append(cmds, x)
			index1, term1, ok := cfg.rafts[leader].Start(x)
			if term1 != term {
				// Term changed while starting
				continue loop
			}
			if !ok {
				// No longer the leader, so term has changed
				continue loop
			}
			if starti+i != index1 {
				t.Fatalf("Start() failed")
			}
		}

		for i := 1; i < iters+1; i++ {
			cmd := cfg.wait(starti+i, servers, term)
			if ix, ok := cmd.(int); ok == false || ix != cmds[i-1] {
				if ix == -1 {
					// term changed -- try again
					continue loop
				}
				t.Fatalf("wrong value %v committed for index %v; expected %v\n", cmd, starti+i, cmds)
			}
		}

		failed := false
		total2 = 0
		for j := 0; j < servers; j++ {
			if t, _ := cfg.rafts[j].GetState(); t != term {
				// term changed -- can't expect low RPC counts
				// need to keep going to update total2
				failed = true
			}
			total2 += cfg.rpcCount(j)
		}

		if failed {
			continue loop
		}

		if total2-total1 > (iters+1+3)*3 {
			t.Fatalf("too many RPCs (%v) for %v entries\n", total2-total1, iters)
		}

		success = true
		break
	}

	if !success {
		t.Fatalf("term changed too often")
	}

	time.Sleep(RaftElectionTimeout)

	total3 := 0
	for j := 0; j < servers; j++ {
		total3 += cfg.rpcCount(j)
	}

	if total3-total2 > 3*20 {
		t.Fatalf("too many RPCs (%v) for 1 second of idleness\n", total3-total2)
	}

	cfg.end()
}

// ----------------------------2C-------------------------------
func TestPersist12C(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2C): basic persistence")

	cfg.one(11, servers, true) // 循环start，index = 1，让persister有更新有内容

	// crash and re-start all
	for i := 0; i < servers; i++ {
		cfg.start1(i, cfg.applier) // start1就是保存原来的persist然后新恢复一个新的make
	}
	// 下线并重新上线所有的servers，和start1不一样
	for i := 0; i < servers; i++ {
		cfg.disconnect(i)
		cfg.connect(i)
	}

	cfg.one(12, servers, true) // 循环start，

	leader1 := cfg.checkOneLeader()
	cfg.disconnect(leader1)
	cfg.start1(leader1, cfg.applier) // 感觉就是做一个替换的操作，并没有改变原来peers的状态
	cfg.connect(leader1)             // 所以需要重新上线

	cfg.one(13, servers, true)

	leader2 := cfg.checkOneLeader()
	cfg.disconnect(leader2)
	cfg.one(14, servers-1, true)
	cfg.start1(leader2, cfg.applier)
	cfg.connect(leader2)

	cfg.wait(4, servers, -1) // wait for leader2 to join before killing i3

	i3 := (cfg.checkOneLeader() + 1) % servers
	cfg.disconnect(i3)
	cfg.one(15, servers-1, true)
	cfg.start1(i3, cfg.applier)
	cfg.connect(i3)

	cfg.one(16, servers, true)

	cfg.end()
}

func TestPersist22C(t *testing.T) {
	servers := 5
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2C): more persistence")

	index := 1
	// 没有上锁，中间有并发
	for iters := 0; iters < 5; iters++ {
		cfg.one(10+index, servers, true) // 先循环start一个Entry，index = 1，persister有所更新和内容
		index++

		// 下线两个follower
		leader1 := cfg.checkOneLeader()
		cfg.disconnect((leader1 + 1) % servers)
		cfg.disconnect((leader1 + 2) % servers)

		cfg.one(10+index, servers-2, true) // 循环start一个Entry，index = 2,这个可以commit
		index++

		// 全部下线
		cfg.disconnect((leader1 + 0) % servers)
		cfg.disconnect((leader1 + 3) % servers)
		cfg.disconnect((leader1 + 4) % servers)

		// 换掉两个server并上线
		cfg.start1((leader1+1)%servers, cfg.applier)
		cfg.start1((leader1+2)%servers, cfg.applier)
		cfg.connect((leader1 + 1) % servers)
		cfg.connect((leader1 + 2) % servers)

		time.Sleep(RaftElectionTimeout)

		// 再换掉一个再上线
		cfg.start1((leader1+3)%servers, cfg.applier)
		cfg.connect((leader1 + 3) % servers)

		cfg.one(10+index, servers-2, true) // 循环start一个Entry，这个应该可以commit并且index = 3
		index++

		// 剩下的server也上线
		cfg.connect((leader1 + 4) % servers)
		cfg.connect((leader1 + 0) % servers)
	}

	cfg.one(1000, servers, true)

	cfg.end()
}

func TestPersist32C(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2C): partitioned leader and one follower crash, leader restarts")

	cfg.one(101, 3, true) // start一边，传入数据

	leader := cfg.checkOneLeader() // 下线一个follower
	cfg.disconnect((leader + 2) % servers)

	cfg.one(102, 2, true) // 再start一个，传入数据

	// shut down和保存leader和另外一个follower，再上线之前的follower
	// 并且复制恢复leader并上线
	cfg.crash1((leader + 0) % servers)
	cfg.crash1((leader + 1) % servers)
	cfg.connect((leader + 2) % servers)
	cfg.start1((leader+0)%servers, cfg.applier)
	cfg.connect((leader + 0) % servers)

	cfg.one(103, 2, true) // 可以commit

	cfg.start1((leader+1)%servers, cfg.applier) // 再复制并上线剩下的follower
	cfg.connect((leader + 1) % servers)

	cfg.one(104, servers, true) // 可以commit

	cfg.end()
}

func TestUnreliableAgree2C(t *testing.T) {
	servers := 5
	cfg := make_config(t, servers, true, false)
	defer cfg.cleanup()

	cfg.begin("Test (2C): unreliable agreement")

	var wg sync.WaitGroup

	for iters := 1; iters < 50; iters++ {
		for j := 0; j < 4; j++ {
			wg.Add(1)
			go func(iters, j int) {
				defer wg.Done()
				cfg.one((100*iters)+j, 1, true)
			}(iters, j)
		}
		cfg.one(iters, 1, true)
	}

	cfg.setunreliable(false)

	wg.Wait()

	cfg.one(100, servers, true)

	cfg.end()
}

func internalChurn(t *testing.T, unreliable bool) {

	servers := 5
	cfg := make_config(t, servers, unreliable, false)
	defer cfg.cleanup()

	if unreliable {
		cfg.begin("Test (2C): unreliable churn")
	} else {
		cfg.begin("Test (2C): churn")
	}

	stop := int32(0)

	// create concurrent clients
	cfn := func(me int, ch chan []int) {
		var ret []int
		ret = nil
		defer func() { ch <- ret }()
		values := []int{}
		for atomic.LoadInt32(&stop) == 0 {
			x := rand.Int()
			index := -1
			ok := false
			for i := 0; i < servers; i++ {
				// try them all, maybe one of them is a leader
				cfg.mu.Lock()
				rf := cfg.rafts[i]
				cfg.mu.Unlock()
				if rf != nil {
					index1, _, ok1 := rf.Start(x)
					if ok1 {
						ok = ok1
						index = index1
					}
				}
			}
			if ok {
				// maybe leader will commit our value, maybe not.
				// but don't wait forever.
				for _, to := range []int{10, 20, 50, 100, 200} {
					nd, cmd := cfg.nCommitted(index)
					if nd > 0 {
						if xx, ok := cmd.(int); ok {
							if xx == x {
								values = append(values, x)
							}
						} else {
							cfg.t.Fatalf("wrong command type")
						}
						break
					}
					time.Sleep(time.Duration(to) * time.Millisecond)
				}
			} else {
				time.Sleep(time.Duration(79+me*17) * time.Millisecond)
			}
		}
		ret = values
	}

	ncli := 3
	cha := []chan []int{}
	for i := 0; i < ncli; i++ {
		cha = append(cha, make(chan []int))
		go cfn(i, cha[i])
	}

	for iters := 0; iters < 20; iters++ {
		if (rand.Int() % 1000) < 200 {
			i := rand.Int() % servers
			cfg.disconnect(i)
		}

		if (rand.Int() % 1000) < 500 {
			i := rand.Int() % servers
			if cfg.rafts[i] == nil {
				cfg.start1(i, cfg.applier)
			}
			cfg.connect(i)
		}

		if (rand.Int() % 1000) < 200 {
			i := rand.Int() % servers
			if cfg.rafts[i] != nil {
				cfg.crash1(i)
			}
		}

		// Make crash/restart infrequent enough that the peers can often
		// keep up, but not so infrequent that everything has settled
		// down from one change to the next. Pick a value smaller than
		// the election timeout, but not hugely smaller.
		time.Sleep((RaftElectionTimeout * 7) / 10)
	}

	time.Sleep(RaftElectionTimeout)
	cfg.setunreliable(false)
	for i := 0; i < servers; i++ {
		if cfg.rafts[i] == nil {
			cfg.start1(i, cfg.applier)
		}
		cfg.connect(i)
	}

	atomic.StoreInt32(&stop, 1)

	values := []int{}
	for i := 0; i < ncli; i++ {
		vv := <-cha[i]
		if vv == nil {
			t.Fatal("client failed")
		}
		values = append(values, vv...)
	}

	time.Sleep(RaftElectionTimeout)

	lastIndex := cfg.one(rand.Int(), servers, true)

	really := make([]int, lastIndex+1)
	for index := 1; index <= lastIndex; index++ {
		v := cfg.wait(index, servers, -1)
		if vi, ok := v.(int); ok {
			really = append(really, vi)
		} else {
			t.Fatalf("not an int")
		}
	}

	for _, v1 := range values {
		ok := false
		for _, v2 := range really {
			if v1 == v2 {
				ok = true
			}
		}
		if ok == false {
			cfg.t.Fatalf("didn't find a value")
		}
	}

	cfg.end()
}

func TestReliableChurn2C(t *testing.T) {
	internalChurn(t, false)
}

func TestUnreliableChurn2C(t *testing.T) {
	internalChurn(t, true)
}

//
// Test the scenarios described in Figure 8 of the extended Raft paper. Each
// iteration asks a leader, if there is one, to insert a command in the Raft
// log.  If there is a leader, that leader will fail quickly with a high
// probability (perhaps without committing the command), or crash after a while
// with low probability (most likey committing the command).  If the number of
// alive servers isn't enough to form a majority, perhaps start a new server.
// The leader in a new term may try to finish replicating log entries that
// haven't been committed yet.
// 测试论文图8描述的场景，每次迭代都让leader在log中插入一个command
// 如果有leader，令其以高概率快速失败（可能没有提交命令），
// 或者在一段时间后以低概率崩溃（最有可能提交命令）。
// 如果存活server的数量不足以形成多数，则可能启动一个新server。
// 新任期的leader可能会尝试完成对尚未提交的entries的复制。
// figure8这两个测试在完成2D进行lastincludedindex的缩减之后可以顺利通过
//
func TestFigure82C(t *testing.T) {
	servers := 5
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2C): Figure 8")

	cfg.one(rand.Int(), 1, true) // start一次，persistent有记录

	nup := servers
	for iters := 0; iters < 1000; iters++ {
		leader := -1
		for i := 0; i < servers; i++ { // 找leader并start一次
			if cfg.rafts[i] != nil {
				_, _, ok := cfg.rafts[i].Start(rand.Int())
				if ok {
					leader = i
				}
			}
		}

		if (rand.Int() % 1000) < 100 {
			ms := rand.Int63() % (int64(RaftElectionTimeout/time.Millisecond) / 2)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		} else {
			ms := (rand.Int63() % 13)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}

		if leader != -1 { // leader存在，让其fail，并且保存persistent
			cfg.crash1(leader)
			nup -= 1
		}

		if nup < 3 { // server数量需要过半
			s := rand.Int() % servers
			if cfg.rafts[s] == nil {
				cfg.start1(s, cfg.applier) // 复制并连接
				cfg.connect(s)
				nup += 1
			}
		}
	}

	for i := 0; i < servers; i++ { // 下线的都复制上线
		if cfg.rafts[i] == nil {
			cfg.start1(i, cfg.applier)
			cfg.connect(i)
		}
	}

	cfg.one(rand.Int(), servers, true)

	cfg.end()
}

// ----------------
func TestFigure8Unreliable2C(t *testing.T) {
	servers := 5
	cfg := make_config(t, servers, true, false)
	defer cfg.cleanup()

	cfg.begin("Test (2C): Figure 8 (unreliable)")
	num := rand.Int() % 10000
	cfg.one(num, 1, true)

	nup := servers
	for iters := 0; iters < 1000; iters++ {
		if iters == 200 {
			cfg.setlongreordering(true) // 延迟回复很长时间，和reliable的区别
		}
		leader := -1
		for i := 0; i < servers; i++ { // 找到leader并start一次
			num := rand.Int() % 10000
			_, _, ok := cfg.rafts[i].Start(num)
			if ok && cfg.connected[i] {
				leader = i
			}
		}

		if (rand.Int() % 1000) < 100 { // 1/10的概率sleep 1000ms, 大部分时间sleep 13ms
			ms := rand.Int63() % (int64(RaftElectionTimeout/time.Millisecond) / 2)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		} else {
			ms := (rand.Int63() % 13)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}

		// 不保存persistent，和reliable的区别, 下面是rand(1000) < 500 , 1/2的概率下线这个leader下线
		if leader != -1 && (rand.Int()%1000) < int(RaftElectionTimeout/time.Millisecond)/2 {
			cfg.disconnect(leader)
			nup -= 1
		}

		if nup < 3 { // 只是重连并不恢复persistent
			s := rand.Int() % servers
			if cfg.connected[s] == false {
				cfg.connect(s)
				nup += 1
			}
		}
	}

	for i := 0; i < servers; i++ { // 全连接上
		if cfg.connected[i] == false {
			cfg.connect(i)
		}
	}

	num = rand.Int() % 10000
	cfg.one(num, servers, true)

	cfg.end()
}

// ---------------------------2D-----------------------------
const MAXLOGSIZE = 2000

func snapcommon(t *testing.T, name string, disconnect bool, reliable bool, crash bool) {
	iters := 30
	servers := 3
	cfg := make_config(t, servers, !reliable, true)
	defer cfg.cleanup()

	cfg.begin(name)

	cfg.one(rand.Int(), servers, true)
	leader1 := cfg.checkOneLeader()

	for i := 0; i < iters; i++ {
		victim := (leader1 + 1) % servers
		sender := leader1
		if i%3 == 1 {
			sender = (leader1 + 1) % servers
			victim = leader1
		}

		if disconnect {
			cfg.disconnect(victim)
			cfg.one(rand.Int(), servers-1, true)
		}
		if crash {
			cfg.crash1(victim)
			cfg.one(rand.Int(), servers-1, true)
		}

		// perhaps send enough to get a snapshot
		nn := (SnapShotInterval / 2) + (rand.Int() % SnapShotInterval)
		for i := 0; i < nn; i++ {
			cfg.rafts[sender].Start(rand.Int())
		}

		// let applier threads catch up with the Start()'s
		if disconnect == false && crash == false {
			// make sure all followers have caught up, so that
			// an InstallSnapshot RPC isn't required for
			// TestSnapshotBasic2D().
			cfg.one(rand.Int(), servers, true)
		} else {
			cfg.one(rand.Int(), servers-1, true)
		}

		if cfg.LogSize() >= MAXLOGSIZE {
			cfg.t.Fatalf("Log size too large")
		}
		if disconnect {
			// reconnect a follower, who maybe behind and
			// needs to rceive a snapshot to catch up.
			cfg.connect(victim)
			cfg.one(rand.Int(), servers, true)
			leader1 = cfg.checkOneLeader()
		}
		if crash {
			cfg.start1(victim, cfg.applierSnap)
			cfg.connect(victim)
			cfg.one(rand.Int(), servers, true)
			leader1 = cfg.checkOneLeader()
		}
	}
	cfg.end()
}

func TestSnapshotBasic2D(t *testing.T) {
	snapcommon(t, "Test (2D): snapshots basic", false, true, false)
}

func TestSnapshotInstall2D(t *testing.T) {
	snapcommon(t, "Test (2D): install snapshots (disconnect)", true, true, false)
}

func TestSnapshotInstallUnreliable2D(t *testing.T) {
	snapcommon(t, "Test (2D): install snapshots (disconnect+unreliable)",
		true, false, false)
}

func TestSnapshotInstallCrash2D(t *testing.T) {
	snapcommon(t, "Test (2D): install snapshots (crash)", false, true, true)
}

func TestSnapshotInstallUnCrash2D(t *testing.T) {
	snapcommon(t, "Test (2D): install snapshots (unreliable+crash)", false, false, true)
}

//
// do the servers persist the snapshots, and
// restart using snapshot along with the
// tail of the log?
//
func TestSnapshotAllCrash2D(t *testing.T) {
	servers := 3
	iters := 5
	cfg := make_config(t, servers, false, true)
	defer cfg.cleanup()

	cfg.begin("Test (2D): crash and restart all servers")

	cfg.one(rand.Int(), servers, true)

	for i := 0; i < iters; i++ {
		// perhaps enough to get a snapshot
		nn := (SnapShotInterval / 2) + (rand.Int() % SnapShotInterval)
		for i := 0; i < nn; i++ {
			cfg.one(rand.Int(), servers, true)
		}

		index1 := cfg.one(rand.Int(), servers, true)

		// crash all
		for i := 0; i < servers; i++ {
			cfg.crash1(i)
		}

		// revive all
		for i := 0; i < servers; i++ {
			cfg.start1(i, cfg.applierSnap)
			cfg.connect(i)
		}

		index2 := cfg.one(rand.Int(), servers, true)
		if index2 < index1+1 {
			t.Fatalf("index decreased from %v to %v", index1, index2)
		}
	}
	cfg.end()
}
