package raft

import (
	"log"
	"math/rand"
	"sync/atomic"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// return currentTerm and whether this server believes it is the leader.
// checkTerms中用到
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isleader := (rf.currentState == Leader)
	return term, isleader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

	}
}

// --------------------------------------自定义----------------------------------------
// 最小值min
func min(num int, num1 int) int {
	if num > num1 {
		return num1
	} else {
		return num
	}
}

// 通过不同的随机种子生成不同的过期时间
func generateOverTime(server int64) int {
	rand.Seed(time.Now().Unix() + server)
	return rand.Intn(MoreVoteTime) + MinVoteTime
}

// candidate’s log is at least as up-to-date as receiver’s log
func (rf *Raft) UpToDate(index int, term int) bool {
	lastIndex := rf.realLastIndex()
	lastTerm := rf.realLastTerm()
	return term > lastTerm || (term == lastTerm && index >= lastIndex)
}

// 获取最后的快照日志下标(代表已存储）
// 是整个raft存储的最后一个，也就是正常leader存储的最后一个，不一定提交了
func (rf *Raft) realLastIndex() int {
	// 如果lastIncludedIndex不为0的话说明之前logs已经清理过一波，
	// 现在的logs是在lastIncludedIndex之后的新的还没有更新的
	// 所以在这个之上再加上logs就是真实的最后一个index
	return len(rf.log) - 1 + rf.lastIncludedIndex
}

// 上面index对应的term
func (rf *Raft) realLastTerm() int {
	// 也就是log数组中最后一个日志对应的term，如果log不是空的话
	// log是空的话说明刚刚已经缩减过，这时候最后一个就是included这个
	if len(rf.log) <= 1 { // 初始会填充一个
		return rf.lastIncludedTerm
	}
	return rf.log[len(rf.log)-1].Term
}

// 根据当前server的某个index得到对应index的任期
func (rf *Raft) indexToTerm(curIndex int) int {
	// 刚好就是lastIncludedIndex
	if curIndex == rf.lastIncludedIndex {
		return rf.lastIncludedTerm
	}
	// 按照距离lastIncludedIndex的长度在log中找到
	return rf.log[curIndex-rf.lastIncludedIndex].Term
}

// 根据当前server的某个index得到对应index的Entry
func (rf *Raft) indexToEntry(curIndex int) Entry {
	return rf.log[curIndex-rf.lastIncludedIndex]
}

// 得到和上面对应的考虑了snapshot的真实的preLog信息，
func (rf *Raft) realPreLog(server int) (int, int) {
	// 注意realLastIndex定义，和server无关，是最后一个提交的index，不一定是server的最后一个
	lastIndex := rf.realLastIndex()
	// server对应的下一个的前一个，
	entryIndex := rf.nextIndex[server] - 1
	//
	if entryIndex == lastIndex+1 {
		entryIndex = lastIndex
	}
	return entryIndex, rf.indexToTerm(entryIndex)
}
