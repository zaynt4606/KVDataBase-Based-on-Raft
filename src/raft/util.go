package raft

import (
	"fmt"
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

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
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
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 被kill了也要返回
	if rf.killed() {
		return -1, -1, false
	}
	// 不是leader返回false
	if rf.currentState != Leader {
		return -1, -1, false
	}

	index := rf.realLastIndex() + 1 // 提交后会出现的index编号，也就是最后一个index + 1
	term := rf.currentTerm          //当前任期
	isLeader := true                // 自己是leader

	// 加入新的entry到leader的log中
	// 这里加入新的Entry，那上面的index其实指的就是这个Entry，也就是新加入的Entry的index
	rf.log = append(rf.log, Entry{Term: term, Index: index, Data: command})
	rf.persist()

	return index, term, isLeader
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
// 这里没有用到，开了3个新的ticker
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should be started
		// and to randomize sleeping time using time.Sleep().
	}
}

// --------------------------------------自定义----------------------------------------

// 通过不同的随机种子生成不同的过期时间
func generateOverTime(server int64) int {
	rand.Seed(time.Now().Unix() + server)
	return rand.Intn(MoreVoteTime) + MinVoteTime
}

func min(a int, b int) int {
	if a <= b {
		return a
	} else {
		return b
	}
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
	if curIndex < rf.lastIncludedIndex {
		fmt.Println("curIndex =", curIndex, "rf.lastIncludedIndex =", rf.lastIncludedIndex)
	}
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
	// 一开始就加了一个Entry在log中，index从1开始，
	if entryIndex == lastIndex+1 {
		entryIndex = lastIndex
	}
	return entryIndex, rf.indexToTerm(entryIndex)
}
