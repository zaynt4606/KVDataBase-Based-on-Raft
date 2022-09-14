package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// 结点的类型
const (
	Follower  = 1
	Candidate = 2
	Leader    = 3
)

const NONE = -1 // 可以指代节点的状态或者其他没有分配时候的值

const (
	// 定义随机生成投票过期时间范围:(MoreVoteTime+MinVoteTime~MinVoteTime)
	MoreVoteTime = 100
	MinVoteTime  = 75

	// HeartbeatSleep heartbeat时间,这个时间要比选举低，才能建立稳定心跳机制
	HeartbeatSleep = 35
	AppliedSleep   = 15
)

// hint中需要自定义的结构来保存有关每个日志条目的信息
type Entry struct {
	Term  int         // 收到leader时的任期号
	Index int         // log index
	Data  interface{} // 指令内容
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Look at the paper's Figure 2 for a description of what
	// Your data here (2A, 2B, 2C).
	// commen use
	electionTimer  time.Time
	heartbeatTimer time.Time

	// 2A
	logIndex      int // current newest log index, start from 1
	currentState  int
	currentLeader int
	// 所有servers需要的持久化变量
	currentTerm int     // 当前任期, start from 0
	votedFor    int     // 当前任期把票投给了谁 candidateId that received vote in current term
	log         []Entry // 日志条目数组

	// 2B
	applyCh chan ApplyMsg

	commitIndex int
	lastApplied int

	// for leader, Reinitialized after election
	nextIndex  []int // index of the next log entry to send to, initialized to leader last log index + 1)
	matchIndex []int // index of highest log entry known to be replicated on server

	// state a Raft server must maintain.

}

// -----------------------------RPC参数--------------------------------
// hint中需要补全的,在论文图2 RequestVote RPC 中有定义和说明
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	// 2A
	Term        int // 需要竞选人的任期
	CandidateID int // 需要竞选人的ID
	// 2B
	LastLogIndex int // 竞选人日志条目最后索引(2D包含快照
	LastLogTerm  int // 候选人最后日志条目的任期号(2D包含快照
}

// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 当前任期，竞选者用来更新，for candidate to update itself
	VoteGranted bool // 竞选者收到投票，true means candidate received vote
}

// hint中需要自定义该结构来实现heartbeats
// leader定期发送该结构，并重置选举超时
// 该结构在论文图2中已经有定义及描述
type AppendEntriesArgs struct {
	// 2A
	Term     int // leader’s term
	LeaderId int // leader自身ID, follower can redirect clients
	// 2B
	PrevLogIndex int     // 用于匹配日志位置是否是合适的，初始化rf.nextIndex[i] - 1
	PrevLogTerm  int     // 用于匹配日志的任期是否是合适的是，是否有冲突, 上一条对应的任期
	Entries      []Entry // 预计存储的日志（为空时就是心跳连接）
	LeaderCommit int     // leader’s commitIndex 指的是最后一个被大多数server复制的日志的index
}

//
// --------------------------------------------------------------------
//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	// 2A
	rf.votedFor = NONE
	rf.currentLeader = NONE
	rf.currentState = NONE
	rf.currentTerm = 0
	// 初始化的时候就加一个进入到log
	rf.log = []Entry{}
	rf.log = append(rf.log, Entry{})

	// 2B
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	// go rf.ticker()
	go rf.electionTicker()

	return rf
}

//
// ----------------------------------------leader选举-------------------------------------------
//
func (rf *Raft) StartElection() {
	// 自己发起选取，改变状态， 论文中server成为Candidates的要求
	rf.currentState = Candidate
	rf.votedFor = rf.me
	rf.currentTerm++
	rf.electionTimer = time.Now()
	// 保存要保存的东西,save Raft's persistent state to stable storage,
	rf.persist()
	voteNum := 1 // 统计票数，自己给自己的一票

	// 遍历所有raft的server
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me { // 跳过Candidate
			continue
		}
		// 开启协程进行竞选
		go func(server int) {
			rf.mu.Lock()             // 创建发送和接收的内容需要上锁
			args := RequestVoteArgs{ // 投票要发的args
				rf.currentTerm,
				rf.me,
				rf.logIndex,
				rf.log[len(rf.log)-1].Term,
			}
			reply := RequestVoteReply{} // 投完票接收的reply
			rf.mu.Unlock()
			// 发送并获得reply，res接到是否发送成功
			// res := rf.peers[server].Call("Raft.RequestVote", &args, &reply)
			res := rf.sendRequestVote(server, &args, &reply) // 下面的RequestVote在这里使用
			rf.electionTimer = time.Now()                    //给别的peer投票的时候也更新，函数里面其实已经更新了
			if res {
				rf.mu.Lock()
				// 自身如果不是Candidate或者任期不符直接退出
				if rf.currentState != Candidate || args.Term != rf.currentTerm {
					rf.mu.Unlock()
					return
				}
				// 返回任期和当前任期不符,重置状态后退出
				if args.Term < reply.Term {
					if rf.currentTerm < reply.Term {
						rf.currentTerm = reply.Term
					}
					rf.currentState = Follower
					rf.votedFor = NONE
					rf.persist()
					rf.mu.Unlock()
					return
				}
				if reply.VoteGranted {
					voteNum++ // 统计票数
					// 票数过半
					if voteNum > len(rf.peers)/2 {
						// 修改状态
						rf.currentState = Leader
						rf.currentLeader = rf.me
						rf.votedFor = NONE

						rf.persist()
						// 更新leader管控信息
						rf.nextIndex = make([]int, len(rf.peers)) // 初始化长度是index + 1
						for i := 0; i < len(rf.peers); i++ {
							rf.nextIndex[i] = rf.logIndex + 1
						}
						rf.matchIndex = make([]int, len(rf.peers))
						rf.matchIndex[rf.me] = rf.log[len(rf.log)-1].Term
						rf.electionTimer = time.Now()

						rf.mu.Unlock()
						return
					}
				}
				rf.mu.Unlock()
				return
			} // 已接受
		}(i) // 实参赋给server

	}
}

//
// example RequestVote RPC handler. 论文中的RequesVote RPC
// 在startElection中发送投票使用到
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Reply false if term < currentTerm, 论文中的第一个要求，
	// args任期更小直接返回false，网络延迟问题
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	// 论文all servers 第二个要求
	if args.Term > rf.currentTerm { // 任期大于当前任期，当前任期迟了，重置rf状态
		rf.currentTerm = args.Term
		rf.currentState = Follower
		rf.votedFor = NONE
		rf.persist()
	}

	// 论文RequesVote RPC第二个要求之一：votedFor is null or candidateId才可以返回true
	if rf.votedFor != NONE && rf.votedFor != args.CandidateID {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	// 论文RequesVote RPC第二个要求之一：candidate’s log is at least as up-to-date as receiver’s log
	if !rf.UpToDate(args.LastLogIndex, args.LastLogTerm) {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	// 更新参数后返回true
	reply.VoteGranted = true
	reply.Term = rf.currentTerm
	rf.votedFor = args.CandidateID
	rf.electionTimer = time.Now()
	rf.persist()
	return
}

// ----------------------------------------ticker----------------------------------------------------
// The ticker go routine starts a new election if this peer hasn't received heartsbeats recently.
func (rf *Raft) electionTicker() {
	for rf.killed() == false {
		nowTime := time.Now()
		// sleep 一个范围内的随机时间
		time.Sleep(time.Duration(generateOverTime(int64(rf.me))) * time.Millisecond)
		rf.mu.Lock()
		if rf.electionTimer.Before(nowTime) && rf.currentState != Leader {
			// 开始选举
			rf.StartElection()
			// rf.electionTimer = time.Now() // 都放到StartElection中实现
		}
		rf.mu.Unlock()
	}
}

// -----------------------------------------------------------------------------------
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
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	// e.Encode(rf.logs)
	// e.Encode(rf.lastIncludeIndex)
	// e.Encode(rf.lastIncludeTerm)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

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
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

	}
}
