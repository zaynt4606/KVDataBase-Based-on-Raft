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
	"fmt"

	// "fmt"
	"sync"
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
// 当每个 Raft peer 意识到连续的日志条目被提交时，
// peer 应该通过传递给 Make() 的 applyCh 向同一服务器上的服务（或测试器）发送 ApplyMsg。
// 将 CommandValid 设置为 true 以指示 ApplyMsg 包含新提交的日志条目。chan用来同步信息的结构体
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
	// 定义随机生成投票过期时间范围:[MinVoteTime, MoreVoteTime+MinVoteTime)
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
	electionTimer time.Time

	// 2A
	// logIndex     int // server的最后一个index，不过用函数realLastIndex替代了
	currentState int
	// currentLeader int
	// 所有servers需要的持久化变量
	currentTerm int // 当前任期, start from 0
	votedFor    int // 当前任期把票投给了谁 candidateId that received vote in current term
	// 2B
	applyCh     chan ApplyMsg // chan中是所有可提交的日志，也就是server保存这条日志的数量过半
	log         []Entry       // 日志条目数组
	commitIndex int           // 先apply进chan中，然后更新commitIndex，也就是chan中最新的一个的index
	lastApplied int           // index of highest log entry applied to state machine
	// (initialized to 0, increases monotonically)

	// for leader, Reinitialized after election
	// nextIndex -index of the next log entry to send to, initialized to leader last log index + 1)
	// matchIndex -index of highest log entry known to be replicated on server
	nextIndex  []int // 下一个appendEntry从哪个peer开始
	matchIndex []int // 已知的某follower的log与leader的log最大匹配到第几个Index,已经apply,过半server都match了，就可以跟新commitIndex了

	//  the snapshot replaces all entries up through and including this index
	lastIncludedIndex int // 日志最后applied对应的index
	lastIncludedTerm  int // 上面index对应的term

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
	// 新日志之前的最后一条index和term
	PrevLogIndex int     // 用于匹配日志位置是否是合适的，初始化rf.nextIndex[i] - 1
	PrevLogTerm  int     // 用于匹配日志的任期是否是合适的是，是否有冲突, 上一条对应的任期
	Entries      []Entry // 预计存储的日志（为空时就是心跳连接）
	LeaderCommit int     // leader’s commitIndex 指的是最后一个被大多数server复制的日志的index
}

// 论文图2中的reusults定义
type AppendEntriesReply struct {
	Term      int  // server可能有比leader更新的term
	Success   bool // server和leader的preLogIndex和PrevLogTerm都匹配才可以接受
	NextIndex int  // 发生conflict,reply传过来的index用来更新leader的nextIndex[i]
}

type InstallSnapshotArgs struct {
	Term              int    // leader's term
	LeaderId          int    //
	LastIncludedIndex int    // snapshot最后applied的日志下标
	LastIncludedTerm  int    // snapshot最后applied的任期
	Data              []byte // snapshot区块的原始字节流数据
	// Done              bool   // true if this is the last chunk
}

type InstallSnapshotReply struct {
	Term int
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

	rf.mu.Lock()
	// Your initialization code here (2A, 2B, 2C).
	// 2A
	rf.votedFor = NONE
	// rf.currentLeader = NONE
	rf.currentState = Follower
	rf.currentTerm = 0

	// 2B, 2C
	// 初始化的时候就加一个进入到log
	rf.log = []Entry{}
	rf.log = append(rf.log, Entry{})

	rf.applyCh = applyCh
	// rf.logIndex = 0
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0

	rf.mu.Unlock()
	// initialize from state persisted before a crash,读取之前persist的成员
	rf.readPersist(persister.ReadRaftState())

	if rf.lastIncludedIndex > 0 { // 读取完persistent之后需要更新一下lastApplied
		// 已经提交有snapshot
		rf.lastApplied = rf.lastIncludedIndex
	}

	// start ticker goroutine to start elections
	// go rf.ticker()
	go rf.electionTicker()
	go rf.appendTicker()
	go rf.committedTicker() // 更新chan的单独的gorouting

	return rf
}

// ----------------------------------------ticker----------------------------------------------------
// The ticker go routine starts a new election if this peer hasn't received heartsbeats recently.
// 2A
func (rf *Raft) electionTicker() {
	for rf.killed() == false {
		nowTime := time.Now()
		// sleep 一个范围内的随机时间
		// time.Sleep的参数对象需要是time.Duration，time.Millisecond就是一个time.Duration对象
		// rf.me是随机种子，让每个server不一样所以可以用server的编号
		time.Sleep(time.Duration(generateOverTime(int64(rf.me))) * time.Millisecond)
		rf.mu.Lock()
		//Timer如果小于sleep睡眠之前定义的时间，说明Timer没被更新为最新的时间，则发起选举
		if rf.electionTimer.Before(nowTime) && rf.currentState != Leader {
			// 开始选举
			rf.StartElection()
			rf.electionTimer = time.Now()
		}
		rf.mu.Unlock()
	}
}

// leader 定时发送更新heartbeat，其他surver接收并且更新日志
// 2A 2B 2C heartbeat，添加日志
func (rf *Raft) appendTicker() {
	for rf.killed() == false {
		// 每35ms发送一个heartbeat，同时更新log
		time.Sleep(HeartbeatSleep * time.Millisecond)
		rf.mu.Lock()
		if rf.currentState == Leader {
			rf.mu.Unlock()
			rf.leaderAppendEntries()
		} else {
			rf.mu.Unlock()
		}
	}
}

// 2B, goroutin不断检查是否可以发起新的提交
func (rf *Raft) committedTicker() {
	for rf.killed() == false {
		// 每15ms检查一次是否可以提交
		time.Sleep(AppliedSleep * time.Millisecond)
		rf.mu.Lock()
		// 已经更新过applied
		if rf.lastApplied >= rf.commitIndex {
			rf.mu.Unlock()
			continue
		}

		// 存储没有更新的一直到需要更新的ApplyMsg，最后一起加入到chan中
		messages := make([]ApplyMsg, 0)
		for rf.lastApplied < rf.commitIndex && rf.lastApplied < rf.realLastIndex() {
			// 一直到commitIndex，但不能超过最后一个，因为不是每个server都存了commitIndex
			rf.lastApplied++
			// 这里不是snapshot，所以SnapshotValid需要是false
			messages = append(messages, ApplyMsg{
				CommandValid:  true,
				Command:       rf.indexToEntry(rf.lastApplied).Data,
				CommandIndex:  rf.lastApplied,
				SnapshotValid: false, // 这里的提交不是snapshot
			})
		}
		rf.mu.Unlock()
		// 加入到chan中，提前解锁
		for _, message := range messages {
			rf.applyCh <- message
		}
	}
}

//
// ----------------------------------------2A-leader选举-------------------------------------------
//
func (rf *Raft) StartElection() {
	// ------------------------------------更新一下Candidate的状态-------------------------------
	// 自己发起选取，改变状态， 论文中server成为Candidates的要求
	rf.currentState = Candidate
	rf.votedFor = rf.me
	rf.currentTerm++
	rf.electionTimer = time.Now()
	// 保存要保存的东西,save Raft's persistent state to stable storage,
	rf.persist()
	// fmt.Println("[test]:Raft", rf.me, "starts a election")

	// ------------------------------------统计每个server的投票情况-----------------------------------------
	voteNum := 1 // 统计票数，自己给自己的一票
	// 遍历所有raft的server
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me { // 跳过Candidate
			continue
		}
		// 开启协程进行竞选
		go func(server int) {

			// -------------------定义发送和接收的PRC-----------------
			rf.mu.Lock()             // 创建发送和接收的内容需要上锁
			args := RequestVoteArgs{ // 投票要发的args
				rf.currentTerm,
				rf.me,
				rf.realLastIndex(),
				rf.realLastTerm(),
			}
			reply := RequestVoteReply{} // 投完票接收的reply
			rf.mu.Unlock()
			// 发送并获得reply，res接到是否发送成功
			// ----------------发送并根据返回RPC更新状态-----------------
			res := rf.sendRequestVote(server, &args, &reply) // raft向server请求选票回复
			if res {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				// 自身如果不是Candidate或者任期不符直接退出, args.Term小可能是因为发出args后rf更新了，这样args就无效了
				// 更新可能是已经产生了新的leader，或者旧的leader重新发过来heartbeat，无论哪种此时rf.currentState也会成为Follower
				if rf.currentState != Candidate || args.Term < rf.currentTerm {
					return
				}

				// 返回任期和当前任期不符,重置状态后退出,reply有更新的term,同样取消选举并且更新当下rf的状态
				if args.Term < reply.Term {
					if rf.currentTerm < reply.Term {
						rf.currentTerm = reply.Term
					}
					rf.currentState = Follower
					rf.votedFor = NONE
					rf.persist()
					return
				}

				// 收到选举票，统计票数
				if reply.VoteGranted && args.Term == rf.currentTerm {
					voteNum++ // 统计票数
					// 票数过半
					if voteNum > len(rf.peers)/2 {
						// 修改状态
						rf.currentState = Leader
						rf.votedFor = NONE
						rf.persist()

						// 更新leader管控的全组信息nextIndex和matchIndex
						rf.nextIndex = make([]int, len(rf.peers))
						for i := 0; i < len(rf.peers); i++ {
							rf.nextIndex[i] = rf.realLastIndex() + 1
						}
						rf.matchIndex = make([]int, len(rf.peers))
						rf.matchIndex[rf.me] = rf.realLastIndex()

						rf.electionTimer = time.Now()
						// fmt.Println("[test]:Raft", rf.me, "elected successfully")
						return
					}

				}
				return
			} // 已接受
		}(i) // 实参赋给server

	}
}

//
// example RequestVote RPC handler. 论文中的RequesVote RPC
// 在startElection中发送投票使用到, 这里的rf并不是上面的Candidate，是Candidate发送给其他server的rf
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
	if args.Term > rf.currentTerm { // 任期大于当前任期，当前任期迟了，更新当前rf状态
		rf.currentTerm = args.Term
		rf.currentState = Follower
		rf.votedFor = NONE
		rf.persist()
	}

	// 论文RequesVote RPC第二个要求之一：votedFor is null or candidateId才可以返回true
	// 投票先到先得，已经有人拿走了票
	if rf.votedFor != NONE && rf.votedFor != args.CandidateID {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	// 论文RequesVote RPC第二个要求之一：candidate’s log is at least as up-to-date as receiver’s log
	// 也就是args的log要比rf的log更新
	if !rf.UpToDate(args.LastLogIndex, args.LastLogTerm) {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	// 没啥问题可以投票给Candidate
	reply.VoteGranted = true
	reply.Term = rf.currentTerm // 没问题的情况下，reply.Term就是args.Term
	rf.votedFor = args.CandidateID
	rf.currentTerm = args.Term
	rf.electionTimer = time.Now()
	rf.persist()
	return
}

// ------------------------------------------2B,2C,2D-日志增量部分------------------------------
// leader定时发送heartbeat的操作
func (rf *Raft) leaderAppendEntries() {
	// 传入的rf是leader，其他的server更新
	for index := range rf.peers {
		if index == rf.me { // 跳过自己
			continue
		}
		// 每个server开启协程
		go func(server int) {
			rf.mu.Lock() // 这个Lock是用来单独创建heartbeatRPC的
			// if rf.currentState != Leader { // 之前判断过才进来，这里其实不用判断
			// 	rf.mu.Unlock()
			// 	return
			// }

			// ---------------------判断是否需要发送snapshot----------------------------------
			// server的日志小于leader的snapshot状态，就需要发送leader的snapshot
			if rf.nextIndex[server]-1 < rf.lastIncludedIndex {
				go rf.LeaderSendSnapshot(server)
				rf.mu.Unlock()
				return
			}

			// ------------------------定义发送的RPC部分-------------------------------------
			// 填充一个要发送的Entryargs
			prevLogIndex, prevLogTerm := rf.realPreLog(server)
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				LeaderCommit: rf.commitIndex,
				// 一致性检查的时候定义Entries
			}
			// args.Entries要根据nextIndex判断是否需要将之前的Entries一起补发
			// 论文中提到的一致性检查
			// If last log index ≥ nextIndex for a follower:
			// send AppendEntries RPC with log entries starting at nextIndex
			if rf.realLastIndex() >= rf.nextIndex[server] {
				entries := make([]Entry, 0)
				// 从nextIndex到最后一个，考虑snapshot,在log中不能直接用nextIndex
				// log中从nextIndex到最后是要去掉log之前的部分的长度
				entries = append(entries, rf.log[rf.nextIndex[server]-rf.lastIncludedIndex:]...)
				args.Entries = entries
			} else {
				args.Entries = []Entry{}
			}

			// 定义一个reply来接收返回的信息
			reply := AppendEntriesReply{}

			// 定义的阶段上锁，发送的阶段不上锁，
			// 并不是只有发送了一个之后才能发送另一个，只要定义的时候只定义一个就好了
			rf.mu.Unlock()
			// -------------------------发送并接收部分----------------------------------
			// 调用下面的func发送并且获得回复
			res := rf.sendAppendEntries(server, &args, &reply)

			// leader接到成功的回复之后，需要操作
			if res {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.currentState != Leader {
					return
				}
				// if rep
				// leader落后了，回头应该发现自己不是leader了，修改状态
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.electionTimer = time.Now() // 同时把收到的回复当作heartbeat
					rf.currentState = Follower
					rf.votedFor = NONE
					rf.persist()
					return
				}

				if reply.Success {
					// 更新commitIndex，[]matchIndex，[]nextIndex
					// 这几个参数只有leader才能修改
					rf.commitIndex = rf.lastIncludedIndex                         // commitIde最小的可能，下面还会更新
					rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries) // match比较激进， commit比较稳重
					rf.nextIndex[server] = rf.matchIndex[server] + 1

					// 一个个index遍历然后统计同步的peers数目，判断要不要提交这个
					// 也就是更新commitIndex
					for index := rf.realLastIndex(); index >= rf.lastIncludedIndex+1; index-- {
						// 从外向里，只要最外面的可以提交了，就不用管前面的了，前面的一定提交了
						num := 1 // 加上了自己
						for i := 0; i < len(rf.peers); i++ {
							if i == rf.me {
								continue
							}
							if rf.matchIndex[i] >= index {
								num++
							}
						}

						// 判断是不是过半了，过半了就可以更新并且break了
						if num >= len(rf.peers)/2+1 && rf.indexToTerm(index) == rf.currentTerm {
							rf.commitIndex = index
							break
						}
					}
				} else {
					if reply.NextIndex != -1 { // 有冲突，任期不同是-1
						rf.nextIndex[server] = reply.NextIndex
					}
				}
			}
		}(index)
	}
}

// 论文图2 AppenEntries RPC中有描述
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// defer fmt.Printf("[	AppendEntries--Return-Rf(%v) 	] arg:%+v, reply:%+v\n", rf.me, args, reply)
	// Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.NextIndex = -1
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	reply.Success = true // 没什么问题的话reply这么设置
	reply.Term = args.Term
	reply.NextIndex = -1
	// -----------------------------heartbeat--------------------------------------
	// 这里插入一个更新server状态的heartbeat的过程,要在上面那个的Term更新之后
	rf.currentTerm = args.Term // 假如有更新的term需要更新
	rf.currentState = Follower
	rf.votedFor = NONE
	rf.persist()
	rf.electionTimer = time.Now() // heartbeat来更新选举时间

	// ----------------------------------------------------------------------------
	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	// 自身的index比发送过来的prev还大，返回冲突的下标+1
	if rf.lastIncludedIndex > args.PrevLogIndex {
		reply.Success = false
		reply.NextIndex = rf.realLastIndex() + 1
		return
	}
	// 自身有缺失
	if rf.realLastIndex() < args.PrevLogIndex {
		reply.Success = false
		reply.NextIndex = rf.realLastIndex()
		return
	}

	// prevlogterm不同，走到这里说明index相同
	// If an existing entry conflicts with a new one
	// (same index but different terms),
	// delete the existing entry and all that follow it (§5.3)
	if args.PrevLogTerm != rf.indexToTerm(args.PrevLogIndex) {
		reply.Success = false
		tempTerm := rf.indexToTerm(args.PrevLogIndex)
		for index := args.PrevLogIndex; index >= rf.lastIncludedIndex; index-- {
			if rf.indexToTerm(index) != tempTerm {
				reply.NextIndex = index + 1
				break
			}
		}
		return
	}
	// server的log补齐，从匹配的[0, args.PrevLogIndex+1-rf.lastIncludedIndex]再加上之后的args.Rntri
	rf.log = append(rf.log[:args.PrevLogIndex+1-rf.lastIncludedIndex], args.Entries...)
	rf.persist()

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	// commitIndex取leaderCommit与last new entry最小值的原因是，虽然应该更新到leaderCommit，但是new entry的下标更小
	// 则说明日志不存在，更新commit的目的是为了applied log，这样会导致日志日志下标溢出
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.realLastIndex())
	}
	// 没啥问题正常返回
	// reply.Success = true
	// reply.Term = args.Term
	// reply.NextIndex = -1
	return

}

// ---------------------------------------2C-Persist部分--------------------------------------------
//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// 每一次涉及到persist的内容就需要save一下
// 将states编码为字节数组，然后传给persister方便存储
//
func (rf *Raft) persist() {
	data := rf.persistData()
	rf.persister.SaveRaftState(data)
}

// SaveStateAndSnapshot需要用到data
func (rf *Raft) persistData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	return data
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []Entry
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		fmt.Println("decode error!")
	} else {
		// 论文中要求的3个persistent
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		// snapshot需要的两个参数
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
	}
}

// ---------------------------------------------2D-snapshot日志收缩-----------------------------------------------------
//

func (rf *Raft) LeaderSendSnapshot(server int) {
	// ------------------------定义发送的RPC部分-------------------------------------
	rf.mu.Lock()
	args := InstallSnapshotArgs{ // 这部分是发送给server更新的
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(), // 主要用来persist的就是这部分字节流化的数据
	}
	reply := InstallSnapshotReply{}
	rf.mu.Unlock()

	// -------------------------发送RPC并处理返回结果---------------------------------
	res := rf.sendInstallSnapshot(server, &args, &reply)

	if res {
		rf.mu.Lock()
		// 自身如果不是leader或者任期不符直接退出
		if rf.currentState != Leader || args.Term != rf.currentTerm {
			rf.mu.Unlock()
			return
		}
		// 发现leader已经过期，更新状态并返回
		if args.Term < reply.Term {
			rf.currentState = Follower
			rf.votedFor = NONE
			rf.persist()
			rf.electionTimer = time.Now()
			rf.mu.Unlock()
			return
		}

		//
		rf.nextIndex[server] = args.LastIncludedIndex + 1
		rf.matchIndex[server] = args.LastIncludedIndex
		rf.mu.Unlock()
		return
	}

}

// InstallSnapshot 主函数, 这里的rf是上面的server
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		// rf.mu.Unlock()
		return
	}

	rf.currentTerm = args.Term // 可能需要更新一下rf的Term
	reply.Term = rf.currentTerm

	rf.currentState = Follower // snapshotRPC也可以当作heartbeat
	rf.votedFor = NONE
	rf.persist()
	rf.electionTimer = time.Now()
	// -----------------------------------------------------------------------
	// 和Snapshot一样的功能，只不过这里是leader控制的，Snapshot是service控制的
	// 自身的snapshot已经比传过来的要大了，就没有必要更新了
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		// rf.mu.Unlock()
		return
	}

	// 把index后到最后一个index对应的Entry装到新的[]log中
	newLog := make([]Entry, 0)
	newLog = append(newLog, Entry{})
	index := args.LastIncludedIndex
	for i := index + 1; i <= rf.realLastIndex(); i++ {
		newLog = append(newLog, rf.indexToEntry(i))
	}

	// 更新
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.log = newLog
	if index > rf.commitIndex { // snapshot apply了也需要更新这两个参数
		rf.commitIndex = index
	}
	if index > rf.lastApplied {
		rf.lastApplied = index
	}

	rf.persister.SaveStateAndSnapshot(rf.persistData(), args.Data)
	// 这里peer需要传递给Make()的applyCh一个ApplyMsg当peer意识到连续的日志被提交时
	// 这个时候SnapshotValid需要时true，其他的传到chan中的msg需要时false
	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	// rf.mu.Unlock() // 这里先解锁再传入chan可以快一些
	rf.applyCh <- msg
}

// the service says it has created a snapshot that has all info up to and including index.
// this means the service no longer needs the log through (and including) that index.
// Raft should now trim its log as much as possible.

// snapshot其实就是service对rf调用的，使rf节点更新自身的快照信息
// index代表是快照apply应用的index,而snapshot代表的是上层service传来的快照字节流，包括了Index之前的数据
// 这个函数的目的是把安装到快照里的日志抛弃，并安装快照数据，同时更新快照下标，
// 属于peers自身主动更新，与leader发送快照不冲突
func (rf *Raft) Snapshot(index int, snapshot []byte) { // 也就是丢掉index之前的log日志
	// Your code here (2D).
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index > rf.commitIndex || // 说明index还没有提交，不应该直接丢弃
		index <= rf.lastIncludedIndex { // 说明index已经被丢弃了
		return
	}

	// 把index后到最后一个index对应的Entry装到新的[]log中
	newLog := make([]Entry, 0)
	newLog = append(newLog, Entry{})
	for i := index + 1; i <= rf.realLastIndex(); i++ {
		newLog = append(newLog, rf.indexToEntry(i))
	}

	// 更新
	if index == rf.realLastIndex()+1 {
		rf.lastIncludedTerm = rf.realLastTerm()
	} else { // 不是最后一个index
		rf.lastIncludedTerm = rf.indexToTerm(index)
	}
	rf.lastIncludedIndex = index
	rf.log = newLog
	if index > rf.commitIndex { // snapshot apply了也需要更新这两个参数
		rf.commitIndex = index
	}
	if index > rf.lastApplied {
		rf.lastApplied = index
	}

	rf.persister.SaveStateAndSnapshot(rf.persistData(), snapshot)
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// 发送了快照，那么你发送的快照就要上传到applyCh，
	// 而同时你的appendEntries也需要进行上传日志，可能会导致冲突。
	// 只要在applied的时候做好同步，加上互斥锁，就可以避免这个问题，
	// 所以因此实验室中也提到这个api已经是废弃的
	return true
}
