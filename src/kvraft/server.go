package kvraft

import (
	// "fmt"
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,字段名称必须以大写字母开头，
	// otherwise RPC will break.
	SeqId    int // op的id
	Key      string
	Value    string
	ClientId int64
	OpType   string // Op是哪种类型，put，append，get
	Index    int    // raft传过来的index
}

type KVServer struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32 // set by Kill()
	maxraftstate int   // snapshot if log grows this big

	// Your definitions here.
	seqMap map[int64]int // 对client来说同一个seq只执行一次,clientId对应的是发送的最后一个seqInd
	// 这里的chan是从上面的raft的applyCh接过来的，主体是raft.ApplyMsg，需要转化过来
	chMap map[int]chan Op // raft的applyCh传过来的 command index - chan(Op) // ApplyMsg中的commandIndex
	// 数据是这里的，但是kvserver不能直接从这里拿
	// 要通过raft返回的applyCh中按顺序拿
	kvPersist         map[string]string // 持久化存储k/v,
	lastIncludedIndex int
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.

// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.seqMap = make(map[int64]int)
	kv.kvPersist = make(map[string]string)
	kv.chMap = make(map[int]chan Op)
	kv.lastIncludedIndex = -1
	// 恢复持久化部分 []byte
	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 && snapshot != nil {
		kv.readSnapshotPersist(snapshot)
	}
	// start goroutines
	go kv.ProcessApplyMsg()
	return kv
}

// -------------------------------------------------RPC---------------------------------------------------------
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// server进程没了，发送args到下一个server吧
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	// 不是leader也是返回
	_, isleader := kv.rf.GetState()
	if !isleader {
		reply.Err = ErrWrongLeader
		return
	}
	// handlers should enter an Op in the Raft log using Start()
	// Op中的index是raft赋值的，应该和start返回的index相同
	op := Op{SeqId: args.SeqId, Key: args.Key, ClientId: args.ClientId, OpType: "Get"}
	// fmt.Printf("[ ----Server[%v]----] : send a Get,op is :%+v \n", kv.me, op)
	index, _, _ := kv.rf.Start(op) // index, term, isLeader,index是最后一个log的index

	ch := kv.getCh(index) // 从chMap中得到的就是raft的applyCh传过来的
	defer func() {        // 删除map中op的index对应的chan
		kv.mu.Lock()
		delete(kv.chMap, op.Index) // 这个和上面的index正常情况下相同，
		kv.mu.Unlock()
	}()

	// 这个ticker也是在这个时间段之后才有的
	ticker := time.NewTicker(100 * time.Millisecond) // 设定接收的时间间隔，过了间隔就换leader
	defer ticker.Stop()                              // 关闭ticker但是通道ticker.C没有关闭
	select {
	// 用replyOp接到
	case replyOp := <-ch:
		// 确认接到的reply是不是传过去的seq，不是重复传过的
		if replyOp.ClientId != op.ClientId || replyOp.SeqId != op.SeqId {
			reply.Err = ErrWrongLeader // 有问题就换leader就完事儿，也不能干别的
		} else { // 没问题
			reply.Err = OK
			kv.mu.Lock()
			reply.Value = kv.kvPersist[op.Key]
			kv.mu.Unlock()
			return
		}
	case <-ticker.C: // 上面的时间间隔内没有接到ch，换一个leader了就
		reply.Err = ErrWrongLeader
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// 和get类似，只要改reply那部分即可
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	// 不是leader也是返回
	_, isleader := kv.rf.GetState()
	if !isleader {
		reply.Err = ErrWrongLeader
		return
	}
	// handlers should enter an Op in the Raft log using Start()
	// Op中的index是raft赋值的，应该和start返回的index相同
	op := Op{SeqId: args.SeqId, ClientId: args.ClientId, Key: args.Key, Value: args.Value, OpType: args.Op}
	// fmt.Printf("[ ----Server[%v]----] : send a Get,op is :%+v \n", kv.me, op)
	index, _, _ := kv.rf.Start(op) // index, term, isLeader,index是最后一个log的index

	ch := kv.getCh(index) // 从chMap中得到的就是raft的applyCh传过来的
	defer func() {        // 删除map中op的index对应的chan
		kv.mu.Lock()
		delete(kv.chMap, op.Index) // 这个和上面的index正常情况下相同，
		kv.mu.Unlock()
	}()

	// 这个ticker也是在这个时间段之后才有的
	ticker := time.NewTicker(100 * time.Millisecond) // 设定接收的时间间隔，过了间隔就换leader
	defer ticker.Stop()                              // 关闭ticker但是通道ticker.C没有关闭
	select {
	// 用replyOp接到
	case replyOp := <-ch:
		// 确认接到的reply是不是传过去的seq，不是重复传过的
		if replyOp.ClientId != op.ClientId || replyOp.SeqId != op.SeqId {
			reply.Err = ErrWrongLeader // 有问题就换leader就完事儿，也不能干别的
		} else { // 没问题
			reply.Err = OK
			return
		}
	case <-ticker.C: // 上面的时间间隔内没有接到ch，换一个leader了就
		reply.Err = ErrWrongLeader
	}

}

// --------------------------------------------------process applyMsg--------------------------------------------
// type ApplyMsg struct {
// 	CommandValid bool
// 	Command      interface{}
// 	CommandIndex int

// 	// For 2D:
// 	SnapshotValid bool   // 是不是snapshot的msg
// 	Snapshot      []byte // 存储perisit的内容
// 	SnapshotTerm  int    // 下面index对应的term
// 	SnapshotIndex int    // raft用lastIncludedIndex来更新
// }

// goroutine不断把chan循环传递给chMap,
func (kv *KVServer) ProcessApplyMsg() {
	for {
		// 如果server不掉线就一直进行这个goroutine
		if kv.killed() {
			return
		}

		select {
		case msg := <-kv.applyCh:
			// msg是command的内容，和下面的snaoshot的msg
			if msg.CommandValid {
				index := msg.CommandIndex
				// 已经存储了传过来的snapshot
				if index <= kv.lastIncludedIndex {
					return
				}
				// command的interface强转化为Op的结构，传入也是Op的类型
				// 可以通过command.(type)查看
				op := msg.Command.(Op)
				// seq是合理的,存储一下msg的真实操作
				if kv.seqNotUsed(op.ClientId, op.SeqId) {
					kv.mu.Lock()
					switch op.OpType {
					case "Put":
						kv.kvPersist[op.Key] = op.Value
					case "Append":
						// 这里就直接作为string加到之前的string上了
						kv.kvPersist[op.Key] += op.Value
					}
					// 更新一下client对应的seq
					kv.seqMap[op.ClientId] = op.SeqId
					kv.mu.Unlock()
				}
				// 传到kv.chMap中index对应的chan中，Get需要从其中获取
				kv.getCh(index) <- op

				// if maxraftstate is -1, you don't need to snapshot.
				// persister保存的state数据的长度
				if kv.maxraftstate != -1 && kv.rf.GetRaftState() > kv.maxraftstate {
					snapshot := kv.PersisterSnapshot()
					kv.rf.Snapshot(index, snapshot)
				}
			}

			// msg是snapshot的msg,server执行snapshot
			if msg.SnapshotValid {
				kv.mu.Lock()
				// 这个函数直接返回true，所以是直接执行
				if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
					kv.readSnapshotPersist(msg.Snapshot)
					kv.lastIncludedIndex = msg.SnapshotIndex // 更新一下snapshot的index
				}
				kv.mu.Unlock()
			}
		}
	}
}

// ---------------------------------------------------persister---------------------------------------------------
func (kv *KVServer) readSnapshotPersist(snapshot []byte) {
	// 判断过snapshot大于0才会进入到这个函数
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var kvPersist map[string]string
	var seqMap map[int64]int
	if d.Decode(&kvPersist) != nil ||
		d.Decode(&seqMap) != nil {
		fmt.Println("decode error")
	} else {
		kv.kvPersist = kvPersist
		kv.seqMap = seqMap
	}
}

func (kv *KVServer) PersisterSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvPersist)
	e.Encode(kv.seqMap)
	data := w.Bytes()
	return data
}

// ------------------------------------------------------Util-----------------------------------------------------
// the tester calls Kill() when a KVServer instance won't be needed again.
// for your convenience, we supply code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// 得到chMap中index对应的chan
func (kv *KVServer) getCh(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, exist := kv.chMap[index] // map返回对应的元素以及元素是否存在,存在就返回true
	// 要是不存在，就赋个空值并返回
	if !exist {
		kv.chMap[index] = make(chan Op, 1)
		ch = kv.chMap[index] // nil
	}
	return ch
}

// 把seq和clientId对应的最后一个seq对比，是不是已经发送过
func (kv *KVServer) seqNotUsed(clientId int64, seqInd int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	lastSeq, exist := kv.seqMap[clientId]
	if !exist { // 这个clientId找不到对应的seq,说明没有用过
		return true
	}
	// 新的seq要大于clientId对应的用过的最大的seq
	return lastSeq < seqInd
}
