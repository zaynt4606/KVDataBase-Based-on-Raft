package shardkv

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

const (
	UpConfigLoopInterval = 100 * time.Millisecond // poll configuration period

	GetTimeout          = 500 * time.Millisecond
	AppOrPutTimeout     = 500 * time.Millisecond
	UpConfigTimeout     = 500 * time.Millisecond
	AddShardsTimeout    = 500 * time.Millisecond
	RemoveShardsTimeout = 500 * time.Millisecond
)

// --------------------------------结构体定义----------------------------------------
type Shard struct {
	KvMap     map[string]string
	ConfigNum int // shard对应的配置号
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	SeqId    int
	OpType   Operation
	Key      string
	Value    string
	UpConfig shardctrler.Config
	ShardId  int
	Shard    Shard
	SeqMap   map[int64]int
}

//
type OpReply struct {
	ClientId int64
	SeqId    int
	Err      Err
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.\
	dead          int32              // Kill用
	config        shardctrler.Config // 需要更新的最近的配置
	lastConfig    shardctrler.Config // 更新之前的最后一个版本的配置，用来对比
	seqMap        map[int64]int      // 用来看是不是重复发送
	chMap         map[int]chan OpReply
	shardsPersist []Shard            // shardId -> Shard
	sck           *shardctrler.Clerk // client 用来使用shard master
}

// ----------------------------------启动服务start-----------------------------------
//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	kv.shardsPersist = make([]Shard, shardctrler.NShards)

	kv.seqMap = make(map[int64]int)

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.masters)
	kv.sck = shardctrler.MakeClerk(kv.ctrlers)
	kv.chMap = make(map[int]chan OpReply)

	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.DecodeSnapshot(snapshot)
	}

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	// 开两个线程，一个用来接收信息，一个用来更新配置
	go kv.ProcessApplyMsg()
	go kv.ProcessConfigDectect()
	return kv
}

// -------------------------------------RPC-------------------------------------------
// type GetArgs struct {
// 	Key string
// 	ClientId  int64
// 	RequestId int
// }
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	shardId := key2shard(args.Key)
	kv.mu.Lock()
	if kv.config.Shards[shardId] != kv.gid {
		reply.Err = ErrWrongGroup
	} else if kv.shardsPersist[shardId].KvMap == nil { // shard的内容还是空说明shard还没传到
		reply.Err = ShardNotArrived
	}
	kv.mu.Unlock()
	if reply.Err == ErrWrongGroup || reply.Err == ShardNotArrived {
		return
	}
	// 发送一个get的command
	command := Op{
		OpType:   GetType,
		ClientId: args.ClientId,
		SeqId:    args.RequestId,
		Key:      args.Key,
	}
	// start ROC出现问题直接返回err
	err := kv.startCommand(command, GetTimeout)
	if err != OK {
		reply.Err = err
		return
	}
	// 给reply的value赋值
	kv.mu.Lock()
	// 赋值前还是要判断一下存在与否
	if kv.config.Shards[shardId] != kv.gid {
		reply.Err = ErrWrongGroup
	} else if kv.shardsPersist[shardId].KvMap == nil {
		reply.Err = ShardNotArrived
	} else {
		reply.Err = OK
		reply.Value = kv.shardsPersist[shardId].KvMap[args.Key]
	}
	kv.mu.Unlock()
	return

}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	shardId := key2shard(args.Key)
	kv.mu.Lock()
	if kv.config.Shards[shardId] != kv.gid {
		reply.Err = ErrWrongGroup
	} else if kv.shardsPersist[shardId].KvMap == nil {
		reply.Err = ShardNotArrived
	}
	kv.mu.Unlock()
	if reply.Err == ErrWrongGroup || reply.Err == ShardNotArrived {
		return
	}
	command := Op{
		OpType:   args.Op,
		ClientId: args.ClientId,
		SeqId:    args.RequestId,
		Key:      args.Key,
		Value:    args.Value,
	}
	reply.Err = kv.startCommand(command, AppOrPutTimeout)
	return
}

// 把shard送到目标
func (kv *ShardKV) AddShard(args *SendShardArg, reply *AddShardReply) {
	command := Op{
		OpType:   AddShardType,
		ClientId: args.ClientId,
		SeqId:    args.RequestId,
		ShardId:  args.ShardId,
		Shard:    args.Shard,
		SeqMap:   args.LastAppliedRequestId,
	}
	reply.Err = kv.startCommand(command, AddShardsTimeout)
	return
}

// -----------------------------processApplyMsg中的操作的处理------------------------
// 更新最新的config的handler
func (kv *ShardKV) upConfig(op Op) {
	curConfig := kv.config
	newConfig := op.UpConfig
	// kv当下的配置号更新直接返回
	if curConfig.Num >= newConfig.Num {
		return
	}
	for shard, gid := range newConfig.Shards {
		// 新配置的gid与当前配置一致并且没有分片
		if gid == kv.gid && curConfig.Shards[shard] == 0 {
			kv.shardsPersist[shard].KvMap = make(map[string]string)
			kv.shardsPersist[shard].ConfigNum = newConfig.Num
		}
	}
	kv.lastConfig = curConfig
	kv.config = newConfig
}

// 把op的shard添加到kv中，并且更新seq
func (kv *ShardKV) addShard(op Op) {
	// 过期或者已经已经加过了
	if kv.shardsPersist[op.ShardId].KvMap != nil || op.Shard.ConfigNum < kv.config.Num {
		return
	}
	// 因为直接=是浅拷贝，所以需要重新构造一个同样的shard给shardsPersist
	kv.shardsPersist[op.ShardId] = kv.cloneShard(op.Shard.ConfigNum, op.Shard.KvMap)
	for clientId, seqId := range op.SeqMap {
		// kv中的clientId对应的seqid，不存在或者更小，需要更新
		if r, ok := kv.seqMap[clientId]; !ok || r < seqId {
			kv.seqMap[clientId] = seqId
		}
	}
}

func (kv *ShardKV) removeShard(op Op) {
	// op的seq落后，直接返回
	if op.SeqId < kv.config.Num {
		return
	}
	kv.shardsPersist[op.ShardId].KvMap = nil
	kv.shardsPersist[op.ShardId].ConfigNum = op.SeqId // 更新配置号
}

// ---------------------------------------Goroutine-------------------------------
//
// 接收信息
func (kv *ShardKV) ProcessApplyMsg() {
	for {
		if kv.killed() {
			return
		}
		select {

		case msg := <-kv.applyCh:

			if msg.CommandValid == true {
				kv.mu.Lock()
				op := msg.Command.(Op)
				reply := OpReply{
					ClientId: op.ClientId,
					SeqId:    op.SeqId,
					Err:      OK,
				}

				if op.OpType == PutType || op.OpType == GetType || op.OpType == AppendType {

					shardId := key2shard(op.Key)

					//
					if kv.config.Shards[shardId] != kv.gid {
						reply.Err = ErrWrongGroup
					} else if kv.shardsPersist[shardId].KvMap == nil {
						// 如果应该存在的切片没有数据那么这个切片就还没到达
						reply.Err = ShardNotArrived
					} else {
						// seq没有重复
						if kv.seqNotUsed(op.ClientId, op.SeqId) {
							kv.seqMap[op.ClientId] = op.SeqId
							switch op.OpType {
							case PutType:
								kv.shardsPersist[shardId].KvMap[op.Key] = op.Value
							case AppendType:
								kv.shardsPersist[shardId].KvMap[op.Key] += op.Value
							case GetType:
								// 如果是Get都不用做
							default:
								log.Fatalf("invalid command type: %v.", op.OpType)
							}
						}
					}
				} else { // 其他的optype类型
					// request from server of other group
					switch op.OpType {

					case UpConfigType:
						kv.upConfig(op)
					case AddShardType:
						// 如果配置号比op的SeqId还低说明不是最新的配置
						if kv.config.Num < op.SeqId {
							reply.Err = ConfigNotArrived
							break
						}
						kv.addShard(op)
					case RemoveShardType:
						// remove operation is from previous UpConfig
						kv.removeShard(op)
					default:
						log.Fatalf("invalid command type: %v.", op.OpType)
					}
				}

				// 如果需要snapshot，且超出其stateSize
				if kv.maxraftstate != -1 && kv.rf.GetRaftState() > kv.maxraftstate {
					snapshot := kv.PersistSnapshot()
					kv.rf.Snapshot(msg.CommandIndex, snapshot)
				}

				ch := kv.getCh(msg.CommandIndex)
				ch <- reply
				kv.mu.Unlock()

			}

			if msg.SnapshotValid == true {
				if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
					// 读取快照的数据
					kv.mu.Lock()
					kv.DecodeSnapshot(msg.Snapshot)
					kv.mu.Unlock()
				}
				continue
			}

		}
	}
}

//
// 检测环境配置
func (kv *ShardKV) ProcessConfigDectect() {
	kv.mu.Lock()

	curConfig := kv.config
	rf := kv.rf
	kv.mu.Unlock()

	for !kv.killed() {
		// only leader needs to deal with configuration tasks
		if _, isLeader := rf.GetState(); !isLeader {
			time.Sleep(UpConfigLoopInterval)
			continue
		}
		kv.mu.Lock()

		// 判断是否把不属于自己的部分给分给别人了
		if !kv.allSent() {
			// 复制一份kv.seqMap
			SeqMap := make(map[int64]int)
			for k, v := range kv.seqMap {
				SeqMap[k] = v
			}
			for shardId, gid := range kv.lastConfig.Shards {

				// 将最新配置里不属于自己的分片分给别人
				if gid == kv.gid &&
					kv.config.Shards[shardId] != kv.gid &&
					kv.shardsPersist[shardId].ConfigNum < kv.config.Num {

					sendDate := kv.cloneShard(kv.config.Num, kv.shardsPersist[shardId].KvMap)

					args := SendShardArg{
						LastAppliedRequestId: SeqMap,
						ShardId:              shardId,
						Shard:                sendDate,
						ClientId:             int64(gid),
						RequestId:            kv.config.Num,
					}

					// shardId -> gid -> server names
					serversList := kv.config.Groups[kv.config.Shards[shardId]]
					servers := make([]*labrpc.ClientEnd, len(serversList))
					for i, name := range serversList {
						servers[i] = kv.make_end(name)
					}

					// 开启协程对每个客户端发送切片(这里发送的应是别的组别，自身的共识组需要raft进行状态修改）
					go func(servers []*labrpc.ClientEnd, args *SendShardArg) {
						index := 0
						start := time.Now()
						for {
							var reply AddShardReply
							// 对自己的共识组内进行add
							ok := servers[index].Call("ShardKV.AddShard", args, &reply)

							// 如果给予切片成功，或者时间超时，这两种情况都需要进行GC掉不属于自己的切片
							if ok && reply.Err == OK || time.Now().Sub(start) >= 2*time.Second {

								// 如果成功
								kv.mu.Lock()
								command := Op{
									OpType:   RemoveShardType,
									ClientId: int64(kv.gid),
									SeqId:    kv.config.Num,
									ShardId:  args.ShardId,
								}
								kv.mu.Unlock()
								kv.startCommand(command, RemoveShardsTimeout)
								break
							}
							index = (index + 1) % len(servers)
							if index == 0 {
								time.Sleep(UpConfigLoopInterval)
							}
						}
					}(servers, &args)
				}
			}
			kv.mu.Unlock()
			time.Sleep(UpConfigLoopInterval)
			continue
		}
		if !kv.allReceived() {
			kv.mu.Unlock()
			time.Sleep(UpConfigLoopInterval)
			continue
		}

		// current configuration is configured, poll for the next configuration
		curConfig = kv.config
		sck := kv.sck
		kv.mu.Unlock()

		newConfig := sck.Query(curConfig.Num + 1)
		if newConfig.Num != curConfig.Num+1 {
			time.Sleep(UpConfigLoopInterval)
			continue
		}

		command := Op{
			OpType:   UpConfigType,
			ClientId: int64(kv.gid),
			SeqId:    newConfig.Num,
			UpConfig: newConfig,
		}
		kv.startCommand(command, UpConfigTimeout)
	}
}

// ---------------------------------------Persist----------------------------------
func (kv *ShardKV) PersistSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(kv.shardsPersist)
	err = e.Encode(kv.seqMap)
	err = e.Encode(kv.maxraftstate)
	err = e.Encode(kv.config)
	err = e.Encode(kv.lastConfig)
	if err != nil {
		log.Fatalf("[%d-%d] fails to take snapshot.", kv.gid, kv.me)
	}
	data := w.Bytes()
	return data
}

func (kv *ShardKV) DecodeSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var shardsPersist []Shard
	var SeqMap map[int64]int
	var MaxRaftState int
	var Config, LastConfig shardctrler.Config

	if d.Decode(&shardsPersist) != nil ||
		d.Decode(&SeqMap) != nil ||
		d.Decode(&MaxRaftState) != nil ||
		d.Decode(&Config) != nil ||
		d.Decode(&LastConfig) != nil {
		log.Fatalf("[Server(%v)] Failed to decode snapshot !!!", kv.me)
	} else {
		kv.shardsPersist = shardsPersist
		kv.seqMap = SeqMap
		kv.maxraftstate = MaxRaftState
		kv.config = Config
		kv.lastConfig = LastConfig
	}
}

// ---------------------------------------Utils-------------------------------------

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead, 1) // (addr, val)将val值存储到前面的addr
	kv.rf.Kill()
}

func (kv *ShardKV) killed() bool {
	// 读取其地址中的值判断是不是上面赋的值，来判断server是不是上面赋的值
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// 得到chMap中index对应的chan
func (kv *ShardKV) getCh(index int) chan OpReply {
	ch, exist := kv.chMap[index] // map返回对应的元素以及元素是否存在,存在就返回true
	// 要是不存在，就赋个空值并返回
	if !exist {
		kv.chMap[index] = make(chan OpReply, 1)
		ch = kv.chMap[index] // nil
	}
	return ch
}

// 把seq和clientId对应的最后一个seq对比，是不是已经发送过
func (kv *ShardKV) seqNotUsed(clientId int64, seqInd int) bool {
	lastSeq, exist := kv.seqMap[clientId]
	if !exist { // 这个clientId找不到对应的seq,说明没有用过
		return true
	}
	// 新的seq要大于clientId对应的用过的最大的seq
	return lastSeq < seqInd
}

// 把kvraft中的RPC整合成一个srartCommand
func (kv *ShardKV) startCommand(command Op, timeoutPeriod time.Duration) Err {
	kv.mu.Lock()
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		kv.mu.Unlock()
		return ErrWrongLeader
	}

	ch := kv.getCh(index)
	kv.mu.Unlock()

	ticker := time.NewTicker(timeoutPeriod)
	defer ticker.Stop()

	select {
	case re := <-ch:
		kv.mu.Lock()
		delete(kv.chMap, index)
		if re.SeqId != command.SeqId || re.ClientId != command.ClientId {
			// One way to do this is for the server to detect that it has lost leadership,
			// by noticing that a different request has appeared at the index returned by Start()
			kv.mu.Unlock()
			return ErrInconsistentData
		}
		kv.mu.Unlock()
		return re.Err

	case <-ticker.C:
		return ErrOverTime
	}
}

// 创建一个shard
func (kv *ShardKV) cloneShard(ConfigNum int, KvMap map[string]string) Shard {

	migrateShard := Shard{
		KvMap:     make(map[string]string),
		ConfigNum: ConfigNum,
	}

	for k, v := range KvMap {
		migrateShard.KvMap[k] = v
	}

	return migrateShard
}

// 已经全部发送
func (kv *ShardKV) allSent() bool {
	for shard, gid := range kv.lastConfig.Shards {
		// 如果当前配置中分片中的信息不匹配，且持久化中的配置号更小，说明还未发送
		if gid == kv.gid && kv.config.Shards[shard] != kv.gid &&
			kv.shardsPersist[shard].ConfigNum < kv.config.Num {
			return false
		}
	}
	return true
}

// 已经全部接收
func (kv *ShardKV) allReceived() bool {
	for shard, gid := range kv.lastConfig.Shards {

		// 判断切片是否都收到了
		if gid != kv.gid && kv.config.Shards[shard] == kv.gid &&
			kv.shardsPersist[shard].ConfigNum < kv.config.Num {
			return false
		}
	}
	return true
}
