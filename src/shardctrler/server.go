package shardctrler

import (
	// "fmt"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const (
	JoinType  = "join"
	LeaveType = "leave"
	MoveType  = "move"
	QueryType = "query"

	JoinOverTime  = 100
	LeaveOverTime = 100
	MoveOverTime  = 100
	QueryOverTime = 100

	// InvalidGid all shards should be assigned to GID zero (an invalid GID).
	InvalidGid = 0
)

// ---------------------------------------------结构体定义及初始化---------------------------------------------
type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	configs []Config // indexed by config num
	// 第一个配置应该编号为零。
	// 它不应包含任何组，并且所有分片都应分配给 GID 零（无效的 GID）
	// type Config struct {     // gid是副本组编号
	// 	Num    int              // config number，对应的版本的配置号
	// 	Shards [NShards]int     // shard -> gid，切片对应的副本组编号
	// 	Groups map[int][]string // gid -> servers[]，副本组对应的组信息(服务器映射名称列表)
	// }

	// Your data here. lab描述中这里不需要persister
	seqMap map[int64]int   // clientId对应的最后一个seqId
	chMap  map[int]chan Op // raft的applyMsg传过来的，用来作为中间chan传递给client
}

type Op struct {
	// Your data here.
	OpType   string // rpc的四个类型，join, leave, move, query
	SeqId    int    // op的id
	ClientId int64
	// rpc需要用到的op
	JoinServers map[int][]string // JoinArgs要传的 new GID -> servers mappings
	LeaveGIDs   []int            // LeaveArgs要传的副本组id
	QueryNum    int              // QueryArgs要传的Num
	MoveShard   int              // MoveArgs要传的Shard
	MoveGID     int              // MoveArgs要传的GID
}

//
// 初始化一个server，开启一个goroutine
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	// Your code here.
	sc.seqMap = make(map[int64]int)
	sc.chMap = make(map[int]chan Op)

	go sc.ProcessApplyMsg()

	return sc
}

// ------------------------------------------------RPC接口----------------------------------------------------
func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	_, isleader := sc.rf.GetState()
	if !isleader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}

	op := Op{OpType: JoinType, SeqId: args.SeqId, ClientId: args.ClientId, JoinServers: args.Servers}
	index, _, _ := sc.rf.Start(op) // index, term, isLeader,index是最后一个log的index
	ch := sc.getCh(index)
	defer func() {
		sc.mu.Lock()
		delete(sc.chMap, index)
		sc.mu.Unlock()
	}()
	// 超时ticker
	ticker := time.NewTicker(JoinOverTime * time.Millisecond) // 设定接收的时间间隔，过了间隔就换leader
	defer ticker.Stop()                                       // 关闭ticker但是通道ticker.C没有关闭

	select {
	case replyOp := <-ch:
		if replyOp.ClientId != op.ClientId || replyOp.SeqId != op.SeqId {
			reply.Err = ErrWrongLeader // 有问题就换leader就完事儿，也不能干别的
			reply.WrongLeader = true
		} else { // 没问题
			reply.Err = OK
			return
		}
	case <-ticker.C:
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	_, isleader := sc.rf.GetState()
	if !isleader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}

	op := Op{OpType: LeaveType, SeqId: args.SeqId, ClientId: args.ClientId, LeaveGIDs: args.GIDs}
	index, _, _ := sc.rf.Start(op) // index, term, isLeader,index是最后一个log的index
	ch := sc.getCh(index)
	defer func() {
		sc.mu.Lock()
		delete(sc.chMap, index)
		sc.mu.Unlock()
	}()
	// 超时ticker
	ticker := time.NewTicker(LeaveOverTime * time.Millisecond) // 设定接收的时间间隔，过了间隔就换leader
	defer ticker.Stop()                                        // 关闭ticker但是通道ticker.C没有关闭
	select {
	case replyOp := <-ch:
		if replyOp.ClientId != op.ClientId || replyOp.SeqId != op.SeqId {
			reply.Err = ErrWrongLeader // 有问题就换leader就完事儿，也不能干别的
			reply.WrongLeader = true
		} else { // 没问题
			reply.Err = OK
			return
		}
	case <-ticker.C:
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
	}

}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	_, isleader := sc.rf.GetState()
	if !isleader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}

	op := Op{OpType: MoveType, SeqId: args.SeqId, ClientId: args.ClientId, MoveShard: args.Shard, MoveGID: args.GID}
	index, _, _ := sc.rf.Start(op) // index, term, isLeader,index是最后一个log的index
	ch := sc.getCh(index)
	defer func() {
		sc.mu.Lock()
		delete(sc.chMap, index)
		sc.mu.Unlock()
	}()
	// 超时ticker
	ticker := time.NewTicker(MoveOverTime * time.Millisecond) // 设定接收的时间间隔，过了间隔就换leader
	defer ticker.Stop()                                       // 关闭ticker但是通道ticker.C没有关闭
	select {
	case replyOp := <-ch:
		if replyOp.ClientId != op.ClientId || replyOp.SeqId != op.SeqId {
			reply.Err = ErrWrongLeader // 有问题就换leader就完事儿，也不能干别的
			reply.WrongLeader = true
		} else { // 没问题
			reply.Err = OK
			return
		}
	case <-ticker.C:
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	_, isleader := sc.rf.GetState()
	if !isleader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}

	op := Op{OpType: QueryType, SeqId: args.SeqId, ClientId: args.ClientId, QueryNum: args.Num}
	index, _, _ := sc.rf.Start(op) // index, term, isLeader,index是最后一个log的index
	ch := sc.getCh(index)
	defer func() {
		sc.mu.Lock()
		delete(sc.chMap, index)
		sc.mu.Unlock()
	}()
	// 超时ticker
	ticker := time.NewTicker(QueryOverTime * time.Millisecond) // 设定接收的时间间隔，过了间隔就换leader
	defer ticker.Stop()                                        // 关闭ticker但是通道ticker.C没有关闭

	select {
	case replyOp := <-ch:
		if replyOp.ClientId != op.ClientId || replyOp.SeqId != op.SeqId {
			reply.Err = ErrWrongLeader // 有问题就换leader就完事儿，也不能干别的
			reply.WrongLeader = true

		} else { // 没问题
			sc.mu.Lock()
			reply.Err = OK
			// sc.seqMap[op.ClientId] = op.SeqId // 其实在循环后面已经添加了，这里可以不用添加
			// 版本号不存在直接返回最新的也就是最后一个配置版本
			if op.QueryNum == -1 || op.QueryNum >= len(sc.configs) {
				reply.Config = sc.configs[len(sc.configs)-1]
			} else {
				reply.Config = sc.configs[op.QueryNum]
			}
			sc.mu.Unlock()
		}
	case <-ticker.C:
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
	}
}

// --------------------------------------循环goroutine---------------------------------------
func (sc *ShardCtrler) ProcessApplyMsg() {
	for {
		select {
		case msg := <-sc.applyCh:
			// 这里没有snapshot
			if msg.CommandValid {
				index := msg.CommandIndex
				// command的interface强转化为Op的结构，传入也是Op的类型
				// 可以通过command.(type)查看
				op := msg.Command.(Op)
				// seq是合理的,存储一下msg的真实操作
				if sc.seqNotUsed(op.ClientId, op.SeqId) {
					sc.mu.Lock()
					switch op.OpType {
					// 几个需要添加configs的type
					case JoinType:
						// fmt.Println("[Join] start Join")
						sc.configs = append(sc.configs, *sc.ProcessJoin(op.JoinServers))
					case LeaveType:
						sc.configs = append(sc.configs, *sc.ProcessLeave(op.LeaveGIDs))
					case MoveType:
						sc.configs = append(sc.configs, *sc.ProcessMove(op.MoveGID, op.MoveShard))
					}
					// 更新一下client对应的seq
					sc.seqMap[op.ClientId] = op.SeqId
					sc.mu.Unlock()
				}
				// 传到kv.chMap中index对应的chan中，Get需要从其中获取
				sc.getCh(index) <- op
			}
		}
	}
}

// --------------------------goroutine对不同RPC的几个操作------------------------------------

// JoinServers只是添加到Config的groups中，有新的gid对应的servers可以用，
// 然后需要shards更新新的映射
// The shardctrler should react by creating a new configuration that includes the new replica groups.
// The new configuration should divide the shards as evenly as possible among the full set of groups,
// and should move as few shards as possible to achieve that goal.
// The shardctrler should allow re-use of a GID if it's not part of the current configuration
// (i.e. a GID should be allowed to Join, then Leave, then Join again).
func (sc *ShardCtrler) ProcessJoin(joinServers map[int][]string) *Config {
	oldConfig := sc.configs[len(sc.configs)-1] // 最近一个版本的config
	newGroups := make(map[int][]string)
	// 取出最新配置的groups组进行填充
	for gid, serverList := range oldConfig.Groups {
		newGroups[gid] = serverList
	}
	// newGroups := oldConfig.Groups              // 最近一个版本的config对应的groups
	for gid, servers := range joinServers { // 添加上需要添加的servers映射
		newGroups[gid] = servers
	}

	// 对shards进行均衡处理
	// gid -> shards的数量,因为在groups中不一定就在shards中对gid有所分配，所以要先用groups初始化放入所有的gid
	gidShardNums := make(map[int]int)
	for gid := range newGroups {
		gidShardNums[gid] = 0
	}
	for _, gid := range oldConfig.Shards {
		if gid != 0 {
			gidShardNums[gid]++
		}
	}
	// 这么统计会有gid没有算进gidShardNums中，在groups里不一定在shards中
	// for _, gid := range oldConfig.Shards {
	// 	if gid != InvalidGid {
	// 		num, ok := gidShardNums[gid]
	// 		if ok {
	// 			gidShardNums[gid] = num + 1

	// 		} else {
	// 			gidShardNums[gid] = 1
	// 		}
	// 	}
	// }

	var newShards = [NShards]int{}
	if len(gidShardNums) > 0 {
		newShards = sc.balanceShards(gidShardNums, oldConfig.Shards)
	}

	// 构造返回Config
	newConfig := Config{
		Num:    oldConfig.Num + 1,
		Shards: newShards, // 这里需要做负载均衡
		Groups: newGroups,
	}
	return &newConfig

}

// type Config struct {     // gid是副本组编号
// 	Num    int              // config number，对应的版本的配置号
// 	Shards [NShards]int     // shard -> gid，切片对应的副本组编号
// 	Groups map[int][]string // gid -> servers[]，副本组对应的组信息(服务器映射名称列表)
// }
//
// The shardctrler should create a new configuration that does not include those groups,
// and that assigns those groups' shards to the remaining groups.
// The new configuration should divide the shards as evenly as possible among the groups,
// and should move as few shards as possible to achieve that goal.
func (sc *ShardCtrler) ProcessLeave(leaveGIDs []int) *Config {
	// 删掉LeaveGIDs中的group
	//
	oldConfig := sc.configs[len(sc.configs)-1] // 最近一个版本的config
	// newGroups := oldConfig.Groups              // 这是浅拷贝，会改变原来的值

	// 深拷贝 最近一个版本的config对应的groups
	newGroups := make(map[int][]string)
	// 取出最新配置的groups组进行填充
	for gid, serverList := range oldConfig.Groups {
		newGroups[gid] = serverList
	}

	leaveGidMap := make(map[int]bool) // 要去掉的leaveGIDs的map，辅助判断是不是要删掉
	// 删掉需要删除的gid
	for _, gid := range leaveGIDs {
		delete(newGroups, gid)  // 删掉要删的group
		leaveGidMap[gid] = true // 记录删掉的标号
	}

	// gid -> shards的数量
	gidShardNums := make(map[int]int)
	// 初始化存在与groups中的gid
	for gid := range newGroups {
		if !leaveGidMap[gid] {
			gidShardNums[gid] = 0
		}
	}
	// newshards中把对应要删除的gid的shard对应的值设为0
	temShards := oldConfig.Shards
	for shard, gid := range oldConfig.Shards {
		if gid != 0 {
			if leaveGidMap[gid] { // 删除掉的gid对应的shards要先设置为0
				temShards[shard] = 0
			} else { // 顺便统计没有删掉的gid对应的shards数目
				gidShardNums[gid]++
			}
		}
	}

	// 对shards进行均衡处理
	var newShards = [NShards]int{}
	if len(gidShardNums) > 0 {
		newShards = sc.balanceShards(gidShardNums, temShards) // 这里需要做负载均衡
	}

	// 构造返回config
	newConfig := Config{
		Num:    oldConfig.Num + 1,
		Shards: newShards, // 这里需要做负载均衡
		Groups: newGroups,
	}
	return &newConfig
}

// The shardctrler should create a new configuration in which the shard is assigned to the group.
// The purpose of `Move` is to allow us to test your software.
// A `Join` or `Leave` following a `Move` will likely un-do the `Move`,
// since `Join` and `Leave` re-balance.
func (sc *ShardCtrler) ProcessMove(MoveGID int, MoveShard int) *Config {
	oldConfig := sc.configs[len(sc.configs)-1] // 最近一个版本的config
	newGroups := oldConfig.Groups              // 最近一个版本的config对应的groups
	newShards := oldConfig.Shards
	newShards[MoveShard] = MoveGID // 添加要添加的新的shard -> gid 对
	// 构造返回config
	newConfig := Config{
		Num:    oldConfig.Num + 1,
		Shards: newShards, // 这里不做负载均衡，用来测试均衡之后这个操作失效
		Groups: newGroups, // groups不变
	}
	return &newConfig

}

// --------------------------------------------old_utils-----------------------------------------------
//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// 得到chMap中index对应的chan
func (sc *ShardCtrler) getCh(index int) chan Op {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	ch, exist := sc.chMap[index] // map返回对应的元素以及元素是否存在,存在就返回true
	// 要是不存在，就赋个空值并返回
	if !exist {
		sc.chMap[index] = make(chan Op, 1)
		ch = sc.chMap[index] // nil
	}
	return ch
}

// 把seq和clientId对应的最后一个seq对比，是不是已经发送过
func (sc *ShardCtrler) seqNotUsed(clientId int64, seqInd int) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	lastSeq, exist := sc.seqMap[clientId]
	if !exist { // 这个clientId找不到对应的seq,说明没有用过
		return true
	}
	// 新的seq要大于clientId对应的用过的最大的seq
	return lastSeq < seqInd
}

// --------------------------------------------new_utils--------------------------------------------------------

// 判断i是不是需要多映射一个余数remainder
// 没有整除多出来的部分从前往后一个个添加
func keepRemainder(length int, remainder int, i int) bool {
	if i < length-remainder {
		return true
	} else {
		return false
	}
}

// gidShardNums是gid -> shards num
// 得到按shards num从大到小排列的 []int数组
func sortGids(gidShardNums map[int]int) []int {
	gidNum := len(gidShardNums) // gid的数量
	sortGids := make([]int, 0)  // 定义一个新的slice
	for gid := range gidShardNums {
		sortGids = append(sortGids, gid)
	}
	// 对sortGid根据shards num来进行冒泡排序
	for i := 0; i < gidNum-1; i++ {
		for j := gidNum - 1; j > i; j-- { // 内层从后往前
			if gidShardNums[sortGids[j]] < gidShardNums[sortGids[j-1]] || // num多的换到前面
				(gidShardNums[sortGids[j]] == gidShardNums[sortGids[j-1]] && // 相同num
					sortGids[j] < sortGids[j-1]) { // gid值大的在前
				sortGids[j], sortGids[j-1] = sortGids[j-1], sortGids[j] // 交换值
			}
		}
	}
	return sortGids
}

// 得到均衡的shards
func (sc *ShardCtrler) balanceShards(gidShardNums map[int]int, oldShards [NShards]int) [NShards]int {
	// return [NShards]int{}
	gidNum := len(gidShardNums)          // gid的数量
	ave := NShards / gidNum              // 平均每个gid要映射的shard数目，还有余
	remainder := NShards % gidNum        // 不会被整除的余数
	sortedGids := sortGids(gidShardNums) // 得到按sahrds数量排序的gid数组

	// 第一遍遍历sortedGIds找到超过ave的gid并把其相应的shards都映射到0
	for i := 0; i < gidNum; i++ {
		shardsNum := ave                          // i对应的gid应该存的shards数目
		if !keepRemainder(gidNum, remainder, i) { // 需要多存一个余数就+1
			shardsNum++
		}
		// 超了
		if gidShardNums[sortedGids[i]] > shardsNum {
			temGid := sortedGids[i]                    // 要处理的gid
			temNum := gidShardNums[temGid] - shardsNum // 要去掉的shards数目
			for shard, gid := range oldShards {
				if temNum <= 0 { // 已经处理够数目了
					break
				}
				if gid == temGid { // 要操作的gid，把相应的shard对应到0
					oldShards[shard] = InvalidGid
					temNum--
				}
			}
			// 更新gid现在对应的shards数目
			gidShardNums[temGid] = shardsNum
		}
	}

	// 第二遍遍历,把不够的gid填满
	for i := 0; i < gidNum; i++ {
		shardsNum := ave                          // i对应的gid应该存的shards数目
		if !keepRemainder(gidNum, remainder, i) { // 需要多存一个余数就+1
			shardsNum++
		}
		// 不够的
		if gidShardNums[sortedGids[i]] < shardsNum {
			temGid := sortedGids[i]                    // 要处理的gid
			temNum := shardsNum - gidShardNums[temGid] // 要添加的shards数目
			for shard, gid := range oldShards {
				if temNum <= 0 { // 已经处理够数目了
					break
				}
				if gid == InvalidGid { // 要操作的gid，把相应的shard对应到0
					oldShards[shard] = temGid
					temNum--
				}
			}
			// 更新gid现在对应的shards数目
			gidShardNums[temGid] = shardsNum
		}
	}
	return oldShards
}
