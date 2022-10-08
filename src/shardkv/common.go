package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

type Err string

const (
	OK                  Err = "OK"
	ErrNoKey                = "ErrNoKey"            // 没有这个key
	ErrWrongGroup           = "ErrWrongGroup"       // 分片对应的gid不是shardKV的gid
	ErrWrongLeader          = "ErrWrongLeader"      // raft的leader错误
	ShardNotArrived         = "ShardNotArrived"     // 还没有这个分片
	ConfigNotArrived        = "ConfigNotArrived"    // 没有找到这个config配置信息
	ErrInconsistentData     = "ErrInconsistentData" // 数据不一致，start的时候从chan得到的id和传入的id不一致
	ErrOverTime             = "ErrOverTime"         // 超时
)

type Operation string

const (
	PutType         Operation = "Put"
	AppendType                = "Append"
	GetType                   = "Get"
	UpConfigType              = "UpConfig"
	AddShardType              = "AddShard"
	RemoveShardType           = "RemoveShard"
)

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    Operation // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  int64
	RequestId int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId  int64
	RequestId int
}

type GetReply struct {
	Err   Err
	Value string
}

type SendShardArg struct {
	LastAppliedRequestId map[int64]int // for receiver to update its state
	ShardId              int
	Shard                Shard // Shard to be sent
	ClientId             int64
	RequestId            int
}

type AddShardReply struct {
	Err Err
}
