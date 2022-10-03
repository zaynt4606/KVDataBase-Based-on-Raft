package shardctrler

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.这里定义有10个切片
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
// num是配置的编号
// 一个切片对应一个组，一个组对应多个切片
type Config struct {
	Num    int              // config number，对应的版本的配置号
	Shards [10]int          // shard -> gid，切片对应的副本组编号
	Groups map[int][]string // gid -> servers[]，副本组对应的组信息(服务器映射名称列表)
}

const (
	// 加几个Err的类型
	OK             = "OK"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

//
//
// 对所有的发送RPC都加上clientId和发送的op编号seqId
type JoinArgs struct {
	Servers  map[int][]string // new GID -> servers mappings
	ClientId int64
	SeqId    int
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs     []int
	ClientId int64
	SeqId    int
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard int
	GID   int

	ClientId int64
	SeqId    int
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num      int // desired config number
	ClientId int64
	SeqId    int
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}
