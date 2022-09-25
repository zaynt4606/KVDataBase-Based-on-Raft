package kvraft

import (
	"crypto/rand"
	"math/big"
	mathrand "math/rand"

	// "time"

	"6.824/labrpc"
)

// client发送给raft服务器的RPC，(内容就是put，append，get的指令)
type Clerk struct {
	servers []*labrpc.ClientEnd // 每个server对应一个raft
	// You will have to modify this struct.
	leaderId int   // 返回的server告诉client哪个server是leader
	clientId int64 // client自己是哪个client
	seqId    int   // paper中说到的防止leader发生crash造成的重复发送op，给op编号排序

}

// 用来随机生成clientId
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand()                     // 随机生成clientId
	ck.leaderId = mathrand.Intn(len(servers)) // 随机一个server的id
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
// reply的err的类型再common中
// OK             = "OK"
// ErrNoKey       = "ErrNoKey"
// ErrWrongLeader = "ErrWrongLeader"
// keeps trying forever in the face of all other errors.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	ck.seqId++                                                        // op的id增加
	args := GetArgs{Key: key, ClientId: ck.clientId, SeqId: ck.seqId} // 定义要发送的RPCArgs
	server := ck.leaderId
	for {
		reply := GetReply{}
		ok := ck.servers[server].Call("KVServer.Get", &args, &reply) // 发送并接收返回的RPC
		if ok {
			if reply.Err == OK { // 得到了结果，返回结果
				ck.leaderId = server // 更新一下leaderId
				return reply.Value   // 返回Get结果
			} else if reply.Err == ErrNoKey { // 没有这个Key,
				ck.leaderId = server // leader正确但是
				return ""            // returns "" if the key does not exist.
			}
			// else if reply.Err == ErrWrongLeader // server不是leader,换一个server，可以直接和不ok合并起来
		}
		// 不ok的话说明server可能挂了，继续换一个server
		server = (server + 1) % len(ck.servers)
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.seqId++
	args := PutAppendArgs{Key: key, Value: value, Op: op, ClientId: ck.clientId, SeqId: ck.seqId}
	server := ck.leaderId
	for {
		reply := PutAppendReply{}
		ok := ck.servers[server].Call("KVServer.PutAppend", &args, &reply)
		if ok {
			if reply.Err == OK { // 得到了结果，返回结果
				ck.leaderId = server // 更新一下leaderId
				return
			}
			// 本来就是put和append，不存在ErrNoKey这个错误
			// else if reply.Err == ErrWrongLeader // 和不ok合并
		}
		server = (server + 1) % len(ck.servers)
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
