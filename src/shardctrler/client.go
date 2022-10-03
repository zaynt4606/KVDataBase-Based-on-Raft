package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	mathrand "math/rand"
	"time"

	"6.824/labrpc"
)

// "职工",和lab3中的kvraft一样的作用
type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clientId int64
	seqId    int
	leaderId int
}

// 用来随机生成clientId
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// 初始化一个要发送的clerk
func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clientId = nrand()
	ck.leaderId = mathrand.Intn(len(servers))
	// Your code here.
	return ck
}

// 所有的发送和接收RPC都要对clerk的几个成员进行更新
func (ck *Clerk) Query(num int) Config {
	// Your code here.
	// 更新rpc编号
	ck.seqId++
	// 补充发送RPC的标记信息
	args := QueryArgs{
		Num:      num,
		ClientId: ck.clientId,
		SeqId:    ck.seqId,
	}

	// try each known server. 还是用lab3中的方式更换leader
	serverId := ck.leaderId
	for {
		var reply QueryReply // 换server的时候需要重新发送一个新的reply，所以在for里面定义
		ok := ck.servers[serverId].Call("ShardCtrler.Query", &args, &reply)
		if ok {
			if reply.Err == OK {
				ck.leaderId = serverId
				return reply.Config
				// 不经历经历下面的sleep直接continue
			} else if reply.Err == ErrWrongLeader {
				serverId = (serverId + 1) % len(ck.servers)
				continue
			}

		}
		// 不ok或者reply的Err是错误leaderid就换一个server继续发
		serverId = (serverId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	// Your code here.
	ck.seqId++
	args := JoinArgs{
		Servers:  servers,
		ClientId: ck.clientId,
		SeqId:    ck.seqId,
	}
	serverId := ck.leaderId

	for {
		var reply JoinReply // 换server的时候需要重新发送一个新的reply，所以在for里面定义
		ok := ck.servers[serverId].Call("ShardCtrler.Join", &args, &reply)
		if ok {
			if reply.Err == OK {
				ck.leaderId = serverId
				return
			} else if reply.Err == ErrWrongLeader {
				serverId = (serverId + 1) % len(ck.servers)
				continue
			}
		}
		// 不ok或者reply的Err是错误leaderid就换一个server继续发
		serverId = (serverId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	// Your code here.
	ck.seqId++
	args := LeaveArgs{
		GIDs:     gids,
		ClientId: ck.clientId,
		SeqId:    ck.seqId,
	}
	serverId := ck.leaderId

	for {
		var reply LeaveReply // 换server的时候需要重新发送一个新的reply，所以在for里面定义
		ok := ck.servers[serverId].Call("ShardCtrler.Leave", &args, &reply)
		if ok {
			if reply.Err == OK {
				ck.leaderId = serverId
				return
			} else if reply.Err == ErrWrongLeader {
				serverId = (serverId + 1) % len(ck.servers)
				continue
			}
		}
		// 不ok或者reply的Err是错误leaderid就换一个server继续发
		serverId = (serverId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.seqId++
	args := MoveArgs{
		Shard:    shard,
		GID:      gid,
		ClientId: ck.clientId,
		SeqId:    ck.seqId,
	}
	serverId := ck.leaderId

	for {
		var reply MoveReply // 换server的时候需要重新发送一个新的reply，所以在for里面定义
		ok := ck.servers[serverId].Call("ShardCtrler.Move", &args, &reply)
		if ok {
			if reply.Err == OK {
				ck.leaderId = serverId
				return
			} else if reply.Err == ErrWrongLeader {
				serverId = (serverId + 1) % len(ck.servers)
				continue
			}
		}
		// 不ok或者reply的Err是错误leaderid就换一个server继续发
		serverId = (serverId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}
