package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	leader          int
	lastCompletedId int64
	n               int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.n = len(servers)
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{nrand(), num, ck.lastCompletedId}
	// Your code here.
	//fmt.Printf("start query\n")
	for {
		// try each known server.
		for i := 0; i < ck.n; i++ {
			servIdx := (ck.leader + i) % ck.n
			srv := ck.servers[servIdx]
			var reply QueryReply
			unreliableRequest(srv, "ShardCtrler.Query", args, &reply)
			if reply.Err == OK {
				ck.leader = servIdx
				ck.lastCompletedId = args.Id
				//fmt.Printf("end query\n")
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	groupOrder := []int{}
	for group := range servers {
		groupOrder = append(groupOrder, group)
	}
	args := &JoinArgs{nrand(), groupOrder, servers, ck.lastCompletedId}
	// Your code here.
	//fmt.Printf("start join\n")
	for {
		// try each known server.
		for i := 0; i < ck.n; i++ {
			servIdx := (ck.leader + i) % ck.n
			srv := ck.servers[servIdx]
			var reply JoinReply
			unreliableRequest(srv, "ShardCtrler.Join", args, &reply)
			if reply.Err == OK {
				ck.leader = servIdx
				ck.lastCompletedId = args.Id
				//fmt.Printf("end join\n")
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{nrand(), gids, ck.lastCompletedId}
	// Your code here.
	//fmt.Printf("start leave\n")
	for {
		// try each known server.
		for i := 0; i < ck.n; i++ {
			servIdx := (ck.leader + i) % ck.n
			srv := ck.servers[servIdx]
			var reply LeaveReply
			//srv.Call("ShardCtrler.Leave", args, &reply)
			unreliableRequest(srv, "ShardCtrler.Leave", args, &reply)
			if reply.Err == OK {
				ck.leader = servIdx
				ck.lastCompletedId = args.Id
				//fmt.Printf("endleave\n")
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{nrand(), shard, gid, ck.lastCompletedId}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	//fmt.Printf("start move\n")
	for {
		// try each known server.
		for i := 0; i < ck.n; i++ {
			servIdx := (ck.leader + i) % ck.n
			srv := ck.servers[servIdx]
			var reply LeaveReply
			unreliableRequest(srv, "ShardCtrler.Move", args, &reply)
			if reply.Err == OK {
				ck.leader = servIdx
				ck.lastCompletedId = args.Id
				//fmt.Printf("end move\n")
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func unreliableRequest(srv *labrpc.ClientEnd, svcMeth string, args interface{}, reply interface{}) {
	done := make(chan bool)
	timeOut := time.After(time.Second)
	go func() {
		done <- srv.Call(svcMeth, args, reply)
	}()
	select {
	case <-done:
	case <-timeOut:
		go func() {
			<-done
		}()
	}
}
