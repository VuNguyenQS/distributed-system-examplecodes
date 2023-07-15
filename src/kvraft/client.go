package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader          int
	lastCompletedId int64
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
	// You'll have to add code here.
	return ck
}

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
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{nrand(), key, ck.lastCompletedId}
	for i := 0; ; i++ {
		// Choose randomly other servers
		reply := GetReply{}
		done := make(chan bool)
		timeout := time.After(time.Second)
		go func() {
			done <- ck.servers[(ck.leader+i)%len(ck.servers)].Call("KVServer.Get", &args, &reply)
		}()

		select {
		case <-done:
			if reply.Err == OK {
				ck.leader = (ck.leader + i) % len(ck.servers)
				ck.lastCompletedId = args.Id
				return reply.Value
			}
		case <-timeout:
			go func() {
				<-done
			}()
		}
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{nrand(), key, value, op, ck.lastCompletedId}
	for i := 0; ; i++ {
		reply := PutAppendReply{}
		done := make(chan bool)
		timeOut := time.After(time.Second)
		go func() {
			done <- ck.servers[(ck.leader+i)%len(ck.servers)].Call("KVServer.PutAppend", &args, &reply)
		}()

		select {
		case <-done:
			if reply.Err == OK {
				ck.leader = (ck.leader + i) % len(ck.servers)
				ck.lastCompletedId = args.Id
				return
			}
		case <-timeOut:
			go func() {
				<-done
			}()
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
