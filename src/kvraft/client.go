package kvraft

import (
	"crypto/rand"
	"fmt"
	"math/big"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader int
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
	args := GetArgs{key}
	var reply GetReply
	i := 0
	//ok := false
	for ; reply.Err != OK; i++ {
		// Choose randomly other servers
		fmt.Printf("get key %v with i is %v\n", key, i)
		reply := GetReply{}
		ck.servers[i%len(ck.servers)].Call("KVServer.Get", &args, &reply)
		/*
			if ok {
				if reply.Err != OK {
					ck.leader = reply.Leader
					i = -1
					ok = false
				}
			}
		*/
	}

	return reply.Value
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
	args := PutAppendArgs{key, value, op}
	var reply PutAppendReply
	i := 0
	for ; reply.Err != OK; i++ {
		// Choose randomly other servers
		//fmt.Printf("PutAppend key %v with i is %v\n", key, i)
		reply = PutAppendReply{}
		ck.servers[i%len(ck.servers)].Call("KVServer.PutAppend", &args, &reply)
		fmt.Printf("reply is %v\n", reply)
		/*
			if ok {
				if reply.Err != OK {
					ck.leader = reply.Leader
					i = -1
					ok = false
				}
			}
		*/
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
