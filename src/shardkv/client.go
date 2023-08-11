package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
	"6.5840/shardctrler"
)

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	completedId [shardctrler.NShards]int64
	//prevVal     map[string]string
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	shard := key2shard(key)
	args := GetArgs{nrand(), key, ck.completedId[shard]}

	for {
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply GetReply
				ok := unreliableRequest(srv, "ShardKV.Get", &args, &reply)
				if ok {
					if reply.Err == OK {
						ck.completedId[shard] = args.Id
						return reply.Value
					}
					if reply.Err == ErrWrongGroup {
						break
					}
					if reply.Err == ErrShardNotPresent {
						time.Sleep(50 * time.Millisecond)
					}
				}

				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}

	return ""
}

func (ck *Clerk) GetDebug(key string) string {
	shard := key2shard(key)
	args := GetArgs{nrand(), key, ck.completedId[shard]}

	for {
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply GetReply
				ok := unreliableRequest(srv, "ShardKV.GetDebug", &args, &reply)
				if ok {
					if reply.Err == OK {
						ck.completedId[shard] = args.Id
						return reply.Value
					}
					if reply.Err == ErrWrongGroup {
						break
					}
					if reply.Err == ErrShardNotPresent {
						time.Sleep(50 * time.Millisecond)
					}
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}

	return ""
}

// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	shard := key2shard(key)
	args := PutAppendArgs{nrand(), key, value, op, ck.completedId[shard]} //ck.prevVal[key],
	//fmt.Printf("key %v\n", key)
	for {
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				ok := unreliableRequest(srv, "ShardKV.PutAppend", &args, &reply)
				if ok {
					if reply.Err == OK {
						ck.completedId[shard] = args.Id
						//ck.prevVal[key] += value
						return
					}
					if reply.Err == ErrWrongGroup {
						break
					}
					if reply.Err == ErrShardNotPresent {
						time.Sleep(50 * time.Millisecond)
					}
				}
			}
			// ... not ok, or ErrWrongLeader
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func unreliableRequest(srv *labrpc.ClientEnd, svcMeth string, args interface{}, reply interface{}) bool {
	done := make(chan bool)
	timeOut := time.After(time.Second)
	go func() {
		done <- srv.Call(svcMeth, args, reply)
	}()
	select {
	case ok := <-done:
		return ok
	case <-timeOut:
		go func() {
			<-done
		}()
		return false
	}
}
