package shardkv

import (
	"os"
	"strconv"
	"sync"
	"sync/atomic"

	"6.5840/kvraft"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	logFile           string
	out               *os.File
	dead              int32
	mck               *shardctrler.Clerk
	db                *Database
	publisher         kvraft.Broadcaster
	isConfigCompleted bool
	oldShardToGroup   [shardctrler.NShards]int
	currentConfig     *shardctrler.Config
	servingShards     map[int]bool
	newShards         map[int]bool
	movedShards       map[int]bool
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	isApplied, value := kv.db.isApplied(args.Id, args.Key)
	if isApplied {
		reply.Err = OK
		reply.Value = value
		return
	}

	switch response := kv.excuteCommand(*args).(type) {
	case GetReply:
		if response.Id == args.Id {
			*reply = response
		}
	case string:
		reply.Err = ErrWrongLeader
	default:
	}
}

func (kv *ShardKV) GetDebug(args *GetArgs, reply *GetReply) {
	// Your code here.

	isApplied, value := kv.db.isApplied(args.Id, args.Key)
	if isApplied {
		reply.Err = OK
		reply.Value = value
		return
	}

	switch response := kv.excuteCommand(*args).(type) {
	case GetReply:
		if response.Id == args.Id {
			*reply = response
		}
		//shard := key2shard(args.Key)
		//fmt.Printf("group %v shard %v \n oldshardGroup %v\n config %v\n OpLog%v\n",
		//	kv.gid, shard, kv.oldShardToGroup, kv.currentConfig, kv.db.shards[shard].OpLog[args.Key])
	case string:
		reply.Err = ErrWrongLeader
	default:
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	isApplied, _ := kv.db.isApplied(args.Id, args.Key)
	if isApplied {
		reply.Err = OK
		return
	}

	switch response := kv.excuteCommand(*args).(type) {
	case PutAppendReply:
		if response.Id == args.Id {
			*reply = response
			//shard := key2shard(args.Key)

		}
	case string:
		reply.Err = ErrWrongLeader

	default:
	}
}

func (kv *ShardKV) isShardAlready(configNum int, shardNum int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if configNum < kv.currentConfig.Num {
		return true
	}

	if configNum == kv.currentConfig.Num && kv.db.isShardPresence(shardNum) {
		return true
	}

	return false

}

func (kv *ShardKV) PutShard(args *PutShardArgs, reply *PutShardReply) {

	if kv.isShardAlready(args.ConfigNum, args.ShardNum) {
		DPrintf(kv.out, "already riecieve shard %v from group %v from config %v",
			args.ShardNum, args.FromGid, args.ConfigNum)
		reply.Err = OK
		return
	}

	switch response := kv.excuteCommand(*args).(type) {
	case PutShardReply:
		if args.ShardNum == response.ShardNum && args.ConfigNum == response.ConfigNum {
			*reply = response
		}
	case string:
		reply.Err = ErrWrongLeader
	default:
	}
}

func (kv *ShardKV) excuteCommand(command interface{}) interface{} {
	idx := make(chan int)
	done := make(chan interface{})
	kv.publisher.Subscribe(idx, done)

	index, _, isLeader := kv.rf.Start(command)

	idx <- index

	if !isLeader {
		return ErrWrongLeader
	}
	//fmt.Printf("server %v group %v command %v \n", kv.me, kv.gid, command)
	return <-done
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
	DPrintf(kv.out, "shutdown\n")
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

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
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.

	labgob.Register(GetArgs{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(PutShardArgs{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(DeleteShard{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.gid = gid
	kv.ctrlers = ctrlers

	kv.logFile = "logInfo/" + "sv" + strconv.Itoa(kv.me) + "gid" + strconv.Itoa(kv.gid) + ".txt"
	kv.out, _ = os.OpenFile(kv.logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)

	kv.db = new(Database)
	for i := 0; i < shardctrler.NShards; i++ {
		kv.db.conds[i] = sync.NewCond(&kv.db.mus[i])
	}
	kv.igestSnap(persister.ReadSnapshot())
	//readPersist(kv.out, persister)

	kv.make_end = make_end
	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	DPrintf(kv.out, "start server\n")
	go kv.applier(persister)
	go kv.pollConfiguration()

	return kv
}
