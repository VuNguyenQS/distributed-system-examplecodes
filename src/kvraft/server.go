package kvraft

import (
	"log"
	"sync"
	"sync/atomic"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op    string
	Key   string
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data      map[string]string
	commitMsg map[int]Op
	cond      sync.Cond
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// Get command
	command := Op{"Get", args.Key, ""}
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.Leader = kv.rf.Leaderis()
		return
	}
	// Wait for index commit
	var commitCommand Op
	ok := false

	kv.mu.Lock()
	defer kv.mu.Unlock()
	for {
		log.Printf("%d: kvserver still waiting\n", kv.me)
		if commitCommand, ok = kv.commitMsg[index]; ok {
			break
		}
		kv.cond.Wait()
	}
	if commitCommand == command {
		delete(kv.commitMsg, index)
		reply.Err = OK
		reply.Value = kv.data[args.Key]
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	command := Op{args.Op, args.Key, args.Value}
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.Leader = kv.rf.Leaderis()
		return
	}
	// Wait for index commit
	var commitCommand Op
	ok := false
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for {
		log.Printf("%d: kvserver still waiting\n", kv.me)
		if commitCommand, ok = kv.commitMsg[index]; ok {
			break
		}
		kv.cond.Wait()
	}
	if commitCommand == command {
		delete(kv.commitMsg, index)
		reply.Err = OK
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.data = make(map[string]string)
	kv.commitMsg = map[int]Op{}
	kv.cond = *sync.NewCond(&kv.mu)

	// You may need initialization code here.
	go kv.applier(kv.applyCh)

	return kv
}

func (kv *KVServer) applier(applyChan chan raft.ApplyMsg) {
	var command Op
	for m := range applyChan {
		log.Printf("%d: applymessage %v\n", kv.me, m)
		if m.CommandValid {
			command = m.Command.(Op)
			//fmt.Println("lock")
			kv.mu.Lock()
			kv.commitMsg[m.CommandIndex] = command
			if command.Op == "Put" {
				kv.data[command.Key] = command.Value
			} else if command.Op == "Append" {
				kv.data[command.Key] = kv.data[command.Key] + command.Value
			}
			kv.cond.Broadcast()
			kv.mu.Unlock()
			//fmt.Println("unlock")
		}
	}
}
