package kvraft

import (
	"bytes"
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
	Id     int64
	Op     string
	Key    string
	Value  string
	PrevId int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	persister      *raft.Persister
	commitedIdx    int
	data           map[string]string
	appliedCommand map[int64]int
	publisher      Broadcaster
}

// Get command
// Check duplicate request

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	command := Op{args.Id, "Get", args.Key, "", args.PrevId}

	if kv.ExcuteCommand(&command) {
		reply.Err = OK
		kv.mu.Lock()
		reply.Value = kv.data[args.Key]
		kv.mu.Unlock()
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	command := Op{args.Id, args.Op, args.Key, args.Value, args.PrevId}

	if kv.ExcuteCommand(&command) {
		reply.Err = OK
	}
}

func (kv *KVServer) ExcuteCommand(command *Op) bool {
	kv.mu.Lock()
	if _, ok := kv.appliedCommand[command.Id]; ok {
		kv.mu.Unlock()
		return true
	}
	kv.mu.Unlock()

	idx := make(chan int)
	done := make(chan Op)
	kv.publisher.Subscribe(idx, done)

	index, _, isLeader := kv.rf.Start(*command)
	idx <- index
	if !isLeader {
		return false
	}

	commitCommand := <-done
	return commitCommand == *command

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
	kv.persister = persister
	kv.ReadBackup()
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.publisher.rf = kv.rf

	// You may need initialization code here.
	go kv.applier(kv.applyCh)

	return kv
}

func (kv *KVServer) applier(applyChan chan raft.ApplyMsg) {
	var command Op
	for m := range applyChan {
		if m.CommandValid {
			command = m.Command.(Op)
			if m.CommandIndex != kv.commitedIdx+1 {
				log.Fatalf("commitedIdx %v while newcommtiedIdx %v\n", kv.commitedIdx, m.CommandIndex)
			}
			kv.commitedIdx = m.CommandIndex
			kv.mu.Lock()

			if _, ok := kv.appliedCommand[command.Id]; !ok {
				if command.Op == "Put" {
					kv.data[command.Key] = command.Value
				} else if command.Op == "Append" {
					kv.data[command.Key] = kv.data[command.Key] + command.Value
				}
				kv.appliedCommand[command.Id] = m.CommandIndex
				delete(kv.appliedCommand, command.PrevId)
			}

			if kv.maxraftstate > 0 && kv.persister.RaftStateSize() > kv.maxraftstate {
				kv.SnapState(kv.commitedIdx)
			}

			kv.mu.Unlock()

			kv.publisher.BroadcastMsg(m)
		} else if m.SnapshotValid {
			kv.ReadBackup()
		}
	}
	// KvServer got killed
	kv.publisher.Terminate()
}

func (kv *KVServer) SnapState(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(index)
	e.Encode(kv.data)
	e.Encode(kv.appliedCommand)
	snap := w.Bytes()
	kv.rf.Snapshot(index, snap)
}

func (kv *KVServer) ReadBackup() {
	backup := kv.persister.ReadSnapshot()
	if backup == nil || len(backup) < 1 {
		kv.data = make(map[string]string)
		kv.appliedCommand = make(map[int64]int)
		return
	}

	r := bytes.NewBuffer(backup)
	d := labgob.NewDecoder(r)
	var commitedIdx int
	var dataBase map[string]string
	var appliedCMD map[int64]int
	if err := d.Decode(&commitedIdx); err != nil {
		log.Fatalf("Decode commitedIdx %v\n", err)
	}
	if err := d.Decode(&dataBase); err != nil {
		log.Fatalf("Decode data %v\n", err)
	}
	if err := d.Decode(&appliedCMD); err != nil {
		log.Fatalf("Decode appliedCommand table %v\n", err)
	}
	kv.commitedIdx = commitedIdx
	kv.data = dataBase
	kv.appliedCommand = appliedCMD
}
