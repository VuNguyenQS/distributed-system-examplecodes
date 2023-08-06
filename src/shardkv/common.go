package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK                 = "OK"
	ErrNoKey           = "ErrNoKey"
	ErrWrongGroup      = "ErrWrongGroup"
	ErrWrongLeader     = "ErrWrongLeader"
	ErrShardNotPresent = "ErrShardNotPresent"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Id    int64
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	PrevId int64
}

type PutAppendReply struct {
	Id  int64
	Err Err
}

type GetArgs struct {
	Id     int64
	Key    string
	PrevId int64
	// You'll have to add definitions here.
}

type GetReply struct {
	Id    int64
	Err   Err
	Value string
}

type PutShardArgs struct {
	ConfigNum      int
	FromGid        int
	ShardNum       int
	AppliedCommand map[int64]bool
	Data           map[string]string
}

type PutShardReply struct {
	ConfigNum int
	ShardNum  int
	Err       Err
}
