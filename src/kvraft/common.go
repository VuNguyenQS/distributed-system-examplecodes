package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
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
	Err string
}

type GetArgs struct {
	Id  int64
	Key string
	// You'll have to add definitions here.
	PrevId int64
}

type GetReply struct {
	Err   string
	Value string
}
