package shardkv

import (
	"fmt"
	"io"
	"log"
	"sync"

	"6.5840/shardctrler"
)

type Shard struct {
	IsValid bool
	//Presence       bool
	FromGid        int
	ConfigNum      int
	History        []int
	AppliedCommand map[int64]bool
	Data           map[string]string
}

type Database struct {
	mus    [shardctrler.NShards]sync.Mutex
	conds  [shardctrler.NShards]*sync.Cond
	shards [shardctrler.NShards]Shard
}

func (db *Database) putAppend(args *PutAppendArgs, reply *PutAppendReply) {
	shardNum := key2shard(args.Key)
	db.mus[shardNum].Lock()
	defer db.mus[shardNum].Unlock()

	shard := &db.shards[shardNum]
	if !shard.IsValid {
		reply.Err = ErrWrongGroup
		return
	}
	if shard.Data != nil {
		reply.Err = OK
		if !shard.AppliedCommand[args.Id] {
			/*
				if args.PrevVal != shard.Data[args.Key] {
					log.Fatalf("key %v of shard %v it's history %v \n expected: \n %v \n reality: \n %v\n",
						args.Key, shardNum, shard.History, args.PrevVal, shard.Data[args.Key])
				}*/
			if args.Op == "Put" {
				shard.Data[args.Key] = args.Value
			} else {
				shard.Data[args.Key] += args.Value
			}
			shard.AppliedCommand[args.Id] = true
			delete(shard.AppliedCommand, args.PrevId)
		}
	} else {
		reply.Err = ErrShardNotPresent
	}
}

func (db *Database) get(args *GetArgs, reply *GetReply) {
	shardNum := key2shard(args.Key)
	db.mus[shardNum].Lock()
	defer db.mus[shardNum].Unlock()

	shard := &db.shards[shardNum]
	if !shard.IsValid {
		reply.Err = ErrWrongGroup
		return
	}
	if shard.Data != nil {
		reply.Err = OK
		reply.Value = shard.Data[args.Key]
		if !shard.AppliedCommand[args.Id] {
			shard.AppliedCommand[args.Id] = true
			delete(shard.AppliedCommand, args.PrevId)
		}
	} else {
		reply.Err = ErrShardNotPresent
	}
}

func (db *Database) isApplied(id int64, key string) (bool, string) {
	shardNum := key2shard(key)
	db.mus[shardNum].Lock()
	defer db.mus[shardNum].Unlock()

	shard := &db.shards[shardNum]
	if !shard.IsValid {
		return false, ""
	}
	if shard.AppliedCommand[id] {
		return true, shard.Data[key]
	}
	return false, ""
}

func (db *Database) putShard(arg *PutShardArgs, w io.Writer, gid int) {
	db.mus[arg.ShardNum].Lock()
	defer db.mus[arg.ShardNum].Unlock()

	shard := &db.shards[arg.ShardNum]
	if shard.IsValid && shard.Data != nil {
		if arg.ConfigNum != shard.ConfigNum || arg.FromGid != shard.FromGid {
			log.Fatalf("shard %v from group %v from config %v come while already from group %v and from config %v\n",
				arg.ShardNum, arg.FromGid, arg.ConfigNum, shard.FromGid, shard.ConfigNum)
		}
		return
	}
	shard.IsValid = true
	shard.FromGid = arg.FromGid
	shard.AppliedCommand = map[int64]bool{}
	shard.Data = map[string]string{}
	for key, value := range arg.Data {
		shard.Data[key] = value
	}
	for id := range arg.AppliedCommand {
		shard.AppliedCommand[id] = true
	}
	shard.History = append(shard.History, arg.History...)
	shard.History = append(shard.History, gid)
	shard.ConfigNum = arg.ConfigNum
	db.conds[arg.ShardNum].Broadcast()
	DPrintf(w, "revieve shard %v from group %v at config %v\n", arg.ShardNum, arg.FromGid, arg.ConfigNum)
}

func (db *Database) waitForShard(shardNum int, done *sync.WaitGroup, w io.Writer, format string) {
	db.mus[shardNum].Lock()
	go func() {
		defer DPrintf(w, format, shardNum)
		defer done.Done()
		defer db.mus[shardNum].Unlock()

		for db.shards[shardNum].Data == nil {
			db.conds[shardNum].Wait()
		}
	}()
}

func (db *Database) setValid(newShard int, isFirst bool, gid int) {

	db.mus[newShard].Lock()
	shard := &db.shards[newShard]
	shard.IsValid = true
	if isFirst {
		shard.AppliedCommand = map[int64]bool{}
		shard.Data = map[string]string{}
		shard.FromGid = 0
		//shard.Presence = true
		shard.ConfigNum = 1
		shard.History = []int{gid}
	}
	db.mus[newShard].Unlock()

}

func (db *Database) setUnValid(movedShards map[int]bool) {
	for shardNum := range movedShards {
		db.mus[shardNum].Lock()
		if !db.shards[shardNum].IsValid {
			fmt.Printf("why movedshard %v is already not valid\n", shardNum)
		}
		db.shards[shardNum].IsValid = false
		db.mus[shardNum].Unlock()
	}
}

func (db *Database) deleteShard(shardNum int) {
	db.mus[shardNum].Lock()
	defer db.mus[shardNum].Unlock()

	shard := &db.shards[shardNum]

	if shard.IsValid {
		return
	}

	if shard.Data == nil {
		return
	}
	shard.ConfigNum = 0
	shard.FromGid = 0
	shard.AppliedCommand = nil
	shard.History = nil
	shard.Data = nil
}

func (db *Database) isShardPresence(shardNum int) bool {
	db.mus[shardNum].Lock()
	defer db.mus[shardNum].Unlock()

	return db.shards[shardNum].Data != nil
}

func (db *Database) getCopyShard(shardNum int) *Shard {
	db.mus[shardNum].Lock()
	defer db.mus[shardNum].Unlock()

	shard := &db.shards[shardNum]

	if shard.IsValid {
		return nil
	}

	if shard.Data == nil {
		return nil
	}
	returnShard := Shard{}
	returnShard.AppliedCommand = map[int64]bool{}
	returnShard.Data = map[string]string{}
	for commandId := range shard.AppliedCommand {
		returnShard.AppliedCommand[commandId] = true
	}
	for key, value := range shard.Data {
		returnShard.Data[key] = value
	}
	returnShard.History = append([]int{}, shard.History...)

	return &returnShard
}
