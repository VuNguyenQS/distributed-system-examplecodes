package shardkv

import (
	"bytes"
	"log"

	"6.5840/labgob"
	"6.5840/raft"
	"6.5840/shardctrler"
)

type DeleteShard struct {
	ConfigNum int
	ShardNum  int
}

func (kv *ShardKV) applier(persister *raft.Persister) {
	for m := range kv.applyCh {
		if m.CommandValid {
			switch arg := m.Command.(type) {
			case GetArgs:
				getRep := GetReply{}
				getRep.Id = arg.Id
				kv.db.get(&arg, &getRep)

				m.Command = getRep

			case PutAppendArgs:
				rep := PutAppendReply{arg.Id, ""}
				kv.db.putAppend(&arg, &rep)

				m.Command = rep

			case PutShardArgs:
				rep := PutShardReply{arg.ConfigNum, arg.ShardNum, OK}
				kv.mu.Lock()
				if kv.currentConfig.Num <= arg.ConfigNum {
					kv.db.putShard(&arg, kv.out, kv.gid)
				}
				kv.mu.Unlock()
				m.Command = rep

			case DeleteShard:
				kv.mu.Lock()
				if kv.currentConfig.Num == arg.ConfigNum {
					if !kv.movedShards[arg.ShardNum] {
						log.Fatalf("while delete shard %v at config %v not in movedShards %v\n",
							arg.ShardNum, arg.ConfigNum, kv.movedShards)
					}
					kv.db.deleteShard(arg.ShardNum)
				}
				kv.mu.Unlock()

			case shardctrler.Config:
				kv.setupConfig(arg)

			}

			if kv.maxraftstate > 0 && persister.RaftStateSize() > kv.maxraftstate {
				kv.createSnap(m.CommandIndex)
			}
			kv.publisher.BroadcastMsg(m)
		} else if m.SnapshotValid {
			kv.igestSnap(m.Snapshot)
		}
	}
	kv.publisher.Terminate()
}

func (kv *ShardKV) createSnap(index int) {

	kv.mu.Lock()
	defer kv.mu.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.isConfigCompleted)
	e.Encode(kv.oldShardToGroup)
	e.Encode(*kv.currentConfig)
	e.Encode(kv.servingShards)
	e.Encode(kv.newShards)
	e.Encode(kv.movedShards)

	for i := 0; i < shardctrler.NShards; i++ {
		kv.db.mus[i].Lock()
		defer kv.db.mus[i].Unlock()
	}
	e.Encode(kv.db.shards)
	snap := w.Bytes()
	kv.rf.Snapshot(index, snap)
}

func (kv *ShardKV) igestSnap(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		kv.isConfigCompleted = true
		kv.currentConfig = &shardctrler.Config{}
		kv.servingShards = map[int]bool{}
		kv.newShards = map[int]bool{}
		kv.movedShards = map[int]bool{}
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var isConfigCompleted bool
	var oldShardToGroup [shardctrler.NShards]int
	var currentConfig shardctrler.Config
	var servingShards map[int]bool
	var newShards map[int]bool
	var movedShards map[int]bool
	var shards [shardctrler.NShards]Shard
	if err := d.Decode(&isConfigCompleted); err != nil {
		log.Fatalf("Decode isConfigComplete %v\n", err)
	}
	if err := d.Decode(&oldShardToGroup); err != nil {
		log.Fatalf("Decode isConfigComplete %v\n", err)
	}
	if err := d.Decode(&currentConfig); err != nil {
		log.Fatalf("Decode currentConfig %v\n", err)
	}
	if err := d.Decode(&servingShards); err != nil {
		log.Fatalf("Decode servingShards %v\n", err)
	}
	if err := d.Decode(&newShards); err != nil {
		log.Fatalf("Decode newShards %v\n", err)
	}
	if err := d.Decode(&movedShards); err != nil {
		log.Fatalf("Decode movedShards %v\n", err)
	}
	if err := d.Decode(&shards); err != nil {
		log.Fatalf("Decode data %v\n", err)
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.isConfigCompleted = isConfigCompleted
	kv.currentConfig = &currentConfig
	kv.oldShardToGroup = oldShardToGroup
	kv.servingShards = servingShards
	kv.newShards = newShards
	kv.movedShards = movedShards
	for i := 0; i < shardctrler.NShards; i++ {
		kv.db.mus[i].Lock()
		defer kv.db.mus[i].Unlock()
	}
	kv.db.shards = shards
}
