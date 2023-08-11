package shardkv

import (
	"log"
	"sync"
	"time"

	"6.5840/shardctrler"
)

func (kv *ShardKV) pollConfiguration() {
	var config shardctrler.Config
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			if isCompleted, configNum := kv.completeCurrentConfig(); isCompleted {
				config = kv.mck.Query(configNum + 1)
				if config.Num == configNum {
					time.Sleep(100 * time.Millisecond)
				} else {
					kv.excuteCommand(config)
				}
			}
		} else {
			time.Sleep(100 * time.Millisecond)
		}

	}
}

func (kv *ShardKV) setupConfig(commingCg shardctrler.Config) {
	// clear for old config
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if commingCg.Num != kv.currentConfig.Num+1 {
		return
	}
	DPrintf(kv.out, "setup newconfig %v shards %v oldConfig %v\n", commingCg.Num, commingCg.Shards, kv.currentConfig.Num)

	if !kv.isConfigCompleted {
		// delete movedshard
		for movedShard := range kv.movedShards {
			kv.db.mus[movedShard].Lock()
			if !kv.db.shards[movedShard].IsValid && kv.db.shards[movedShard].Data != nil {
				log.Fatalf("why shard %v at config %v is not deleted", movedShard, kv.currentConfig.Num)
			}
			kv.db.mus[movedShard].Unlock()
			//kv.db.deleteShard(movedShard)
		}
	}

	kv.isConfigCompleted = false
	kv.oldShardToGroup = kv.currentConfig.Shards
	kv.currentConfig = &commingCg
	ShardtoGroup := kv.currentConfig.Shards
	oldShards := kv.servingShards
	shards := map[int]bool{}
	newShards := map[int]bool{}
	isFirst := kv.currentConfig.Num == 1
	for shardNum := 0; shardNum < shardctrler.NShards; shardNum++ {
		group := ShardtoGroup[shardNum]
		if group == kv.gid {
			if oldShards[shardNum] {
				delete(oldShards, shardNum)
			} else {
				kv.db.setValid(shardNum, isFirst, kv.gid)
				newShards[shardNum] = true
			}
			shards[shardNum] = true
		}
	}
	kv.servingShards = shards
	kv.newShards = newShards
	kv.movedShards = oldShards

	kv.db.setUnValid(kv.movedShards)
}

func (kv *ShardKV) completeCurrentConfig() (bool, int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	config := *kv.currentConfig
	DPrintf(kv.out, "try complete config %v\n", config.Num)
	if kv.isConfigCompleted {
		DPrintf(kv.out, "config %v is arready completed\n", kv.currentConfig.Num)
		return true, kv.currentConfig.Num
	}

	var done sync.WaitGroup
	for newShard := range kv.newShards {
		done.Add(1)
		kv.db.waitForShard(newShard, &done, kv.out, "newShard %v has came\n")
	}

	for movedShard := range kv.movedShards {
		shard := kv.db.getCopyShard(movedShard)
		if shard != nil {
			done.Add(1)
			args := PutShardArgs{config.Num, kv.gid, movedShard, shard.AppliedCommand, shard.Data, shard.History}
			desGID := config.Shards[movedShard]

			go func(format string, shard int) {
				defer done.Done()

				desServers := config.Groups[desGID]
				for !kv.killed() {
					for _, server := range desServers {
						reply := PutShardReply{}
						done := make(chan bool)
						timeOut := time.After(time.Second)
						//kv.make_end(server).Call("ShardKV.PutShard", &args, &reply)
						go func() {
							done <- kv.make_end(server).Call("ShardKV.PutShard", &args, &reply)
						}()

						select {
						case <-done:
							if reply.Err == OK {
								DPrintf(kv.out, format, shard, desGID)
								command := DeleteShard{args.ConfigNum, args.ShardNum}
								for !kv.killed() {
									switch response := kv.excuteCommand(command).(type) {
									case DeleteShard:
										if args.ShardNum == response.ShardNum && args.ConfigNum == response.ConfigNum {
											return
										}
									case string:
										//Not leader
										return
									default:
									}
								}
							}
						case <-timeOut:
							go func() {
								<-done
							}()
						}
					}
				}
			}("moved shard %v to group %v success\n", movedShard)
		}
	}

	kv.mu.Unlock()
	done.Wait()

	kv.mu.Lock()

	if kv.killed() {
		return false, 0
	}
	if kv.currentConfig.Num == config.Num {
		kv.isConfigCompleted = true
		DPrintf(kv.out, "succeed to complete config %v\n", config.Num)
	} else {
		DPrintf(kv.out, "config %v overcome config %v\n", kv.currentConfig.Num, config.Num)
	}
	return kv.isConfigCompleted, kv.currentConfig.Num

}
