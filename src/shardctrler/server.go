package shardctrler

import (
	"log"
	"sync"

	"6.5840/kvraft"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	publisher       kvraft.Broadcaster
	configs         []Config // indexed by config num
	appliedCommand  map[int64]int
	Shards          [NShards]int
	gids            []int
	gidToIdx        map[int]int
	Groups          map[int][]string
	groupShard      map[int][]int
	collectedShards []int
}

type Op interface {
	// Your data here.
	getId()
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.mu.Lock()
	if _, ok := sc.appliedCommand[args.Id]; ok {
		reply.Err = OK
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()
	commitCommand := sc.excuteCommand(*args)
	if commitCommand != nil && commitCommand.Id == args.Id && commitCommand.PrevId == args.PrevId {
		reply.Err = OK
		/*
			raft.DPrintf("groups %v join configNum %v and its config %v\n",
				args.GroupOrder, commitCommand.Cg.Num, commitCommand.Cg.Shards)*/

	}

}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.mu.Lock()
	if _, ok := sc.appliedCommand[args.Id]; ok {
		reply.Err = OK
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()
	commitCommand := sc.excuteCommand(*args)
	if commitCommand != nil && commitCommand.Id == args.Id && commitCommand.PrevId == args.PrevId {
		reply.Err = OK
		/*raft.DPrintf("groups %v leave configNum %v and its config %v\n",
		args.GIDs, commitCommand.Cg.Num, commitCommand.Cg.Shards)*/
	}

}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.mu.Lock()
	if _, ok := sc.appliedCommand[args.Id]; ok {
		reply.Err = OK
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()
	commitCommand := sc.excuteCommand(*args)
	if commitCommand != nil && commitCommand.Id == args.Id && commitCommand.PrevId == args.PrevId {
		reply.Err = OK
		/*raft.DPrintf("group %v leave configNum %v and its config %v\n",
		args.GID, commitCommand.Cg.Num, commitCommand.Cg.Shards)*/
	}

}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.mu.Lock()
	if _, ok := sc.appliedCommand[args.Id]; ok {
		reply.Err = OK
		if args.Num < 0 || args.Num >= len(sc.configs) {
			reply.Config = sc.configs[len(sc.configs)-1]
		} else {
			reply.Config = sc.configs[args.Num]
		}
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	commitCommand := sc.excuteCommand(*args)
	if commitCommand != nil && commitCommand.Id == args.Id && commitCommand.PrevId == args.PrevId {
		reply.Err = OK
		sc.mu.Lock()
		if args.Num < 0 || args.Num >= len(sc.configs) {
			reply.Config = sc.configs[len(sc.configs)-1]
		} else {
			reply.Config = sc.configs[args.Num]
		}
		sc.mu.Unlock()
	}
}

func (sc *ShardCtrler) excuteCommand(command interface{}) *CommitCommand {

	idx := make(chan int)
	done := make(chan interface{})
	sc.publisher.Subscribe(idx, done)

	index, _, isLeader := sc.rf.Start(command)
	idx <- index
	if !isLeader {
		return nil
	}
	switch result := (<-done).(type) {
	case CommitCommand:
		return &result
	default:
		return nil
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	//labgob.Register(Op)
	labgob.Register(QueryArgs{})
	labgob.Register(JoinArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(LeaveArgs{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.appliedCommand = map[int64]int{}
	sc.Groups = map[int][]string{}
	sc.groupShard = map[int][]int{}
	sc.gidToIdx = map[int]int{}

	// Your code here.

	sc.collectedShards = make([]int, NShards)
	for i := range sc.collectedShards {
		sc.collectedShards[i] = i
	}
	go sc.applier()

	return sc
}

func (sc *ShardCtrler) applier() {
	for m := range sc.applyCh {
		if m.CommandValid {
			sc.mu.Lock()
			switch args := m.Command.(type) {
			case JoinArgs:
				if _, ok := sc.appliedCommand[args.Id]; !ok {
					sc.runJoin(args.GroupOrder, args.Servers)

					sc.appliedCommand[args.Id] = m.CommandIndex
					delete(sc.appliedCommand, args.PrevId)
				}
				m.Command = CommitCommand{args.Id, args.PrevId}

			case LeaveArgs:

				if _, ok := sc.appliedCommand[args.Id]; !ok {
					sc.runLeave(args.GIDs)

					sc.appliedCommand[args.Id] = m.CommandIndex
					delete(sc.appliedCommand, args.PrevId)
				}
				m.Command = CommitCommand{args.Id, args.PrevId}

			case MoveArgs:

				if _, ok := sc.appliedCommand[args.Id]; !ok {
					sc.runMove(args.Shard, args.GID)

					sc.appliedCommand[args.Id] = m.CommandIndex
					delete(sc.appliedCommand, args.PrevId)
				}
				m.Command = CommitCommand{args.Id, args.PrevId}
			case QueryArgs:
				if _, ok := sc.appliedCommand[args.Id]; !ok {
					sc.appliedCommand[args.Id] = m.CommandIndex
					delete(sc.appliedCommand, args.PrevId)
				}
				m.Command = CommitCommand{args.Id, args.PrevId}
			}
			sc.mu.Unlock()
			sc.publisher.BroadcastMsg(m)
		}
	}
	sc.publisher.Terminate()
}

func (sc *ShardCtrler) runJoin(groupOrder []int, Servers map[int][]string) {
	for _, gid := range groupOrder {
		sc.gidToIdx[gid] = len(sc.gids)
		sc.gids = append(sc.gids, gid)
		sc.Groups[gid] = Servers[gid]
		sc.groupShard[gid] = []int{}
	}
	sc.distributeShards()
	sc.Shards = constructShardSlice(sc.groupShard)

	sc.configs = append(sc.configs,
		Config{len(sc.configs), sc.Shards, cloneGroup(sc.Groups)})
}

func (sc *ShardCtrler) runLeave(groups []int) {
	for _, group := range groups {
		index := sc.gidToIdx[group]
		delete(sc.gidToIdx, group)

		lastIdx := len(sc.gids) - 1
		lastGroup := sc.gids[lastIdx]
		sc.gids[index] = lastGroup
		sc.gids = sc.gids[:lastIdx]
		sc.gidToIdx[lastGroup] = index

		sc.collectedShards = append(sc.collectedShards, sc.groupShard[group]...)
		delete(sc.groupShard, group)
		delete(sc.Groups, group)
	}
	sc.distributeShards()
	sc.Shards = constructShardSlice(sc.groupShard)

	sc.configs = append(sc.configs,
		Config{len(sc.configs), sc.Shards, cloneGroup(sc.Groups)})
}

func (sc *ShardCtrler) runMove(shard int, newGroup int) {
	oldGroup := sc.Shards[shard]
	if shards, ok := sc.groupShard[oldGroup]; ok {
		if !removeShard(&shards, shard) {
			log.Fatalf("shard %v not in group %v", shard, oldGroup)
		}
		sc.groupShard[oldGroup] = shards
	} else {
		log.Fatalf("Group g %v is non-existent", oldGroup)
	}

	sc.groupShard[newGroup] = append(sc.groupShard[newGroup], shard)
	sc.Shards[shard] = newGroup

	sc.configs = append(sc.configs,
		Config{len(sc.configs), sc.Shards, cloneGroup(sc.Groups)})
}

func removeShard(shards *[]int, givenShard int) bool {
	for i, shard := range *shards {
		if shard == givenShard {
			(*shards)[i] = (*shards)[0]
			*shards = (*shards)[1:]
			return true
		}
	}
	return false
}

func cloneGroup(groups map[int][]string) map[int][]string {
	replicaGroups := map[int][]string{}
	for g, servers := range groups {
		replicaGroups[g] = servers
	}
	return replicaGroups
}

func constructShardSlice(groupShard map[int][]int) [NShards]int {
	var Shards [NShards]int
	for group, shards := range groupShard {
		for _, shard := range shards {
			Shards[shard] = group
		}
	}
	return Shards
}

func (sc *ShardCtrler) distributeShards() {
	if len(sc.gids) == 0 {
		return
	}

	q := NShards / len(sc.gids)
	r := NShards % len(sc.gids)
	// r group with q + 1 shards
	// numGruops - r with q shards
	// Collect shards
	var lessQGroups []int
	var qSizeGroups []int
	for _, group := range sc.gids {
		shards := sc.groupShard[group]
		if len(shards) < q {
			lessQGroups = append(lessQGroups, group)
		} else if len(shards) == q {
			qSizeGroups = append(qSizeGroups, group)
		} else {
			end := q
			if r > 0 {
				end++
				r--
			}
			sc.collectedShards = append(sc.collectedShards, shards[end:]...)
			sc.groupShard[group] = shards[:end]
		}
	}

	for _, g := range lessQGroups {
		gShards := sc.groupShard[g]
		numShard := q - len(gShards)
		if r > 0 {
			numShard++
			r--
		}
		sc.groupShard[g] = append(gShards, sc.collectedShards[:numShard]...)
		sc.collectedShards = sc.collectedShards[numShard:]
	}

	for _, g := range qSizeGroups {

		if len(sc.collectedShards) > 0 {
			sc.groupShard[g] = append(sc.groupShard[g], sc.collectedShards[0])
			sc.collectedShards = sc.collectedShards[1:]
		} else {
			break
		}
	}
}
