package shardctrler

import (
	"fmt"
	"testing"
)

func TesDistribute(t *testing.T) {
	sc := ShardCtrler{}
	sc.Groups = map[int][]string{}
	sc.groupShard = map[int][]int{}
	sc.gidToIdx = map[int]int{}

	// Your code here.
	servers := map[int][]string{}
	servers[14] = []string{}
	servers[30] = []string{}
	servers[5] = []string{}

	sc.collectedShards = make([]int, NShards)
	for i := range sc.collectedShards {
		sc.collectedShards[i] = i
	}

	sc.runJoin([]int{14, 30, 5}, servers)
	fmt.Println("run join")
	fmt.Println(sc.Shards)
	fmt.Println(sc.groupShard)

	sc.runLeave([]int{14})
	fmt.Println("run leave")
	fmt.Println(sc.Shards)
	fmt.Println(sc.groupShard)

	sc.runMove(9, 30)
	fmt.Println("run move")
	fmt.Println(sc.Shards)
	fmt.Println(sc.groupShard)
}
