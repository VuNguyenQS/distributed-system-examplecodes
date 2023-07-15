package kvraft

import (
	"sync"

	"6.5840/raft"
)

type Subscriber struct {
	alive   bool
	channel chan raft.ApplyMsg
}

type Broadcaster struct {
	mu         sync.Mutex
	terminated bool
	rf         *raft.Raft // for debug purpose
	subs       []*Subscriber
}

func (b *Broadcaster) Subscribe(idx chan int, done chan Op) {
	sub := Subscriber{true, make(chan raft.ApplyMsg)}

	go func() {
		defer sub.Unsubsribe()
		var earlyCommingMsgs []raft.ApplyMsg
		for {
			select {
			case index := <-idx:
				if index == -1 {
					return
				}
				if len(earlyCommingMsgs) > 0 {
					firstIdx := earlyCommingMsgs[0].CommandIndex
					if firstIdx > index {
						//fmt.Printf("We miss index %v and earliestIdx collected is %v\n", index, firstIdx)
						close(done)
						return
					}
					if index-firstIdx <= len(earlyCommingMsgs)-1 {
						if earlyCommingMsgs[index-firstIdx].CommandIndex != index {
							//fmt.Printf("SnapshotIdx %v why we recieve this index %v while expect %v\n", b.rf.SnapshotIdx, buffer[index-firstIdx].CommandIndex, index)
							close(done)
						} else {
							done <- earlyCommingMsgs[index-firstIdx].Command.(Op)
						}
						return
					}
				}
				for msg := range sub.channel {
					if msg.CommandIndex == index {
						done <- msg.Command.(Op)
						return
					} else if msg.CommandIndex > index {
						//fmt.Printf("Show me snapshotIdx %v commandIdx %v and index %v\n", b.rf.SnapshotIdx, msg.CommandIndex, index)
						break
					}
				}
				close(done)
				return
			case msg := <-sub.channel:
				earlyCommingMsgs = append(earlyCommingMsgs, msg)
			}
		}
	}()

	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.terminated {
		b.subs = append(b.subs, &sub)
	}
}

func (b *Broadcaster) BroadcastMsg(msg raft.ApplyMsg) {
	b.mu.Lock()
	defer b.mu.Unlock()

	activeSubIdx := 0
	for i := 0; i < len(b.subs); i++ {
		if b.subs[i].alive {
			b.subs[i].channel <- msg
		} else {
			close(b.subs[i].channel)
			b.subs[i] = b.subs[activeSubIdx]
			activeSubIdx++
		}
	}
	if activeSubIdx > 0 {
		b.subs = b.subs[activeSubIdx:]
	}
}

func (b *Broadcaster) Terminate() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.terminated = true
	for _, sub := range b.subs {
		close(sub.channel)
	}
}

func (sub *Subscriber) Unsubsribe() {
	// turn off communication channel
	sub.alive = false
	go func() {
		for m := range sub.channel {
			m.CommandValid = true // do something for avoid compiler error
		}
	}()
}
