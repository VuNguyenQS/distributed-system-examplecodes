package raft

import (
	"time"
)

// become follower at new detected term

func (rf *Raft) startNewTerm(newTerm int) {
	rf.Term = newTerm
	rf.Session++
	rf.state = follower
	rf.VotedFor = -1
	rf.persist()
	go rf.waitLeader(rf.Session, rf.Term)
}

// follower activity
func (rf *Raft) waitLeader(session int, term int) {
	// wait for timeout and transit to candidate
	if rf.killed() {
		return
	}
	DFprintln(rf.out, "follwer start wait at term ", term, " in session ", session, " with timeout ", rf.timeout)
	time.Sleep(rf.timeout)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if session == rf.Session {
		DFprintln(rf.out, rf.state, " wait for ", rf.timeout, "is out")
		rf.electLeader()
	}
}
