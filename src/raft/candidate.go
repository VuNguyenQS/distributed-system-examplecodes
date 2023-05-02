package raft

import (
	"fmt"
	"time"
)

func (rf *Raft) electLeader() {
	// ramdomnized timeout
	rf.state = candidate
	term := rf.Term
	electOut := false // debug info
	for !rf.killed() && rf.Term == term && rf.state == candidate {
		if electOut {
			fmt.Fprintln(rf.out, "candidate wait at term ", rf.Term, "is out")
		}
		rf.Term++
		rf.VotedFor = rf.me
		term = rf.Term
		rf.persist()
		rf.timeout = randomTimeout()
		args := RequestVoteArgs{
			rf.me,
			term,
			rf.SnapshotIdx + len(rf.Log),
			rf.Log[len(rf.Log)-1].Term,
		}
		rf.mu.Unlock()
		fmt.Fprintln(rf.out, "candidate start at term ", term, " with timeout ", rf.timeout, " and args ", args)
		votes := 1
		for peerId := 0; peerId < rf.numPeers; peerId++ {
			if peerId != rf.me {
				go rf.sendRequestVote(peerId, &votes, &args, &RequestVoteReply{})
			}
		}
		time.Sleep(rf.timeout)
		electOut = true
		rf.mu.Lock()
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	CandidateId int
	Term        int
	LogLen      int
	LastLogTerm int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	VoteGranted bool
	Term        int
	VoteId      int
	LogLen      int
	LastLogTerm int
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, votes *int, args *RequestVoteArgs, reply *RequestVoteReply) {
	for !rf.killed() && rf.Term == args.Term && rf.state == candidate && *votes <= rf.numPeers/2 {
		if ok := rf.peers[server].Call("Raft.RequestVote", args, reply); ok {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			fmt.Fprintln(rf.out, "we demand vote at term ", args.Term, " and recieve reply ", *reply, " at state ", rf.state, " at term ", rf.Term, " with current vote ", *votes)
			if reply.VoteGranted {
				if rf.Term == args.Term && rf.state == candidate {
					*votes += 1
					if *votes > rf.numPeers/2 {
						rf.state = leader
						rf.leader.leaderId = rf.me
						rf.leader.Term = rf.Term
						rf.leaderWork(rf.Term)
					}
				}
			} else {
				if reply.Term > rf.Term {
					rf.startNewTerm(reply.Term)
				}
			}

			return
		}
	}
}
