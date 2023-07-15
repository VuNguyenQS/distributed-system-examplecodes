package raft

import (
	"fmt"
	"time"
)

// Try to send appendReq to follwer until either we reieve reply or we got crash or we no longer at leader term
func (rf *Raft) sendAppendRequest(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := false
	var resReply AppendEntriesReply
	for !rf.killed() && rf.Term == args.Term && !ok {
		resReply = AppendEntriesReply{}
		ok = rf.peers[server].Call("Raft.AppendEntries", args, &resReply)
	}
	if ok {
		*reply = resReply
	}
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := false
	var resReply InstallSnapshotReply
	for !rf.killed() && rf.Term == args.Term && !ok {
		resReply = InstallSnapshotReply{}
		ok = rf.peers[server].Call("Raft.InstallSnapshot", args, &resReply)
	}
	if ok {
		*reply = resReply
	}
	return ok
}

func (rf *Raft) leaderWork(leaderTerm int) {
	fmt.Fprintln(rf.out, "become leader at term ", leaderTerm)
	for i := 0; !rf.killed() && rf.Term == leaderTerm; i++ {
		fmt.Fprintf(rf.out, "send hearbeat at %v\n", i)
		args := AppendEntriesArgs{}
		args.CommitedIdx = rf.commitedIdx
		args.LeaderId = rf.me
		args.Term = rf.Term
		args.PrevIdx = len(rf.Log) - 1 + rf.SnapshotIdx
		args.PrevTerm = rf.Log[len(rf.Log)-1].Term
		rf.mu.Unlock()
		for peerId := 0; peerId < len(rf.peers); peerId++ {
			if peerId != rf.me {
				go rf.heartbeat(peerId, args)
			}
		}
		time.Sleep(50 * time.Millisecond)
		rf.mu.Lock()
	}
}

type AppendEntriesArgs struct {
	Term        int
	LeaderId    int
	PrevIdx     int
	PrevTerm    int
	CommitedIdx int
	Entries     []LogEntry
}

type AppendEntriesReply struct {
	Success   bool
	Term      int
	LastIndex int
}

func (rf *Raft) heartbeat(server int, args AppendEntriesArgs) {
	reply := AppendEntriesReply{}
	if ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply); ok {
		if !reply.Success {
			rf.mu.Lock()
			if rf.Term < reply.Term {
				rf.startNewTerm(reply.Term)
				rf.mu.Unlock()
			} else {
				rf.mu.Unlock()
				rf.bringFollowerUp(server, &args)
			}
		}
	}
}

// notice about lock, i still don't lock here
func (rf *Raft) beginReplicate(index int, args *AppendEntriesArgs) {
	replicas := 1
	serverCommited := []int{rf.me}
	for i := 0; i < rf.numPeers; i++ {
		if i != rf.me {
			go rf.replicateCommand(i, index, *args, &replicas, &serverCommited)
		}
	}
}

func (rf *Raft) replicateCommand(server int, index int, args AppendEntriesArgs, replicas *int, serverCommited *[]int) {

	if ok := rf.bringFollowerUp(server, &args); ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.commitedIdx < index {
			*replicas++
			*serverCommited = append(*serverCommited, server)
			if !rf.killed() && *replicas > rf.numPeers/2 {
				fmt.Fprintln(rf.out, "replicas ", *replicas, "current commitedIdx ", rf.commitedIdx, " snapshotIdx ", rf.SnapshotIdx, " and index ", index, " commited", " servers ", *serverCommited)
				rf.commandApplyMsg(index)
				rf.commitedIdx = index
			}
		}
	}

}

type InstallSnapshotArgs struct {
	Term          int
	Snapshot      []byte
	SnapshotIndex int
	//SnapshotTerm  int
	Log         []LogEntry
	CommitedIdx int
}

type InstallSnapshotReply struct {
	Term int
}

// make a follwer up to date to leader at log index i
// this function return if we match up follwer to leader until index or other cases below
// if leader at lower term than follwer
// if leader is crash or no longer a leader anymore
func (rf *Raft) bringFollowerUp(server int, args *AppendEntriesArgs) bool {
	// prepare first part args
	reply := AppendEntriesReply{}
	ok := rf.sendAppendRequest(server, args, &reply)
	for ok {

		if reply.Success {
			return true
		}

		rf.mu.Lock()

		if reply.Term > rf.Term {
			rf.startNewTerm(reply.Term)
			rf.mu.Unlock()
			return false
		}

		if rf.Term == args.Term {
			// This case we are still the leader
			// Find index in log where term is match or less than reply term
			i := reply.LastIndex - rf.SnapshotIdx
			for ; i > -1 && rf.Log[i].Term > reply.Term; i-- {
			}

			if i < 0 {
				// Install snapShot include
				// send snapshot request
				// send entrire log upto index
				argsSnapshot := InstallSnapshotArgs{
					rf.Term,
					rf.SnapShot,
					rf.SnapshotIdx,
					//rf.SnapshotTerm,
					rf.Log,
					rf.commitedIdx,
				}
				rf.mu.Unlock()

				replySnapshot := InstallSnapshotReply{}

				if ok = rf.sendInstallSnapshot(server, &argsSnapshot, &replySnapshot); !ok {
					return false
				}

				if replySnapshot.Term == argsSnapshot.Term {
					return true
				}

				rf.mu.Lock()
				if replySnapshot.Term > rf.Term {
					rf.startNewTerm(replySnapshot.Term)
				}
				rf.mu.Unlock()
				return false
			}
			// second part args
			args.PrevIdx = i + rf.SnapshotIdx
			args.PrevTerm = rf.Log[i].Term
			args.Entries = append([]LogEntry{}, rf.Log[i+1:]...)
			args.CommitedIdx = rf.commitedIdx
			rf.mu.Unlock()
			// send request
			ok = rf.sendAppendRequest(server, args, &reply)
		} else {
			// this raft is in new term
			rf.mu.Unlock()
			return false
		}
	}
	return false
}
