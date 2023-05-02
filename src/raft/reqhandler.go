package raft

import (
	"fmt"
	"log"
)

// Append entry handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	logChange := false
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Fprintln(rf.out, "leaderId ", args.LeaderId, " at term ", args.Term, " our state ", rf.state, " at term ", rf.Term)
	fmt.Fprintln(rf.out, "prevIdx ", args.PrevIdx, " prevTerm ", args.PrevTerm, " commitIdx ", args.CommitedIdx, " entries ", args.Entries)
	fmt.Fprintln(rf.out, "our log at first ", rf.Log)

	if args.Term < rf.Term {
		reply.Success = false
		reply.Term = rf.Term
	} else {
		// Compare two logs

		// Transform args log index
		leaderPrevIdx := args.PrevIdx - rf.SnapshotIdx
		lastIdx := len(rf.Log) - 1
		commitedIdx := rf.commitedIdx - rf.SnapshotIdx

		// Check match log at previousIdx
		if leaderPrevIdx <= commitedIdx || (leaderPrevIdx <= lastIdx && args.PrevTerm == rf.Log[leaderPrevIdx].Term) {
			// 2 log match at previous index
			// Check to see if we already have entries from args
			numOfEntries := len(args.Entries)
			if numOfEntries > 0 {
				// This is a real append entries request not a heartbeat from leader
				idxOfLastNew := leaderPrevIdx + numOfEntries
				if idxOfLastNew > lastIdx ||
					(idxOfLastNew > commitedIdx && rf.Log[idxOfLastNew].Term != args.Entries[numOfEntries-1].Term) {
					// We dont have these entries
					if leaderPrevIdx <= commitedIdx {
						rf.Log = rf.Log[:commitedIdx+1]
						args.Entries = args.Entries[commitedIdx-leaderPrevIdx:]
					} else {
						rf.Log = rf.Log[:leaderPrevIdx+1]
					}

					rf.Log = append(rf.Log, args.Entries...)
					logChange = true
				}
			}
			reply.Success = true
			// need to change this
		} else {
			i := leaderPrevIdx
			if i > lastIdx {
				i = lastIdx
			}
			for ; i > commitedIdx && rf.Log[i].Term > args.PrevTerm; i-- {
			}
			reply.Success = false
			reply.LastIndex = i + rf.SnapshotIdx
			reply.Term = rf.Log[i].Term
		}

		// Check term of leader is new
		if args.Term > rf.Term {
			rf.startNewTerm(args.Term)
			// accept new leader
			rf.leader.leaderId = args.LeaderId
			rf.leader.Term = args.Term
		} else {
			if rf.leader.leaderId != args.LeaderId && rf.leader.Term == args.Term {
				log.Fatalf("old leader %v and see new leader %v at same term %v", rf.leader.leaderId, args.LeaderId, args.Term)
			}
			if logChange {
				rf.persist()
			}
			rf.state = follower
			rf.Session++
			go rf.waitLeader(rf.Session, rf.Term)
		}
	}
	// For debug
	fmt.Fprintln(rf.out, "our log after ", rf.Log, " commitIdx ", rf.commitedIdx)
	fmt.Fprintln(rf.out, " success ", reply.Success, " lastIdx ", reply.LastIndex, " term ", reply.Term)

	// Commit new entry if possible
	if !rf.killed() && reply.Success && rf.commitedIdx < args.CommitedIdx {
		rf.commandApplyMsg(args.CommitedIdx)
		rf.commitedIdx = args.CommitedIdx
	}

}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.Term
	if args.Term > rf.Term {
		log.Fatal("why send Snapshot at firt time")
	}
	if args.Term == rf.Term {
		fmt.Fprintf(rf.out, "leaderSnapIdx %v leaderloglen %v lDcmIdx %v , oursnapIdx %v and ourloglen %v ourcmidx%v\n",
			args.SnapshotIndex, len(args.Log), args.CommitedIdx, rf.SnapshotIdx, len(rf.Log), rf.commitedIdx)

		// Set new wait
		rf.Session++
		go rf.waitLeader(rf.Session, rf.Term)

		if args.SnapshotIndex > rf.commitedIdx {
			//receive snap
			rf.SnapShot = args.Snapshot
			rf.SnapshotIdx = args.SnapshotIndex
			//rf.SnapshotTerm = args.SnapshotTerm
			rf.Log = args.Log
			rf.commitedIdx = rf.SnapshotIdx
			rf.persist()

			// prepare snapshot apply msg
			snapMsg := ApplyMsg{}
			snapMsg.SnapshotValid = true
			snapMsg.Snapshot = rf.SnapShot
			snapMsg.SnapshotIndex = rf.SnapshotIdx
			//snapMsg.SnapshotTerm = rf.SnapshotTerm

			// send msg
			if !rf.killed() {
				rf.applyChannel <- snapMsg
				rf.applyChannel <- ApplyMsg{}
			} else {
				return
			}

		}

		lastNewEntry := args.SnapshotIndex + len(args.Log) - 1 - rf.SnapshotIdx
		if lastNewEntry >= len(rf.Log) ||
			((lastNewEntry > rf.commitedIdx-rf.SnapshotIdx) && args.Log[len(args.Log)-1].Term != rf.Log[lastNewEntry].Term) {

			args.Log = args.Log[rf.commitedIdx+1-args.SnapshotIndex:]
			rf.Log = rf.Log[:rf.commitedIdx+1-rf.SnapshotIdx]
			rf.Log = append(rf.Log, args.Log...)
			rf.persist()
		}

		if !rf.killed() && rf.commitedIdx < args.CommitedIdx {
			rf.commandApplyMsg(args.CommitedIdx)
			rf.commitedIdx = args.CommitedIdx
		}

	}
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// debug info
	fmt.Fprintln(rf.out, "requestvote from server ", args.CandidateId, " at term ", args.Term, " and we are at sate ", rf.state, " at term ", rf.Term)

	reply.Term = rf.Term
	reply.LastLogTerm = rf.Log[len(rf.Log)-1].Term
	reply.LogLen = rf.SnapshotIdx + len(rf.Log)
	reply.VoteId = rf.me

	if args.Term < rf.Term {
		reply.VoteGranted = false
	} else {
		upToDate := (args.LastLogTerm > reply.LastLogTerm) || (args.LastLogTerm == reply.LastLogTerm && args.LogLen >= reply.LogLen)
		if args.Term > rf.Term {
			// Case detect new term
			// Complete the rest of the response
			reply.VoteGranted = upToDate
			if reply.VoteGranted {
				rf.VotedFor = args.CandidateId
			} else {
				rf.VotedFor = -1
				rf.timeout = randomTimeout()
			}
			// Back to follwer at new term
			rf.Term = args.Term
			rf.persist()
			rf.Session++
			rf.state = follower
			go rf.waitLeader(rf.Session, rf.Term)
		} else {
			// Case: response to candidate at current term
			reply.VoteGranted = upToDate && (rf.VotedFor == -1 || rf.VotedFor == args.CandidateId)
			if reply.VoteGranted {
				rf.VotedFor = args.CandidateId
				rf.persist()
				rf.Session++
				rf.state = follower
				go rf.waitLeader(rf.Session, rf.Term)
			}
		}
	}
	fmt.Fprintln(rf.out, "we reply ", reply)
}
