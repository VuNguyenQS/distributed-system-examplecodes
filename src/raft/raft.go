package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type State string

const (
	follower  State = "follower"
	candidate       = "candidate"
	leader          = "leader"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	logFile      string
	applyChannel chan ApplyMsg
	commitedIdx  int
	leader       LeaderInfo
	numPeers     int
	Term         int
	VotedFor     int
	state        State
	timeout      time.Duration
	Session      int
	Log          []LogEntry
	SnapShot     []byte
	//SnapshotTerm int
	SnapshotIdx int
	//	lastIndex   int
	out *os.File
}

type LogEntry struct {
	Command interface{}
	Term    int
}

type LeaderInfo struct {
	leaderId int
	Term     int
}

func (rf *Raft) commandApplyMsg(newCommittedIdx int) {
	for i := rf.commitedIdx + 1; i <= newCommittedIdx; i++ {
		msg := ApplyMsg{}
		msg.CommandValid = true
		msg.CommandIndex = i
		msg.Command = rf.Log[i-rf.SnapshotIdx].Command
		rf.applyChannel <- msg
		rf.applyChannel <- ApplyMsg{}
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.Term
	isleader = rf.state == leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Term)
	e.Encode(rf.Log)
	//e.Encode(rf.SnapshotTerm)
	e.Encode(rf.SnapshotIdx)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.SnapShot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		rf.Log = []LogEntry{{nil, 0}}
		rf.VotedFor = -1
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currenTerm int
	var logs []LogEntry
	var votedFor int
	//var snapshotTerm int
	var snapshotIdx int
	if err := d.Decode(&votedFor); err != nil {
		log.Fatalf("Decode votedfor %v\n", err)
	}
	if err := d.Decode(&currenTerm); err != nil {
		log.Fatalf("Decode term %v\n", err)
	}
	if err := d.Decode(&logs); err != nil {
		log.Fatalf("Decode logs %v\n", err)
	}
	//if err := d.Decode(&snapshotTerm); err != nil {
	//	log.Fatalf("Decode snapTerm %v\n", err)
	//}
	if err := d.Decode(&snapshotIdx); err != nil {
		log.Fatalf("Decode startIdx %v\n", err)
	}

	rf.VotedFor = votedFor
	rf.Term = currenTerm
	rf.Log = logs
	//rf.SnapshotTerm = snapshotTerm
	rf.SnapshotIdx = snapshotIdx
	rf.commitedIdx = rf.SnapshotIdx
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	// Dont send snapshot msg
	// Trim the log and save snap shot
	localIdx := index - rf.SnapshotIdx
	rf.SnapShot = snapshot
	rf.SnapshotIdx = index
	//rf.SnapshotTerm = rf.Log[localIdx].Term
	rf.Log = append([]LogEntry{}, rf.Log[localIdx:]...)

	rf.persist()
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !rf.killed() && rf.state == leader {
		rf.Log = append(rf.Log, LogEntry{command, rf.Term})
		index = len(rf.Log) - 1 + rf.SnapshotIdx
		term = rf.Term
		rf.persist()
		DFprintln(rf.out, "rieceve command ", command, " with idx ", index, " commitIdx ", rf.commitedIdx)
		isLeader = true
		args := AppendEntriesArgs{
			term,
			rf.me,
			index - 1,
			rf.Log[index-1-rf.SnapshotIdx].Term,
			rf.commitedIdx,
			[]LogEntry{rf.Log[len(rf.Log)-1]},
		}
		go rf.beginReplicate(index, &args)
	}

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	// Close applyChannel to halt kv goroutine
	go func() {
		rf.mu.Lock()
		close(rf.applyChannel)
		rf.mu.Unlock()
	}()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func randomTimeout() time.Duration {
	ms := 200 + (rand.Int63() % 200)
	return time.Duration(ms) * time.Millisecond
}

func (rf *Raft) startup() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.persist()
	rf.numPeers = len(rf.peers)
	rf.timeout = randomTimeout()
	//rf.out, _ = os.Create("Id" + strconv.Itoa(rf.me) + ".txt")
	rf.state = follower
	go rf.waitLeader(rf.Session, rf.Term)
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.applyChannel = applyCh
	rf.logFile = "logInfo/Id" + strconv.Itoa(rf.me) + ".txt"
	rf.out, _ = os.OpenFile(rf.logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.SnapShot = persister.ReadSnapshot()

	DFprintln(rf.out, "SERVER START OR RESTART WITH LOG ", rf.Log)

	// start ticker goroutine to start elections
	//go rf.ticker()
	go rf.startup()

	return rf
}

func (rf *Raft) Leaderis() int {
	return rf.leader.leaderId
}
