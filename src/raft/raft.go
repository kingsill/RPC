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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// XXX 可能修改
// A Go object implementing a single Raft peer.
type Raft struct {
	mu             sync.Mutex          // Lock to protect shared access to this peer's state
	peers          []*labrpc.ClientEnd // RPC end points of all peers
	persister      *Persister          // Object to hold this peer's persisted state
	me             int                 // this peer's index into peers[]
	dead           int32               // set by Kill()
	isLeader       bool
	timing         bool
	electedTimeOut time.Duration
	lastRevTime    time.Time
	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//Persistent state
	currentTerm int
	votedFor    int       //candidateId that received vote in current term
	log         []Entries //log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	//volatile on all server
	committedIndex int
	lastApplied    int

	//volatile on leader
	nextIndex  []int //initialized to leader last log index + 1
	matchIndex []int //initialized to 0, increases monotonically
}

// entry的结构
type Entries struct {
	Command string //command for state machine
	Term    int    //term when entry was received by leader (first index is 1)
	Index   int    //index of entry in log
}

// XXX
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Your code here (3A).

	return rf.currentTerm, rf.isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// XXX
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int

	PrevLogTerm  int
	Entries      []Entries
	LeaderCommit int
}

// XXX
type AppendEntriesReply struct {
	Term    int
	Success bool
}

// TODO AppendEntries
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	var entryTerm int

	if len(rf.log) == 0 {
		entryTerm = 0
	} else {
		entryTerm = rf.log[args.PrevLogIndex].Term
	}
	if args.PrevLogTerm == entryTerm {
	}

	//同步term
	if args.Term >= rf.currentTerm {
		rf.currentTerm = args.Term
		rf.isLeader = false
		rf.currentTerm = args.Term
	}

	//重置心跳时间
	rf.lastRevTime = time.Now()
	DPrintf("%v成为follower,重置花费时间%v", rf.me, time.Since(rf.lastRevTime))

}

// TODO 什么时候发送AppendEntries
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// TODO 维持心跳。leader才有
func (rf *Raft) HeartBeats() {
	for rf.killed() == false {
		for rf.isLeader == true {
			for i, _ := range rf.peers {
				if i != rf.me {
					args := &AppendEntriesArgs{
						Term:     rf.currentTerm,
						LeaderId: rf.me,
					}
					reply := &AppendEntriesReply{}
					go rf.sendAppendEntries(i, args, reply)
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// XXX
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int //candidate's term
	CandidateId  int //candidate id
	LastLogIndex int //index of candidate's last log entry
	LastLogTerm  int //term of candidate's last log entry
}

// XXX
// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  //currentTerm, for candidate to update itself
	VoteGranted bool //true means candidate received vote
}

// TODO
// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("请求来自：%v，本机ID：%v,本机超时状态：%v", args, rf.me, time.Since(rf.lastRevTime))

	if args.Term < rf.currentTerm {
		DPrintf("%v过时", args.CandidateId)
		reply.VoteGranted = false
		reply.Term = args.Term
		return
	}

	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		DPrintf("已经投过票，%v", rf.votedFor)
		reply.VoteGranted = false
		return
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		lastLogIndex := len(rf.log)
		lastLogTerm := 0
		if lastLogIndex > 0 {
			lastLogTerm = rf.log[lastLogIndex-1].Term
		}

		if (args.LastLogTerm > lastLogTerm) || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
			rf.votedFor = args.CandidateId
			rf.currentTerm = args.Term
			rf.lastRevTime = time.Now()
			reply.VoteGranted = true
			DPrintf("投票给%v", args.CandidateId)
		}
	}
	reply.Term = rf.currentTerm
}

// TODO
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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
	isLeader := true

	// Your code here (3B).

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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// TODO 开始选举的定时
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.

		//只有candidate和follower才能发起选举
		if rf.isLeader != true {

			//超时
			if time.Since(rf.lastRevTime) > rf.electedTimeOut {
				DPrintf("%v超时", rf.me)
				rf.startElection()
			}
		}

		// pause for a random amount of time between 150 and 500
		// milliseconds.
		ms := 100 + (rand.Int63() % 500)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		rf.votedFor = -1
	}
}

// TODO 选举
func (rf *Raft) startElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.lastRevTime = time.Now()

	sucNum := len(rf.peers) / 2
	DPrintf("%v开始竞选，需要票数%v", rf, sucNum)
	num := 0

	args := &RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}
	if len(rf.log) == 0 {
		args.LastLogIndex = 0
		args.LastLogTerm = 0
	} else {
		args.LastLogIndex = rf.log[len(rf.log)-1].Index
		args.LastLogTerm = rf.log[len(rf.log)-1].Term
	}

	c := make(chan bool)

	for i, _ := range rf.peers {
		if i != rf.me {
			i := i
			DPrintf("请求%v投票", i)
			reply := &RequestVoteReply{}

			go func() {

				rf.sendRequestVote(i, args, reply)

				if reply.VoteGranted {
					num++
				} else if reply.Term > rf.currentTerm { //如果没有被投票且发现自己的term过时
					rf.isLeader = false
					//结束选举，回退到follower（只要不开始选举就是follower，这里一直占据锁）
				}
				DPrintf("%v当前票数:%v", rf.me, num)
				if num >= sucNum {
					rf.isLeader = true
					DPrintf("%v成功", rf.me)
					c <- true
				}
			}()
		}
	}

	select {
	case <-c:
		for i, _ := range rf.peers {
			if i != rf.me {
				args := &AppendEntriesArgs{}
				reply := &AppendEntriesReply{}
				go rf.sendAppendEntries(i, args, reply)
			}
		}
	case <-time.After(1 * time.Second):
		DPrintf("%v选举超时", rf.me)
	}

}

// TODO
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister,
	applyCh chan ApplyMsg) *Raft {

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.votedFor = -1

	//设定心跳超时时间
	rf.electedTimeOut = time.Millisecond * 500
	//初始化为设置当前时间
	rf.lastRevTime = time.Now()

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections

	go rf.ticker() //RequestVote RPC

	go rf.HeartBeats()

	return rf
}
