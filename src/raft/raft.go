package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(Command interface{}) (index, term, isleader)
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

const (
	leader int = iota
	candidate
	follower
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//persistent
	currentTerm int
	votedFor    int
	log         []Entry

	//Volatile
	committedIndex int
	lastApplied    int

	nextIndex  []int
	matchIndex []int

	//custom
	state int
	time  time.Time
}
type Entry struct {
	Command interface{}
	Term    int
	Index   int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isLeader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	if rf.state == leader {
		isLeader = true
	}

	return term, isLeader
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

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	//leader需退位
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//DPrintf("args:%v", args)

	//当前服务器的term比candidate大
	if rf.currentTerm > args.Term {
		DPrintf("requestVote outTime,%v deny", rf.me)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm

		return
	}

	//当前服务器term小
	if args.Term > rf.currentTerm {
		rf.state = follower
		rf.votedFor = -1
		rf.currentTerm = args.Term
	}

	reply.Term = rf.currentTerm

	//没投过票
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) || (args.Term > rf.currentTerm) {
		lastLogTerm, lastLogIndex := rf.getLastLog()
		switch lastLogTerm == args.LastLogTerm {
		case true:
			if args.LastLogIndex < lastLogIndex {
				reply.VoteGranted = false
				DPrintf("vote falied")
				return
			}
		case false:
			if args.LastLogTerm < lastLogTerm {
				reply.VoteGranted = false
				DPrintf("vote falied")
				return
			}
		}
		DPrintf("vote success")
		reply.VoteGranted = true
		rf.state = follower
		rf.time = time.Now()
		rf.votedFor = args.CandidateId
	}
	rf.currentTerm = args.Term

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	//DPrintf("%v sendRequestVOte", rf.me)
	rf.mu.Lock()
	if ok && reply.Term < rf.currentTerm { //忽略过时消息
		DPrintf("ignore timeout Msg")
		ok = false
	}
	rf.mu.Unlock()

	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next Command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// Command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the Command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	index = -1
	term = -1
	isLeader = false

	rf.mu.Lock()
	defer rf.mu.Unlock()
	//检查是否为leader
	if rf.state != leader {
		return
	}

	// Your code here (3B).
	index, _ = rf.getLastLog()
	term = rf.currentTerm
	isLeader = true
	entry := Entry{Command: command, Term: rf.currentTerm, Index: index + 1}
	rf.log = append(rf.log, entry)
	DPrintf("leader: %v received new entru,log:%v", rf.me, rf.log)

	return
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

// TODO ticker
// 选举和超时定时器
func (rf *Raft) ticker() {
	heartTime := 100 * time.Millisecond
	for rf.killed() == false {
		rf.mu.Lock()
		msOut := time.Duration(150+(rand.Int63()%300)) * time.Millisecond
		// Your code here (3A)
		// Check if a leader election should be started.
		session := time.Since(rf.time)
		//DPrintf("%v session:%v", rf.me, session)

		//leader不会超时
		if session > msOut && rf.state != leader {
			//开始选举
			rf.mu.Unlock()
			go rf.startElection()
			// pause for a random amount of time between 50 and 350
			// milliseconds.
			time.Sleep(5 * time.Millisecond)
		} else if session > heartTime && rf.state == leader { //心跳计时
			rf.mu.Unlock()
			go rf.heartBeats()
			time.Sleep(5 * time.Millisecond)
			//间隔
		} else {
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}

	}
}

// 开始选举
func (rf *Raft) startElection() {
	rf.mu.Lock()
	//DPrintf("%v become candidate", rf.me)
	rf.state = candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	//重置定时器
	rf.time = time.Now()
	needVote := len(rf.peers) / 2
	args := &RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me}
	args.LastLogIndex, args.LastLogTerm = rf.getLastLog()
	counts := 0
	rf.mu.Unlock()
	//发送请求投票RPC

	for i, _ := range rf.peers {
		if i != rf.me {
			go func(i int) {
				reply := &RequestVoteReply{}
				if rf.sendRequestVote(i, args, reply) {
					rf.mu.Lock()
					if reply.VoteGranted == true {
						counts++
						//成功,follower不能直接跳到leader
						if counts >= needVote {
							counts = -2
							DPrintf("%v election success", rf.me)
							rf.leaderInit()
						}
					} else if reply.Term > rf.currentTerm {
						rf.state = follower
						rf.votedFor = -1
						rf.currentTerm = reply.Term
					}
					rf.mu.Unlock()
				}
			}(i)
		}
	}
}

// TODO Make
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

	// Your initialization code here (3A, 3B, 3C).
	rf.time = time.Now()
	rf.state = follower
	rf.votedFor = -1 //使用-1指没投过票nil

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	//go rf.heartBeats()
	go rf.apply(applyCh)

	return rf
}

// 获取最后一个日志的index和term
func (rf *Raft) getLastLog() (Index int, term int) {
	length := len(rf.log)
	log := rf.log
	//无日志
	if length == 0 {
		return 0, 0
	} else {
		Index = log[length-1].Index
		term = log[length-1].Term
		return
	}
}

// 获取最后一个日志的index和term
func (rf *Raft) getItsLastLog(log []Entry) (Index int, term int) {
	length := len(log)
	//无日志
	if length == 0 {
		return 0, 0
	} else {
		Index = log[length-1].Index
		term = log[length-1].Term
		return
	}
}

func (rf *Raft) fillAppendArgs(args *AppendEntriesArgs, server int, isNote bool) {
	nextIndex := rf.nextIndex[server]
	matchIndex := rf.matchIndex[server]
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.LeaderCommit = rf.committedIndex
	args.PrevLogIndex = nextIndex - 1

	if nextIndex == 1 {
		args.PrevLogTerm = 0
	} else {
		args.PrevLogTerm = rf.log[nextIndex-2].Term
	}

	//nextIndex 1 []   term 0   [1 1]
	//nextIndex 2 [1]  0 term 1
	//nextIndex 3 [1 1] 1 term 1

	//通用

	lastLogIndex, _ := rf.getLastLog()

	if isNote { //leader通告

	} else { //正常心跳消息
		if matchIndex < lastLogIndex { //leader的日志的最新消息比matchIndex大，说明未同步，需要发送entries
			if args.PrevLogIndex == 0 {
				args.Entries = rf.log
			} else {
				args.Entries = rf.log[nextIndex-1:]
			}

		}
	}

}

// 初始化leader
func (rf *Raft) leaderInit() {
	rf.state = leader
	nextIndex := rf.nextIndex
	rf.votedFor = -1

	//初始化nextIndex、matchIndex
	index, _ := rf.getLastLog()
	for i, _ := range nextIndex {
		//初始化leader的参数
		rf.nextIndex[i] = index + 1
		rf.matchIndex[i] = 0

		if i != rf.me { //发送宣布心跳
			go func(i int) {
				rf.mu.Lock()
				args := &AppendEntriesArgs{}
				rf.fillAppendArgs(args, i, true)
				reply := &AppendEntriesReply{}
				currentTerm := rf.currentTerm
				rf.mu.Unlock()

				if rf.sendAppendEntries(i, args, reply) {
					if reply.Term > currentTerm {
						rf.mu.Lock()
						DPrintf("leader 退位")
						rf.state = follower
						rf.currentTerm = reply.Term
						rf.mu.Unlock()
					}
				}
			}(i)
		}

	}
}

func (rf *Raft) heartBeats() {
	rf.mu.Lock()
	rf.timeUpdate()
	currentTerm := rf.currentTerm
	rf.mu.Unlock()

	for i, _ := range rf.peers {
		reply := &AppendEntriesReply{}
		if i != rf.me {
			go func(i int) {
				args := &AppendEntriesArgs{}
				rf.mu.Lock()
				rf.fillAppendArgs(args, i, false)
				rf.mu.Unlock()
				if rf.sendAppendEntries(i, args, reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					//DPrintf("%v", reply)
					if reply.Success {
						if len(args.Entries) != 0 {
							lastLog, _ := rf.getItsLastLog(args.Entries)
							rf.nextIndex[i] = lastLog + 1
							rf.matchIndex[i] = lastLog
						}
					} else {
						if reply.Term > currentTerm {
							DPrintf("leader退位")
							rf.state = follower
							rf.currentTerm = reply.Term
						} else if len(args.Entries) != 0 {
							rf.nextIndex[i]--
						}
					}
				}
			}(i)

		}
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	XTerm  int //冲突entry的term
	XIndex int //xterm的第一条entru的index
	XLen   int //日志长度
}

func (rf *Raft) getTerm(Index int) (term int) {
	firstIndex, _ := rf.getLastLog()
	if firstIndex == 0 {
		term = 0
	} else {
		term = rf.log[Index-firstIndex].Term
	}
	return
}

func (rf *Raft) getFirstIndex(log []Entry) (index int, isNull bool) {
	if len(log) == 0 {
		return 0, true
	}

	return log[0].Index, false
}

// AppendEntries figure2
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//过时的先剃出去
	if args.Term < rf.currentTerm { //1.
		reply.Success = false
		return
	}
	DPrintf("appendArgs:%v", args)
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	rf.state = follower
	reply.Term = rf.currentTerm
	reply.Success = true

	//DPrintf("time UPDATE")
	rf.timeUpdate()

	if rf.getTerm(args.PrevLogIndex) != args.PrevLogTerm { //2.
		reply.Success = false
		return
	}
	if len(args.Entries) != 0 {
		rf.dealConflict(args, reply) //3.

		rf.changeLog(args.Entries, args.PrevLogIndex) //4.
	}

	if args.LeaderCommit > rf.committedIndex { //5.
		newIndex, _ := rf.getLastLog()
		rf.committedIndex = min(args.LeaderCommit, newIndex)
	}

	reply.XLen = len(rf.log)

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	//收到过期回复
	if reply.Term < rf.currentTerm {
		ok = false
	}

	rf.mu.Unlock()
	return ok
}

// 对应figure2的append的3.4
func (rf *Raft) changeLog(entries []Entry, index int) {
	if len(entries) == 0 {
		return
	}

	index1, isNull := rf.getFirstIndex(rf.log)

	if isNull { //rf log 为空
		rf.log = append(rf.log, entries...)
	} else {
		rf.log = append(rf.log[index-index1:], entries...)
	}
	//DPrintf("log now:%v", rf.log)
}

func (rf *Raft) dealConflict(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	first, null := rf.getFirstIndex(rf.log)
	lastIndex, _ := rf.getLastLog()
	log := rf.log
	logFirstIndex := args.Entries[0].Index
	if null || logFirstIndex > lastIndex { //follower的log为空或者不如entries长
		reply.XTerm = 0
		reply.XIndex = 0
		return
	}

	for i := logFirstIndex; i < lastIndex; i++ {
		if log[i-first].Term != args.Entries[i-logFirstIndex].Term {
			reply.XTerm = log[i-first].Term
			reply.XIndex = i
			rf.log = rf.log[:i-first]
			return
		}
	}

}

func (rf *Raft) apply(applyCh chan ApplyMsg) {
	for !rf.killed() {
		rf.mu.Lock()
		limits := len(rf.peers) / 2
		nextApply := rf.lastApplied + 1
		state := rf.state
		committedIndex := rf.committedIndex
		matchIndex := make([]int, len(rf.matchIndex)) // 保存 matchIndex 副本
		copy(matchIndex, rf.matchIndex)
		lastApplied := rf.lastApplied
		firstIndex, _ := rf.getFirstIndex(rf.log)
		log := rf.log
		rf.mu.Unlock()

		if state == leader {
			counts := 0
			if nextApply > committedIndex {
				for i, _ := range matchIndex {
					if matchIndex[i] >= nextApply {
						counts++
					}
				}
				if counts >= limits+1 {
					rf.mu.Lock()
					rf.committedIndex = nextApply
					committedIndex = nextApply
					DPrintf("leader update committed,%v", nextApply)
					rf.mu.Unlock()
				}
			}
		}

		if committedIndex > lastApplied {
			var applyMsg ApplyMsg
			applyMsg.CommandValid = true
			applyMsg.CommandIndex = nextApply
			applyMsg.Command = log[nextApply-firstIndex].Command
			applyCh <- applyMsg
			DPrintf("apply %v", applyMsg)
			rf.mu.Lock()
			rf.lastApplied++
			rf.mu.Unlock()
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) timeUpdate() {
	rf.time = time.Now()
}
