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

// XXX ApplyMsg
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
	Command interface{} //command for state machine
	Term    int         //term when entry was received by leader (first index is 1)
	Index   int         //index of entry in log
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

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

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int //新的entry之前的一个entry的index

	PrevLogTerm  int       //PrevLogIndex对应的entry的term
	Entries      []Entries //新的entry，不是全部的，为了效率可能传多个
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// TODO AppendEntries
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = true

	//判断是否leader过时
	if args.Term < rf.currentTerm {
		//DPrintf("ID:%v,leader过时", args.LeaderId)
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	//entry不为空，说明是正式的leader发送的有消息的心跳，否则为上位宣称
	if len(args.Entries) != 0 {
		DPrintf("收到有效内容")
		//log中第一个entry的Index
		iniIndex := 0
		if len(rf.log) != 0 {
			iniIndex = rf.log[0].Index
			//推算prevLogIndex对应的entry的实际log位置
			order := args.PrevLogIndex - iniIndex
			//检查prevLogIndex
			if order < 0 || rf.log[order].Term != args.PrevLogTerm {
				reply.Success = false
				return
			}
			//修剪log
			if len(rf.log) != 0 {
				rf.log = append(rf.log[:order], args.Entries...)
				DPrintf("received")
			} else {
				rf.log = args.Entries
				DPrintf("received")

			}
		}

		//修改commitedIndex
		if args.LeaderCommit > rf.committedIndex {
			rf.committedIndex = min(args.LeaderCommit, rf.log[len(rf.log)-1].Index)
		}
	}

	//同步term
	rf.currentTerm = args.Term
	rf.isLeader = false
	//重置心跳时间和是否投票
	rf.lastRevTime = time.Now()
	rf.votedFor = -1
	//DPrintf("%v成为follower", rf.me)
}

// XXX 什么时候发送AppendEntries
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	/*	DPrintf("%v给%v发送AppendEntries", rf.me, server)

		tim := make(chan bool)

		go func() {
			time.Sleep(10 * time.Millisecond)
			tim <- false
		}()
		tim <- rf.peers[server].Call("Raft.AppendEntries", args, reply)

		return <-tim*/
	//DPrintf("%v给%v发送AppendEntries", rf.me, server)

	// 创建一个结果通道
	resultChan := make(chan bool)

	// 启动一个Goroutine进行RPC调用
	go func() {
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		resultChan <- ok
	}()

	select {
	case result := <-resultChan:
		//成功的话更新matchindex表
		if reply.Success {
			if len(args.Entries) != 0 {
				rf.matchIndex[server] = args.Entries[len(args.Entries)-1].Index
				rf.nextIndex[server] = args.Entries[len(args.Entries)-1].Index + 1
			} else {
				rf.nextIndex[server]--
			}
		}

		return result // RPC调用成功，返回结果
	case <-time.After(10 * time.Millisecond):
		return false // 超时，返回false
	}
}

type AppendEntriesResult struct {
	Success bool
	Number  int
	TimeOut bool
}

// TODO 维持心跳。leader才有
func (rf *Raft) HeartBeats() {
	for rf.killed() == false {
		for rf.isLeader == true {
			rf.mu.Lock()
			nums := len(rf.peers)
			resultCh := make(chan AppendEntriesResult, nums)
			peers := rf.peers
			me := rf.me
			DPrintf("me:%v", me)
			currentTerm := rf.currentTerm
			matchIndex := rf.matchIndex

			DPrintf("matchIndex:%v", matchIndex)

			commitedIndex := rf.committedIndex
			entries := rf.log
			leaderCommit := rf.committedIndex

			iniIndex := 0
			if len(entries) != 0 {
				iniIndex = entries[0].Index
			}
			rf.mu.Unlock()
			DPrintf("entries:%v", entries)

			for i, _ := range peers {
				if i != me {
					go func(i int) {
						args := &AppendEntriesArgs{
							Term:         currentTerm,
							LeaderId:     me,
							PrevLogIndex: matchIndex[i],
							LeaderCommit: leaderCommit,
						}
						if matchIndex[me] > commitedIndex && len(entries) != 0 {
							if len(entries) == 1 {
								args.Entries = entries
							} else {
								args.Entries = entries[matchIndex[i]-iniIndex:]
							}
							DPrintf("Entries:%v", args.Entries)
						}

						prevLogTerm := 0

						if len(entries) != 0 {
							prevLogTerm = entries[matchIndex[me]-iniIndex].Term
						}
						args.PrevLogTerm = prevLogTerm
						reply := &AppendEntriesReply{}

						ok := rf.sendAppendEntries(i, args, reply)

						// 将 reply 的 success 状态发送到 channel 中
						resultCh <- AppendEntriesResult{
							TimeOut: ok,
							Success: reply.Success,
							Number:  reply.Term,
						}
					}(i)

				}
			}
			// 收集所有协程的结果
			stop := false

			for j := 0; j < nums-1; j++ {
				result := <-resultCh
				if !result.Success && result.TimeOut { //说明自己的term小于被请求对象
					stop = true
					//DPrintf("%v恢复follower,", rf.me)
					rf.mu.Lock()
					rf.isLeader = false
					rf.currentTerm = max(result.Number, rf.currentTerm)
					rf.lastRevTime = time.Now()
					rf.mu.Unlock()
					break
				}
			}

			if stop {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int //candidate's term
	CandidateId  int //candidate id
	LastLogIndex int //index of candidate's last log entry
	LastLogTerm  int //term of candidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  //currentTerm, for candidate to update itself
	VoteGranted bool //true means candidate received vote
}

// XXX 投票限制
// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//在接到投票请求是是否刷新心跳超时
	//rf.lastRevTime = time.Now()

	//DPrintf("请求来自：%v,term:%v，本机ID：%v,本机当前term：%v", args.CandidateId, args.Term, rf.me, rf.currentTerm)
	//如果candidate的term小于当前的term，返回false，并且告知candidate当前的term
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		//DPrintf("%v过时", args.CandidateId)
		reply.VoteGranted = false
		return
	}

	//如果term没问题，判断是否已经投过票
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		//DPrintf("已经投过票，%v", rf.votedFor)
		reply.VoteGranted = false
		return
	}

	lastLogIndex := rf.committedIndex
	lastLogTerm := 0
	if lastLogIndex > 0 {
		lastLogTerm = rf.log[lastLogIndex].Term
	}

	//判断日志新旧
	if (args.LastLogTerm > lastLogTerm) || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
		rf.votedFor = args.CandidateId

		reply.VoteGranted = true
		//DPrintf("投票给%v", args.CandidateId)
	}
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
	return ok
}

// TODO Start
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
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index = 1
	if len(rf.log) != 0 {
		//index = rf.log[len(rf.log)-1].Index + 1
		index = rf.nextIndex[rf.me]
		rf.nextIndex[rf.me]++
	}
	isLeader = rf.isLeader
	term = rf.currentTerm

	if !isLeader {
		return
	}

	entry := Entries{
		Command: command,
		Index:   index,
		Term:    term,
	}

	//将新的entry加入log
	rf.log = append(rf.log, entry)
	rf.matchIndex[rf.me]++
	// Your code here (3B).
	DPrintf("收到消息")
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

// XXX 开始选举的定时
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.

		//只有candidate和follower才能发起选举

		if rf.isLeader != true {

			//超时
			if time.Since(rf.lastRevTime) > rf.electedTimeOut {
				//DPrintf("%v超时,开始选举", rf.me)
				rf.startElection()
			}
		}
		// pause for a random amount of time between 150 and 500
		// milliseconds.
		ms := 150 + (rand.Int63() % 150)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// XXX 选举
func (rf *Raft) startElection() {
	var LastLogIndex, LastLogTerm int
	//首先对rf进行读取
	rf.mu.Lock()
	rf.currentTerm++
	currentTerm := rf.currentTerm
	rf.votedFor = rf.me
	sucNum := len(rf.peers) / 2
	candidateID := rf.me
	if len(rf.log) == 0 {
		LastLogIndex = 0
		LastLogTerm = 0
	} else {
		LastLogIndex = rf.log[len(rf.log)-1].Index
		LastLogTerm = rf.log[len(rf.log)-1].Term
	}
	peers := rf.peers
	rf.mu.Unlock()

	//DPrintf("ID:%v;Term:%v开始竞选，需要票数%v", candidateID, currentTerm, sucNum)
	num := 0

	args := &RequestVoteArgs{
		Term:         currentTerm,
		CandidateId:  candidateID,
		LastLogIndex: LastLogIndex,
		LastLogTerm:  LastLogTerm,
	}

	c := make(chan bool)

	for i, _ := range peers {
		if i != candidateID {
			i := i
			//DPrintf("请求%v投票", i)
			reply := &RequestVoteReply{}
			go func() {

				rf.sendRequestVote(i, args, reply)

				if reply.VoteGranted {
					num++
				} else if reply.Term > currentTerm { //如果没有被投票且发现自己的term过时
					currentTerm = reply.Term
					//DPrintf("candidate %v 发现自己不配当candidate", candidateID)
					//结束选举，回退到follower（只要不开始选举就是follower，这里一直占据锁）
				}
				//DPrintf("%v当前票数:%v", candidateID, num)
				if num >= sucNum {
					c <- true
				}
			}()
		}
	}

	select {
	case <-c:
		rf.mu.Lock()
		rf.isLeader = true
		lastLogIndex := 0
		if len(rf.log) != 0 {
			//DPrintf("%v", rf.log)
			lastLogIndex = rf.log[len(rf.log)].Index + 1
		}

		for i, _ := range rf.nextIndex {
			rf.nextIndex[i] = lastLogIndex
		}
		rf.mu.Unlock()

		//DPrintf("%v成功,当前term%v", candidateID, currentTerm)
		for i, _ := range peers {
			if i != candidateID {
				args := &AppendEntriesArgs{Term: currentTerm,
					LeaderId: candidateID}
				reply := &AppendEntriesReply{}
				go rf.sendAppendEntries(i, args, reply)
			}
		}
	case <-time.After(50 * time.Millisecond):
		//rf.mu.Lock()
		//rf.currentTerm--
		//rf.mu.Unlock()
		//DPrintf("%v选举超时或者不成功", candidateID)
	}
	rf.mu.Lock()
	rf.votedFor = -1
	rf.mu.Unlock()
}

func (rf *Raft) apply(applyCh chan ApplyMsg) {
	for rf.killed() == false && rf.isLeader {
		rf.mu.Lock()

		committedIndex := rf.committedIndex
		matchIndex := rf.matchIndex
		th := len(rf.peers) / 2
		rf.mu.Unlock()

		counts := 0

		for _, v := range matchIndex {
			if v > committedIndex {
				counts++
			}
		}

		applyMsg := ApplyMsg{
			CommandValid: true,
			//Command:      rf.log[committedIndex].Command,
			CommandIndex: committedIndex + 1,
		}

		if counts >= th {
			rf.mu.Lock()
			rf.committedIndex++

			initIndex := rf.log[0].Index
			applyMsg.Command = rf.log[committedIndex+1-initIndex]
			rf.mu.Unlock()
		}

		applyCh <- applyMsg

		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
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
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister,
	applyCh chan ApplyMsg) *Raft {

	nextIndex := make([]int, len(peers))
	matchIndex := make([]int, len(peers))
	log := make([]Entries, 0)

	rf := &Raft{
		peers:          peers,
		persister:      persister,
		me:             me,
		votedFor:       -1,
		electedTimeOut: time.Millisecond * 300, //设定心跳超时时间,开始选举
		lastRevTime:    time.Now(),             //初始化为设置当前时间
		currentTerm:    0,
		isLeader:       false,
		nextIndex:      nextIndex,
		matchIndex:     matchIndex,
		committedIndex: 0,
		log:            log,
	}

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections

	go rf.ticker() //RequestVote RPC

	go rf.HeartBeats()

	go rf.apply(applyCh)

	return rf
}
