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
	"6.5840/labgob"
	"bytes"
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

type NeedP struct {
	CurrentTerm int
	VotedFor    int
	Log         []Entries
	Committed   int
}

// TODO persist
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
	P := &NeedP{rf.currentTerm, rf.votedFor, rf.log, rf.committedIndex}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(P)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// TODO readPersist
// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	//Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var P NeedP
	d.Decode(&P)
	rf.currentTerm = P.CurrentTerm
	rf.votedFor = P.VotedFor
	rf.log = P.Log
	rf.committedIndex = P.Committed
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

// XXX AppendEntries
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = true
	rf.lastRevTime = time.Now()

	//判断是否leader过时
	//if args.Term < rf.currentTerm && args.LeaderCommit < rf.committedIndex {
	if args.Term < rf.currentTerm {
		//DPrintf("Args Term:%v", args.Term)
		//DPrintf("currentTerm:%v", rf.currentTerm)
		//DPrintf("ID:%v,leader过时消息护着index小，follower当前term：%v，candidate term:%v", args.LeaderId, rf.currentTerm, args.Term)
		reply.Success = false
		reply.Term = rf.currentTerm
		rf.persist()
		return
	}

	//entry不为空，说明是正式的leader发送的有消息的心跳，否则为上位宣称
	//或者二者刚刚同步，本次心跳没有新消息
	if len(args.Entries) != 0 {
		//DPrintf("收到有效内容")

		//正常消息，非第一条
		if len(rf.log) != 0 && args.PrevLogTerm != -1 {

			iniIndex := rf.log[0].Index //1

			//推算prevLogIndex对应的entry的实际log位置
			order := args.PrevLogIndex - iniIndex
			//0			1			1

			//检查prevLogIndex
			//if order < 0 || order >= len(rf.log) || rf.log[order].Term != args.PrevLogTerm {
			if order < 0 || order >= len(rf.log) || rf.log[order].Term != args.PrevLogTerm { //不匹配问题，不是要减next的地方
				reply.Success = false
				return
			}

			//修剪log
			rf.log = append(rf.log[:order+1], args.Entries...)
			rf.persist()
			DPrintf("received1,id:%v", rf.me)

		} else { //全场第一条消息

			////prevLogIndex为0，则合理，否错错误
			//if args.PrevLogIndex != 0 {
			//	reply.Success = false
			//	return
			//}
			//直接保留所有log
			rf.log = args.Entries
			rf.persist()
			DPrintf("received2,id:%v", rf.me)
		}

		//修改commitedIndex
		//如果没错，log已经有内容
		if args.LeaderCommit > rf.committedIndex {
			rf.committedIndex = min(args.LeaderCommit, rf.log[len(rf.log)-1].Index)
			rf.persist()
			DPrintf("follower,1committedIndex:%v", rf.committedIndex)
		}

	} else if args.LeaderCommit != -1 { //正常领导者的心跳
		if len(rf.log) != 0 {
			if args.LeaderCommit > rf.committedIndex && args.Term == rf.log[len(rf.log)-1].Term {
				rf.committedIndex = min(args.LeaderCommit, rf.log[len(rf.log)-1].Index)
				rf.persist()
				DPrintf("follower,2committedIndex:%v", rf.committedIndex)
			}
		} else if args.LeaderCommit > rf.committedIndex {
			rf.committedIndex = 0
			rf.persist()
			DPrintf("follower,3committedIndex:%v", rf.committedIndex)
		}
	}

	//同步term
	rf.currentTerm = args.Term
	rf.persist()
	rf.isLeader = false
	//重置心跳时间和是否投票
	rf.lastRevTime = time.Now()
	rf.votedFor = -1
	rf.persist()
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

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 创建一个结果通道
	resultChan := make(chan bool)

	// 启动一个Goroutine进行RPC调用
	go func() {
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		resultChan <- ok
	}()

	select {
	case result := <-resultChan:
		//DPrintf("%vsendAppendEntries有结果", server)
		//成功的话更新matchindex表
		if reply.Success {
			if len(args.Entries) != 0 {
				rf.matchIndex[server] = args.Entries[len(args.Entries)-1].Index
				rf.nextIndex[server] = args.Entries[len(args.Entries)-1].Index + 1
			}
		} else {
			if rf.currentTerm < reply.Term { //leader过时导致失败
				rf.isLeader = false

				DPrintf("id%v被发现超时，term由%v到%v", rf.me, rf.currentTerm, reply.Term)
				rf.currentTerm = reply.Term
				rf.log = rf.log[:rf.committedIndex]
				rf.persist()
			} else { //其他失败
				DPrintf("id:%v,减nextIndex", server)
				rf.nextIndex[server]--
			}
		}

		return result // RPC调用成功，返回结果
	case <-time.After(10 * time.Millisecond):
		//DPrintf("%vsendAppendEntries超时", server)
		return false // 超时，返回false
	}
}

type AppendEntriesResult struct {
	Success bool
	Number  int
	TimeOut bool
}

// XXX 维持心跳。leader才有
func (rf *Raft) HeartBeats() {
	for rf.killed() == false {
		for rf.isLeader == true && rf.killed() == false {
			rf.mu.Lock()
			//nums := len(rf.peers)
			//resultCh := make(chan AppendEntriesResult, nums)
			peers := rf.peers
			me := rf.me
			//DPrintf("me:%v", me)
			currentTerm := rf.currentTerm
			matchIndex := rf.matchIndex //0
			nextIndex := rf.nextIndex   //2

			//DPrintf("matchIndex:%v", matchIndex)

			//commitedIndex := rf.committedIndex
			entries := rf.log
			leaderCommit := rf.committedIndex
			rf.lastRevTime = time.Now()
			iniIndex := 0
			if len(entries) != 0 {
				iniIndex = entries[0].Index //1
			}
			rf.mu.Unlock()

			//DPrintf("entries:%v", entries)

			for i, _ := range peers {
				if i != me {
					go func(i int) {
						args := &AppendEntriesArgs{
							Term:         currentTerm,
							LeaderId:     me,
							PrevLogIndex: nextIndex[i] - 1, //1
							LeaderCommit: leaderCommit,
						}
						//DPrintf("matchIndex:%v", matchIndex)
						//DPrintf("nextIndex:%v", nextIndex)
						prevLogTerm := 0

						//如果有新消息，log一定存在 全是leader信息
						//if matchIndex[me] > commitedIndex && len(entries) != 0 {
						if nextIndex[i] < matchIndex[me]+1 && len(entries) != 0 { //有过start的leader

							//考虑消息累计，leader有多个还没发给follower
							//如何判断是别人的全场第一条：
							//nextIndex是否为1，已经判断过leader是否有内容
							if nextIndex[i] == 1 { //第一条
								args.Entries = entries
							} else if nextIndex[i] <= 0 { //全覆盖
								args.Entries = entries
								prevLogTerm = -1
							} else if nextIndex[i] > len(entries) { //leader的日志比follower的短，直接覆盖？
								args.Entries = entries
								prevLogTerm = -1
							} else { //不止一条的话传follower没有的
								//这里要使用nextIndex了
								//DPrintf("id:%v,nextIndex:%v,initIndex:%v", i, nextIndex[i], iniIndex)
								args.Entries = entries[nextIndex[i]-iniIndex:] //2-1

								//如果新的leader，match清零怎么办
								prevLogTerm = entries[nextIndex[i]-iniIndex-1].Term
							}

							//if len(entries) == 1 { //只有一条的话肯定就是这一条
							//	//leader只有一条，则全场第一条
							//	args.Entries = entries
							//	//prevLogTerm = entries[0].Term
							//	//prevLogTerm = 0
							//
							//} else { //不止一条的话传follower没有的
							//	//这里要使用nextIndex了
							//	args.Entries = entries[nextIndex[i]-iniIndex:] //2-1
							//
							//	//如果新的leader，match清零怎么办
							//	prevLogTerm = entries[nextIndex[i]-iniIndex-1].Term
							//}
						}

						//即使没有新消息，也应该根据follower的nextIndex和自己的matchIndex来发送follower没有的日志
						//if nextIndex[i]<matchIndex[me]-1 {
						//
						//}
						if matchIndex[me] != 0 { //没有start过的新leader

						}
						//新继任的leader也应该同步

						args.PrevLogTerm = prevLogTerm
						reply := &AppendEntriesReply{}

						//DPrintf("prev:(%v,%v)", args.PrevLogIndex, args.PrevLogTerm)
						//DPrintf("args entry:%v", args.Entries)
						//ok := rf.sendAppendEntries(i, args, reply)
						rf.sendAppendEntries(i, args, reply)

						// 将 reply 的 success 状态发送到 channel 中
						//resultCh <- AppendEntriesResult{
						//	TimeOut: ok,
						//	Success: reply.Success,
						//	Number:  reply.Term,
						//}
					}(i)

				}
			}
			// 收集所有协程的结果
			//stop := false
			//
			//for j := 0; j < nums-1; j++ {
			//	result := <-resultCh
			//	if !result.Success && !result.TimeOut { //说明自己的term小于被请求对象
			//		stop = true
			//		//DPrintf("%v恢复follower,", rf.me)
			//		rf.mu.Lock()
			//		rf.isLeader = false
			//		rf.currentTerm = max(result.Number, rf.currentTerm)
			//		rf.lastRevTime = time.Now()
			//		rf.mu.Unlock()
			//		break
			//	}
			//}

			//if stop {
			//	break
			//}
			time.Sleep(40 * time.Millisecond)
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
	defer rf.persist()
	//在接到投票请求是是否刷新心跳超时
	//rf.lastRevTime = time.Now()

	//DPrintf("请求来自：%v,term:%v，本机ID：%v,本机当前term：%v", args.CandidateId, args.Term, rf.me, rf.currentTerm)
	//如果candidate的term小于当前的term，返回false，并且告知candidate当前的term
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		//DPrintf("%v过时投票", args.CandidateId)
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
	//lastLogTerm := 0

	//说明已经收到过信息，log有内容
	if lastLogIndex > 0 {
		//lastLogTerm = rf.log[lastLogIndex-1].Term
	}

	//判断日志新旧
	//if (args.LastLogTerm > lastLogTerm) ||args.LastLogIndex >= lastLogIndex {
	if args.LastLogIndex >= lastLogIndex {
		rf.votedFor = args.CandidateId

		reply.VoteGranted = true
		//DPrintf("投票给%v", args.CandidateId)
		rf.lastRevTime = time.Now()
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

// XXX Start
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

	//首先判断leader
	if !rf.isLeader {
		return
	}
	isLeader = true

	DPrintf("leader：%v，command:%v", rf.me, command)

	////检测是否已经提交
	//if command == rf.log[rf.committedIndex].Command {
	//	index = rf.committedIndex
	//	return
	//}

	rf.matchIndex[rf.me] = rf.nextIndex[rf.me]
	//			1					1
	//DPrintf("matchIndex:%v", rf.matchIndex)

	index = rf.nextIndex[rf.me]
	term = rf.currentTerm
	//			1
	//DPrintf("NextIndex:%v", rf.nextIndex)

	rf.nextIndex[rf.me]++
	//        2

	//DPrintf("%v", command)

	entry := Entries{
		Command: command,
		Index:   index,
		Term:    rf.currentTerm,
	}

	//将新的entry加入log
	rf.log = append(rf.log, entry)
	//DPrintf("leaderLog:%v", rf.log)
	// Your code here (3B).

	rf.persist()

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

		if !rf.isLeader {

			//超时
			if time.Since(rf.lastRevTime) > rf.electedTimeOut {
				//DPrintf("%v超时,开始选举,时间：%v", rf.me, time.Since(rf.lastRevTime))
				rf.startElection()
			}
		}
		// pause for a random amount of time between 150 and 500
		// milliseconds.
		ms := 250 + (rand.Int63() % 150)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// XXX 选举
func (rf *Raft) startElection() {
	var LastLogIndex, LastLogTerm int
	//首先对rf进行读取
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	sucNum := len(rf.peers) / 2
	if len(rf.log) == 0 {
		LastLogIndex = 0
		LastLogTerm = 0
	} else {
		LastLogIndex = rf.log[len(rf.log)-1].Index
		LastLogTerm = rf.log[len(rf.log)-1].Term
	}
	rf.persist()
	defer rf.mu.Unlock()
	defer rf.persist()
	//DPrintf("ID:%v;Term:%v开始竞选，需要票数%v", rf.me, rf.currentTerm, sucNum)
	num := 0

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: LastLogIndex,
		LastLogTerm:  LastLogTerm,
	}

	c := make(chan bool)

	for i, _ := range rf.peers {
		if i != rf.me {
			i := i
			//DPrintf("请求%v投票", i)
			reply := &RequestVoteReply{}
			go func() {

				rf.sendRequestVote(i, args, reply)

				if reply.VoteGranted {
					num++
				} else if reply.Term > rf.currentTerm { //如果没有被投票且发现自己的term过时

					//DPrintf("candidate %v 发现自己不配当candidate,当前term：%v，别人term：%v", rf.me, rf.currentTerm, reply.Term)
					rf.currentTerm = reply.Term
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
	case <-c: //当选

		lastLogIndex := 1

		//之前收到过消息
		if len(rf.log) != 0 {
			//DPrintf("%v", rf.log)
			lastLogIndex = rf.log[len(rf.log)-1].Index + 1
		}

		DPrintf("leader%v nextIndex:%v,log:%v", rf.me, lastLogIndex, rf.log)

		//初始化nextIndex 初始化matchIndex
		for i, _ := range rf.nextIndex {
			rf.nextIndex[i] = lastLogIndex
			rf.matchIndex[i] = 0

		}
		rf.isLeader = true

		//DPrintf("%v成功,当前term%v", candidateID, currentTerm)
		for i, _ := range rf.peers {
			if i != rf.me {
				args := &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					LeaderCommit: -1} //使用leadercommitted为-1来象征宣称信息
				reply := &AppendEntriesReply{}
				go rf.sendAppendEntries(i, args, reply)
			}
		}

	case <-time.After(50 * time.Millisecond): //超时
		//rf.mu.Lock()
		//rf.currentTerm--
		//rf.mu.Unlock()
		//DPrintf("%v选举超时或者不成功", candidateID)
	}
	rf.votedFor = -1
}

// XXX apply
func (rf *Raft) apply(applyCh chan ApplyMsg) {
	for rf.killed() == false {

		//leader角度
		for rf.isLeader && rf.killed() == false {
			rf.mu.Lock()
			//me := rf.me
			committedIndex := rf.committedIndex
			matchIndex := rf.matchIndex
			th := len(rf.peers) / 2
			lastApplied := rf.lastApplied
			rf.mu.Unlock()

			counts := 0
			//DPrintf("id:%v,committedIndex:%v", me, committedIndex)
			for _, v := range matchIndex {
				if committedIndex != -1 && v > committedIndex {
					counts++
				}
			}

			applyMsg := ApplyMsg{
				CommandValid: true,
				//Command:      rf.log[committedIndex].Command,
				CommandIndex: lastApplied + 1,
			}
			if counts > th {
				rf.mu.Lock()
				rf.committedIndex++
				rf.persist()
				DPrintf("leader,id:%v,log:%v,committedIndex:%v", rf.me, rf.log, rf.committedIndex)
				initIndex := rf.log[0].Index
				applyMsg.Command = rf.log[committedIndex+1-initIndex].Command
				//DPrintf("id:%v,commandIndex:%v", rf.me, committedIndex+1)

				//DPrintf("state:%v,一条消息过半认同", rf.isLeader)
				applyCh <- applyMsg

				rf.lastApplied++
				rf.mu.Unlock()
			}
			time.Sleep(30 * time.Millisecond)
		}
		//follower角度
		for !rf.isLeader && rf.killed() == false {
			rf.mu.Lock()
			//DPrintf("follower:%v try apply", rf.me)
			committedIndex := rf.committedIndex
			lastApplied := rf.lastApplied
			log := rf.log

			if committedIndex > lastApplied {
				//1					0

				DPrintf("follower,id:%v,log:%v,committedIndex:%v", rf.me, rf.log, rf.committedIndex)
				iniIndex := 1
				//DPrintf("log:%v", log)
				if len(log) != 0 {
					iniIndex = rf.log[0].Index
				}
				//DPrintf("id:%v,commandIndex:%v", rf.me, committedIndex)

				//DPrintf("commitedIndex:%v", committedIndex)
				applyMsg := ApplyMsg{
					CommandValid: true,
					Command:      log[lastApplied+1-iniIndex].Command,
					CommandIndex: lastApplied + 1,
				}

				//DPrintf("state:%v,leader确认commited，apply", rf.isLeader)
				applyCh <- applyMsg

				rf.lastApplied++

			}
			rf.mu.Unlock()
			time.Sleep(30 * time.Millisecond)
		}
		time.Sleep(20 * time.Millisecond)
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
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister,
	applyCh chan ApplyMsg) *Raft {

	nextIndex := make([]int, len(peers))
	for i, _ := range nextIndex {
		nextIndex[i] = 1
	}
	matchIndex := make([]int, len(peers))
	log := make([]Entries, 0)

	rf := &Raft{
		peers:          peers,
		persister:      persister,
		me:             me,
		votedFor:       -1,
		electedTimeOut: time.Millisecond * 400, //设定心跳超时时间,开始选举
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
