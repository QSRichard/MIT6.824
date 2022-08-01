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

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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
type LogEntry struct {
	Command interface{}
	Term    int
}

// 当前角色
const ROLE_LEADER = "Leader"
const ROLE_FOLLOWER = "Follower"
const ROLE_CANDIDATES = "Candidates"

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 所有服务器，持久化状态
	currentTerm       int 
	votedFor          int
	log               []LogEntry
	// lastIncludedIndex int
	// lastIncludedTerm  int

	// 所有服务器，易失状态
	// commitIndex int
	// lastApplied int

	//Leader，易失状态
	// nextIndex  []int //	每个follower的log同步起点索引（初始为leader log的最后一项）
	// matchIndex []int // 每个follower的log同步进度（初始为0），和nextIndex强关联

	// 所有服务器，选举相关状态
	role           string
	leaderId       int
	lastActiveTime time.Time
	lastBroadcastTime time.Time

	// applyCh chan ApplyMsg // 应用层的提交队列

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A).
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.role == ROLE_LEADER
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.currentTerm)
	// e.Encode(rf.votedFor)
	// e.Encode(rf.currentTerm)
	// e.Encode(rf.lastIncludedIndex)
	// e.Encode(rf.lastIncludedTerm)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
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
}


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}


//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
 
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
 
	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.role = ROLE_FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.leaderId = -1
	}
 
	// 投票
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		lastLogTerm := 0
		if len(rf.log) != 0 {
			lastLogTerm = rf.log[len(rf.log)-1].Term
		}
		if args.LastLogTerm < lastLogTerm || args.LastLogIndex < len(rf.log) {
			return
		}
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		// 重置时间
		rf.lastActiveTime = time.Now()
	}
	rf.persist()
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed(){

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using

		time.Sleep(1 * time.Millisecond)
		func(){
		rf.mu.Lock()
		defer rf.mu.Unlock()

		now := time.Now()
		// 随机设置超时时间
		timeout := time.Duration(5+rand.Int31n(15)) * time.Millisecond
		elapses := now.Sub(rf.lastActiveTime)

		// 超时 role 转变
		if rf.role == ROLE_FOLLOWER  && elapses > timeout{
			rf.role = ROLE_CANDIDATES
			DPrintf("RaftNode[%d] CurrentTerm %d To Candidate", rf.me, rf.currentTerm)
		}
		// 请求vote
		if rf.role == ROLE_CANDIDATES && elapses >= timeout {

			// routine 操作
			rf.lastActiveTime = now
			rf.currentTerm += 1
			rf.votedFor = rf.me
			rf.persist()

			// 请求投票req
			request := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: len(rf.log),
			}
			if len(rf.log) != 0 {
				request.LastLogTerm = rf.log[len(rf.log)-1].Term
			}

			rf.mu.Unlock()

			voteCount := 1   // 收到投票个数（先给自己投1票）
			finishCount := 1 // 收到应答个数
			voteResultChan := make(chan *RequestVoteReply, len(rf.peers))
				
			// 并发发送VoteRequest
			for peerId := 0; peerId < len(rf.peers); peerId++ {
				go func(id int) {
					DPrintf("RaftNode[%d] CurrentTerm %d Send VoteRequest", rf.me, rf.currentTerm)
					if id == rf.me {
						return
					}
					resp := RequestVoteReply{}
					if ok := rf.sendRequestVote(id, &request, &resp); ok {
						voteResultChan <- &resp
					} else {
						voteResultChan <- nil
					}
				}(peerId)
			}

			maxTerm := 0
			for {
				select {
				case reply := <-voteResultChan:
					finishCount += 1
					DPrintf("RaftNode[%d] CurrentTerm %d Select FinishCount %d", rf.me, rf.currentTerm, finishCount)
					if reply != nil {
						if reply.VoteGranted {
							voteCount += 1
						}
						if reply.Term > maxTerm {
							maxTerm = reply.Term
						}
					}
					if finishCount == len(rf.peers) || voteCount > len(rf.peers)/2 {
						DPrintf("RaftNode[%d] CurrentTerm %d Goto VoteEnd", rf.me, rf.currentTerm)
						goto VOTE_END
					}
				}
			}
		VOTE_END:
			rf.mu.Lock()
			// 如果角色改变了，则忽略本轮投票结果
			if rf.role != ROLE_CANDIDATES {
				DPrintf("RaftNode[%d] CurrentTerm %d in VoteEnd but role not candidate", rf.me, rf.currentTerm)
				return
			}
			// 发现了更高的任期，切回follower
			if maxTerm > rf.currentTerm {
				rf.role = ROLE_FOLLOWER
				rf.leaderId = -1
				rf.currentTerm = maxTerm
				rf.votedFor = -1
				rf.persist()
				DPrintf("RaftNode[%d] CurrentTerm %d in VoteEnd but maxTerm greater currentTerm", rf.me, rf.currentTerm)
				return
			}
			// 赢得大多数选票，则成为leader
			if voteCount > len(rf.peers)/2 {
				rf.role = ROLE_LEADER
				rf.leaderId = rf.me
				rf.lastBroadcastTime = time.Unix(0, 0) // 令appendEntries广播立即执行
				DPrintf("RaftNode[%d] CurrentTerm %d Goto Leader", rf.me, rf.currentTerm)
				return
			}
		}
	}()
}
}


func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		return
	}

	// 发现更大的任期，则转为该任期的follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = ROLE_FOLLOWER
		rf.votedFor = -1
		rf.leaderId = -1
		// 继续向下走
	}

	// 认识新的leader
	rf.leaderId = args.LeaderId
	// 刷新活跃时间
	rf.lastActiveTime = time.Now()

	rf.persist()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) runAppendEntriesLoop() {
	for !rf.killed() {
		time.Sleep(1 * time.Millisecond)
 
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
 
			if rf.role != ROLE_LEADER {
				return
			}
 
			//发送心跳包
			now := time.Now()
			if now.Sub(rf.lastBroadcastTime) < 100*time.Millisecond {
				return
			}
			rf.lastBroadcastTime = time.Now()
 
			for peerId := 0; peerId < len(rf.peers); peerId++ {
				if peerId == rf.me {
					continue
				}
 
				args := AppendEntriesArgs{}
				args.Term = rf.currentTerm
				args.LeaderId = rf.me

				go func(id int, args1 *AppendEntriesArgs) {
					DPrintf("RaftNode[%d] appendEntries starts, myTerm[%d] peerId[%d]", rf.me, args1.Term, id)
					reply := AppendEntriesReply{}
					if ok := rf.sendAppendEntries(id, args1, &reply); ok {
						rf.mu.Lock()
						defer rf.mu.Unlock()
						if reply.Term > rf.currentTerm { // 变成follower
							rf.role = ROLE_FOLLOWER
							rf.leaderId = -1
							rf.currentTerm = reply.Term
							rf.votedFor = -1
							rf.persist()
						}
						DPrintf("RaftNode[%d] appendEntries ends, peerTerm[%d] myCurrentTerm[%d] myRole[%s]", rf.me, reply.Term, rf.currentTerm, rf.role)
					}
				}(peerId, &args)
			}
		}()
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	// 初始设置
	rf.role = ROLE_FOLLOWER
	rf.leaderId = -1
	rf.votedFor = -1
	rf.lastActiveTime = time.Now()
 
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	// leader 动作 
	go rf.runAppendEntriesLoop()
	DPrintf("Raftnode[%d]启动", me)

	return rf
}
