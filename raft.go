package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// create a new Raft server.
//		rf = Make(...)
// start agreement on a new log entry
//		rf.Start(command interface{}) (Index, Term, isleader)
// ask a Raft for its current Term, and whether it thinks it is leader
//		rf.GetState() (Term, isLeader)
// each time a new entry is committed to the log, each Raft peer should send
// an ApplyMsg to the service (or tester) in the same server.
//		ApplyMsg
//

import (
	"math/rand"
	"labrpc"
	"sync"
	"time"
	"fmt"
	// "sort"
)
// import "bytes"
// import "labgob"



func min(a int, b int)int{
	if a <= b{
		return a
	}
	return b
}

//
// as each Raft peer becomes aware that successive log Entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	Snapshot	 []byte
}

type LogEntry struct {
	Command     interface{}
	Term 		int
	Index		int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's Index into peers[]

	applyCh chan ApplyMsg         // Channel for the commit to the state machine

	// Your data here (3, 4).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//Persistent state on all servers
	currentTerm	int //latest Term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor		int //CandidateId that received vote in current Term (or null if none)
	log					[]LogEntry


	//Voltile state on all servers
	commitIndex int
	lastApplied int

	//Volatile state on leaders (Reinitialized after election)
	nextIndex []int
	matchIndex []int

	state int //0-0; 1-1; 2-2
	count int // number of voters
	timer *time.Timer // Timer

	isKilled bool // kill the raft
	isKilledChan chan bool

	//channel
	heartbeatCh chan int
	isLeaderCh chan bool
	cmdCh chan bool
}




//election time
func duration() time.Duration{
	return time.Millisecond*time.Duration(rand.Int()%200+400)
}

/** heartbeat time */
func duration_heartbeat() time.Duration{
	return time.Millisecond*100
}


func (rf *Raft) convertToFollower(Term int){
	rf.currentTerm = Term
	rf.state = 0
	rf.count = 0
	rf.votedFor = -1
}


func (rf *Raft) asFollower(){
	rf.mu.Lock()
	fmt.Println(rf.me, " follower")
	fmt.Println(rf.me, " ",rf.log)
	fmt.Println()
	rf.mu.Unlock()
	select{
	case <-time.After(duration()):
		rf.mu.Lock()
		rf.state = 1
		rf.mu.Unlock()
	case <-rf.heartbeatCh:
	}
}

func (rf *Raft) asCandidate(){
	rf.mu.Lock()
	fmt.Println(rf.me, " candidate")
	fmt.Println(rf.me, " ",rf.log)
	fmt.Println()
	rf.mu.Unlock()
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.count = 1
	rf.mu.Unlock()
	rf.sendRequestVoteAll()
	select{
	case isLeader := <-rf.isLeaderCh:
		if isLeader{
			rf.mu.Lock()
			rf.state = 2
			for i := 0; i < len(rf.peers); i++{
				rf.matchIndex[i] = 0
				rf.nextIndex[i] = len(rf.log) + 1
			}
			go rf.send_Append_Entries_To_All()
			rf.mu.Unlock()
		}

	// Rules for server, If AppendEtries RPC received from new leader: convert to Follower
	case currentTerm := <-rf.heartbeatCh:
		rf.mu.Lock()
		rf.convertToFollower(currentTerm)
		rf.mu.Unlock()
	case <- time.After(duration()):
	}
}



func (rf *Raft) asLeader(){
	rf.mu.Lock()
	fmt.Println(rf.me, " leader")
	fmt.Println(rf.me, " ", rf.log)
	fmt.Println()
	rf.mu.Unlock()
	select{
	case <-time.After(duration_heartbeat()):
		rf.send_Append_Entries_To_All()
	case <-rf.cmdCh:
		fmt.Println(rf.me, " leader received command")
		fmt.Println(rf.me, " ", rf.log)
		fmt.Println()
		rf.send_Append_Entries_To_All()
	}
}


type AppendEntriesArgs struct {
	Term			int //leaders term
	LeaderId		int //so followers can redirect clients
	PrevLogIndex	int //index of log entry immediately preceding new ones
	PrevLogTerm	int //term of prevlogindex entry
	Entries []LogEntry //log entries to store
	LeaderCommit	int //leaders commit index
}

type AppendEntriesReply struct {
	Term		int // current term for leader to update itself
	Success		bool //
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func(rf *Raft) send_Append_Entries_To_One(ind int){
	rf.mu.Lock()
	// fmt.Println(ind, "xd0")
	if rf.state != 2{
		rf.mu.Unlock()
		return
	}
	term := rf.currentTerm
	entries := make([]LogEntry,0)
	if (len(rf.log) != 0){
		entries = rf.log[rf.nextIndex[ind] - 1:]
	}
	// fmt.Println(ind, " xd2")
	prevLogIndex := rf.matchIndex[ind]
	prevLogTerm := 0
	if prevLogIndex != 0{
		prevLogTerm = rf.log[prevLogIndex - 1].Term
	}
	leaderCommit := rf.commitIndex
	rf.mu.Unlock()

	args := AppendEntriesArgs{
		term,
		rf.me,
		prevLogIndex,
		prevLogTerm,
		entries,
		leaderCommit,
	}
	reply := AppendEntriesReply{}
	ok := false
	for !ok{
		rf.mu.Lock()
		state := rf.state
		killed := rf.isKilled
		rf.mu.Unlock()
		if killed || (state != 2){break}
		ok = rf.sendAppendEntries(ind, &args, &reply)
	}

	if (!reply.Success) && (reply.Term > term){
		rf.mu.Lock()
		rf.convertToFollower(reply.Term)
		rf.mu.Unlock()
		return
	}
	if(!reply.Success){
		rf.mu.Lock()
		rf.nextIndex[ind] -= 1
		rf.mu.Unlock()
		go rf.send_Append_Entries_To_One(ind)
		return
	}
	if(reply.Success){
		rf.mu.Lock()
		rf.nextIndex[ind] = len(rf.log) + 1
		rf.matchIndex[ind] = len(rf.log)
		rf.mu.Unlock()
		return
	}

}


func (rf *Raft) send_Append_Entries_To_All(){
	rf.mu.Lock()
	state := rf.state
	rf.mu.Unlock()
	if state != 2{
		return
	}

	for ind, _ := range rf.peers{
		rf.mu.Lock()
		rf_state := rf.state
		isKilled := rf.isKilled
		rf.mu.Unlock()
		if (rf_state != 2 || isKilled == true){
			break
		}
		if ind == rf.me{
			continue
		}
		go rf.send_Append_Entries_To_One(ind)
	}
}


func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	fmt.Println(rf.me, "haha1")
	fmt.Println(rf.me, args.Entries)
	rf_currentTerm := rf.currentTerm
	rf.mu.Unlock()

	reply.Term = rf_currentTerm
	reply.Success = false

	if args.Term > rf_currentTerm{
		rf.mu.Lock()
		rf.convertToFollower(args.Term)
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
	}
	// fmt.Println(rf.me, "haha2")

	//rule 1 Reply false if Term < currentTerm
	if args.Term <rf_currentTerm{
		return
	}

	//rule 2 reply flase if log doesn't contain an entry at prevLogIndex
	rf.mu.Lock()
	if (len(rf.log) < args.PrevLogIndex){
		rf.mu.Unlock()
		return
	}
	// fmt.Println(rf.me, "haha3")
	if (args.PrevLogIndex != 0 ) &&(rf.log[args.PrevLogIndex - 1].Term != args.PrevLogTerm){
		rf.log = rf.log[:(args.PrevLogIndex - 1)]
		rf.mu.Unlock()
		return
	}
	// fmt.Println(rf.me, "haha4")
	rf.mu.Unlock()

	//rule 4 append any new entreis not in log
	rf.mu.Lock()
	reply.Success = true
	for _, e := range args.Entries{
		rf.log = append(rf.log, e)
	}
	if args.LeaderCommit > rf.commitIndex{rf.commitIndex = min(args.LeaderCommit,len(rf.log))}
	rf.mu.Unlock()
	rf.heartbeatCh<-rf_currentTerm
}

//
// return currentTerm and whether this server
// believes it is the leader.
//
func (rf *Raft) GetState() (int, bool) {
	var Term int
	var isleader bool

	// Your code here (3).
	rf.mu.Lock()
	Term = rf.currentTerm
	state:=rf.state
	rf.mu.Unlock()

	if state == 2{
		isleader = true
	}else{
		isleader = false
	}

	return Term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (4).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)


	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.currentTerm)
	// e.Encode(rf.votedFor)
	// e.Encode(rf.log)
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
	// Your code here (4).
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



	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var currentTerm int
	// var votedFor int
	// var log []*LogEntry
	//
	// if d.Decode(&currentTerm) != nil ||
	// 	d.Decode(&votedFor) != nil ||
	// 	d.Decode(&log) != nil{
	// 		return
	// } else{
	// 	rf.currentTerm = currentTerm
	// 	rf.votedFor = votedFor
	// 	rf.log = log
	// }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (3, 4).
	Term int //cadidates term
	CandidateId int //candidate requesting vote
	LastLogIndex int //index of candidates last log entry
	LastLogTerm int //term of candidates last log entry

}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (3).
	Term		int		//currentTerm, for candidate to update itself
	VoteGranted	bool	//true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3, 4).
	rf.mu.Lock()
	rf_currentTerm := rf.currentTerm
	rf_votedFor := rf.votedFor
	reply.Term = rf.currentTerm
	rf.mu.Unlock()

	reply.VoteGranted = false
	if args.Term <rf_currentTerm{
		return
	}

	rf.mu.Lock()
	if args.Term > rf_currentTerm {
		rf.convertToFollower(args.Term)
	}
	rf_votedFor = rf.votedFor
	if rf_votedFor == -1 || rf_votedFor == args.CandidateId{
		term := 0
		if len(rf.log) != 0{
			term = rf.log[len(rf.log) - 1].Term
		}
		if ((term == args.LastLogTerm) && (len(rf.log) > args.LastLogIndex)) || (term > args.LastLogTerm){
			rf.mu.Unlock()
			return
		}
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
		}
	rf.mu.Unlock()
}

//
// example code to send a RequestVote RPC to a server.
// server is the Index of the target server in rf.peers[].
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

//send request vote to all server
func (rf *Raft) sendRequestVoteAll(){
	rf.mu.Lock()
	rf.count = 1
	rf_me := rf.me
	rf_state := rf.state
	rf.mu.Unlock()

	for i:=0;i<len(rf.peers);i++{
		if i == rf_me || rf_state != 1{
			continue
		}
		go func(rf *Raft, i int){
			rf.mu.Lock()
			term := 0
			if len(rf.log) != 0{
				term = rf.log[len(rf.log)-1].Term
			}
			args := RequestVoteArgs{
																rf.currentTerm,
																rf.me,
																len(rf.log),
																term,
															}
			rf.mu.Unlock()

			reply := RequestVoteReply{}
			rf.sendRequestVote(i,&args,&reply)

			if reply.VoteGranted == true{
				rf.mu.Lock()
				rf.count++
				rf_count := rf.count
				len_peers := len(rf.peers)
				rf.mu.Unlock()
				if rf_count == len_peers/2+len_peers%2{
					rf.isLeaderCh<-true
				}
			}else{
				rf.mu.Lock()
				if reply.Term > rf.currentTerm{
					rf.currentTerm = reply.Term
					rf.state = 0
					rf.votedFor = -1
					rf.count = 0
				}
				rf.mu.Unlock()
			}
		}(rf,i)
	}
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
// the first return value is the Index that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	Index := -1
	Term := -1

	// Your code here (4).
	rf.mu.Lock()
	if (rf.state != 2) {
		rf.mu.Unlock()
		return Index, Term, false
	}else{
		fmt.Println(rf.me, " received command")
		Term = rf.currentTerm
		rf.log = append(rf.log,LogEntry{command,rf.currentTerm, len(rf.log) + 1})
		Index = len(rf.log)
		rf.nextIndex[rf.me] = len(rf.log) + 1
		rf.matchIndex[rf.me] = len(rf.log)

		rf.cmdCh <- true
	}

	rf.mu.Unlock()

	return Index, Term, true
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.mu.Lock()
	rf.isKilled = true
	rf.mu.Unlock()
	close(rf.isKilledChan)
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
	rf.isKilled = false

	rf.applyCh = applyCh

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry,0)
	rf.nextIndex = make([]int,len(rf.peers))
	rf.matchIndex = make([]int,len(rf.peers))
	for i := 0; i < len(rf.peers); i++{
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = len(rf.log) + 1
	}

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.state = 0
	rf.timer = time.NewTimer(duration())
	rf.count = 0

	rf.heartbeatCh = make(chan int)
	rf.isLeaderCh = make(chan bool)
	rf.isKilledChan = make(chan bool)
	rf.cmdCh = make(chan bool)


	go func (rf * Raft) {
		for true {
			select{
			case <- rf.isKilledChan:
				return
			default:
				rf.mu.Lock()
				rf_state:=rf.state
				rf.mu.Unlock()
				switch rf_state{
				case 0:
					rf.asFollower()
				case 1:
					rf.asCandidate()
				case 2:
					rf.asLeader()
				}
			}
		}
	}(rf)
	rf.readPersist(persister.ReadRaftState())

	return rf
}
