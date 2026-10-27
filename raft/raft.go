package raft

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Config configures a Raft node.
type Config struct {
	ID    int   // this node's ID
	Peers []int // IDs of every node in the cluster, including this one

	ElectionTimeoutMin time.Duration // default 300ms
	ElectionTimeoutMax time.Duration // default 600ms
	HeartbeatInterval  time.Duration // default 50ms
	RPCTimeout         time.Duration // default ElectionTimeoutMin/3
	MaxEntriesPerRPC   int           // default 256

	Logger *slog.Logger
}

func (c *Config) setDefaults() {
	if c.ElectionTimeoutMin == 0 {
		c.ElectionTimeoutMin = 300 * time.Millisecond
	}
	if c.ElectionTimeoutMax <= c.ElectionTimeoutMin {
		c.ElectionTimeoutMax = 2 * c.ElectionTimeoutMin
	}
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = 50 * time.Millisecond
	}
	if c.RPCTimeout == 0 {
		// Well under the election timeout, so one lost message can't hold
		// up heartbeats long enough to trigger an election.
		c.RPCTimeout = c.ElectionTimeoutMin / 3
	}
	if c.MaxEntriesPerRPC == 0 {
		c.MaxEntriesPerRPC = 256
	}
	if c.Logger == nil {
		c.Logger = slog.New(slog.DiscardHandler)
	}
}

// Raft is a single Raft node.
type Raft struct {
	mu        sync.Mutex
	cfg       Config
	id        int
	peers     []int // other nodes
	transport Transport
	storage   Storage
	applyCh   chan<- ApplyMsg
	log       *slog.Logger
	rng       *rand.Rand

	// Persistent state (mirrored in storage).
	currentTerm uint64
	votedFor    int
	entries     []LogEntry // entries[0] is a sentinel holding the snapshot's index and term
	snapshot    []byte

	// Volatile state.
	role             Role
	leaderID         int
	commitIndex      uint64
	lastApplied      uint64
	electionDeadline time.Time
	leaderContact    time.Time // last time we heard from a valid leader
	preVoteRound     uint64
	pendingSnapshot  *Snapshot // installed but not yet delivered to the service

	// Leader state.
	nextIndex   map[int]uint64
	matchIndex  map[int]uint64
	lastAck     map[int]time.Time // when each peer last answered in this term
	selfDurable uint64            // our own log is fsynced up to here

	// Durability. Writes happen under mu so they reach storage in order;
	// fsyncs happen outside mu so a slow disk doesn't stall heartbeats.
	writeSeq  uint64        // number of storage writes so far (under mu)
	syncMu    sync.Mutex    // one fsync at a time; waiters share the result
	syncedSeq atomic.Uint64 // writes up to here are durable
	syncCh    chan struct{} // wakes the leader's log syncer

	applyCond   *sync.Cond
	replicateCh map[int]chan struct{}
	stopCh      chan struct{}
	stopped     bool
	wg          sync.WaitGroup
}

// New creates a node, restores any persisted state and starts it.
// Committed entries and snapshots are delivered on applyCh in log order.
func New(cfg Config, transport Transport, storage Storage, applyCh chan<- ApplyMsg) (*Raft, error) {
	cfg.setDefaults()
	ps, err := storage.Load()
	if err != nil {
		return nil, fmt.Errorf("raft: load state: %w", err)
	}
	rf := &Raft{
		cfg:         cfg,
		id:          cfg.ID,
		transport:   transport,
		storage:     storage,
		applyCh:     applyCh,
		log:         cfg.Logger.With("node", cfg.ID),
		rng:         rand.New(rand.NewSource(time.Now().UnixNano() + int64(cfg.ID))),
		currentTerm: ps.HardState.Term,
		votedFor:    ps.HardState.VotedFor,
		snapshot:    ps.Snapshot.Data,
		role:        Follower,
		leaderID:    None,
		replicateCh: map[int]chan struct{}{},
		stopCh:      make(chan struct{}),
		syncCh:      make(chan struct{}, 1),
	}
	if ps.HardState.Term == 0 {
		rf.votedFor = None // nobody can be voted for in term 0
	}
	rf.entries = append([]LogEntry{{Index: ps.Snapshot.Index, Term: ps.Snapshot.Term}}, ps.Entries...)
	rf.commitIndex = ps.Snapshot.Index
	rf.lastApplied = ps.Snapshot.Index
	if ps.Snapshot.Index > 0 {
		snap := ps.Snapshot
		rf.pendingSnapshot = &snap // delivered to the service before any entries
	}
	for _, p := range cfg.Peers {
		if p != cfg.ID {
			rf.peers = append(rf.peers, p)
			rf.replicateCh[p] = make(chan struct{}, 1)
		}
	}
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.resetElectionTimerLocked()

	rf.wg.Add(3 + len(rf.peers))
	go rf.ticker()
	go rf.applier()
	go rf.leaderSyncer()
	for _, p := range rf.peers {
		go rf.replicator(p)
	}
	return rf, nil
}

// Stop shuts the node down. It does not close applyCh.
func (rf *Raft) Stop() {
	rf.mu.Lock()
	if rf.stopped {
		rf.mu.Unlock()
		return
	}
	rf.stopped = true
	close(rf.stopCh)
	rf.applyCond.Broadcast()
	rf.mu.Unlock()
	rf.wg.Wait()
}

// ---- log helpers (caller holds mu) ----

func (rf *Raft) firstIndex() uint64 { return rf.entries[0].Index }
func (rf *Raft) lastIndex() uint64  { return rf.entries[len(rf.entries)-1].Index }
func (rf *Raft) lastTerm() uint64   { return rf.entries[len(rf.entries)-1].Term }

// termAt returns the term of the entry at index i, which must be in
// [firstIndex, lastIndex].
func (rf *Raft) termAt(i uint64) uint64 { return rf.entries[i-rf.firstIndex()].Term }

// slice returns a copy of entries in [lo, hi].
func (rf *Raft) slice(lo, hi uint64) []LogEntry {
	if lo > hi {
		return nil
	}
	f := rf.firstIndex()
	return append([]LogEntry(nil), rf.entries[lo-f:hi-f+1]...)
}

func (rf *Raft) quorum() int { return (len(rf.peers)+1)/2 + 1 }

func (rf *Raft) mustPersist(err error) {
	if err != nil {
		// Continuing without durable state could break safety, so crash.
		panic(fmt.Sprintf("raft %d: storage failure: %v", rf.id, err))
	}
}

func (rf *Raft) persistHardStateLocked() {
	rf.mustPersist(rf.storage.SaveHardState(HardState{Term: rf.currentTerm, VotedFor: rf.votedFor}))
	rf.writeSeq++
}

func (rf *Raft) writeEntriesLocked(entries []LogEntry) {
	rf.mustPersist(rf.storage.AppendEntries(entries))
	rf.writeSeq++
}

func (rf *Raft) markSynced(seq uint64) {
	for {
		cur := rf.syncedSeq.Load()
		if seq <= cur || rf.syncedSeq.CompareAndSwap(cur, seq) {
			return
		}
	}
}

// syncTo makes writes up to seq durable. Called without mu. Concurrent
// callers coalesce: whoever gets syncMu first fsyncs for everyone queued.
func (rf *Raft) syncTo(seq uint64) {
	if rf.syncedSeq.Load() >= seq {
		return
	}
	rf.syncMu.Lock()
	defer rf.syncMu.Unlock()
	if rf.syncedSeq.Load() >= seq {
		return
	}
	rf.mu.Lock()
	target := rf.writeSeq // everything written so far is covered by this fsync
	rf.mu.Unlock()
	rf.mustPersist(rf.storage.Sync())
	rf.markSynced(target)
}

// syncAll makes every write so far durable. Called without mu.
func (rf *Raft) syncAll() {
	rf.mu.Lock()
	seq := rf.writeSeq
	rf.mu.Unlock()
	rf.syncTo(seq)
}

// leaderSyncer fsyncs the leader's own new entries in the background, in
// parallel with sending them to followers (Raft thesis 10.2.1). The leader
// counts itself toward a commit only for entries that are durable.
func (rf *Raft) leaderSyncer() {
	defer rf.wg.Done()
	for {
		select {
		case <-rf.stopCh:
			return
		case <-rf.syncCh:
		}
		rf.mu.Lock()
		if rf.role != Leader {
			rf.mu.Unlock()
			continue
		}
		seq, idx, term := rf.writeSeq, rf.lastIndex(), rf.currentTerm
		rf.mu.Unlock()

		rf.syncTo(seq)

		rf.mu.Lock()
		if !rf.stopped && rf.role == Leader && rf.currentTerm == term && idx > rf.selfDurable {
			rf.selfDurable = idx
			rf.advanceCommitLocked()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) resetElectionTimerLocked() {
	span := rf.cfg.ElectionTimeoutMax - rf.cfg.ElectionTimeoutMin
	rf.electionDeadline = time.Now().Add(rf.cfg.ElectionTimeoutMin + time.Duration(rf.rng.Int63n(int64(span))))
}

// stepDownLocked moves to a newer term (if term is newer) and becomes a follower.
func (rf *Raft) stepDownLocked(term uint64) {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = None
		rf.persistHardStateLocked()
	}
	if rf.role != Follower {
		rf.log.Debug("stepping down", "term", rf.currentTerm, "was", rf.role)
	}
	rf.role = Follower
}

// ---- elections ----

func (rf *Raft) ticker() {
	defer rf.wg.Done()
	t := time.NewTicker(10 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-rf.stopCh:
			return
		case <-t.C:
		}
		rf.mu.Lock()
		if rf.role != Leader && time.Now().After(rf.electionDeadline) {
			rf.startPreVoteLocked()
		} else if rf.role == Leader && !rf.hasQuorumLocked() {
			// Check quorum: a leader that can't reach a majority steps
			// down so clients stop waiting on it.
			rf.log.Info("lost quorum, stepping down", "term", rf.currentTerm)
			rf.stepDownLocked(rf.currentTerm)
			rf.leaderID = None
			rf.resetElectionTimerLocked()
		}
		rf.mu.Unlock()
	}
}

// startPreVoteLocked checks that a majority would vote for us before we bump
// our term. A node that was partitioned away can't win a pre-vote, so when it
// rejoins it doesn't force the healthy leader to step down.
func (rf *Raft) startPreVoteLocked() {
	rf.resetElectionTimerLocked()
	rf.leaderID = None // we timed out waiting for it, so don't advertise it
	if rf.quorum() == 1 {
		rf.startElectionLocked()
		return
	}
	rf.preVoteRound++
	round, term := rf.preVoteRound, rf.currentTerm
	args := &RequestVoteArgs{
		Term:         rf.currentTerm + 1,
		CandidateID:  rf.id,
		LastLogIndex: rf.lastIndex(),
		LastLogTerm:  rf.lastTerm(),
		PreVote:      true,
	}
	votes := 1
	for _, p := range rf.peers {
		go func(peer int) {
			ctx, cancel := context.WithTimeout(context.Background(), rf.cfg.RPCTimeout)
			defer cancel()
			reply, err := rf.transport.RequestVote(ctx, peer, args)
			if err != nil {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.stopped {
				return
			}
			if reply.Term > rf.currentTerm {
				rf.stepDownLocked(reply.Term)
				return
			}
			if rf.preVoteRound != round || rf.currentTerm != term || rf.role == Leader || !reply.VoteGranted {
				return
			}
			votes++
			if votes == rf.quorum() {
				rf.startElectionLocked()
			}
		}(p)
	}
}

func (rf *Raft) startElectionLocked() {
	rf.currentTerm++
	rf.role = Candidate
	rf.votedFor = rf.id
	rf.leaderID = None
	rf.persistHardStateLocked()
	// Our vote for ourselves must be durable before we count it. Elections
	// are rare, so syncing under the lock here is fine.
	rf.mustPersist(rf.storage.Sync())
	rf.markSynced(rf.writeSeq)
	rf.resetElectionTimerLocked()
	rf.log.Debug("starting election", "term", rf.currentTerm)

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.id,
		LastLogIndex: rf.lastIndex(),
		LastLogTerm:  rf.lastTerm(),
	}
	votes := 1
	if votes >= rf.quorum() {
		rf.becomeLeaderLocked()
		return
	}
	for _, p := range rf.peers {
		go func(peer int) {
			ctx, cancel := context.WithTimeout(context.Background(), rf.cfg.RPCTimeout)
			defer cancel()
			reply, err := rf.transport.RequestVote(ctx, peer, args)
			if err != nil {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.stopped {
				return
			}
			if reply.Term > rf.currentTerm {
				rf.stepDownLocked(reply.Term)
				return
			}
			if rf.role != Candidate || rf.currentTerm != args.Term || !reply.VoteGranted {
				return
			}
			votes++
			if votes >= rf.quorum() {
				rf.becomeLeaderLocked()
			}
		}(p)
	}
}

func (rf *Raft) hasQuorumLocked() bool {
	alive := 1
	cutoff := time.Now().Add(-rf.cfg.ElectionTimeoutMax)
	for _, p := range rf.peers {
		if rf.lastAck[p].After(cutoff) {
			alive++
		}
	}
	return alive >= rf.quorum()
}

func (rf *Raft) becomeLeaderLocked() {
	rf.role = Leader
	rf.leaderID = rf.id
	rf.log.Info("became leader", "term", rf.currentTerm)
	rf.nextIndex = map[int]uint64{}
	rf.matchIndex = map[int]uint64{}
	rf.lastAck = map[int]time.Time{}
	rf.selfDurable = 0 // set by leaderSyncer once the no-op below is durable
	now := time.Now()
	for _, p := range rf.peers {
		rf.nextIndex[p] = rf.lastIndex() + 1
		rf.matchIndex[p] = 0
		rf.lastAck[p] = now // grace period of one election timeout
	}
	// A no-op in the new term lets the leader commit entries from earlier
	// terms (Raft §5.4.2) and serve ReadIndex requests.
	rf.appendLocked(nil)
}

// HandleRequestVote implements Handler. Like every handler, it replies only
// once the state it wrote is durable.
func (rf *Raft) HandleRequestVote(args *RequestVoteArgs) (*RequestVoteReply, error) {
	reply, err := rf.handleRequestVote(args)
	if err == nil {
		rf.syncAll()
	}
	return reply, err
}

func (rf *Raft) handleRequestVote(args *RequestVoteArgs) (*RequestVoteReply, error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.stopped {
		return nil, ErrStopped
	}
	if args.PreVote {
		// Answer without changing any state. Refuse if we have a live
		// leader: then the candidate is the one with the problem.
		reply := &RequestVoteReply{Term: rf.currentTerm}
		upToDate := args.LastLogTerm > rf.lastTerm() ||
			(args.LastLogTerm == rf.lastTerm() && args.LastLogIndex >= rf.lastIndex())
		hasLeader := rf.role == Leader || time.Since(rf.leaderContact) < rf.cfg.ElectionTimeoutMin
		reply.VoteGranted = args.Term > rf.currentTerm && upToDate && !hasLeader
		return reply, nil
	}
	if args.Term > rf.currentTerm {
		rf.stepDownLocked(args.Term)
	}
	reply := &RequestVoteReply{Term: rf.currentTerm}
	if args.Term < rf.currentTerm {
		return reply, nil
	}
	upToDate := args.LastLogTerm > rf.lastTerm() ||
		(args.LastLogTerm == rf.lastTerm() && args.LastLogIndex >= rf.lastIndex())
	if (rf.votedFor == None || rf.votedFor == args.CandidateID) && upToDate {
		rf.votedFor = args.CandidateID
		rf.persistHardStateLocked()
		rf.resetElectionTimerLocked()
		reply.VoteGranted = true
	}
	return reply, nil
}

// ---- replication ----

// Start proposes a command. It returns immediately; the command is committed
// once it shows up on the apply channel at the returned index and term.
func (rf *Raft) Start(command []byte) (index, term uint64, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.stopped || rf.role != Leader {
		return 0, 0, false
	}
	e := rf.appendLocked(command)
	return e.Index, e.Term, true
}

func (rf *Raft) appendLocked(command []byte) LogEntry {
	e := LogEntry{Index: rf.lastIndex() + 1, Term: rf.currentTerm, Command: command}
	rf.entries = append(rf.entries, e)
	rf.writeEntriesLocked([]LogEntry{e})
	select {
	case rf.syncCh <- struct{}{}:
	default:
	}
	rf.triggerReplicationLocked()
	return e
}

func (rf *Raft) triggerReplicationLocked() {
	for _, p := range rf.peers {
		select {
		case rf.replicateCh[p] <- struct{}{}:
		default:
		}
	}
}

func (rf *Raft) replicator(peer int) {
	defer rf.wg.Done()
	t := time.NewTicker(rf.cfg.HeartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-rf.stopCh:
			return
		case <-rf.replicateCh[peer]:
		case <-t.C:
		}
		rf.replicateOnce(peer)
	}
}

// replicateOnce sends one AppendEntries (or InstallSnapshot) to peer.
func (rf *Raft) replicateOnce(peer int) {
	rf.mu.Lock()
	if rf.stopped || rf.role != Leader {
		rf.mu.Unlock()
		return
	}
	if rf.nextIndex[peer] <= rf.firstIndex() {
		rf.sendSnapshotLocked(peer) // unlocks
		return
	}
	prev := rf.nextIndex[peer] - 1
	last := rf.lastIndex()
	if limit := prev + uint64(rf.cfg.MaxEntriesPerRPC); last > limit {
		last = limit
	}
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.id,
		PrevLogIndex: prev,
		PrevLogTerm:  rf.termAt(prev),
		Entries:      rf.slice(prev+1, last),
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), rf.cfg.RPCTimeout)
	reply, err := rf.transport.AppendEntries(ctx, peer, args)
	cancel()
	if err != nil {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.stopped {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.stepDownLocked(reply.Term)
		return
	}
	if rf.role != Leader || rf.currentTerm != args.Term {
		return // stale reply
	}
	rf.lastAck[peer] = time.Now()
	if reply.Success {
		match := args.PrevLogIndex + uint64(len(args.Entries))
		if match > rf.matchIndex[peer] {
			rf.matchIndex[peer] = match
		}
		if match+1 > rf.nextIndex[peer] {
			rf.nextIndex[peer] = match + 1
		}
		rf.advanceCommitLocked()
		if rf.nextIndex[peer] <= rf.lastIndex() {
			rf.kick(peer) // more to send
		}
		return
	}
	// Use the follower's conflict hint to back up quickly.
	next := reply.ConflictIndex
	if reply.ConflictTerm != 0 {
		for i := rf.lastIndex(); i > rf.firstIndex(); i-- {
			if t := rf.termAt(i); t == reply.ConflictTerm {
				next = i + 1
				break
			} else if t < reply.ConflictTerm {
				break
			}
		}
	}
	if next < 1 {
		next = 1
	}
	if next <= rf.matchIndex[peer] {
		next = rf.matchIndex[peer] + 1
	}
	rf.nextIndex[peer] = next
	rf.kick(peer)
}

func (rf *Raft) kick(peer int) {
	select {
	case rf.replicateCh[peer] <- struct{}{}:
	default:
	}
}

func (rf *Raft) sendSnapshotLocked(peer int) {
	args := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderID:          rf.id,
		LastIncludedIndex: rf.firstIndex(),
		LastIncludedTerm:  rf.entries[0].Term,
		Data:              rf.snapshot,
	}
	rf.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 4*rf.cfg.RPCTimeout)
	reply, err := rf.transport.InstallSnapshot(ctx, peer, args)
	cancel()
	if err != nil {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.stopped {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.stepDownLocked(reply.Term)
		return
	}
	if rf.role != Leader || rf.currentTerm != args.Term {
		return
	}
	rf.lastAck[peer] = time.Now()
	if args.LastIncludedIndex > rf.matchIndex[peer] {
		rf.matchIndex[peer] = args.LastIncludedIndex
	}
	if args.LastIncludedIndex+1 > rf.nextIndex[peer] {
		rf.nextIndex[peer] = args.LastIncludedIndex + 1
	}
	rf.advanceCommitLocked()
	rf.kick(peer)
}

// advanceCommitLocked commits the highest index stored on a majority, but
// only if it is from the current term (Raft §5.4.2).
func (rf *Raft) advanceCommitLocked() {
	if rf.role != Leader {
		return
	}
	matches := []uint64{rf.selfDurable}
	for _, p := range rf.peers {
		matches = append(matches, rf.matchIndex[p])
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i] > matches[j] })
	n := matches[rf.quorum()-1]
	if n > rf.commitIndex && n >= rf.firstIndex() && rf.termAt(n) == rf.currentTerm {
		rf.commitIndex = n
		rf.applyCond.Broadcast()
	}
}

// HandleAppendEntries implements Handler.
func (rf *Raft) HandleAppendEntries(args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	reply, err := rf.handleAppendEntries(args)
	if err == nil {
		rf.syncAll() // a success reply promises the entries are durable
	}
	return reply, err
}

func (rf *Raft) handleAppendEntries(args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.stopped {
		return nil, ErrStopped
	}
	reply := &AppendEntriesReply{Term: rf.currentTerm}
	if args.Term < rf.currentTerm {
		return reply, nil
	}
	rf.stepDownLocked(args.Term) // same term: a candidate yields to the leader
	reply.Term = rf.currentTerm
	rf.leaderID = args.LeaderID
	rf.leaderContact = time.Now()
	rf.resetElectionTimerLocked()

	prev, prevTerm, entries := args.PrevLogIndex, args.PrevLogTerm, args.Entries
	if prev < rf.firstIndex() {
		// The start of this batch is already in our snapshot (and therefore
		// committed); skip the overlapping part.
		skip := rf.firstIndex() - prev
		if skip > uint64(len(entries)) {
			reply.Success = true
			return reply, nil
		}
		entries = entries[skip:]
		prev, prevTerm = rf.firstIndex(), rf.entries[0].Term
	}
	if prev > rf.lastIndex() {
		reply.ConflictIndex = rf.lastIndex() + 1
		return reply, nil
	}
	if t := rf.termAt(prev); t != prevTerm {
		reply.ConflictTerm = t
		i := prev
		for i > rf.firstIndex()+1 && rf.termAt(i-1) == t {
			i--
		}
		reply.ConflictIndex = i
		return reply, nil
	}

	for i, e := range entries {
		idx := prev + 1 + uint64(i)
		if idx <= rf.lastIndex() && rf.termAt(idx) == e.Term {
			continue // already have it
		}
		// Conflict or new entries: drop our suffix and append the rest.
		rf.entries = append(rf.entries[:idx-rf.firstIndex()], entries[i:]...)
		rf.writeEntriesLocked(entries[i:])
		break
	}

	if args.LeaderCommit > rf.commitIndex {
		lastNew := prev + uint64(len(entries))
		c := args.LeaderCommit
		if lastNew < c {
			c = lastNew
		}
		if c > rf.commitIndex {
			rf.commitIndex = c
			rf.applyCond.Broadcast()
		}
	}
	reply.Success = true
	return reply, nil
}

// ---- snapshots ----

// Snapshot tells Raft the service has saved its state up to and including
// index, so the log up to there can be thrown away.
//
// The snapshot is written to disk without holding the Raft lock, so a large
// snapshot doesn't delay heartbeats; the in-memory log is trimmed after.
func (rf *Raft) Snapshot(index uint64, data []byte) {
	rf.mu.Lock()
	if rf.stopped || index <= rf.firstIndex() || index > rf.commitIndex || index > rf.lastIndex() {
		rf.mu.Unlock()
		return
	}
	term := rf.termAt(index)
	rf.mu.Unlock()

	rf.mustPersist(rf.storage.SaveSnapshot(Snapshot{Index: index, Term: term, Data: data}))

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Committed entries never change, but another snapshot may have
	// overtaken this one while we were writing.
	if rf.stopped || index <= rf.firstIndex() || index > rf.lastIndex() || rf.termAt(index) != term {
		return
	}
	rest := rf.slice(index+1, rf.lastIndex())
	rf.entries = append([]LogEntry{{Index: index, Term: term}}, rest...)
	rf.snapshot = data
}

// HandleInstallSnapshot implements Handler.
func (rf *Raft) HandleInstallSnapshot(args *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	reply, err := rf.handleInstallSnapshot(args)
	if err == nil {
		rf.syncAll()
	}
	return reply, err
}

func (rf *Raft) handleInstallSnapshot(args *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	rf.mu.Lock()
	if rf.stopped {
		rf.mu.Unlock()
		return nil, ErrStopped
	}
	reply := &InstallSnapshotReply{Term: rf.currentTerm}
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return reply, nil
	}
	rf.stepDownLocked(args.Term)
	reply.Term = rf.currentTerm
	rf.leaderID = args.LeaderID
	rf.leaderContact = time.Now()
	rf.resetElectionTimerLocked()
	if args.LastIncludedIndex <= rf.commitIndex {
		rf.mu.Unlock()
		return reply, nil // nothing new
	}
	rf.mu.Unlock()

	// Save the snapshot first, without the lock. It holds committed state,
	// so it's safe to keep even if we end up not using it below.
	snap := Snapshot{Index: args.LastIncludedIndex, Term: args.LastIncludedTerm, Data: args.Data}
	rf.mustPersist(rf.storage.SaveSnapshot(snap))

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.stopped {
		return nil, ErrStopped
	}
	reply.Term = rf.currentTerm
	if args.LastIncludedIndex <= rf.commitIndex {
		return reply, nil
	}
	var rest []LogEntry
	if args.LastIncludedIndex < rf.lastIndex() && rf.termAt(args.LastIncludedIndex) == args.LastIncludedTerm {
		// Our log agrees with the snapshot: keep the entries after it.
		rest = rf.slice(args.LastIncludedIndex+1, rf.lastIndex())
	} else {
		// Our log is behind or conflicts: replace all of it.
		rf.mustPersist(rf.storage.ResetLog())
		rf.writeSeq++
	}
	rf.entries = append([]LogEntry{{Index: args.LastIncludedIndex, Term: args.LastIncludedTerm}}, rest...)
	rf.snapshot = args.Data
	rf.commitIndex = args.LastIncludedIndex
	rf.pendingSnapshot = &snap
	rf.applyCond.Broadcast()
	return reply, nil
}

// ---- applying ----

func (rf *Raft) applier() {
	defer rf.wg.Done()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for {
		for !rf.stopped && rf.pendingSnapshot == nil && rf.commitIndex <= rf.lastApplied {
			rf.applyCond.Wait()
		}
		if rf.stopped {
			return
		}
		if snap := rf.pendingSnapshot; snap != nil {
			rf.pendingSnapshot = nil
			msg := ApplyMsg{SnapshotValid: true, Snapshot: snap.Data, SnapshotIndex: snap.Index, SnapshotTerm: snap.Term}
			rf.mu.Unlock()
			ok := rf.deliver(msg)
			rf.mu.Lock()
			if !ok {
				return
			}
			if snap.Index > rf.lastApplied {
				rf.lastApplied = snap.Index
			}
			continue
		}
		if rf.lastApplied < rf.firstIndex() {
			// Shouldn't happen: anything before firstIndex was delivered
			// as a snapshot. Skip ahead rather than index out of range.
			rf.lastApplied = rf.firstIndex()
			continue
		}
		batch := rf.slice(rf.lastApplied+1, rf.commitIndex)
		rf.mu.Unlock()
		for _, e := range batch {
			if !rf.deliver(ApplyMsg{CommandValid: true, Command: e.Command, CommandIndex: e.Index, CommandTerm: e.Term}) {
				rf.mu.Lock()
				return
			}
		}
		rf.mu.Lock()
		if n := batch[len(batch)-1].Index; n > rf.lastApplied {
			rf.lastApplied = n
		}
	}
}

func (rf *Raft) deliver(msg ApplyMsg) bool {
	select {
	case rf.applyCh <- msg:
		return true
	case <-rf.stopCh:
		return false
	}
}

// ---- reads and status ----

// ReadIndex confirms this node is still the leader and returns a commit
// index. Once the service has applied up to that index, a local read reflects
// every write that completed before ReadIndex was called (Raft thesis §6.4).
func (rf *Raft) ReadIndex(ctx context.Context) (uint64, error) {
	rf.mu.Lock()
	if rf.stopped {
		rf.mu.Unlock()
		return 0, ErrStopped
	}
	if rf.role != Leader {
		rf.mu.Unlock()
		return 0, ErrNotLeader
	}
	if rf.termAt(rf.commitIndex) != rf.currentTerm {
		rf.mu.Unlock()
		return 0, ErrNotReady
	}
	readIndex, term := rf.commitIndex, rf.currentTerm
	type call struct {
		peer int
		args *AppendEntriesArgs
	}
	var calls []call
	for _, p := range rf.peers {
		prev := rf.nextIndex[p] - 1
		if prev > rf.lastIndex() {
			prev = rf.lastIndex()
		}
		if prev < rf.firstIndex() {
			prev = rf.firstIndex()
		}
		calls = append(calls, call{p, &AppendEntriesArgs{
			Term: term, LeaderID: rf.id, PrevLogIndex: prev, PrevLogTerm: rf.termAt(prev), LeaderCommit: rf.commitIndex,
		}})
	}
	need := rf.quorum() - 1
	rf.mu.Unlock()

	if need == 0 {
		return readIndex, nil
	}
	acks := make(chan bool, len(calls))
	for _, c := range calls {
		go func(c call) {
			cctx, cancel := context.WithTimeout(ctx, rf.cfg.RPCTimeout)
			defer cancel()
			reply, err := rf.transport.AppendEntries(cctx, c.peer, c.args)
			if err != nil {
				acks <- false
				return
			}
			if reply.Term > term {
				rf.mu.Lock()
				if !rf.stopped && reply.Term > rf.currentTerm {
					rf.stepDownLocked(reply.Term)
				}
				rf.mu.Unlock()
				acks <- false
				return
			}
			acks <- true // the peer still accepts us as leader for this term
		}(c)
	}
	got, failed := 0, 0
	for got < need {
		select {
		case ok := <-acks:
			if ok {
				got++
			} else if failed++; failed > len(calls)-need {
				return 0, ErrNotLeader
			}
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != Leader || rf.currentTerm != term {
		return 0, ErrNotLeader
	}
	return readIndex, nil
}

// Status returns a snapshot of the node's state.
func (rf *Raft) Status() Status {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return Status{
		ID:            rf.id,
		Role:          rf.role,
		Term:          rf.currentTerm,
		VotedFor:      rf.votedFor,
		LeaderID:      rf.leaderID,
		CommitIndex:   rf.commitIndex,
		LastApplied:   rf.lastApplied,
		LastLogIndex:  rf.lastIndex(),
		LastLogTerm:   rf.lastTerm(),
		SnapshotIndex: rf.firstIndex(),
	}
}

// State returns the current term and whether this node believes it is leader.
func (rf *Raft) State() (term uint64, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

// LogLen returns the number of entries kept in memory after the last snapshot.
func (rf *Raft) LogLen() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return len(rf.entries) - 1
}
