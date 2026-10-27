// Package raft implements the Raft consensus algorithm: leader election,
// log replication, persistence and log compaction via snapshots.
//
// The package is transport- and storage-agnostic. A node talks to its peers
// through a Transport and persists its state through a Storage, which keeps
// the core algorithm easy to test with an in-memory network.
package raft

import (
	"context"
	"errors"
	"fmt"
)

// None marks "no node", e.g. an empty vote or an unknown leader.
const None = -1

// LogEntry is one entry in the replicated log. A nil Command is a no-op that
// a new leader appends to commit entries from earlier terms.
type LogEntry struct {
	Index   uint64 `json:"index"`
	Term    uint64 `json:"term"`
	Command []byte `json:"command,omitempty"`
}

// HardState is the part of a node's state that must survive a crash before
// it answers any RPC.
type HardState struct {
	Term     uint64 `json:"term"`
	VotedFor int    `json:"votedFor"`
}

// Snapshot is a compacted prefix of the log: the service's state after
// applying every entry up to and including Index.
type Snapshot struct {
	Index uint64 `json:"index"`
	Term  uint64 `json:"term"`
	Data  []byte `json:"data,omitempty"`
}

// PersistentState is everything a node needs to restart after a crash.
type PersistentState struct {
	HardState HardState
	Snapshot  Snapshot
	Entries   []LogEntry // entries after Snapshot.Index, in order
}

// Storage durably stores a node's Raft state.
//
// SaveHardState, AppendEntries and ResetLog may buffer their writes; Sync
// makes every earlier write durable. Raft calls Sync before it acts on
// written state (replying to an RPC, counting its own vote, or counting its
// own log toward a commit), so many writes can share one fsync.
//
// SaveSnapshot is durable when it returns. Raft calls it without holding
// its lock, so it may run concurrently with the other methods; a slow
// snapshot write must not block appends.
type Storage interface {
	// Load returns the saved state. Entries at or below the snapshot index
	// are dropped.
	Load() (PersistentState, error)
	SaveHardState(hs HardState) error
	// AppendEntries writes entries to the log. If entries[0].Index is not
	// past the current last index, the existing suffix from that index on is
	// replaced.
	AppendEntries(entries []LogEntry) error
	// ResetLog drops every log entry. It's used after installing a snapshot
	// that replaces a conflicting log.
	ResetLog() error
	// SaveSnapshot replaces the snapshot if snap is newer than the saved one,
	// and may then discard log entries it covers.
	SaveSnapshot(snap Snapshot) error
	Sync() error
}

// ApplyMsg is delivered on the apply channel when an entry is committed or a
// snapshot must be installed in the service.
type ApplyMsg struct {
	CommandValid bool
	Command      []byte
	CommandIndex uint64
	CommandTerm  uint64

	SnapshotValid bool
	Snapshot      []byte
	SnapshotIndex uint64
	SnapshotTerm  uint64
}

// RequestVoteArgs is sent by candidates to gather votes.
type RequestVoteArgs struct {
	Term         uint64 `json:"term"`
	CandidateID  int    `json:"candidateId"`
	LastLogIndex uint64 `json:"lastLogIndex"`
	LastLogTerm  uint64 `json:"lastLogTerm"`
	// PreVote asks "would you vote for me?" without changing anyone's term
	// (Raft thesis §9.6). Term is the term the candidate would use.
	PreVote bool `json:"preVote,omitempty"`
}

type RequestVoteReply struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"voteGranted"`
}

// AppendEntriesArgs replicates entries and doubles as a heartbeat.
type AppendEntriesArgs struct {
	Term         uint64     `json:"term"`
	LeaderID     int        `json:"leaderId"`
	PrevLogIndex uint64     `json:"prevLogIndex"`
	PrevLogTerm  uint64     `json:"prevLogTerm"`
	Entries      []LogEntry `json:"entries,omitempty"`
	LeaderCommit uint64     `json:"leaderCommit"`
}

// AppendEntriesReply carries a conflict hint so the leader can skip back a
// whole term at a time instead of one entry per round trip.
type AppendEntriesReply struct {
	Term          uint64 `json:"term"`
	Success       bool   `json:"success"`
	ConflictIndex uint64 `json:"conflictIndex"`
	ConflictTerm  uint64 `json:"conflictTerm"`
}

// InstallSnapshotArgs sends a whole snapshot to a follower that is too far
// behind to catch up from the log.
type InstallSnapshotArgs struct {
	Term              uint64 `json:"term"`
	LeaderID          int    `json:"leaderId"`
	LastIncludedIndex uint64 `json:"lastIncludedIndex"`
	LastIncludedTerm  uint64 `json:"lastIncludedTerm"`
	Data              []byte `json:"data"`
}

type InstallSnapshotReply struct {
	Term uint64 `json:"term"`
}

// Transport sends RPCs to other nodes.
type Transport interface {
	RequestVote(ctx context.Context, to int, args *RequestVoteArgs) (*RequestVoteReply, error)
	AppendEntries(ctx context.Context, to int, args *AppendEntriesArgs) (*AppendEntriesReply, error)
	InstallSnapshot(ctx context.Context, to int, args *InstallSnapshotArgs) (*InstallSnapshotReply, error)
}

// Handler receives RPCs from other nodes. *Raft implements it.
type Handler interface {
	HandleRequestVote(args *RequestVoteArgs) (*RequestVoteReply, error)
	HandleAppendEntries(args *AppendEntriesArgs) (*AppendEntriesReply, error)
	HandleInstallSnapshot(args *InstallSnapshotArgs) (*InstallSnapshotReply, error)
}

var (
	ErrNotLeader = errors.New("raft: not the leader")
	ErrNotReady  = errors.New("raft: leader has not committed an entry in its term yet")
	ErrStopped   = errors.New("raft: node stopped")
)

// Role is a node's current role.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	}
	return "unknown"
}

// MarshalText makes Role show up as a readable string in JSON.
func (r Role) MarshalText() ([]byte, error) { return []byte(r.String()), nil }

func (r *Role) UnmarshalText(b []byte) error {
	switch string(b) {
	case "follower":
		*r = Follower
	case "candidate":
		*r = Candidate
	case "leader":
		*r = Leader
	default:
		return fmt.Errorf("raft: unknown role %q", b)
	}
	return nil
}

// Status is a point-in-time view of a node, used by tooling and the visualizer.
type Status struct {
	ID            int    `json:"id"`
	Role          Role   `json:"role"`
	Term          uint64 `json:"term"`
	VotedFor      int    `json:"votedFor"`
	LeaderID      int    `json:"leaderId"`
	CommitIndex   uint64 `json:"commitIndex"`
	LastApplied   uint64 `json:"lastApplied"`
	LastLogIndex  uint64 `json:"lastLogIndex"`
	LastLogTerm   uint64 `json:"lastLogTerm"`
	SnapshotIndex uint64 `json:"snapshotIndex"`
}
