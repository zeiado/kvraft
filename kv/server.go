// Package kv is a linearizable key-value store replicated with Raft.
//
// Writes go through the Raft log. Each client tags its writes with a
// (ClientID, Seq) pair so a retried write is applied exactly once. Reads skip
// the log and use Raft's ReadIndex protocol, which keeps them linearizable
// without writing anything to disk.
package kv

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/zeiado/kvraft/raft"
)

// Op types.
const (
	OpGet    = "get"
	OpPut    = "put"
	OpDelete = "delete"
	OpCAS    = "cas" // compare-and-swap: set Value only if current value == Expected
)

// Op is a client request.
type Op struct {
	Type     string `json:"type"`
	Key      string `json:"key"`
	Value    string `json:"value,omitempty"`
	Expected string `json:"expected,omitempty"`
	ClientID string `json:"clientId,omitempty"`
	Seq      uint64 `json:"seq,omitempty"`
}

// Result is the outcome of an Op.
type Result struct {
	Value string `json:"value,omitempty"`
	Found bool   `json:"found"`           // key existed (get, delete, cas)
	OK    bool   `json:"ok"`              // cas succeeded; always true for put/delete
	Index uint64 `json:"index,omitempty"` // log index the op was applied at (writes)
}

var (
	ErrWrongLeader = errors.New("kv: not the leader")
	ErrStopped     = errors.New("kv: server stopped")
)

// LeaderError is returned when a request reaches a non-leader. Leader is a
// hint (raft.None if unknown).
type LeaderError struct{ Leader int }

func (e *LeaderError) Error() string { return ErrWrongLeader.Error() }
func (e *LeaderError) Unwrap() error { return ErrWrongLeader }

// Endpoint is anything that can execute an Op: a local Server, or a remote
// one over HTTP.
type Endpoint interface {
	Do(ctx context.Context, op Op) (Result, error)
}

type session struct {
	Seq    uint64 `json:"seq"`
	Result Result `json:"result"`
}

type appliedOp struct {
	term     uint64
	clientID string
	seq      uint64
	result   Result
}

// Config configures a Server.
type Config struct {
	// SnapshotThreshold triggers a snapshot once the Raft log holds this many
	// entries. 0 disables snapshots.
	SnapshotThreshold int
	// UnsafeLocalReads lets any replica, leader or not, answer reads from
	// its local state without ReadIndex (the classic "read from a replica"
	// mistake). It is WRONG on purpose and exists only to show that the chaos
	// checker catches stale reads. Never enable it otherwise.
	UnsafeLocalReads bool
	Logger           *slog.Logger
}

// Server is one replica of the store.
type Server struct {
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	cfg     Config
	log     *slog.Logger

	mu          sync.Mutex
	data        map[string]string
	sessions    map[string]session // last write result per client, for dedup
	lastApplied uint64
	appliedCh   chan struct{} // closed and replaced on every apply
	waiters     map[uint64]chan appliedOp
	stopped     bool

	stopCh chan struct{}
	done   chan struct{}
}

// NewServer starts a replica. newRaft builds the Raft node using the given
// apply channel; this lets callers choose transport and storage.
func NewServer(cfg Config, newRaft func(applyCh chan<- raft.ApplyMsg) (*raft.Raft, error)) (*Server, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	s := &Server{
		applyCh:   make(chan raft.ApplyMsg, 256),
		cfg:       cfg,
		log:       cfg.Logger,
		data:      map[string]string{},
		sessions:  map[string]session{},
		appliedCh: make(chan struct{}),
		waiters:   map[uint64]chan appliedOp{},
		stopCh:    make(chan struct{}),
		done:      make(chan struct{}),
	}
	rf, err := newRaft(s.applyCh)
	if err != nil {
		return nil, err
	}
	s.rf = rf
	go s.applyLoop()
	return s, nil
}

// Raft exposes the underlying node (for status and tests).
func (s *Server) Raft() *raft.Raft { return s.rf }

// Stop shuts down the replica and its Raft node.
func (s *Server) Stop() {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return
	}
	s.stopped = true
	s.mu.Unlock()
	s.rf.Stop()
	close(s.stopCh)
	<-s.done
}

// Do executes an op. It returns a *LeaderError if this replica isn't the
// leader; the caller should retry elsewhere.
func (s *Server) Do(ctx context.Context, op Op) (Result, error) {
	switch op.Type {
	case OpGet:
		return s.get(ctx, op.Key)
	case OpPut, OpDelete, OpCAS:
		return s.write(ctx, op)
	}
	return Result{}, errors.New("kv: unknown op type " + op.Type)
}

func (s *Server) leaderErr() error {
	return &LeaderError{Leader: s.rf.Status().LeaderID}
}

func (s *Server) get(ctx context.Context, key string) (Result, error) {
	if s.cfg.UnsafeLocalReads {
		s.mu.Lock()
		defer s.mu.Unlock()
		v, ok := s.data[key]
		return Result{Value: v, Found: ok, OK: true}, nil
	}
	var idx uint64
	for {
		var err error
		idx, err = s.rf.ReadIndex(ctx)
		if err == nil {
			break
		}
		if errors.Is(err, raft.ErrNotReady) {
			// New leader hasn't committed its no-op yet; that takes one round trip.
			select {
			case <-time.After(5 * time.Millisecond):
				continue
			case <-ctx.Done():
				return Result{}, ctx.Err()
			}
		}
		if errors.Is(err, raft.ErrNotLeader) {
			return Result{}, s.leaderErr()
		}
		return Result{}, err
	}
	if err := s.waitApplied(ctx, idx); err != nil {
		return Result{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data[key]
	return Result{Value: v, Found: ok, OK: true}, nil
}

func (s *Server) waitApplied(ctx context.Context, idx uint64) error {
	for {
		s.mu.Lock()
		if s.stopped {
			s.mu.Unlock()
			return ErrStopped
		}
		if s.lastApplied >= idx {
			s.mu.Unlock()
			return nil
		}
		ch := s.appliedCh
		s.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *Server) write(ctx context.Context, op Op) (Result, error) {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return Result{}, ErrStopped
	}
	// Fast path: this exact request was already applied.
	if op.ClientID != "" {
		if sess, ok := s.sessions[op.ClientID]; ok && op.Seq <= sess.Seq {
			s.mu.Unlock()
			if op.Seq == sess.Seq {
				return sess.Result, nil
			}
			return Result{}, errors.New("kv: request is older than the client's latest")
		}
	}
	s.mu.Unlock()

	cmd, err := json.Marshal(op)
	if err != nil {
		return Result{}, err
	}
	idx, term, isLeader := s.rf.Start(cmd)
	if !isLeader {
		return Result{}, s.leaderErr()
	}
	ch := make(chan appliedOp, 1)
	s.mu.Lock()
	s.waiters[idx] = ch
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		if s.waiters[idx] == ch {
			delete(s.waiters, idx)
		}
		s.mu.Unlock()
	}()

	select {
	case a := <-ch:
		// A different entry at our index means we lost leadership and the
		// new leader overwrote it.
		if a.term != term || a.clientID != op.ClientID || a.seq != op.Seq {
			return Result{}, s.leaderErr()
		}
		return a.result, nil
	case <-ctx.Done():
		return Result{}, ctx.Err()
	case <-s.stopCh:
		return Result{}, ErrStopped
	}
}

func (s *Server) applyLoop() {
	defer close(s.done)
	for {
		select {
		case <-s.stopCh:
			return
		case m := <-s.applyCh:
			if m.SnapshotValid {
				s.installSnapshot(m.Snapshot, m.SnapshotIndex)
				continue
			}
			if m.CommandValid {
				s.applyCommand(m)
			}
		}
	}
}

func (s *Server) applyCommand(m raft.ApplyMsg) {
	s.mu.Lock()
	if m.CommandIndex <= s.lastApplied {
		s.mu.Unlock()
		return // already covered by a snapshot
	}
	a := appliedOp{term: m.CommandTerm}
	if m.Command != nil { // nil = leader no-op
		var op Op
		if err := json.Unmarshal(m.Command, &op); err != nil {
			s.log.Error("bad command in log", "index", m.CommandIndex, "err", err)
		} else {
			a.clientID, a.seq = op.ClientID, op.Seq
			a.result = s.applyOpLocked(op, m.CommandIndex)
		}
	}
	s.lastApplied = m.CommandIndex
	close(s.appliedCh)
	s.appliedCh = make(chan struct{})
	if ch, ok := s.waiters[m.CommandIndex]; ok {
		ch <- a
		delete(s.waiters, m.CommandIndex)
	}
	needSnap := s.cfg.SnapshotThreshold > 0 && s.rf.LogLen() >= s.cfg.SnapshotThreshold
	var snap []byte
	if needSnap {
		snap = s.encodeLocked()
	}
	idx := s.lastApplied
	s.mu.Unlock()
	if needSnap {
		s.rf.Snapshot(idx, snap)
	}
}

func (s *Server) applyOpLocked(op Op, index uint64) Result {
	if op.ClientID != "" {
		if sess, ok := s.sessions[op.ClientID]; ok && op.Seq <= sess.Seq {
			return sess.Result // duplicate: don't apply twice
		}
	}
	var r Result
	cur, found := s.data[op.Key]
	switch op.Type {
	case OpPut:
		s.data[op.Key] = op.Value
		r = Result{OK: true, Found: found}
	case OpDelete:
		delete(s.data, op.Key)
		r = Result{OK: true, Found: found}
	case OpCAS:
		if found && cur == op.Expected {
			s.data[op.Key] = op.Value
			r = Result{OK: true, Found: true, Value: op.Value}
		} else {
			r = Result{OK: false, Found: found, Value: cur}
		}
	}
	r.Index = index
	if op.ClientID != "" {
		s.sessions[op.ClientID] = session{Seq: op.Seq, Result: r}
	}
	return r
}

type snapshotState struct {
	Data     map[string]string  `json:"data"`
	Sessions map[string]session `json:"sessions"`
}

func (s *Server) encodeLocked() []byte {
	b, _ := json.Marshal(snapshotState{Data: s.data, Sessions: s.sessions})
	return b
}

func (s *Server) installSnapshot(data []byte, index uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if index <= s.lastApplied {
		return
	}
	var st snapshotState
	if err := json.Unmarshal(data, &st); err != nil {
		s.log.Error("bad snapshot", "err", err)
		return
	}
	if st.Data == nil {
		st.Data = map[string]string{}
	}
	if st.Sessions == nil {
		st.Sessions = map[string]session{}
	}
	s.data, s.sessions = st.Data, st.Sessions
	s.lastApplied = index
	close(s.appliedCh)
	s.appliedCh = make(chan struct{})
	// Waiters at or below the snapshot index can't tell if their entry
	// survived; fail them so the client retries (dedup keeps it safe).
	for idx, ch := range s.waiters {
		if idx <= index {
			ch <- appliedOp{term: 0}
			delete(s.waiters, idx)
		}
	}
}

// Len returns the number of keys (for tooling).
func (s *Server) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.data)
}
