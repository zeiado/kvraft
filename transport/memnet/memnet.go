// Package memnet is an in-process network for Raft nodes that can simulate
// crashes, partitions, message loss and delay. Tests, the chaos runner and
// the visualizer all run clusters on it.
package memnet

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/zeiado/kvraft/raft"
)

var ErrUnreachable = errors.New("memnet: unreachable")

// Network connects nodes by ID.
type Network struct {
	mu       sync.Mutex
	handlers map[int]raft.Handler
	group    map[int]int // nodes can talk only within the same group
	rng      *rand.Rand

	// Unreliable mode.
	dropRate float64
	maxDelay time.Duration

	rpcs   uint64
	tracer func(Message)
}

// Message describes one RPC for observers such as the visualizer.
type Message struct {
	From, To int
	// Kind is "prevote", "vote", "heartbeat" (no entries), "append" or "snapshot".
	Kind string
	// Outcome is "ok" or "rejected" (the receiver said no: vote denied, log
	// mismatch, stale term), or "lost" (partitioned, crashed or dropped).
	Outcome string
}

// SetTracer registers f to be called for every RPC once its fate is known.
// f must be fast; it runs on the sender's goroutine.
func (n *Network) SetTracer(f func(Message)) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.tracer = f
}

func (n *Network) trace(from, to int, args, reply any, delivered bool) {
	n.mu.Lock()
	f := n.tracer
	n.mu.Unlock()
	if f == nil {
		return
	}
	m := Message{From: from, To: to, Outcome: "lost"}
	switch a := args.(type) {
	case *raft.RequestVoteArgs:
		m.Kind = "vote"
		if a.PreVote {
			m.Kind = "prevote"
		}
		if delivered {
			m.Outcome = "rejected"
			if r, ok := reply.(*raft.RequestVoteReply); ok && r.VoteGranted {
				m.Outcome = "ok"
			}
		}
	case *raft.AppendEntriesArgs:
		m.Kind = "heartbeat"
		if len(a.Entries) > 0 {
			m.Kind = "append"
		}
		if delivered {
			m.Outcome = "rejected"
			if r, ok := reply.(*raft.AppendEntriesReply); ok && r.Success {
				m.Outcome = "ok"
			}
		}
	case *raft.InstallSnapshotArgs:
		m.Kind = "snapshot"
		if delivered {
			m.Outcome = "ok"
			if r, ok := reply.(*raft.InstallSnapshotReply); ok && r.Term > a.Term {
				m.Outcome = "rejected"
			}
		}
	}
	f(m)
}

func New(seed int64) *Network {
	return &Network{
		handlers: map[int]raft.Handler{},
		group:    map[int]int{},
		rng:      rand.New(rand.NewSource(seed)),
	}
}

// Attach registers (or replaces) the handler for a node and connects it.
func (n *Network) Attach(id int, h raft.Handler) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.handlers[id] = h
}

// Detach removes a node, e.g. when it crashes.
func (n *Network) Detach(id int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.handlers, id)
}

// Partition splits nodes into groups that can't reach each other. Nodes not
// listed end up isolated from everyone.
func (n *Network) Partition(groups ...[]int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.group = map[int]int{}
	for g, ids := range groups {
		for _, id := range ids {
			n.group[id] = g + 1
		}
	}
	for id := range n.handlers {
		if _, ok := n.group[id]; !ok {
			n.group[id] = -1 - id // a group of its own
		}
	}
}

// Isolate cuts a single node off from all others.
func (n *Network) Isolate(id int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.group[id] = -1 - id
}

// Heal reconnects everything.
func (n *Network) Heal() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.group = map[int]int{}
}

// SetUnreliable makes the network drop a fraction of messages and delay
// the rest by up to maxDelay. Pass 0, 0 to make it reliable again.
func (n *Network) SetUnreliable(dropRate float64, maxDelay time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.dropRate, n.maxDelay = dropRate, maxDelay
}

// Groups returns each node's partition group (0 = the default group).
func (n *Network) Groups() map[int]int {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := map[int]int{}
	for k, v := range n.group {
		out[k] = v
	}
	return out
}

// RPCCount returns the number of RPCs delivered so far.
func (n *Network) RPCCount() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.rpcs
}

func (n *Network) connectedLocked(a, b int) bool {
	if _, ok := n.handlers[b]; !ok {
		return false
	}
	return n.group[a] == n.group[b]
}

// Connected reports whether a can currently reach b.
func (n *Network) Connected(a, b int) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.connectedLocked(a, b)
}

// fault decides the fate of one message: whether it's dropped and how long
// it's delayed.
func (n *Network) fault() (drop bool, delay time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.dropRate > 0 && n.rng.Float64() < n.dropRate {
		drop = true
	}
	if n.maxDelay > 0 {
		delay = time.Duration(n.rng.Int63n(int64(n.maxDelay)))
	}
	return
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// call delivers one RPC from -> to, applying partitions and faults to both
// the request and the reply.
func call[A any, R any](ctx context.Context, n *Network, from, to int, args A, fn func(raft.Handler, A) (R, error)) (R, error) {
	r, err := deliver(ctx, n, from, to, args, fn)
	// A cancelled call means the sender stopped caring (for example a
	// ReadIndex round that already heard from a majority), not a lost message.
	if !errors.Is(err, context.Canceled) {
		n.trace(from, to, args, r, err == nil)
	}
	return r, err
}

func deliver[A any, R any](ctx context.Context, n *Network, from, to int, args A, fn func(raft.Handler, A) (R, error)) (R, error) {
	var zero R
	n.mu.Lock()
	h, ok := n.handlers[to]
	connected := ok && n.connectedLocked(from, to)
	n.mu.Unlock()
	if !connected {
		// Simulate a timeout rather than an instant failure, so code paths
		// behave like they would against a real dead or partitioned peer.
		_ = sleepCtx(ctx, 10*time.Millisecond)
		return zero, ErrUnreachable
	}
	drop, delay := n.fault()
	if err := sleepCtx(ctx, delay); err != nil {
		return zero, err
	}
	if drop {
		<-ctx.Done()
		return zero, ErrUnreachable
	}

	type result struct {
		r   R
		err error
	}
	done := make(chan result, 1)
	go func() {
		r, err := fn(h, args)
		done <- result{r, err}
	}()
	var res result
	select {
	case res = <-done:
	case <-ctx.Done():
		return zero, ctx.Err()
	}
	if res.err != nil {
		return zero, res.err
	}

	n.mu.Lock()
	n.rpcs++
	stillConnected := n.connectedLocked(to, from) || from == to
	if _, alive := n.handlers[from]; !alive {
		stillConnected = false
	}
	n.mu.Unlock()
	if !stillConnected {
		return zero, ErrUnreachable
	}
	drop, delay = n.fault()
	if err := sleepCtx(ctx, delay); err != nil {
		return zero, err
	}
	if drop {
		return zero, ErrUnreachable
	}
	return res.r, nil
}

// Endpoint returns a raft.Transport that sends RPCs as node `from`.
func (n *Network) Endpoint(from int) raft.Transport { return &endpoint{n: n, from: from} }

type endpoint struct {
	n    *Network
	from int
}

func (e *endpoint) RequestVote(ctx context.Context, to int, a *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	return call(ctx, e.n, e.from, to, a, raft.Handler.HandleRequestVote)
}

func (e *endpoint) AppendEntries(ctx context.Context, to int, a *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	return call(ctx, e.n, e.from, to, a, raft.Handler.HandleAppendEntries)
}

func (e *endpoint) InstallSnapshot(ctx context.Context, to int, a *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	return call(ctx, e.n, e.from, to, a, raft.Handler.HandleInstallSnapshot)
}
