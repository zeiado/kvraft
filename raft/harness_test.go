package raft_test

import (
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/zeiado/kvraft/raft"
	"github.com/zeiado/kvraft/transport/memnet"
)

// harness runs a cluster on an in-memory network and checks that every node
// applies the same command at each index.
type harness struct {
	t        *testing.T
	n        int
	net      *memnet.Network
	storages []*raft.MemoryStorage
	nodes    []*raft.Raft
	stops    []chan struct{}

	mu        sync.Mutex
	applied   []map[uint64]string // per node: index -> command
	lastIdx   []uint64
	errMsg    string
	snapEvery uint64
}

func newHarness(t *testing.T, n int, snapEvery uint64) *harness {
	t.Helper()
	h := &harness{
		t:         t,
		n:         n,
		net:       memnet.New(time.Now().UnixNano()),
		storages:  make([]*raft.MemoryStorage, n),
		nodes:     make([]*raft.Raft, n),
		stops:     make([]chan struct{}, n),
		applied:   make([]map[uint64]string, n),
		lastIdx:   make([]uint64, n),
		snapEvery: snapEvery,
	}
	for i := 0; i < n; i++ {
		h.storages[i] = raft.NewMemoryStorage()
		h.start(i)
	}
	t.Cleanup(h.shutdown)
	return h
}

func (h *harness) peers() []int {
	p := make([]int, h.n)
	for i := range p {
		p[i] = i
	}
	return p
}

func (h *harness) start(i int) {
	h.mu.Lock()
	h.applied[i] = map[uint64]string{}
	h.lastIdx[i] = 0
	h.mu.Unlock()

	ch := make(chan raft.ApplyMsg, 64)
	stop := make(chan struct{})
	cfg := raft.Config{
		ID:                 i,
		Peers:              h.peers(),
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval:  30 * time.Millisecond,
		RPCTimeout:         100 * time.Millisecond,
	}
	rf, err := raft.New(cfg, h.net.Endpoint(i), h.storages[i], ch)
	if err != nil {
		h.t.Fatal(err)
	}
	h.nodes[i] = rf
	h.stops[i] = stop
	h.net.Attach(i, rf)
	go h.applyLoop(i, rf, ch, stop)
}

func (h *harness) applyLoop(i int, rf *raft.Raft, ch chan raft.ApplyMsg, stop chan struct{}) {
	for {
		select {
		case <-stop:
			return
		case m := <-ch:
			h.mu.Lock()
			switch {
			case m.SnapshotValid:
				var state map[uint64]string
				if err := json.Unmarshal(m.Snapshot, &state); err != nil {
					h.fail("node %d: bad snapshot: %v", i, err)
				}
				h.applied[i] = state
				h.lastIdx[i] = m.SnapshotIndex
				for idx, cmd := range state {
					h.checkLocked(i, idx, cmd)
				}
			case m.CommandValid:
				if m.CommandIndex != h.lastIdx[i]+1 {
					h.fail("node %d: applied index %d out of order (last %d)", i, m.CommandIndex, h.lastIdx[i])
				}
				h.lastIdx[i] = m.CommandIndex
				if m.Command != nil {
					cmd := string(m.Command)
					h.checkLocked(i, m.CommandIndex, cmd)
					h.applied[i][m.CommandIndex] = cmd
				}
				if h.snapEvery > 0 && m.CommandIndex%h.snapEvery == 0 {
					data, _ := json.Marshal(h.applied[i])
					h.mu.Unlock()
					rf.Snapshot(m.CommandIndex, data)
					continue
				}
			}
			h.mu.Unlock()
		}
	}
}

func (h *harness) checkLocked(node int, idx uint64, cmd string) {
	for j, m := range h.applied {
		if other, ok := m[idx]; ok && other != cmd {
			h.fail("index %d: node %d applied %q but node %d applied %q", idx, node, cmd, j, other)
		}
	}
}

func (h *harness) fail(format string, args ...any) {
	if h.errMsg == "" {
		h.errMsg = fmt.Sprintf(format, args...)
	}
}

func (h *harness) checkErr() {
	h.t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.errMsg != "" {
		h.t.Fatal(h.errMsg)
	}
}

func (h *harness) crash(i int) {
	if h.nodes[i] == nil {
		return
	}
	h.net.Detach(i)
	h.nodes[i].Stop()
	h.storages[i].Crash() // lose writes that weren't synced
	close(h.stops[i])
	h.nodes[i] = nil
}

func (h *harness) restart(i int) {
	h.crash(i)
	h.start(i)
}

func (h *harness) shutdown() {
	for i := range h.nodes {
		h.crash(i)
	}
}

// checkOneLeader waits for exactly one leader in the newest term and returns it.
func (h *harness) checkOneLeader() int {
	h.t.Helper()
	for iter := 0; iter < 20; iter++ {
		time.Sleep(150 * time.Millisecond)
		leaders := map[uint64][]int{}
		for i, rf := range h.nodes {
			if rf == nil {
				continue
			}
			if term, ok := rf.State(); ok {
				leaders[term] = append(leaders[term], i)
			}
		}
		var newest uint64
		for term, ls := range leaders {
			if len(ls) > 1 {
				h.t.Fatalf("term %d has %d leaders", term, len(ls))
			}
			if term > newest {
				newest = term
			}
		}
		if len(leaders) > 0 {
			return leaders[newest][0]
		}
	}
	h.t.Fatal("expected a leader, found none")
	return -1
}

func (h *harness) checkNoLeader() {
	h.t.Helper()
	for i, rf := range h.nodes {
		if rf == nil || !h.net.Connected(i, i) {
			continue
		}
		if _, ok := rf.State(); ok {
			connected := 0
			for j := range h.nodes {
				if j != i && h.net.Connected(i, j) {
					connected++
				}
			}
			if connected+1 > h.n/2 {
				continue
			}
			h.t.Fatalf("node %d is leader without a majority", i)
		}
	}
}

func (h *harness) terms() uint64 {
	var term uint64
	for _, rf := range h.nodes {
		if rf != nil {
			t, _ := rf.State()
			if term == 0 {
				term = t
			} else if t != term {
				h.t.Fatalf("nodes disagree on term: %d vs %d", term, t)
			}
		}
	}
	return term
}

// nCommitted returns how many nodes applied index and the command there.
func (h *harness) nCommitted(idx uint64) (int, string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	count, cmd := 0, ""
	for _, m := range h.applied {
		if c, ok := m[idx]; ok {
			count++
			cmd = c
		}
	}
	return count, cmd
}

// one submits cmd and waits until at least `expected` nodes apply it.
// With retry, it resubmits if the leader fails before committing.
func (h *harness) one(cmd string, expected int, retry bool) uint64 {
	h.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	starts := 0
	for time.Now().Before(deadline) {
		var idx uint64
		found := false
		for k := 0; k < h.n; k++ {
			starts = (starts + 1) % h.n
			if rf := h.nodes[starts]; rf != nil {
				if i, _, ok := rf.Start([]byte(cmd)); ok {
					idx, found = i, true
					break
				}
			}
		}
		if found {
			t1 := time.Now().Add(2 * time.Second)
			for time.Now().Before(t1) {
				n, c := h.nCommitted(idx)
				if n >= expected && c == cmd {
					h.checkErr()
					return idx
				}
				time.Sleep(20 * time.Millisecond)
			}
			if !retry {
				h.t.Fatalf("one(%q) failed to reach agreement", cmd)
			}
		} else {
			time.Sleep(50 * time.Millisecond)
		}
	}
	h.checkErr()
	h.t.Fatalf("one(%q) failed to reach agreement", cmd)
	return 0
}

func cmd(i int) string { return strconv.Itoa(i) }
