// Package sim runs a whole kvraft cluster inside one process on an in-memory
// network, with controls to crash nodes, restart them and partition the
// network. Tests, the chaos runner and the visualizer are built on it.
package sim

import (
	"context"
	"log/slog"
	"sort"
	"sync"
	"time"

	"fmt"
	"path/filepath"

	"github.com/zeiado/kvraft/kv"
	"github.com/zeiado/kvraft/raft"
	"github.com/zeiado/kvraft/storage"
	"github.com/zeiado/kvraft/transport/memnet"
)

// Options configure a simulated cluster.
type Options struct {
	Nodes             int
	Seed              int64
	SnapshotThreshold int  // see kv.Config; 0 disables snapshots
	UnsafeLocalReads  bool // see kv.Config; for demonstrating the checker only
	// DataDir, if set, stores each node's state on disk under DataDir/<id>
	// with the real file storage, instead of in memory.
	DataDir           string
	ElectionTimeout   time.Duration
	HeartbeatInterval time.Duration
	Logger            *slog.Logger
}

// Cluster is a simulated cluster. Each node's storage survives crashes, like
// a disk would.
type Cluster struct {
	mu      sync.Mutex
	opts    Options
	Net     *memnet.Network
	memory  []*raft.MemoryStorage  // used when DataDir is empty
	files   []*storage.FileStorage // used when DataDir is set
	servers []*kv.Server           // nil while crashed
}

func New(opts Options) *Cluster {
	if opts.Nodes == 0 {
		opts.Nodes = 5
	}
	if opts.Seed == 0 {
		opts.Seed = time.Now().UnixNano()
	}
	if opts.ElectionTimeout == 0 {
		opts.ElectionTimeout = 150 * time.Millisecond
	}
	if opts.HeartbeatInterval == 0 {
		opts.HeartbeatInterval = 30 * time.Millisecond
	}
	c := &Cluster{
		opts:    opts,
		Net:     memnet.New(opts.Seed),
		memory:  make([]*raft.MemoryStorage, opts.Nodes),
		files:   make([]*storage.FileStorage, opts.Nodes),
		servers: make([]*kv.Server, opts.Nodes),
	}
	for i := range c.servers {
		c.memory[i] = raft.NewMemoryStorage()
		c.Restart(i)
	}
	return c
}

// N returns the cluster size.
func (c *Cluster) N() int { return c.opts.Nodes }

func (c *Cluster) peers() []int {
	p := make([]int, c.opts.Nodes)
	for i := range p {
		p[i] = i
	}
	return p
}

// Restart starts node i (crashing it first if it's running).
func (c *Cluster) Restart(i int) {
	c.Kill(i)
	cfg := raft.Config{
		ID:                 i,
		Peers:              c.peers(),
		ElectionTimeoutMin: c.opts.ElectionTimeout,
		ElectionTimeoutMax: 2 * c.opts.ElectionTimeout,
		HeartbeatInterval:  c.opts.HeartbeatInterval,
		Logger:             c.opts.Logger,
	}
	srv, err := kv.NewServer(kv.Config{SnapshotThreshold: c.opts.SnapshotThreshold, UnsafeLocalReads: c.opts.UnsafeLocalReads, Logger: c.opts.Logger},
		func(applyCh chan<- raft.ApplyMsg) (*raft.Raft, error) {
			return raft.New(cfg, c.Net.Endpoint(i), c.storageFor(i), applyCh)
		})
	if err != nil {
		panic(fmt.Sprintf("sim: restart node %d: %v", i, err))
	}
	c.mu.Lock()
	c.servers[i] = srv
	c.mu.Unlock()
	c.Net.Attach(i, srv.Raft())
}

// Kill crashes node i. Its storage is kept.
func (c *Cluster) Kill(i int) {
	c.mu.Lock()
	srv := c.servers[i]
	c.servers[i] = nil
	c.mu.Unlock()
	if srv != nil {
		c.Net.Detach(i)
		srv.Stop()
		if c.opts.DataDir == "" {
			c.memory[i].Crash() // lose writes that weren't synced, like a power cut
		} else {
			c.files[i].Close()
			c.files[i] = nil
		}
	}
}

func (c *Cluster) storageFor(i int) raft.Storage {
	if c.opts.DataDir == "" {
		return c.memory[i]
	}
	fs, err := storage.Open(filepath.Join(c.opts.DataDir, fmt.Sprint(i)), storage.Options{SegmentBytes: 64 << 10})
	if err != nil {
		panic(err)
	}
	c.files[i] = fs
	return fs
}

// Alive reports whether node i is running.
func (c *Cluster) Alive(i int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.servers[i] != nil
}

// Server returns node i's server, or nil if it is down.
func (c *Cluster) Server(i int) *kv.Server {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.servers[i]
}

// Shutdown stops every node.
func (c *Cluster) Shutdown() {
	for i := range c.servers {
		c.Kill(i)
	}
}

// Endpoints returns one kv.Endpoint per node. They keep working across
// restarts and fail fast while a node is down.
func (c *Cluster) Endpoints() []kv.Endpoint {
	eps := make([]kv.Endpoint, c.opts.Nodes)
	for i := range eps {
		eps[i] = nodeEndpoint{c, i}
	}
	return eps
}

// Client returns a new client for the cluster.
func (c *Cluster) Client() *kv.Client { return kv.NewClient(c.Endpoints()) }

type nodeEndpoint struct {
	c  *Cluster
	id int
}

func (e nodeEndpoint) Do(ctx context.Context, op kv.Op) (kv.Result, error) {
	srv := e.c.Server(e.id)
	if srv == nil {
		select {
		case <-time.After(10 * time.Millisecond):
		case <-ctx.Done():
		}
		return kv.Result{}, kv.ErrStopped
	}
	return srv.Do(ctx, op)
}

// NodeStatus describes one node for dashboards.
type NodeStatus struct {
	raft.Status
	Alive bool `json:"alive"`
	Group int  `json:"group"`
	Keys  int  `json:"keys"`
}

// Status returns every node's state.
func (c *Cluster) Status() []NodeStatus {
	groups := c.Net.Groups()
	out := make([]NodeStatus, c.opts.Nodes)
	for i := range out {
		out[i].ID = i
		out[i].Group = groups[i]
		if srv := c.Server(i); srv != nil {
			out[i].Status = srv.Raft().Status()
			out[i].Alive = true
			out[i].Keys = srv.Len()
		} else {
			out[i].Status = raft.Status{ID: i, LeaderID: raft.None, VotedFor: raft.None}
		}
	}
	return out
}

// Leader returns the ID of the leader with the highest term, or -1.
func (c *Cluster) Leader() int {
	st := c.Status()
	sort.Slice(st, func(i, j int) bool { return st[i].Term > st[j].Term })
	for _, s := range st {
		if s.Alive && s.Role == raft.Leader {
			return s.ID
		}
	}
	return -1
}

// WaitLeader blocks until there's a leader or the timeout passes.
func (c *Cluster) WaitLeader(timeout time.Duration) int {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if l := c.Leader(); l >= 0 {
			return l
		}
		time.Sleep(10 * time.Millisecond)
	}
	return -1
}
