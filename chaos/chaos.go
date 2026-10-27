// Package chaos runs a simulated cluster under random faults (crashes,
// restarts, partitions, message loss and delay) while concurrent clients
// read and write. It records every operation and checks the history for
// linearizability with porcupine.
package chaos

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/zeiado/kvraft/kv"
	"github.com/zeiado/kvraft/sim"
)

// Config controls a chaos run.
type Config struct {
	Seed              int64
	Nodes             int           // default 5
	Clients           int           // default 8
	Keys              int           // default 5; fewer keys = more contention
	Duration          time.Duration // fault phase length, default 5s
	Crashes           bool
	Partitions        bool
	Unreliable        bool // drop 10% of messages and delay others up to 25ms
	SnapshotThreshold int
	UnsafeLocalReads  bool   // deliberately break reads to show the checker works
	DataDir           string // if set, nodes use real on-disk storage here
	// VisualizePath, if set, writes porcupine's HTML visualization there
	// when the history is not linearizable.
	VisualizePath string
	Logf          func(format string, args ...any)
}

// Report summarizes a run.
type Report struct {
	Seed         int64
	Ops          int
	Faults       []string
	Result       porcupine.CheckResult
	CheckTime    time.Duration
	LeaderTerms  uint64
	FinalCommits uint64
}

func (r Report) Linearizable() bool { return r.Result == porcupine.Ok }

type recorder struct {
	mu    sync.Mutex
	start time.Time
	ops   []porcupine.Operation
}

func (r *recorder) now() int64 { return int64(time.Since(r.start)) }

func (r *recorder) add(client int, in opInput, out opOutput, call, ret int64) {
	r.mu.Lock()
	r.ops = append(r.ops, porcupine.Operation{ClientId: client, Input: in, Output: out, Call: call, Return: ret})
	r.mu.Unlock()
}

// Run executes one chaos run.
func Run(cfg Config) (Report, error) {
	if cfg.Nodes == 0 {
		cfg.Nodes = 5
	}
	if cfg.Clients == 0 {
		cfg.Clients = 8
	}
	if cfg.Keys == 0 {
		cfg.Keys = 10
	}
	if cfg.Duration == 0 {
		cfg.Duration = 5 * time.Second
	}
	if cfg.Seed == 0 {
		cfg.Seed = time.Now().UnixNano()
	}
	logf := cfg.Logf
	if logf == nil {
		logf = func(string, ...any) {}
	}
	rng := rand.New(rand.NewSource(cfg.Seed))
	report := Report{Seed: cfg.Seed}

	var electionTimeout time.Duration // sim default
	if cfg.DataDir != "" {
		// Real fsyncs can take tens of milliseconds; give RPCs room.
		electionTimeout = 500 * time.Millisecond
	}
	c := sim.New(sim.Options{ElectionTimeout: electionTimeout, Nodes: cfg.Nodes, Seed: cfg.Seed, SnapshotThreshold: cfg.SnapshotThreshold, UnsafeLocalReads: cfg.UnsafeLocalReads, DataDir: cfg.DataDir})
	defer c.Shutdown()
	if c.WaitLeader(5*time.Second) < 0 {
		return report, fmt.Errorf("no leader elected")
	}
	if cfg.Unreliable {
		c.Net.SetUnreliable(0.1, 25*time.Millisecond)
	}

	rec := &recorder{start: time.Now()}
	stopClients := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < cfg.Clients; i++ {
		wg.Add(1)
		seed := rng.Int63()
		go func(id int) {
			defer wg.Done()
			runClient(id, c, rec, rand.New(rand.NewSource(seed)), cfg, stopClients)
		}(i)
	}

	// Nemesis: inject a fault, let it sit, repair, repeat.
	deadline := time.Now().Add(cfg.Duration)
	for time.Now().Before(deadline) {
		f := inject(c, rng, cfg)
		if f != "" {
			report.Faults = append(report.Faults, f)
			logf("fault: %s", f)
		}
		time.Sleep(time.Duration(200+rng.Intn(600)) * time.Millisecond)
		if rng.Intn(2) == 0 {
			c.Net.Heal()
		}
		for i := 0; i < cfg.Nodes; i++ {
			if !c.Alive(i) && rng.Intn(2) == 0 {
				c.Restart(i)
				logf("restart node %d", i)
			}
		}
	}

	// Repair everything so in-flight client requests can finish.
	c.Net.Heal()
	c.Net.SetUnreliable(0, 0)
	for i := 0; i < cfg.Nodes; i++ {
		if !c.Alive(i) {
			c.Restart(i)
		}
	}
	close(stopClients)
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		return report, fmt.Errorf("clients did not finish after the cluster healed (liveness bug)")
	}

	for _, s := range c.Status() {
		if s.Term > report.LeaderTerms {
			report.LeaderTerms = s.Term
		}
		if s.CommitIndex > report.FinalCommits {
			report.FinalCommits = s.CommitIndex
		}
	}

	report.Ops = len(rec.ops)
	t0 := time.Now()
	res, info := porcupine.CheckOperationsVerbose(Model, rec.ops, time.Minute)
	report.Result = res
	report.CheckTime = time.Since(t0)
	if res == porcupine.Illegal && cfg.VisualizePath != "" {
		if err := porcupine.VisualizePath(Model, info, cfg.VisualizePath); err != nil {
			logf("could not write visualization: %v", err)
		}
	}
	return report, nil
}

func inject(c *sim.Cluster, rng *rand.Rand, cfg Config) string {
	n := cfg.Nodes
	var kinds []string
	if cfg.Crashes {
		kinds = append(kinds, "crash", "crash-leader", "power-outage")
	}
	if cfg.Partitions {
		kinds = append(kinds, "partition", "isolate-leader")
	}
	if len(kinds) == 0 {
		return ""
	}
	alive := 0
	for i := 0; i < n; i++ {
		if c.Alive(i) {
			alive++
		}
	}
	switch kinds[rng.Intn(len(kinds))] {
	case "crash":
		if alive <= n/2+1 {
			return "" // keep a quorum alive most of the time so progress is possible
		}
		i := rng.Intn(n)
		c.Kill(i)
		return fmt.Sprintf("crash node %d", i)
	case "crash-leader":
		if l := c.Leader(); l >= 0 {
			c.Kill(l)
			return fmt.Sprintf("crash leader %d", l)
		}
	case "power-outage":
		// Every node loses power at once, dropping anything not fsynced,
		// then comes back. Catches acknowledging writes before they're durable.
		for i := 0; i < n; i++ {
			c.Kill(i)
		}
		for i := 0; i < n; i++ {
			c.Restart(i)
		}
		return "power outage: all nodes crashed and restarted"
	case "partition":
		perm := rng.Perm(n)
		cut := 1 + rng.Intn(n/2)
		c.Net.Partition(perm[:cut], perm[cut:])
		return fmt.Sprintf("partition %v | %v", perm[:cut], perm[cut:])
	case "isolate-leader":
		if l := c.Leader(); l >= 0 {
			c.Net.Isolate(l)
			return fmt.Sprintf("isolate leader %d", l)
		}
	}
	return ""
}

func runClient(id int, c *sim.Cluster, rec *recorder, rng *rand.Rand, cfg Config, stop chan struct{}) {
	cl := c.Client()
	eps := c.Endpoints()
	ctx := context.Background()
	keys := cfg.Keys
	for j := 0; ; j++ {
		select {
		case <-stop:
			return
		default:
		}
		key := fmt.Sprintf("k%d", rng.Intn(keys))
		val := fmt.Sprintf("c%d-%d", id, j)
		var in opInput
		switch r := rng.Intn(10); {
		case r < 4:
			in = opInput{Type: kv.OpGet, Key: key}
		case r < 7:
			in = opInput{Type: kv.OpPut, Key: key, Value: val}
		case r < 8:
			in = opInput{Type: kv.OpDelete, Key: key}
		default:
			// CAS against a value this or another client might have written.
			exp := fmt.Sprintf("c%d-%d", rng.Intn(id+1), rng.Intn(j+1))
			in = opInput{Type: kv.OpCAS, Key: key, Expected: exp, Value: val}
		}
		op := kv.Op{Type: in.Type, Key: in.Key, Value: in.Value, Expected: in.Expected}
		call := rec.now()
		var res kv.Result
		var err error
		if cfg.UnsafeLocalReads && in.Type == kv.OpGet {
			// Read from any replica, as a "fast reads" optimization might.
			res, err = eps[rng.Intn(len(eps))].Do(ctx, op)
			if err != nil {
				continue // node down; this read never happened
			}
		} else {
			// The client retries forever; requests complete once the
			// cluster heals, so every operation gets a return time.
			res, err = cl.Do(ctx, op)
			if err != nil {
				return
			}
		}
		rec.add(id, in, opOutput{Value: res.Value, Found: res.Found, OK: res.OK}, call, rec.now())
		// A little think time keeps histories small enough to check quickly.
		time.Sleep(time.Duration(rng.Intn(3)) * time.Millisecond)
	}
}
