package kv_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zeiado/kvraft/kv"
	"github.com/zeiado/kvraft/sim"
)

func ctx(t *testing.T, d time.Duration) context.Context {
	c, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return c
}

func TestBasicOps(t *testing.T) {
	c := sim.New(sim.Options{Nodes: 3})
	defer c.Shutdown()
	cl := c.Client()
	cx := ctx(t, 10*time.Second)

	if _, found, err := cl.Get(cx, "a"); err != nil || found {
		t.Fatalf("get missing key: found=%v err=%v", found, err)
	}
	if err := cl.Put(cx, "a", "1"); err != nil {
		t.Fatal(err)
	}
	if v, found, err := cl.Get(cx, "a"); err != nil || !found || v != "1" {
		t.Fatalf("get = %q %v %v", v, found, err)
	}
	if ok, _ := cl.CAS(cx, "a", "wrong", "2"); ok {
		t.Fatal("CAS with wrong expected value succeeded")
	}
	if ok, _ := cl.CAS(cx, "a", "1", "2"); !ok {
		t.Fatal("CAS with right expected value failed")
	}
	if found, _ := cl.Delete(cx, "a"); !found {
		t.Fatal("delete reported key missing")
	}
	if _, found, _ := cl.Get(cx, "a"); found {
		t.Fatal("key still present after delete")
	}
}

// A retried write (same client ID and seq) must apply only once.
func TestDuplicateWriteAppliedOnce(t *testing.T) {
	c := sim.New(sim.Options{Nodes: 3})
	defer c.Shutdown()
	cx := ctx(t, 10*time.Second)
	c.Client().Put(cx, "n", "0") // wait for the cluster to be ready
	l := c.Leader()
	srv := c.Server(l)

	op := kv.Op{Type: kv.OpCAS, Key: "n", Expected: "0", Value: "1", ClientID: "c1", Seq: 1}
	r1, err := srv.Do(cx, op)
	if err != nil || !r1.OK {
		t.Fatalf("first CAS: %+v %v", r1, err)
	}
	r2, err := srv.Do(cx, op)
	if err != nil || !r2.OK || r2.Index != r1.Index {
		t.Fatalf("retried CAS should return the cached result, got %+v %v", r2, err)
	}
}

func TestLeaderFailover(t *testing.T) {
	c := sim.New(sim.Options{Nodes: 5})
	defer c.Shutdown()
	cl := c.Client()
	cx := ctx(t, 20*time.Second)
	for i := 0; i < 5; i++ {
		if err := cl.Put(cx, fmt.Sprint("k", i), fmt.Sprint(i)); err != nil {
			t.Fatal(err)
		}
	}
	for round := 0; round < 3; round++ {
		l := c.WaitLeader(5 * time.Second)
		c.Kill(l)
		if err := cl.Put(cx, "round", fmt.Sprint(round)); err != nil {
			t.Fatal(err)
		}
		c.Restart(l)
	}
	for i := 0; i < 5; i++ {
		v, _, err := cl.Get(cx, fmt.Sprint("k", i))
		if err != nil || v != fmt.Sprint(i) {
			t.Fatalf("k%d = %q, %v", i, v, err)
		}
	}
}

// A leader cut off in a minority must not serve stale reads.
func TestNoStaleReadFromMinorityLeader(t *testing.T) {
	c := sim.New(sim.Options{Nodes: 5})
	defer c.Shutdown()
	cx := ctx(t, 20*time.Second)
	cl := c.Client()
	cl.Put(cx, "x", "old")

	old := c.WaitLeader(5 * time.Second)
	var majority []int
	for i := 0; i < 5; i++ {
		if i != old && i != (old+1)%5 {
			majority = append(majority, i)
		}
	}
	c.Net.Partition([]int{old, (old + 1) % 5}, majority)

	// Write through the majority side.
	maj := kv.NewClient(c.Endpoints())
	if err := maj.Put(cx, "x", "new"); err != nil {
		t.Fatal(err)
	}
	// Reading directly from the old leader must fail, not return "old".
	rctx, cancel := context.WithTimeout(cx, time.Second)
	defer cancel()
	r, err := c.Server(old).Do(rctx, kv.Op{Type: kv.OpGet, Key: "x"})
	if err == nil && r.Value == "old" {
		t.Fatal("minority leader served a stale read")
	}
}

func TestSnapshotsKeepLogSmall(t *testing.T) {
	c := sim.New(sim.Options{Nodes: 3, SnapshotThreshold: 50})
	defer c.Shutdown()
	cl := c.Client()
	cx := ctx(t, 30*time.Second)

	c.WaitLeader(5 * time.Second)
	lagger := (c.Leader() + 1) % 3
	c.Kill(lagger)
	for i := 0; i < 300; i++ {
		if err := cl.Put(cx, fmt.Sprint("k", i%20), fmt.Sprint(i)); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 3; i++ {
		if s := c.Server(i); s != nil && s.Raft().LogLen() > 100 {
			t.Fatalf("node %d log has %d entries", i, s.Raft().LogLen())
		}
	}
	// The lagging node must catch up via InstallSnapshot and agree.
	c.Restart(lagger)
	c.Kill((lagger + 1) % 3) // force the lagger into the quorum
	v, _, err := cl.Get(cx, "k19")
	if err != nil || v != "299" {
		t.Fatalf("k19 = %q, %v", v, err)
	}
}

func TestConcurrentClients(t *testing.T) {
	c := sim.New(sim.Options{Nodes: 3})
	defer c.Shutdown()
	cx := ctx(t, 30*time.Second)
	const clients, perClient = 10, 30
	var wg sync.WaitGroup
	var errs atomic.Int32
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cl := c.Client()
			for j := 0; j < perClient; j++ {
				// Each client increments its own counter with CAS.
				key := fmt.Sprint("ctr", i)
				cur, _, err := cl.Get(cx, key)
				if err != nil {
					errs.Add(1)
					return
				}
				if cur == "" {
					if err := cl.Put(cx, key, "x"); err != nil {
						errs.Add(1)
					}
					continue
				}
				if ok, err := cl.CAS(cx, key, cur, cur+"x"); err != nil || !ok {
					errs.Add(1)
				}
			}
		}(i)
	}
	wg.Wait()
	if errs.Load() > 0 {
		t.Fatalf("%d client errors", errs.Load())
	}
	cl := c.Client()
	for i := 0; i < clients; i++ {
		v, _, _ := cl.Get(cx, fmt.Sprint("ctr", i))
		if len(v) != perClient {
			t.Fatalf("ctr%d has length %d, want %d", i, len(v), perClient)
		}
	}
}
