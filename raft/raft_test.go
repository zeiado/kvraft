package raft_test

import (
	"math/rand"
	"testing"
	"time"
)

func TestInitialElection(t *testing.T) {
	h := newHarness(t, 3, 0)
	h.checkOneLeader()
	term1 := h.terms()
	time.Sleep(time.Second)
	if term2 := h.terms(); term1 != term2 {
		t.Logf("warning: term changed with no failures (%d -> %d)", term1, term2)
	}
	h.checkOneLeader()
}

func TestReElection(t *testing.T) {
	h := newHarness(t, 3, 0)
	l1 := h.checkOneLeader()

	h.net.Isolate(l1)
	h.checkOneLeader()

	// Old leader rejoins: still exactly one leader.
	h.net.Heal()
	l2 := h.checkOneLeader()

	// No quorum: no leader.
	h.net.Partition([]int{l2}, []int{(l2 + 1) % 3}, []int{(l2 + 2) % 3})
	time.Sleep(time.Second)
	h.checkNoLeader()

	h.net.Partition([]int{(l2 + 1) % 3, (l2 + 2) % 3}, []int{l2})
	h.checkOneLeader()
	h.net.Heal()
	h.checkOneLeader()
}

func TestManyElections(t *testing.T) {
	h := newHarness(t, 7, 0)
	h.checkOneLeader()
	for i := 0; i < 10; i++ {
		var cut []int
		for len(cut) < 3 {
			c := rand.Intn(7)
			dup := false
			for _, x := range cut {
				dup = dup || x == c
			}
			if !dup {
				cut = append(cut, c)
			}
		}
		var rest []int
		for j := 0; j < 7; j++ {
			in := false
			for _, x := range cut {
				in = in || x == j
			}
			if !in {
				rest = append(rest, j)
			}
		}
		h.net.Partition(rest, cut)
		h.checkOneLeader()
		h.net.Heal()
	}
	h.checkOneLeader()
}

func TestBasicAgree(t *testing.T) {
	h := newHarness(t, 3, 0)
	for i := 1; i <= 5; i++ {
		h.one(cmd(i*100), 3, false)
	}
}

func TestFollowerFailure(t *testing.T) {
	h := newHarness(t, 3, 0)
	h.one("101", 3, false)
	l := h.checkOneLeader()
	h.net.Isolate((l + 1) % 3)
	h.one("102", 2, false)
	h.one("103", 2, false)

	// Both followers gone: nothing can commit.
	h.net.Isolate((l + 2) % 3)
	idx, _, ok := h.nodes[l].Start([]byte("104"))
	if !ok {
		t.Fatal("leader rejected Start")
	}
	time.Sleep(time.Second)
	if n, _ := h.nCommitted(idx); n > 0 {
		t.Fatalf("%d nodes committed without a majority", n)
	}
}

func TestRejoinWithConflictingLog(t *testing.T) {
	h := newHarness(t, 3, 0)
	h.one("101", 3, true)

	l1 := h.checkOneLeader()
	h.net.Isolate(l1)
	// These never commit: the old leader is alone.
	h.nodes[l1].Start([]byte("102"))
	h.nodes[l1].Start([]byte("103"))
	h.nodes[l1].Start([]byte("104"))

	h.one("103b", 2, true)

	l2 := h.checkOneLeader()
	// Old leader l1 rejoins with the remaining follower; l2 is cut off.
	if (l2+1)%3 != l1 {
		h.net.Partition([]int{l1, (l2 + 1) % 3}, []int{l2})
	} else {
		h.net.Partition([]int{l1, (l2 + 2) % 3}, []int{l2})
	}
	h.one("104b", 2, true)

	h.net.Heal()
	h.one("105", 3, true)
}

func TestBackupFast(t *testing.T) {
	// A leader and one follower get far ahead of the others, which have a
	// long uncommitted log from an old term; the conflict hint must let the
	// new leader repair them quickly.
	h := newHarness(t, 5, 0)
	h.one("start", 5, true)

	l := h.checkOneLeader()
	h.net.Partition([]int{l, (l + 1) % 5}, []int{(l + 2) % 5, (l + 3) % 5, (l + 4) % 5})
	for i := 0; i < 50; i++ {
		h.nodes[l].Start([]byte("lost" + cmd(i)))
	}
	time.Sleep(500 * time.Millisecond)

	h.net.Partition([]int{(l + 2) % 5, (l + 3) % 5, (l + 4) % 5}, []int{l, (l + 1) % 5})
	for i := 0; i < 50; i++ {
		h.one("kept"+cmd(i), 3, true)
	}
	h.net.Heal()
	h.one("end", 5, true)
}

func TestPersistRestartAll(t *testing.T) {
	h := newHarness(t, 3, 0)
	h.one("11", 3, true)
	for i := 0; i < 3; i++ {
		h.restart(i)
	}
	h.one("12", 3, true)

	l := h.checkOneLeader()
	h.restart(l)
	h.one("13", 3, true)

	l = h.checkOneLeader()
	h.crash(l)
	h.one("14", 2, true)
	h.start(l)

	idx := h.one("15", 3, true)
	// Everyone re-applied the full log from storage after restarting.
	for i := uint64(1); i <= idx; i++ {
		if n, _ := h.nCommitted(i); n != 0 && n < 3 {
			t.Fatalf("index %d applied on %d nodes, want 3", i, n)
		}
	}
}

// TestFigure8Unreliable repeatedly crashes leaders on an unreliable network
// (Raft paper Figure 8 scenario) and checks that committed entries stay put.
func TestFigure8Unreliable(t *testing.T) {
	if testing.Short() {
		t.Skip("slow")
	}
	h := newHarness(t, 5, 0)
	h.net.SetUnreliable(0.1, 20*time.Millisecond)
	h.one(cmd(rand.Intn(10000)), 1, true)

	up := 5
	for iter := 0; iter < 200; iter++ {
		leader := -1
		for i, rf := range h.nodes {
			if rf != nil {
				if _, _, ok := rf.Start([]byte(cmd(rand.Intn(10000)))); ok {
					leader = i
				}
			}
		}
		if rand.Intn(1000) < 100 {
			time.Sleep(time.Duration(rand.Intn(500)) * time.Millisecond)
		} else {
			time.Sleep(time.Duration(rand.Intn(13)) * time.Millisecond)
		}
		if leader != -1 && rand.Intn(1000) < 500 {
			h.crash(leader)
			up--
		}
		if up < 3 {
			s := rand.Intn(5)
			if h.nodes[s] == nil {
				h.start(s)
				up++
			}
		}
		h.checkErr()
	}
	for i := range h.nodes {
		if h.nodes[i] == nil {
			h.start(i)
		}
	}
	h.net.SetUnreliable(0, 0)
	h.one(cmd(rand.Intn(10000)), 5, true)
}

func TestSnapshotInstall(t *testing.T) {
	h := newHarness(t, 3, 10)
	h.one("first", 3, true)

	l := h.checkOneLeader()
	victim := (l + 1) % 3
	h.crash(victim)
	for i := 0; i < 60; i++ {
		h.one(cmd(i), 2, true)
	}
	if n := h.nodes[l].LogLen(); n > 20 {
		t.Fatalf("leader log has %d entries, expected snapshots to compact it", n)
	}
	// The victim restarts far behind the leader's snapshot and must catch
	// up via InstallSnapshot.
	h.start(victim)
	h.one("after", 3, true)
	h.checkErr()
}

func TestSnapshotRestart(t *testing.T) {
	h := newHarness(t, 3, 10)
	for i := 0; i < 35; i++ {
		h.one(cmd(i), 3, true)
	}
	for i := 0; i < 3; i++ {
		h.restart(i)
	}
	h.one("x", 3, true)
	h.checkErr()
}

func TestSingleNode(t *testing.T) {
	h := newHarness(t, 1, 0)
	h.checkOneLeader()
	h.one("a", 1, false)
	h.restart(0)
	h.one("b", 1, true)
}

// With Pre-Vote, a follower that was cut off can't win elections while it's
// alone, so when it rejoins the leader keeps its term.
func TestRejoiningNodeDoesNotDisruptLeader(t *testing.T) {
	h := newHarness(t, 3, 0)
	h.one("a", 3, true)
	l := h.checkOneLeader()
	term, _ := h.nodes[l].State()

	f := (l + 1) % 3
	h.net.Isolate(f)
	time.Sleep(2 * time.Second) // many election timeouts
	if ft, _ := h.nodes[f].State(); ft != term {
		t.Fatalf("isolated follower bumped its term %d -> %d", term, ft)
	}
	h.net.Heal()
	h.one("b", 3, true)
	if l2 := h.checkOneLeader(); l2 != l {
		t.Fatalf("leader changed from %d to %d after a follower rejoined", l, l2)
	}
	if t2, _ := h.nodes[l].State(); t2 != term {
		t.Fatalf("term changed %d -> %d after a follower rejoined", term, t2)
	}
}
