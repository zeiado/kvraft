// Command kvsim runs a simulated cluster in one process and serves a live
// web visualizer. From the browser you can crash nodes, partition the
// network and watch the cluster elect leaders and keep serving writes.
//
//	go run ./cmd/kvsim -nodes 5 -addr :8080
package main

import (
	"context"
	"embed"
	"encoding/json"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"math/rand"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/zeiado/kvraft/raft"
	"github.com/zeiado/kvraft/sim"
	"github.com/zeiado/kvraft/transport/memnet"
)

//go:embed web
var webFS embed.FS

type event struct {
	At   time.Time `json:"at"`
	Kind string    `json:"kind"` // leader | fault | heal | info
	Text string    `json:"text"`
}

// msgEvent is one RPC, numbered so the browser can ask for "everything
// after seq N".
type msgEvent struct {
	Seq     uint64 `json:"seq"`
	From    int    `json:"from"`
	To      int    `json:"to"`
	Kind    string `json:"kind"`
	Outcome string `json:"outcome"`
}

const msgRing = 2000

type app struct {
	c *sim.Cluster

	msgMu  sync.Mutex
	msgs   []msgEvent // ring buffer of the latest messages
	msgSeq uint64

	mu         sync.Mutex
	events     []event
	lastLeader int
	lastTerm   uint64
	okOps      int64
	failedOps  int64
	opsHistory []int // ok ops per 250ms tick, last 120 ticks
	latencies  []time.Duration
	unreliable bool
	paused     bool
}

func (a *app) logEvent(kind, format string, args ...any) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.events = append(a.events, event{At: time.Now(), Kind: kind, Text: fmt.Sprintf(format, args...)})
	if len(a.events) > 100 {
		a.events = a.events[len(a.events)-100:]
	}
}

// workload runs clients that keep writing and reading, so the UI shows
// whether the cluster is making progress.
func (a *app) workload(clients int) {
	for i := 0; i < clients; i++ {
		go func(i int) {
			cl := a.c.Client()
			cl.AttemptTimeout = 300 * time.Millisecond
			for j := 0; ; j++ {
				a.mu.Lock()
				paused := a.paused
				a.mu.Unlock()
				if paused {
					time.Sleep(100 * time.Millisecond)
					continue
				}
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				t0 := time.Now()
				var err error
				key := fmt.Sprintf("key-%d", rand.Intn(50))
				if j%3 == 0 {
					_, _, err = cl.Get(ctx, key)
				} else {
					err = cl.Put(ctx, key, fmt.Sprintf("client%d-%d", i, j))
				}
				cancel()
				a.mu.Lock()
				if err == nil {
					a.okOps++
					a.latencies = append(a.latencies, time.Since(t0))
					if len(a.latencies) > 500 {
						a.latencies = a.latencies[len(a.latencies)-500:]
					}
				} else {
					a.failedOps++
				}
				a.mu.Unlock()
				time.Sleep(time.Duration(5+rand.Intn(20)) * time.Millisecond)
			}
		}(i)
	}
}

// watch records leader changes and throughput samples.
func (a *app) watch() {
	var lastOK int64
	for range time.Tick(250 * time.Millisecond) {
		l := a.c.Leader()
		var term uint64
		for _, s := range a.c.Status() {
			if s.ID == l {
				term = s.Term
			}
		}
		a.mu.Lock()
		if l != a.lastLeader || term != a.lastTerm {
			changed := l
			a.lastLeader, a.lastTerm = l, term
			a.mu.Unlock()
			if changed >= 0 {
				a.logEvent("leader", "node %d is leader for term %d", changed, term)
			} else {
				a.logEvent("info", "no leader")
			}
			a.mu.Lock()
		}
		a.opsHistory = append(a.opsHistory, int(a.okOps-lastOK))
		if len(a.opsHistory) > 120 {
			a.opsHistory = a.opsHistory[1:]
		}
		lastOK = a.okOps
		a.mu.Unlock()
	}
}

func (a *app) recordMessage(m memnet.Message) {
	a.msgMu.Lock()
	defer a.msgMu.Unlock()
	a.msgSeq++
	ev := msgEvent{Seq: a.msgSeq, From: m.From, To: m.To, Kind: m.Kind, Outcome: m.Outcome}
	if len(a.msgs) < msgRing {
		a.msgs = append(a.msgs, ev)
	} else {
		a.msgs[(a.msgSeq-1)%msgRing] = ev
	}
}

// messagesSince returns messages after seq, at most limit of the newest.
func (a *app) messagesSince(seq uint64, limit int) ([]msgEvent, uint64) {
	a.msgMu.Lock()
	defer a.msgMu.Unlock()
	var out []msgEvent
	for _, m := range a.msgs {
		if m.Seq > seq {
			out = append(out, m)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	if len(out) > limit {
		out = out[len(out)-limit:]
	}
	return out, a.msgSeq
}

type stateResp struct {
	Nodes      []sim.NodeStatus `json:"nodes"`
	Events     []event          `json:"events"`
	OkOps      int64            `json:"okOps"`
	FailedOps  int64            `json:"failedOps"`
	OpsHistory []int            `json:"opsHistory"`
	P50Micros  int64            `json:"p50Micros"`
	P99Micros  int64            `json:"p99Micros"`
	Unreliable bool             `json:"unreliable"`
	Paused     bool             `json:"paused"`
	RPCs       uint64           `json:"rpcs"`
	Links      [][]bool         `json:"links"` // links[i][j]: can i reach j
	Messages   []msgEvent       `json:"messages"`
	MsgSeq     uint64           `json:"msgSeq"`
}

func (a *app) state() stateResp {
	nodes := a.c.Status()
	n := len(nodes)
	links := make([][]bool, n)
	for i := range links {
		links[i] = make([]bool, n)
		for j := range links[i] {
			links[i][j] = i != j && a.c.Net.Connected(i, j)
		}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	lat := append([]time.Duration(nil), a.latencies...)
	p50, p99 := int64(-1), int64(-1) // -1 = no data yet
	if len(lat) > 0 {
		sortDurations(lat)
		p50 = lat[len(lat)/2].Microseconds()
		p99 = lat[(len(lat)-1)*99/100].Microseconds()
	}
	return stateResp{
		Nodes: nodes, Events: append([]event(nil), a.events...),
		OkOps: a.okOps, FailedOps: a.failedOps, OpsHistory: append([]int(nil), a.opsHistory...),
		P50Micros: p50, P99Micros: p99, Unreliable: a.unreliable, Paused: a.paused,
		RPCs: a.c.Net.RPCCount(), Links: links,
	}
}

func (a *app) stateWithMessages(since uint64) stateResp {
	s := a.state()
	s.Messages, s.MsgSeq = a.messagesSince(since, 400)
	return s
}

func sortDurations(d []time.Duration) {
	for i := 1; i < len(d); i++ {
		for j := i; j > 0 && d[j] < d[j-1]; j-- {
			d[j], d[j-1] = d[j-1], d[j]
		}
	}
}

func (a *app) action(w http.ResponseWriter, r *http.Request) {
	id, _ := strconv.Atoi(r.URL.Query().Get("id"))
	n := a.c.N()
	if id < 0 || id >= n {
		http.Error(w, "bad id", http.StatusBadRequest)
		return
	}
	switch r.PathValue("action") {
	case "kill":
		a.c.Kill(id)
		a.logEvent("fault", "crashed node %d", id)
	case "restart":
		a.c.Restart(id)
		a.logEvent("heal", "restarted node %d", id)
	case "isolate":
		a.c.Net.Isolate(id)
		a.logEvent("fault", "isolated node %d from the network", id)
	case "kill-leader":
		if l := a.c.Leader(); l >= 0 {
			a.c.Kill(l)
			a.logEvent("fault", "crashed leader %d", l)
		}
	case "partition":
		perm := rand.Perm(n)
		cut := (n - 1) / 2
		a.c.Net.Partition(perm[:cut], perm[cut:])
		a.logEvent("fault", "partitioned %v | %v", perm[:cut], perm[cut:])
	case "split-no-quorum":
		var groups [][]int
		for i := 0; i < n; i += 2 {
			g := []int{i}
			if i+1 < n {
				g = append(g, i+1)
			}
			groups = append(groups, g)
		}
		a.c.Net.Partition(groups...)
		a.logEvent("fault", "split into %v (no majority anywhere)", groups)
	case "heal":
		a.c.Net.Heal()
		for i := 0; i < n; i++ {
			if !a.c.Alive(i) {
				a.c.Restart(i)
			}
		}
		a.logEvent("heal", "healed network and restarted all nodes")
	case "unreliable":
		a.mu.Lock()
		a.unreliable = !a.unreliable
		on := a.unreliable
		a.mu.Unlock()
		if on {
			a.c.Net.SetUnreliable(0.15, 30*time.Millisecond)
			a.logEvent("fault", "network now drops 15%% of messages")
		} else {
			a.c.Net.SetUnreliable(0, 0)
			a.logEvent("heal", "network reliable again")
		}
	case "pause":
		a.mu.Lock()
		a.paused = !a.paused
		a.mu.Unlock()
	default:
		http.Error(w, "unknown action", http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func main() {
	nodes := flag.Int("nodes", 5, "cluster size")
	clients := flag.Int("clients", 4, "background clients generating load")
	addr := flag.String("addr", "localhost:8080", "listen address")
	slow := flag.Float64("slow", 4, "slow time down by this factor so elections are visible (1 = real speed)")
	snap := flag.Int("snapshot-threshold", 300, "snapshot after this many log entries")
	flag.Parse()

	c := sim.New(sim.Options{
		Nodes:             *nodes,
		SnapshotThreshold: *snap,
		ElectionTimeout:   time.Duration(float64(150*time.Millisecond) * *slow),
		HeartbeatInterval: time.Duration(float64(30*time.Millisecond) * *slow),
	})
	a := &app{c: c, lastLeader: raft.None}
	c.Net.SetTracer(a.recordMessage)
	a.logEvent("info", "started %d-node cluster", *nodes)
	go a.watch()
	a.workload(*clients)

	mux := http.NewServeMux()
	static, _ := fs.Sub(webFS, "web")
	mux.Handle("GET /", http.FileServerFS(static))
	mux.HandleFunc("GET /api/state", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		since, _ := strconv.ParseUint(r.URL.Query().Get("since"), 10, 64)
		_ = json.NewEncoder(w).Encode(a.stateWithMessages(since))
	})
	mux.HandleFunc("POST /api/{action}", a.action)
	log.Printf("kvsim: %d nodes, visualizer at http://%s", *nodes, *addr)
	log.Fatal(http.ListenAndServe(*addr, mux))
}
