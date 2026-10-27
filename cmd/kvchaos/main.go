// Command kvchaos runs many randomized fault-injection runs against an
// in-process cluster and checks each history for linearizability.
//
//	kvchaos -runs 100 -duration 3s
//	kvchaos -runs 20 -unsafe-reads   # break reads on purpose; the checker should catch it
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/zeiado/kvraft/chaos"
)

func main() {
	runs := flag.Int("runs", 10, "number of runs")
	duration := flag.Duration("duration", 3*time.Second, "fault phase length per run")
	nodes := flag.Int("nodes", 5, "cluster size")
	clients := flag.Int("clients", 8, "concurrent clients")
	keys := flag.Int("keys", 10, "number of distinct keys")
	seed := flag.Int64("seed", 0, "first seed (0 = random); run i uses seed+i")
	crashes := flag.Bool("crashes", true, "crash and restart nodes")
	partitions := flag.Bool("partitions", true, "partition the network")
	unreliable := flag.Bool("unreliable", true, "drop and delay messages")
	snapshot := flag.Int("snapshot-threshold", 200, "snapshot after this many log entries (0 = never)")
	unsafe := flag.Bool("unsafe-reads", false, "serve reads without ReadIndex (deliberately broken)")
	vizDir := flag.String("viz-dir", ".", "where to write the HTML visualization of a failing history")
	verbose := flag.Bool("v", false, "print every fault")
	flag.Parse()

	if *seed == 0 {
		*seed = time.Now().UnixNano()
	}
	var ok, bad, unknown, totalOps int
	start := time.Now()
	for i := 0; i < *runs; i++ {
		s := *seed + int64(i)
		cfg := chaos.Config{
			Seed: s, Nodes: *nodes, Clients: *clients, Keys: *keys, Duration: *duration,
			Crashes: *crashes, Partitions: *partitions, Unreliable: *unreliable,
			SnapshotThreshold: *snapshot, UnsafeLocalReads: *unsafe,
			VisualizePath: fmt.Sprintf("%s/kvchaos-%d.html", *vizDir, s),
		}
		if *verbose {
			cfg.Logf = func(f string, a ...any) { fmt.Printf("    "+f+"\n", a...) }
		}
		r, err := chaos.Run(cfg)
		if err != nil {
			fmt.Printf("run %3d seed=%d ERROR: %v\n", i+1, s, err)
			bad++
			continue
		}
		totalOps += r.Ops
		verdict := "linearizable"
		switch {
		case r.Linearizable():
			ok++
		case r.Result == "Unknown":
			unknown++
			verdict = "UNKNOWN (checker timed out)"
		default:
			bad++
			verdict = "NOT LINEARIZABLE -> " + cfg.VisualizePath
		}
		fmt.Printf("run %3d seed=%d ops=%-6d faults=%-3d terms=%-3d %s\n", i+1, s, r.Ops, len(r.Faults), r.LeaderTerms, verdict)
	}
	fmt.Printf("\n%d runs, %d operations in %v: %d linearizable, %d violations, %d unknown\n",
		*runs, totalOps, time.Since(start).Round(time.Second), ok, bad, unknown)
	if bad > 0 {
		os.Exit(1)
	}
}
