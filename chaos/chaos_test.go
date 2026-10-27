package chaos

import (
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
)

func TestChaos(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
	}{
		{"crashes", Config{Crashes: true}},
		{"partitions", Config{Partitions: true}},
		{"unreliable", Config{Unreliable: true}},
		{"everything+snapshots", Config{Crashes: true, Partitions: true, Unreliable: true, SnapshotThreshold: 100}},
		{"crashes+disk", Config{Crashes: true, Partitions: true, SnapshotThreshold: 100, DataDir: "<tmp>"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := tc.cfg
			cfg.Duration = 4 * time.Second
			if testing.Short() {
				cfg.Duration = time.Second
			}
			cfg.VisualizePath = t.TempDir() + "/history.html"
			if cfg.DataDir != "" {
				cfg.DataDir = t.TempDir()
			}
			r, err := Run(cfg)
			if err != nil {
				t.Fatalf("seed %d: %v", r.Seed, err)
			}
			t.Logf("seed=%d ops=%d faults=%d terms=%d check=%v result=%s",
				r.Seed, r.Ops, len(r.Faults), r.LeaderTerms, r.CheckTime.Round(time.Millisecond), r.Result)
			minOps := 50
			if cfg.DataDir != "" {
				minOps = 20 // fsync-bound; some laptop disks take 50ms+ per fsync
			}
			if r.Ops < minOps {
				t.Fatalf("only %d operations completed; cluster made too little progress", r.Ops)
			}
			if r.Result != porcupine.Ok {
				t.Fatalf("seed %d: history is %s (see %s)", r.Seed, r.Result, cfg.VisualizePath)
			}
		})
	}
}

// The checker must catch a broken history, otherwise the chaos test proves nothing.
func TestCheckerCatchesStaleRead(t *testing.T) {
	ops := []porcupine.Operation{
		{ClientId: 0, Input: opInput{Type: "put", Key: "x", Value: "1"}, Output: opOutput{OK: true}, Call: 0, Return: 10},
		{ClientId: 0, Input: opInput{Type: "put", Key: "x", Value: "2"}, Output: opOutput{OK: true}, Call: 20, Return: 30},
		{ClientId: 1, Input: opInput{Type: "get", Key: "x"}, Output: opOutput{Value: "1", Found: true}, Call: 40, Return: 50},
	}
	if porcupine.CheckOperations(Model, ops) {
		t.Fatal("a stale read was accepted as linearizable")
	}
}
