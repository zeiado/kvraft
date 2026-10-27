// Command kvserver runs one node of a kvraft cluster.
//
//	kvserver -id 0 -peers 0=http://localhost:8001,1=http://localhost:8002,2=http://localhost:8003 -data ./data/0
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/zeiado/kvraft/kv"
	"github.com/zeiado/kvraft/raft"
	"github.com/zeiado/kvraft/storage"
	"github.com/zeiado/kvraft/transport/httpnet"
)

func main() {
	id := flag.Int("id", 0, "this node's ID")
	peersFlag := flag.String("peers", "", "comma-separated id=url for every node, including this one")
	listen := flag.String("listen", "", "address to listen on (default: the port from this node's peer URL)")
	dataDir := flag.String("data", "", "data directory (default ./data/<id>)")
	snapThreshold := flag.Int("snapshot-threshold", 10000, "snapshot after this many log entries (0 = never)")
	electionTimeout := flag.Duration("election-timeout", 300*time.Millisecond, "minimum election timeout")
	noSync := flag.Bool("no-sync", false, "skip fsync (faster, unsafe on power loss)")
	verbose := flag.Bool("v", false, "debug logging")
	flag.Parse()

	level := slog.LevelInfo
	if *verbose {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	peers, err := parsePeers(*peersFlag)
	if err != nil {
		fatal(logger, "bad -peers", err)
	}
	self, ok := peers[*id]
	if !ok {
		fatal(logger, "bad -peers", fmt.Errorf("no entry for -id %d", *id))
	}
	if *listen == "" {
		u, _ := url.Parse(self)
		*listen = ":" + u.Port()
	}
	if *dataDir == "" {
		*dataDir = fmt.Sprintf("data/%d", *id)
	}

	store, err := storage.Open(*dataDir, storage.Options{NoSync: *noSync})
	if err != nil {
		fatal(logger, "open storage", err)
	}
	var ids []int
	for p := range peers {
		ids = append(ids, p)
	}
	sort.Ints(ids)
	rcfg := raft.Config{
		ID:                 *id,
		Peers:              ids,
		ElectionTimeoutMin: *electionTimeout,
		HeartbeatInterval:  *electionTimeout / 6,
		Logger:             logger,
	}
	transport := httpnet.NewTransport(peers)
	srv, err := kv.NewServer(kv.Config{SnapshotThreshold: *snapThreshold, Logger: logger},
		func(applyCh chan<- raft.ApplyMsg) (*raft.Raft, error) {
			return raft.New(rcfg, transport, store, applyCh)
		})
	if err != nil {
		fatal(logger, "start server", err)
	}

	mux := http.NewServeMux()
	httpnet.RegisterRaft(mux, srv.Raft())
	httpnet.RegisterKV(mux, srv, 2*time.Second)
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		st := struct {
			raft.Status
			Keys int `json:"keys"`
		}{srv.Raft().Status(), srv.Len()}
		_ = json.NewEncoder(w).Encode(st)
	})

	httpSrv := &http.Server{Addr: *listen, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		logger.Info("listening", "addr", *listen, "id", *id, "data", *dataDir)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fatal(logger, "http", err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	logger.Info("shutting down")
	httpSrv.Close()
	srv.Stop()
	store.Close()
}

func parsePeers(s string) (map[int]string, error) {
	peers := map[int]string{}
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		idStr, u, ok := strings.Cut(part, "=")
		if !ok {
			return nil, fmt.Errorf("%q: want id=url", part)
		}
		id, err := strconv.Atoi(idStr)
		if err != nil {
			return nil, fmt.Errorf("%q: bad id", part)
		}
		peers[id] = strings.TrimRight(u, "/")
	}
	if len(peers) == 0 {
		return nil, fmt.Errorf("no peers given")
	}
	return peers, nil
}

func fatal(l *slog.Logger, msg string, err error) {
	l.Error(msg, "err", err)
	os.Exit(1)
}
