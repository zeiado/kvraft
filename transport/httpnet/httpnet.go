// Package httpnet carries Raft RPCs and client requests over HTTP with JSON
// bodies. It's simple to debug with curl and has no code generation step.
package httpnet

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/zeiado/kvraft/kv"
	"github.com/zeiado/kvraft/raft"
)

// Transport sends Raft RPCs to peers at the given base URLs.
type Transport struct {
	peers  map[int]string // node ID -> base URL, e.g. http://node1:8080
	client *http.Client
}

func NewTransport(peers map[int]string) *Transport {
	return &Transport{
		peers: peers,
		client: &http.Client{Transport: &http.Transport{
			MaxIdleConnsPerHost: 16,
			IdleConnTimeout:     30 * time.Second,
		}},
	}
}

func post[R any](ctx context.Context, c *http.Client, url string, body any) (*R, error) {
	b, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("%s: %s: %s", url, resp.Status, bytes.TrimSpace(msg))
	}
	var r R
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return nil, err
	}
	return &r, nil
}

func (t *Transport) url(to int, path string) (string, error) {
	base, ok := t.peers[to]
	if !ok {
		return "", fmt.Errorf("httpnet: unknown peer %d", to)
	}
	return base + path, nil
}

func (t *Transport) RequestVote(ctx context.Context, to int, a *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	u, err := t.url(to, "/raft/vote")
	if err != nil {
		return nil, err
	}
	return post[raft.RequestVoteReply](ctx, t.client, u, a)
}

func (t *Transport) AppendEntries(ctx context.Context, to int, a *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	u, err := t.url(to, "/raft/append")
	if err != nil {
		return nil, err
	}
	return post[raft.AppendEntriesReply](ctx, t.client, u, a)
}

func (t *Transport) InstallSnapshot(ctx context.Context, to int, a *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	u, err := t.url(to, "/raft/snapshot")
	if err != nil {
		return nil, err
	}
	return post[raft.InstallSnapshotReply](ctx, t.client, u, a)
}

func handle[A any, R any](fn func(*A) (*R, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var a A
		if err := json.NewDecoder(r.Body).Decode(&a); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		reply, err := fn(&a)
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(reply)
	}
}

// RegisterRaft serves Raft RPCs for h on mux.
func RegisterRaft(mux *http.ServeMux, h raft.Handler) {
	mux.Handle("POST /raft/vote", handle(h.HandleRequestVote))
	mux.Handle("POST /raft/append", handle(h.HandleAppendEntries))
	mux.Handle("POST /raft/snapshot", handle(h.HandleInstallSnapshot))
}

// kvResponse is the wire format for /kv responses.
type kvResponse struct {
	Result kv.Result `json:"result"`
	Error  string    `json:"error,omitempty"`
	Leader *int      `json:"leader,omitempty"` // set on wrong-leader errors
}

// RegisterKV serves client requests (POST /kv) for srv on mux.
func RegisterKV(mux *http.ServeMux, srv *kv.Server, requestTimeout time.Duration) {
	mux.HandleFunc("POST /kv", func(w http.ResponseWriter, r *http.Request) {
		var op kv.Op
		if err := json.NewDecoder(r.Body).Decode(&op); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
		defer cancel()
		res, err := srv.Do(ctx, op)
		resp := kvResponse{Result: res}
		if err != nil {
			resp.Error = err.Error()
			var le *kv.LeaderError
			if errors.As(err, &le) {
				l := le.Leader
				resp.Leader = &l
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})
}

// KVEndpoint is a kv.Endpoint that talks to a server over HTTP.
type KVEndpoint struct {
	URL    string
	Client *http.Client
}

func (e *KVEndpoint) Do(ctx context.Context, op kv.Op) (kv.Result, error) {
	c := e.Client
	if c == nil {
		c = http.DefaultClient
	}
	r, err := post[kvResponse](ctx, c, e.URL+"/kv", op)
	if err != nil {
		return kv.Result{}, err
	}
	if r.Error != "" {
		if r.Leader != nil {
			return kv.Result{}, &kv.LeaderError{Leader: *r.Leader}
		}
		return kv.Result{}, errors.New(r.Error)
	}
	return r.Result, nil
}
