package kv

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"time"
)

// Client talks to a cluster and retries until each request succeeds. Writes
// keep the same sequence number across retries, so they apply exactly once.
//
// A Client is safe for concurrent use, but it issues one request at a time:
// the server's dedup table only remembers each client's latest write.
type Client struct {
	mu        sync.Mutex
	endpoints []Endpoint // indexed by node ID
	leader    int        // server to try next
	rr        int        // round-robin cursor, so bad leader hints can't trap us in a loop
	id        string
	seq       uint64

	// AttemptTimeout bounds each try against one server. Default 500ms.
	AttemptTimeout time.Duration
}

// NewClient creates a client. endpoints[i] must be node i.
func NewClient(endpoints []Endpoint) *Client {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return &Client{endpoints: endpoints, id: hex.EncodeToString(b[:]), AttemptTimeout: 500 * time.Millisecond}
}

// ID returns the client's unique ID.
func (c *Client) ID() string { return c.id }

func (c *Client) Get(ctx context.Context, key string) (string, bool, error) {
	r, err := c.Do(ctx, Op{Type: OpGet, Key: key})
	return r.Value, r.Found, err
}

func (c *Client) Put(ctx context.Context, key, value string) error {
	_, err := c.Do(ctx, Op{Type: OpPut, Key: key, Value: value})
	return err
}

func (c *Client) Delete(ctx context.Context, key string) (bool, error) {
	r, err := c.Do(ctx, Op{Type: OpDelete, Key: key})
	return r.Found, err
}

// CAS sets key to value if its current value is expected.
func (c *Client) CAS(ctx context.Context, key, expected, value string) (bool, error) {
	r, err := c.Do(ctx, Op{Type: OpCAS, Key: key, Expected: expected, Value: value})
	return r.OK, err
}

// Do runs op against the cluster, retrying until it succeeds or ctx ends.
func (c *Client) Do(ctx context.Context, op Op) (Result, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if op.Type != OpGet {
		c.seq++
		op.ClientID, op.Seq = c.id, c.seq
	}
	backoff := 5 * time.Millisecond
	followedHint := false
	for {
		actx, cancel := context.WithTimeout(ctx, c.AttemptTimeout)
		r, err := c.endpoints[c.leader].Do(actx, op)
		cancel()
		if err == nil {
			return r, nil
		}
		if ctx.Err() != nil {
			return Result{}, ctx.Err()
		}
		var le *LeaderError
		if errors.As(err, &le) && !followedHint && le.Leader >= 0 && le.Leader < len(c.endpoints) && le.Leader != c.leader {
			c.leader = le.Leader
			followedHint = true
			continue // follow the hint right away, but only once in a row
		}
		followedHint = false
		c.rr = (c.rr + 1) % len(c.endpoints)
		c.leader = c.rr
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return Result{}, ctx.Err()
		}
		if backoff < 100*time.Millisecond {
			backoff *= 2
		}
	}
}
