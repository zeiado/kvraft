// Command kvctl is a command-line client for a kvraft cluster.
//
//	kvctl -servers http://localhost:8001,http://localhost:8002,http://localhost:8003 put greeting hello
//	kvctl get greeting
//	kvctl status
//	kvctl bench -clients 32 -duration 10s
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/zeiado/kvraft/kv"
	"github.com/zeiado/kvraft/raft"
	"github.com/zeiado/kvraft/transport/httpnet"
)

const usage = `usage: kvctl [-servers url,url,...] <command> [args]

commands:
  get <key>
  put <key> <value>
  delete <key>
  cas <key> <expected> <new>
  status                         show every node's Raft state
  bench [-clients N] [-duration D] [-keys K] [-reads PCT]

-servers defaults to $KVRAFT_SERVERS or http://localhost:8001..8005.
Server i in the list must be node i.
`

func main() {
	def := os.Getenv("KVRAFT_SERVERS")
	if def == "" {
		def = "http://localhost:8001,http://localhost:8002,http://localhost:8003,http://localhost:8004,http://localhost:8005"
	}
	servers := flag.String("servers", def, "comma-separated server URLs, in node ID order")
	timeout := flag.Duration("timeout", 10*time.Second, "overall timeout for a command")
	flag.Usage = func() { fmt.Fprint(os.Stderr, usage) }
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		flag.Usage()
		os.Exit(2)
	}
	urls := strings.Split(*servers, ",")
	httpClient := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 256}}
	var eps []kv.Endpoint
	for _, u := range urls {
		eps = append(eps, &httpnet.KVEndpoint{URL: strings.TrimRight(u, "/"), Client: httpClient})
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	cl := kv.NewClient(eps)

	need := func(n int) {
		if len(args) != n+1 {
			flag.Usage()
			os.Exit(2)
		}
	}
	var err error
	switch args[0] {
	case "get":
		need(1)
		var v string
		var found bool
		if v, found, err = cl.Get(ctx, args[1]); err == nil {
			if !found {
				fmt.Println("(not found)")
				os.Exit(1)
			}
			fmt.Println(v)
		}
	case "put":
		need(2)
		if err = cl.Put(ctx, args[1], args[2]); err == nil {
			fmt.Println("OK")
		}
	case "delete":
		need(1)
		var found bool
		if found, err = cl.Delete(ctx, args[1]); err == nil {
			fmt.Println(map[bool]string{true: "deleted", false: "(not found)"}[found])
		}
	case "cas":
		need(3)
		var ok bool
		if ok, err = cl.CAS(ctx, args[1], args[2], args[3]); err == nil {
			if !ok {
				fmt.Println("FAILED (value did not match)")
				os.Exit(1)
			}
			fmt.Println("OK")
		}
	case "status":
		status(urls, httpClient)
	case "bench":
		bench(args[1:], eps)
	default:
		flag.Usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func status(urls []string, c *http.Client) {
	tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "NODE\tURL\tROLE\tTERM\tLEADER\tCOMMIT\tAPPLIED\tLAST LOG\tSNAPSHOT\tKEYS")
	for i, u := range urls {
		var st struct {
			raft.Status
			Keys int `json:"keys"`
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(u, "/")+"/status", nil)
		resp, err := c.Do(req)
		if err == nil {
			err = json.NewDecoder(resp.Body).Decode(&st)
			resp.Body.Close()
		}
		cancel()
		if err != nil {
			fmt.Fprintf(tw, "%d\t%s\tDOWN\t-\t-\t-\t-\t-\t-\t-\n", i, u)
			continue
		}
		leader := "-"
		if st.LeaderID >= 0 {
			leader = fmt.Sprint(st.LeaderID)
		}
		fmt.Fprintf(tw, "%d\t%s\t%s\t%d\t%s\t%d\t%d\t%d\t%d\t%d\n", i, u, st.Role, st.Term, leader,
			st.CommitIndex, st.LastApplied, st.LastLogIndex, st.SnapshotIndex, st.Keys)
	}
	tw.Flush()
}

func bench(args []string, eps []kv.Endpoint) {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	clients := fs.Int("clients", 16, "concurrent clients")
	duration := fs.Duration("duration", 10*time.Second, "how long to run")
	keys := fs.Int("keys", 1000, "key space size")
	reads := fs.Int("reads", 50, "percentage of operations that are reads")
	fs.Parse(args)

	var mu sync.Mutex
	var lat []time.Duration
	var errs int
	end := time.Now().Add(*duration)
	var wg sync.WaitGroup
	for i := 0; i < *clients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cl := kv.NewClient(eps)
			var local []time.Duration
			for j := 0; time.Now().Before(end); j++ {
				key := fmt.Sprintf("bench-%d", (i*7919+j)%*keys)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				t0 := time.Now()
				var err error
				if (j*37+i*11)%100 < *reads { // spreads reads and writes evenly
					_, _, err = cl.Get(ctx, key)
				} else {
					err = cl.Put(ctx, key, fmt.Sprint(j))
				}
				cancel()
				if err != nil {
					mu.Lock()
					errs++
					mu.Unlock()
					continue
				}
				local = append(local, time.Since(t0))
			}
			mu.Lock()
			lat = append(lat, local...)
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	if len(lat) == 0 {
		fmt.Println("no successful operations")
		os.Exit(1)
	}
	sort.Slice(lat, func(a, b int) bool { return lat[a] < lat[b] })
	pct := func(p float64) time.Duration { return lat[int(float64(len(lat)-1)*p)].Round(10 * time.Microsecond) }
	fmt.Printf("clients=%d reads=%d%% duration=%v\n", *clients, *reads, *duration)
	fmt.Printf("ops=%d errors=%d throughput=%.0f ops/s\n", len(lat), errs, float64(len(lat))/duration.Seconds())
	fmt.Printf("latency p50=%v p90=%v p99=%v max=%v\n", pct(0.5), pct(0.9), pct(0.99), lat[len(lat)-1].Round(10*time.Microsecond))
}
