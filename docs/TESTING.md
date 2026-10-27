# Testing

```bash
go test ./...                 # everything (about 1.5 minutes)
go test -short ./...          # shorter chaos runs
go test -race ./...           # needs cgo; CI runs this on Linux
go run ./cmd/kvchaos -runs 50 # more randomized fault injection
```

## Chaos runs

`kvchaos` prints one line per run:

```
run   3 seed=1793131067816473402 ops=273    faults=7   terms=18  linearizable
```

- **ops**: completed client operations in the history
- **faults**: faults injected (crashes, partitions, power outages)
- **terms**: the highest Raft term reached, a rough measure of how many elections happened

To replay a run, pass its seed: `kvchaos -runs 1 -seed 1793131067816473402 -v`. Fault timing and the workload come from the seed, but goroutine scheduling doesn't, so replays are similar rather than identical.

If a history isn't linearizable, porcupine writes an HTML visualization (`kvchaos-<seed>.html`). It shows each client's operations on a timeline and the longest prefix that could be linearized.

Useful flags:

| Flag | Effect |
|---|---|
| `-crashes=false`, `-partitions=false`, `-unreliable=false` | Turn off a fault type |
| `-snapshot-threshold 0` | Never snapshot |
| `-unsafe-reads` | Deliberately break reads; you should see violations |
| `-keys 3` | Fewer keys means more contention, which makes bugs easier to find |

## Proving the tests can fail

A test suite that never fails proves nothing, so two broken variants were checked during development:

| Broken variant | How | Result |
|---|---|---|
| Reads served by any replica without ReadIndex | `-unsafe-reads` (kept as a flag; CI runs it and expects failure) | 4 of 4 runs flagged |
| Followers acknowledge AppendEntries before fsync | Removed `syncAll()` from `HandleAppendEntries` | 10 of 12 runs flagged, all from the power-outage fault |

The second one only showed up after the *power outage* fault was added. Crashing one node at a time never loses a majority's unsynced data, so the bug was invisible until every node could lose power together.

## Benchmarks

```bash
docker compose -f deploy/docker-compose.yml up -d --build
docker run --rm --network deploy_default --entrypoint kvctl <image> \
  -servers http://node0:8080,http://node1:8080,http://node2:8080,http://node3:8080,http://node4:8080 \
  bench -clients 64 -duration 10s
```

Run the client inside the Docker network if you can. On some Windows machines, security software adds about 10 ms to every loopback connection, which hides the store's own latency.
