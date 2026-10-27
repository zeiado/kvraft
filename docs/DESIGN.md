# Design notes

This document explains the main decisions in kvraft and why they're safe. It assumes you know the basics of Raft ([the paper](https://raft.github.io/raft.pdf)). Section numbers like §6.4 refer to Diego Ongaro's [PhD thesis](https://github.com/ongardie/dissertation).

## 1. Raft core (`raft/`)

One mutex guards a node's state. Background goroutines:

- **ticker**: starts an election when the timeout fires, and makes a leader step down when it loses quorum.
- **replicator**, one per peer: sends AppendEntries when there are new entries or a heartbeat is due. It sends InstallSnapshot when the peer needs entries that were compacted away.
- **applier**: delivers committed entries and snapshots to the service, in order, on a channel. It never holds the lock while sending.
- **leaderSyncer**: fsyncs the leader's own new entries (see §3).

The log is a slice whose element 0 is a sentinel holding the last snapshot's index and term. That makes "the entry before the first real one" uniform whether or not there's a snapshot.

**Fast backtracking.** When AppendEntries fails, the follower replies with the conflicting term and the first index it has for that term. The leader skips a whole term per round trip instead of one entry (§5.3 of the paper). `TestBackupFast` exercises 50 divergent entries.

**No-op on election.** A new leader appends an empty entry in its own term. That's the only way to commit entries left over from earlier terms (Figure 8 in the paper), and ReadIndex needs it too.

## 2. Liveness extensions

**Pre-Vote (§9.6).** Before bumping its term, a would-be candidate asks "would you vote for me?" Peers say yes only if its log is up to date *and* they haven't heard from a leader recently. A node that was cut off can't win a pre-vote, so when it rejoins it doesn't force the healthy leader to step down with its inflated term. In the chaos runs this cut term churn under partitions roughly in half. `TestRejoiningNodeDoesNotDisruptLeader` checks it directly.

**Check-quorum.** A leader that hasn't heard from a majority within an election timeout steps down. Without this, a leader stranded in a minority keeps accepting requests that can never commit, and clients wait on it until they time out.

**Bad leader hints.** A follower clears its leader hint when its election timer fires. The client follows at most one hint in a row, then falls back to round-robin. Before this fix, two nodes in a minority could each point at the other and trap a client forever. `TestNoStaleReadFromMinorityLeader` caught it.

## 3. Durability and group commit

The naive approach, fsync inside the Raft lock on every write, stalls everything behind the disk. On the laptop this was built on, fsync averaged 57 ms with a worst case of 295 ms. That's longer than the RPC timeout, so the cluster kept holding elections.

kvraft splits a write into two halves:

1. **Write (under the lock).** The record goes to the OS page cache. Writes reach the file in exactly the order the in-memory log changed.
2. **Sync (outside the lock).** `syncTo(seq)` fsyncs everything written so far. Concurrent callers coalesce: while one fsync runs, others queue, and the next fsync covers all of them.

The safety rules:

- **Followers reply only after syncing.** A successful AppendEntries reply tells the leader "I have these entries", so they must be durable. The same goes for granting a vote.
- **The leader persists in parallel with replication (§10.2.1).** It sends new entries to followers right away, while `leaderSyncer` fsyncs its own copy. When counting replicas for commit, it counts *itself* only up to `selfDurable`, not up to its last index.
- **Candidates sync their own vote before counting it.** Elections are rare, so this one fsync happens under the lock.

**How we know it works.** `MemoryStorage` models a disk cache: writes are visible right away but survive `Crash()` only if `Sync` ran. The chaos tester's *power outage* fault crashes every node at once. Removing the follower's sync before replying makes the checker report violations in 10 of 12 runs, and with the sync in place every run passes.

## 4. Storage format (`storage/`)

```
wal-0000000000000001.log    [len u32][crc32c u32][type u8][json] ...
wal-0000000000000002.log
snapshot.bin                [crc32c u32][json {index, term, data}]
```

- **Segments.** A new segment starts every 16 MB, and each one begins with the current hard state (term and vote). Compaction is then just deleting old segment files whose entries are all covered by the snapshot. No rewrite, and no lock that appends need.
- **Snapshots outside the lock.** `raft.Snapshot()` writes `snapshot.bin` (temp file, fsync, rename, directory fsync) *before* taking the Raft lock to trim the in-memory log. Before this change, snapshotting under the lock caused an election every 10,000 entries under load. After it, a 40,000-write benchmark with four snapshots stayed on one leader.
- **Torn writes.** On startup, a record that is cut short or has a bad CRC at the end of a segment is truncated, and any later segments are deleted. Nothing after that point was acknowledged, because Sync hadn't returned. A bad CRC *before* the tail is reported as corruption instead of being silently skipped.
- **Reset record.** When a follower installs a snapshot that conflicts with its log, it writes a `reset` record after the snapshot is durable, so replay drops the stale entries.

## 5. The KV layer (`kv/`)

**Writes** go through the log. Each one carries `(clientID, seq)`. The state machine remembers each client's last applied `seq` and result, so a retried write returns the cached result instead of applying twice. The dedup table is part of the snapshot, so it survives restarts and travels with InstallSnapshot.

**Reads** don't touch the log. They use ReadIndex (§6.4):

1. The leader records `readIndex = commitIndex`. This is only valid once it has committed an entry in its current term, which the no-op guarantees.
2. It sends a heartbeat round and waits for a majority to acknowledge it as leader for this term.
3. It waits until its state machine has applied up to `readIndex`, then reads locally.

This is linearizable without writing anything to disk. The `-unsafe-reads` flag swaps it for "any replica answers from its local state", which is the classic stale-read bug. The checker flags it in every run.

**Detecting lost leadership.** A writer waits for its entry to be applied at the index `Start` returned. If a *different* entry shows up there (a different term or a different client), the leader lost its position, and the write is reported as failed so the client retries elsewhere. The dedup table keeps that retry safe.

## 6. Testing pyramid

| Layer | What it checks |
|---|---|
| `storage` tests | round trip, truncation, torn tails, mid-file corruption, segment rotation and compaction, concurrent append and sync |
| `raft` tests | election, re-election, no leader without quorum, agreement, conflicting logs, fast backup, restarts, Figure 8 on an unreliable network, snapshot install, Pre-Vote |
| `kv` tests | operations, exactly-once retries, failover, no stale reads from a minority leader, snapshots, concurrent CAS |
| `chaos` | randomized faults plus a porcupine linearizability check, on in-memory and on-disk storage |
| CI | everything above under `-race`, 30 chaos runs, and a negative test showing `-unsafe-reads` is caught |
