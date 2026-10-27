package storage

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/zeiado/kvraft/raft"
)

func ent(idx, term uint64, cmd string) raft.LogEntry {
	return raft.LogEntry{Index: idx, Term: term, Command: []byte(cmd)}
}

func open(t *testing.T, dir string, opts Options) (*FileStorage, raft.PersistentState) {
	t.Helper()
	s, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	ps, err := s.Load()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s, ps
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func segments(t *testing.T, dir string) []string {
	t.Helper()
	m, _ := filepath.Glob(filepath.Join(dir, "wal-*.log"))
	return m
}

func TestEmpty(t *testing.T) {
	_, ps := open(t, t.TempDir(), Options{})
	if ps.HardState.VotedFor != raft.None || ps.HardState.Term != 0 || len(ps.Entries) != 0 {
		t.Fatalf("unexpected state %+v", ps)
	}
}

func TestRoundTripAndTruncate(t *testing.T) {
	dir := t.TempDir()
	s, _ := open(t, dir, Options{})
	must(t, s.SaveHardState(raft.HardState{Term: 2, VotedFor: 1}))
	must(t, s.AppendEntries([]raft.LogEntry{ent(1, 1, "a"), ent(2, 1, "b"), ent(3, 2, "c")}))
	// A new leader overwrites index 2 onward.
	must(t, s.AppendEntries([]raft.LogEntry{ent(2, 3, "B"), ent(3, 3, "C"), ent(4, 3, "D")}))
	must(t, s.SaveHardState(raft.HardState{Term: 3, VotedFor: 2}))
	must(t, s.Sync())
	s.Close()

	_, ps := open(t, dir, Options{})
	want := []raft.LogEntry{ent(1, 1, "a"), ent(2, 3, "B"), ent(3, 3, "C"), ent(4, 3, "D")}
	if !reflect.DeepEqual(ps.Entries, want) {
		t.Fatalf("entries = %+v, want %+v", ps.Entries, want)
	}
	if ps.HardState != (raft.HardState{Term: 3, VotedFor: 2}) {
		t.Fatalf("hard state = %+v", ps.HardState)
	}
}

func TestTornTailIsDropped(t *testing.T) {
	dir := t.TempDir()
	s, _ := open(t, dir, Options{})
	must(t, s.AppendEntries([]raft.LogEntry{ent(1, 1, "a")}))
	must(t, s.AppendEntries([]raft.LogEntry{ent(2, 1, "b")}))
	must(t, s.Sync())
	s.Close()

	// Simulate a crash halfway through writing the last record.
	path := segments(t, dir)[0]
	info, _ := os.Stat(path)
	must(t, os.Truncate(path, info.Size()-5))

	s2, ps := open(t, dir, Options{})
	if len(ps.Entries) != 1 || ps.Entries[0].Index != 1 {
		t.Fatalf("entries after torn write = %+v", ps.Entries)
	}
	// Writing after recovery must produce a readable log.
	must(t, s2.AppendEntries([]raft.LogEntry{ent(2, 2, "b2")}))
	must(t, s2.Sync())
	s2.Close()
	_, ps = open(t, dir, Options{})
	if len(ps.Entries) != 2 || string(ps.Entries[1].Command) != "b2" {
		t.Fatalf("entries = %+v", ps.Entries)
	}
}

func TestCorruptionInMiddleIsAnError(t *testing.T) {
	dir := t.TempDir()
	s, _ := open(t, dir, Options{})
	must(t, s.AppendEntries([]raft.LogEntry{ent(1, 1, "aaaa")}))
	must(t, s.AppendEntries([]raft.LogEntry{ent(2, 1, "bbbb")}))
	must(t, s.Sync())
	s.Close()

	path := segments(t, dir)[0]
	data, _ := os.ReadFile(path)
	data[12] ^= 0xff // flip a byte inside the first record
	must(t, os.WriteFile(path, data, 0o644))

	s2, err := Open(dir, Options{})
	must(t, err)
	if _, err := s2.Load(); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Load error = %v, want ErrCorrupt", err)
	}
}

func TestSegmentsRotateAndCompact(t *testing.T) {
	dir := t.TempDir()
	s, _ := open(t, dir, Options{SegmentBytes: 1024})
	must(t, s.SaveHardState(raft.HardState{Term: 7, VotedFor: 2}))
	for i := uint64(1); i <= 200; i++ {
		must(t, s.AppendEntries([]raft.LogEntry{ent(i, 7, "some-command-payload")}))
	}
	must(t, s.Sync())
	before := len(segments(t, dir))
	if before < 5 {
		t.Fatalf("expected several segments, got %d", before)
	}

	must(t, s.SaveSnapshot(raft.Snapshot{Index: 150, Term: 7, Data: []byte("state@150")}))
	after := len(segments(t, dir))
	if after >= before {
		t.Fatalf("compaction deleted nothing: %d -> %d segments", before, after)
	}
	must(t, s.AppendEntries([]raft.LogEntry{ent(201, 8, "x")}))
	must(t, s.Sync())
	s.Close()

	_, ps := open(t, dir, Options{SegmentBytes: 1024})
	if ps.Snapshot.Index != 150 || string(ps.Snapshot.Data) != "state@150" {
		t.Fatalf("snapshot = %+v", ps.Snapshot)
	}
	if len(ps.Entries) != 51 || ps.Entries[0].Index != 151 || ps.Entries[50].Index != 201 {
		t.Fatalf("got %d entries starting at %d", len(ps.Entries), ps.Entries[0].Index)
	}
	// The vote was in a deleted segment; every segment re-records it.
	if ps.HardState != (raft.HardState{Term: 7, VotedFor: 2}) {
		t.Fatalf("hard state = %+v", ps.HardState)
	}
}

func TestOlderSnapshotIsIgnored(t *testing.T) {
	dir := t.TempDir()
	s, _ := open(t, dir, Options{})
	must(t, s.SaveSnapshot(raft.Snapshot{Index: 10, Term: 1, Data: []byte("new")}))
	must(t, s.SaveSnapshot(raft.Snapshot{Index: 5, Term: 1, Data: []byte("old")}))
	s.Close()
	_, ps := open(t, dir, Options{})
	if ps.Snapshot.Index != 10 || string(ps.Snapshot.Data) != "new" {
		t.Fatalf("snapshot = %+v", ps.Snapshot)
	}
}

// After installing a snapshot that conflicts with the log, the reset record
// must drop the old entries even though they come after the snapshot index.
func TestResetLog(t *testing.T) {
	dir := t.TempDir()
	s, _ := open(t, dir, Options{})
	must(t, s.AppendEntries([]raft.LogEntry{ent(1, 1, "a"), ent(2, 1, "b"), ent(3, 1, "c"), ent(4, 1, "d")}))
	must(t, s.SaveSnapshot(raft.Snapshot{Index: 2, Term: 2, Data: []byte("s")}))
	must(t, s.ResetLog())
	must(t, s.AppendEntries([]raft.LogEntry{ent(3, 2, "C")}))
	must(t, s.Sync())
	s.Close()

	_, ps := open(t, dir, Options{})
	want := []raft.LogEntry{ent(3, 2, "C")}
	if !reflect.DeepEqual(ps.Entries, want) {
		t.Fatalf("entries = %+v, want %+v", ps.Entries, want)
	}
}

// A torn record in an older segment means the crash happened before that
// Sync finished, so nothing after it was acknowledged and later segments go.
func TestTornOlderSegmentDropsLaterOnes(t *testing.T) {
	dir := t.TempDir()
	s, _ := open(t, dir, Options{SegmentBytes: 256})
	for i := uint64(1); i <= 40; i++ {
		must(t, s.AppendEntries([]raft.LogEntry{ent(i, 1, "payload")}))
	}
	must(t, s.Sync())
	s.Close()
	segs := segments(t, dir)
	if len(segs) < 3 {
		t.Fatalf("want >= 3 segments, got %d", len(segs))
	}
	info, _ := os.Stat(segs[1])
	must(t, os.Truncate(segs[1], info.Size()-3))

	_, ps := open(t, dir, Options{SegmentBytes: 256})
	if got := len(segments(t, dir)); got != 2 {
		t.Fatalf("want 2 segments left, got %d", got)
	}
	for i, e := range ps.Entries {
		if e.Index != uint64(i+1) {
			t.Fatalf("log has a gap at position %d: %+v", i, e)
		}
	}
}

// Sync must not block appends, and appends during a Sync must not be lost.
func TestConcurrentAppendAndSync(t *testing.T) {
	dir := t.TempDir()
	s, _ := open(t, dir, Options{SegmentBytes: 4096})
	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				must(t, s.Sync())
			}
		}
	}()
	for i := uint64(1); i <= 500; i++ {
		must(t, s.AppendEntries([]raft.LogEntry{ent(i, 1, "v")}))
	}
	close(stop)
	wg.Wait()
	must(t, s.Sync())
	s.Close()
	_, ps := open(t, dir, Options{SegmentBytes: 4096})
	if len(ps.Entries) != 500 {
		t.Fatalf("got %d entries, want 500", len(ps.Entries))
	}
}
