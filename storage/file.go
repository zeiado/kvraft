// Package storage provides durable, crash-safe Raft storage on disk.
//
// Layout of the data directory:
//
//	wal-0000000000000001.log  write-ahead log, split into segments
//	wal-0000000000000002.log
//	snapshot.bin              latest snapshot, replaced atomically
//
// Each WAL record is framed as [4B length][4B CRC32-C][payload], where
// payload = [1B type][JSON body].
//
// Design goals:
//   - Appends only write to the OS page cache. Sync fsyncs, so many appends
//     share one fsync (group commit), and the fsync never holds the lock
//     that appends take.
//   - Snapshots are written to their own file, outside any lock appends
//     need. Compaction then just deletes whole old segments.
//   - On startup a torn record (crash mid-write) is truncated, and anything
//     after it is dropped: nothing past that point was ever acknowledged.
package storage

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/zeiado/kvraft/raft"
)

const (
	snapName = "snapshot.bin"

	recHardState byte = 1
	recEntries   byte = 2
	recReset     byte = 3 // drop every log entry (after installing a snapshot)

	maxRecord = 256 << 20
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// ErrCorrupt means data that was already acknowledged is damaged.
var ErrCorrupt = errors.New("storage: corrupt data")

// Options tweak FileStorage behaviour.
type Options struct {
	// NoSync skips fsync. Faster, but a power loss can lose acknowledged
	// writes. Only for benchmarks.
	NoSync bool
	// SegmentBytes is the size at which a new WAL segment starts. Default 16MB.
	SegmentBytes int64
}

type segment struct {
	seq      uint64
	path     string
	f        *os.File // nil once closed (old, fully synced segments)
	size     int64
	maxIndex uint64 // highest log index written to this segment
	dirty    bool   // has writes that aren't fsynced yet
}

// FileStorage implements raft.Storage on the local filesystem.
type FileStorage struct {
	dir  string
	opts Options

	mu       sync.Mutex // guards everything below; never held during fsync
	segs     []*segment // oldest first; the last one is being appended to
	hs       raft.HardState
	dirDirty bool   // a segment was created since the last directory fsync
	snapIdx  uint64 // index of the durable snapshot

	syncMu sync.Mutex // serializes Sync
	snapMu sync.Mutex // serializes SaveSnapshot
}

// Open opens (or creates) storage in dir. Call Load before anything else.
func Open(dir string, opts Options) (*FileStorage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	if opts.SegmentBytes == 0 {
		opts.SegmentBytes = 16 << 20
	}
	return &FileStorage{dir: dir, opts: opts}, nil
}

// Close closes all open files.
func (s *FileStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var first error
	for _, seg := range s.segs {
		if seg.f != nil {
			if err := seg.f.Close(); err != nil && first == nil {
				first = err
			}
			seg.f = nil
		}
	}
	return first
}

func segName(seq uint64) string { return fmt.Sprintf("wal-%016d.log", seq) }

func listSegments(dir string) ([]uint64, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var seqs []uint64
	for _, e := range ents {
		n := e.Name()
		if !strings.HasPrefix(n, "wal-") || !strings.HasSuffix(n, ".log") {
			continue
		}
		seq, err := strconv.ParseUint(strings.TrimSuffix(strings.TrimPrefix(n, "wal-"), ".log"), 10, 64)
		if err == nil {
			seqs = append(seqs, seq)
		}
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	return seqs, nil
}

// Load reads the snapshot and replays the WAL.
func (s *FileStorage) Load() (raft.PersistentState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ps := raft.PersistentState{HardState: raft.HardState{VotedFor: raft.None}}

	snap, err := readSnapshot(filepath.Join(s.dir, snapName))
	if err != nil {
		return ps, err
	}
	ps.Snapshot = snap
	s.snapIdx = snap.Index

	seqs, err := listSegments(s.dir)
	if err != nil {
		return ps, err
	}
	torn := false
	for _, seq := range seqs {
		path := filepath.Join(s.dir, segName(seq))
		if torn {
			// Everything after a torn record was never acknowledged.
			if err := os.Remove(path); err != nil {
				return ps, err
			}
			continue
		}
		seg := &segment{seq: seq, path: path}
		f, err := os.OpenFile(path, os.O_RDWR, 0o644)
		if err != nil {
			return ps, err
		}
		good, isTorn, err := replay(f, func(typ byte, body []byte) error {
			switch typ {
			case recHardState:
				return json.Unmarshal(body, &ps.HardState)
			case recEntries:
				var entries []raft.LogEntry
				if err := json.Unmarshal(body, &entries); err != nil {
					return err
				}
				if len(entries) > 0 {
					ps.Entries = splice(ps.Entries, snap.Index, entries)
					if last := entries[len(entries)-1].Index; last > seg.maxIndex {
						seg.maxIndex = last
					}
				}
			case recReset:
				ps.Entries = nil
			default:
				return fmt.Errorf("unknown record type %d", typ)
			}
			return nil
		})
		if err != nil {
			f.Close()
			return ps, fmt.Errorf("%s: %w", path, err)
		}
		if isTorn {
			torn = true
			if err := f.Truncate(good); err != nil {
				f.Close()
				return ps, err
			}
		}
		seg.size = good
		f.Close()
		s.segs = append(s.segs, seg)
	}
	s.hs = ps.HardState

	// Reopen the last segment for appending, or start the first one.
	if len(s.segs) == 0 {
		if err := s.newSegmentLocked(1); err != nil {
			return ps, err
		}
	} else {
		last := s.segs[len(s.segs)-1]
		f, err := os.OpenFile(last.path, os.O_RDWR, 0o644)
		if err != nil {
			return ps, err
		}
		if _, err := f.Seek(last.size, io.SeekStart); err != nil {
			f.Close()
			return ps, err
		}
		last.f = f
	}
	return ps, nil
}

// replay calls fn for every intact record. It returns the offset just past
// the last intact record and whether the file ends in a torn record.
func replay(f *os.File, fn func(typ byte, body []byte) error) (int64, bool, error) {
	info, err := f.Stat()
	if err != nil {
		return 0, false, err
	}
	size := info.Size()
	r := bufio.NewReaderSize(f, 1<<16)
	var off int64
	var hdr [8]byte
	for off < size {
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			return off, true, nil
		}
		n := binary.LittleEndian.Uint32(hdr[:4])
		sum := binary.LittleEndian.Uint32(hdr[4:])
		end := off + 8 + int64(n)
		if n == 0 || n > maxRecord || end > size {
			return off, true, nil // torn or zero-filled tail
		}
		payload := make([]byte, n)
		if _, err := io.ReadFull(r, payload); err != nil {
			return off, true, nil
		}
		if crc32.Checksum(payload, crcTable) != sum {
			if end == size {
				return off, true, nil // the last record was half-written
			}
			return off, false, fmt.Errorf("%w: bad checksum at offset %d", ErrCorrupt, off)
		}
		if err := fn(payload[0], payload[1:]); err != nil {
			return off, false, fmt.Errorf("%w: record at offset %d: %v", ErrCorrupt, off, err)
		}
		off = end
	}
	return off, false, nil
}

func encodeRecord(typ byte, v any) ([]byte, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	payload := append([]byte{typ}, body...)
	rec := make([]byte, 8+len(payload))
	binary.LittleEndian.PutUint32(rec[:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(rec[4:8], crc32.Checksum(payload, crcTable))
	copy(rec[8:], payload)
	return rec, nil
}

// newSegmentLocked starts a new segment and writes the current hard state
// into it, so older segments can be deleted without losing the vote.
func (s *FileStorage) newSegmentLocked(seq uint64) error {
	path := filepath.Join(s.dir, segName(seq))
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	seg := &segment{seq: seq, path: path, f: f, dirty: true}
	s.segs = append(s.segs, seg)
	s.dirDirty = true
	return s.writeLocked(recHardState, s.hs, 0)
}

func (s *FileStorage) writeLocked(typ byte, v any, maxIndex uint64) error {
	cur := s.segs[len(s.segs)-1]
	if cur.f == nil {
		return errors.New("storage: not loaded")
	}
	rec, err := encodeRecord(typ, v)
	if err != nil {
		return err
	}
	if _, err := cur.f.Write(rec); err != nil {
		return err
	}
	cur.size += int64(len(rec))
	cur.dirty = true
	if maxIndex > cur.maxIndex {
		cur.maxIndex = maxIndex
	}
	return nil
}

func (s *FileStorage) appendRecord(typ byte, v any, maxIndex uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.segs) == 0 {
		return errors.New("storage: not loaded")
	}
	if cur := s.segs[len(s.segs)-1]; cur.size >= s.opts.SegmentBytes {
		// The old segment stays open until the next Sync fsyncs it.
		if err := s.newSegmentLocked(cur.seq + 1); err != nil {
			return err
		}
	}
	if typ == recHardState {
		s.hs = v.(raft.HardState)
	}
	return s.writeLocked(typ, v, maxIndex)
}

func (s *FileStorage) SaveHardState(hs raft.HardState) error {
	return s.appendRecord(recHardState, hs, 0)
}

func (s *FileStorage) AppendEntries(entries []raft.LogEntry) error {
	if len(entries) == 0 {
		return nil
	}
	return s.appendRecord(recEntries, entries, entries[len(entries)-1].Index)
}

// ResetLog drops every log entry. Raft calls it after installing a snapshot
// that replaces the whole log; the snapshot must already be saved.
func (s *FileStorage) ResetLog() error {
	return s.appendRecord(recReset, struct{}{}, 0)
}

// Sync makes every earlier write durable. Appends can proceed while it runs.
func (s *FileStorage) Sync() error {
	s.syncMu.Lock()
	defer s.syncMu.Unlock()

	s.mu.Lock()
	var files []*os.File
	for _, seg := range s.segs {
		if seg.dirty && seg.f != nil {
			files = append(files, seg.f)
			seg.dirty = false // a write that races with the fsync sets it again
		}
	}
	dirDirty := s.dirDirty
	s.dirDirty = false
	s.mu.Unlock()

	if !s.opts.NoSync {
		for _, f := range files {
			if err := f.Sync(); err != nil {
				return err
			}
		}
		if dirDirty {
			syncDir(s.dir)
		}
	}

	// Close old segments that are fully durable; nothing writes to them.
	s.mu.Lock()
	for _, seg := range s.segs[:len(s.segs)-1] {
		if seg.f != nil && !seg.dirty {
			seg.f.Close()
			seg.f = nil
		}
	}
	s.mu.Unlock()
	return nil
}

type snapshotFile struct {
	Index uint64 `json:"index"`
	Term  uint64 `json:"term"`
	Data  []byte `json:"data"`
}

// SaveSnapshot durably replaces the snapshot (if snap is newer) and deletes
// WAL segments it makes obsolete. It may run concurrently with appends.
func (s *FileStorage) SaveSnapshot(snap raft.Snapshot) error {
	s.snapMu.Lock()
	defer s.snapMu.Unlock()
	s.mu.Lock()
	cur := s.snapIdx
	s.mu.Unlock()
	if snap.Index <= cur {
		return nil
	}

	body, err := json.Marshal(snapshotFile{Index: snap.Index, Term: snap.Term, Data: snap.Data})
	if err != nil {
		return err
	}
	frame := make([]byte, 4+len(body))
	binary.LittleEndian.PutUint32(frame[:4], crc32.Checksum(body, crcTable))
	copy(frame[4:], body)
	if err := s.writeAtomic(snapName, frame); err != nil {
		return err
	}

	// Delete the oldest segments whose entries are all covered by the
	// snapshot. Only closed (fully synced) segments, and only a prefix, so
	// replay order is preserved.
	s.mu.Lock()
	s.snapIdx = snap.Index
	var doomed []string
	for len(s.segs) > 1 {
		seg := s.segs[0]
		if seg.f != nil || seg.maxIndex > snap.Index {
			break
		}
		doomed = append(doomed, seg.path)
		s.segs = s.segs[1:]
	}
	s.mu.Unlock()
	for _, p := range doomed {
		if err := os.Remove(p); err != nil {
			return err
		}
	}
	return nil
}

func (s *FileStorage) writeAtomic(name string, data []byte) error {
	final := filepath.Join(s.dir, name)
	tmp := final + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if !s.opts.NoSync {
		if err := f.Sync(); err != nil {
			f.Close()
			return err
		}
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, final); err != nil {
		return err
	}
	if !s.opts.NoSync {
		syncDir(s.dir)
	}
	return nil
}

// syncDir makes renames and new files durable. It's a no-op on Windows,
// which doesn't support fsync on directories.
func syncDir(dir string) {
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		d.Close()
	}
}

func readSnapshot(path string) (raft.Snapshot, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return raft.Snapshot{}, nil
	}
	if err != nil {
		return raft.Snapshot{}, err
	}
	if len(data) < 4 || crc32.Checksum(data[4:], crcTable) != binary.LittleEndian.Uint32(data[:4]) {
		return raft.Snapshot{}, fmt.Errorf("%w: snapshot checksum mismatch", ErrCorrupt)
	}
	var sf snapshotFile
	if err := json.Unmarshal(data[4:], &sf); err != nil {
		return raft.Snapshot{}, err
	}
	return raft.Snapshot{Index: sf.Index, Term: sf.Term, Data: sf.Data}, nil
}

// splice writes entries into log (which starts right after base), replacing
// any existing suffix from entries[0].Index on.
func splice(log []raft.LogEntry, base uint64, entries []raft.LogEntry) []raft.LogEntry {
	first := entries[0].Index
	if first <= base {
		skip := base + 1 - first
		if skip >= uint64(len(entries)) {
			return log
		}
		entries = entries[skip:]
		first = entries[0].Index
	}
	keep := first - base - 1
	if keep > uint64(len(log)) {
		keep = uint64(len(log))
	}
	return append(log[:keep:keep], entries...)
}
