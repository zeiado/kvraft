package raft

import "sync"

// MemoryStorage keeps state in memory and models a disk with a write cache:
// writes are visible right away but only become durable on Sync. Crash
// throws away anything not yet synced, so tests catch code that acts on
// state before it's durable. It's meant for tests and simulations.
type MemoryStorage struct {
	mu sync.Mutex
	// Working copy (the "page cache").
	hs      HardState
	snap    Snapshot
	entries []LogEntry
	// Durable copy (the "platter").
	dHS      HardState
	dSnap    Snapshot
	dEntries []LogEntry
}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{hs: HardState{VotedFor: None}, dHS: HardState{VotedFor: None}}
}

func (m *MemoryStorage) Load() (PersistentState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return PersistentState{
		HardState: m.hs,
		Snapshot:  m.snap,
		Entries:   append([]LogEntry(nil), m.entries...),
	}, nil
}

func (m *MemoryStorage) SaveHardState(hs HardState) error {
	m.mu.Lock()
	m.hs = hs
	m.mu.Unlock()
	return nil
}

func (m *MemoryStorage) AppendEntries(entries []LogEntry) error {
	if len(entries) == 0 {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries = spliceEntries(m.entries, m.snap.Index, entries)
	return nil
}

func (m *MemoryStorage) Sync() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.syncLocked()
	return nil
}

func (m *MemoryStorage) syncLocked() {
	m.dHS, m.dSnap = m.hs, m.snap
	m.dEntries = append(m.dEntries[:0:0], m.entries...)
}

// SaveSnapshot is durable when it returns, like the file implementation.
func (m *MemoryStorage) SaveSnapshot(snap Snapshot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if snap.Index <= m.dSnap.Index {
		return nil
	}
	m.snap, m.dSnap = snap, snap
	m.entries = trimThrough(m.entries, snap.Index)
	m.dEntries = trimThrough(m.dEntries, snap.Index)
	return nil
}

// ResetLog drops every entry (buffered until Sync, like other writes).
func (m *MemoryStorage) ResetLog() error {
	m.mu.Lock()
	m.entries = nil
	m.mu.Unlock()
	return nil
}

// trimThrough drops entries with index <= idx.
func trimThrough(log []LogEntry, idx uint64) []LogEntry {
	for i, e := range log {
		if e.Index > idx {
			return append([]LogEntry(nil), log[i:]...)
		}
	}
	return nil
}

// Crash discards every write since the last Sync, as a power loss would.
func (m *MemoryStorage) Crash() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.hs, m.snap = m.dHS, m.dSnap
	m.entries = append([]LogEntry(nil), m.dEntries...)
}

// spliceEntries writes entries into log (which starts right after base),
// replacing any existing suffix from entries[0].Index on.
func spliceEntries(log []LogEntry, base uint64, entries []LogEntry) []LogEntry {
	first := entries[0].Index
	if first <= base {
		// Entries covered by the snapshot are already compacted; drop them.
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
