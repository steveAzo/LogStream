package broker

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

// OffsetStore tracks the last committed offset per topic for one consumer group.
// It persists state to a single JSON file so consumers survive restarts.
//
// File format: {"events": 47, "payments": 102}
type OffsetStore struct {
	path    string
	mu      sync.Mutex
	offsets map[string]uint64 // topic name → last committed offset
}

// NewOffsetStore opens or creates an offset store at path.
// If the file exists, it loads the current offsets from it.
func NewOffsetStore(path string) (*OffsetStore, error) {
	s := &OffsetStore{
		path:    path,
		offsets: make(map[string]uint64),
	}

	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		// No file yet — fresh store, zero offsets everywhere. That's fine.
		return s, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read offset store %s: %w", path, err)
	}

	if err := json.Unmarshal(data, &s.offsets); err != nil {
		return nil, fmt.Errorf("parse offset store %s: %w", path, err)
	}

	return s, nil
}

// Commit records that this consumer group has processed all messages up to
// (and including) offset on topic, then flushes to disk.
func (s *OffsetStore) Commit(topic string, offset uint64) error {
	// TODO: implement Commit
	//
	// Step 1 — lock the mutex, update the in-memory map, then unlock:
	//     s.mu.Lock()
	//     s.offsets[topic] = offset
	//     s.mu.Unlock()
	//   (unlock before the disk write so other goroutines aren't blocked
	//    while we do I/O — but snapshot the map first)
	//
	// Step 2 — serialize the offsets map to JSON bytes:
	//     s.mu.Lock()
	//     data, err := json.Marshal(s.offsets)
	//     s.mu.Unlock()
	//     if err != nil { return err }
	//
	// Step 3 — write to disk atomically with os.WriteFile:
	//     return os.WriteFile(s.path, data, 0644)
	//   0644 = owner read/write, everyone else read (same permission as segments)
	s.mu.Lock()
	s.offsets[topic] = offset
	s.mu.Unlock() 

	s.mu.Lock()
	data, err := json.Marshal(s.offsets)
	s.mu.Unlock()
	if err != nil { return err }

	return os.WriteFile(s.path, data, 0644)

}

// Get returns the last committed offset for topic.
// Returns (0, false) if no offset has been committed yet for this topic.
func (s *OffsetStore) Get(topic string) (uint64, bool) {
	// TODO: implement Get
	//
	// Lock the mutex, read from the map, unlock, return.
	// Map lookups in Go return two values: value, ok
	//     val, ok := s.offsets[topic]
	// ok is false if the key doesn't exist.
	s.mu.Lock()
	val, ok := s.offsets[topic] 
	s.mu.Unlock()
	if !ok { return 0, false }

	return val, true
}
