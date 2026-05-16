package broker

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"sync"
)

// OffsetStore tracks the last committed offset per (topic, partition) for one
// consumer group. Persisted as a flat JSON map with composite keys:
//
//	{"events:0": 47, "events:2": 102}
type OffsetStore struct {
	path    string
	mu      sync.Mutex
	offsets map[string]uint64 // "topic:partition" → last committed offset
}

func NewOffsetStore(path string) (*OffsetStore, error) {
	s := &OffsetStore{
		path:    path,
		offsets: make(map[string]uint64),
	}

	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
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

func key(topic string, partition int) string {
	return topic + ":" + strconv.Itoa(partition)
}

// Commit records that this group has processed all messages up to offset
// on the given topic partition, then flushes to disk.
func (s *OffsetStore) Commit(topic string, partition int, offset uint64) error {
	s.mu.Lock()
	s.offsets[key(topic, partition)] = offset
	data, err := json.Marshal(s.offsets)
	s.mu.Unlock()
	if err != nil {
		return err
	}
	return os.WriteFile(s.path, data, 0644)
}

// Get returns the last committed offset for the given topic partition.
// Returns (0, false) if nothing has been committed yet.
func (s *OffsetStore) Get(topic string, partition int) (uint64, bool) {
	s.mu.Lock()
	val, ok := s.offsets[key(topic, partition)]
	s.mu.Unlock()
	return val, ok
}
