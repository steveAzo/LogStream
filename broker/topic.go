package broker

import (
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
)

const DefaultNumPartitions = 3

// Topic holds multiple Partitions and routes messages to them by key.
//
// Routing rule: hash(key) % numPartitions → partition index.
// Same key always lands in the same partition → per-key ordering guaranteed.
// No key → partition 0.
type Topic struct {
	name       string
	partitions []*Partition
}

// NewTopic opens or creates a topic at dir with numPartitions partitions.
// Each partition lives in its own subdirectory: <dir>/partition-0/, etc.
func NewTopic(dir string, numPartitions int, maxSegmentSize uint64) (*Topic, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create topic dir %s: %w", dir, err)
	}

	t := &Topic{name: filepath.Base(dir)}

	for i := 0; i < numPartitions; i++ {
		partDir := filepath.Join(dir, fmt.Sprintf("partition-%d", i))
		p, err := NewPartition(partDir, maxSegmentSize)
		if err != nil {
			return nil, fmt.Errorf("open partition %d: %w", i, err)
		}
		t.partitions = append(t.partitions, p)
	}
	return t, nil
}

// routeToPartition returns which partition index this key belongs to.
// This is the core of Kafka's partitioning model.
func (t *Topic) routeToPartition(key []byte) int {

	if len(key) == 0 {
		return 0
	}

	h := fnv.New32a()
	h.Write(key)
	return int(h.Sum32()) % len(t.partitions)
}

// Append routes key+value to the correct partition and appends both.
// Returns the partition index and the offset within that partition.
func (t *Topic) Append(key, value []byte) (partition int, offset uint64, err error) {
	partition = t.routeToPartition(key)
	offset, err = t.partitions[partition].Append(key, value)
	return
}

// ReadAt reads the key+value record at offset in the given partition.
func (t *Topic) ReadAt(partition int, offset uint64) (key, value []byte, err error) {
	if partition < 0 || partition >= len(t.partitions) {
		return nil, nil, fmt.Errorf("partition %d out of range (have %d)", partition, len(t.partitions))
	}
	return t.partitions[partition].ReadAt(offset)
}

// NumPartitions returns the number of partitions in this topic.
func (t *Topic) NumPartitions() int {
	return len(t.partitions)
}

// Close closes all partitions.
func (t *Topic) Close() error {
	for _, p := range t.partitions {
		if err := p.Close(); err != nil {
			return err
		}
	}
	return nil
}

// ensure fnv is used (imported above for routeToPartition TODO)
var _ = fnv.New32a
