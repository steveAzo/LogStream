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

// AppendToPartition writes directly to a specific partition, bypassing key routing.
// Used by followers receiving replicated writes from the leader — the leader
// already determined the partition, so followers must write to the same one.
func (t *Topic) AppendToPartition(partitionIdx int, key, value []byte) (uint64, error) {
	if partitionIdx < 0 || partitionIdx >= len(t.partitions) {
		return 0, fmt.Errorf("partition %d out of range (have %d)", partitionIdx, len(t.partitions))
	}
	return t.partitions[partitionIdx].Append(key, value)
}

// NumPartitions returns the number of partitions in this topic.
func (t *Topic) NumPartitions() int {
	return len(t.partitions)
}

// PartitionAt returns the partition at the given index.
func (t *Topic) PartitionAt(idx int) *Partition {
	return t.partitions[idx]
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
