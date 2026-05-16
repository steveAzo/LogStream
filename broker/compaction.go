package broker

import (
	"fmt"
	"os"
)

// record is a key+value pair read from the log during compaction.
type record struct {
	key   []byte
	value []byte
}

// Compact rewrites a partition keeping only the latest value per key.
//
// The algorithm is the core of DDIA Chapter 3's log compaction:
//   - Scan every record in every segment, oldest to newest
//   - A map[key]→record accumulates the state: later writes overwrite earlier ones
//   - Write the surviving records into one new segment
//   - Replace the old segments on disk
//
// Trade-off: offsets are reset to 0 after compaction, so committed consumer
// offsets become invalid. Real Kafka handles this gracefully; we don't.
func Compact(t *Topic, partitionIdx int) error {
	if partitionIdx < 0 || partitionIdx >= len(t.partitions) {
		return fmt.Errorf("partition %d out of range", partitionIdx)
	}
	p := t.partitions[partitionIdx]

	latest := make(map[string]record)

	for _, seg := range p.segments {
		offset := seg.baseOffset 
		for offset < seg.nextOffset {
			key, value, err := seg.ReadAt(offset)
			if err != nil { return err }
			latest[string(key)] = record{key: key, value: value}
			offset += uint64(2*RecordHeaderSize + len(key) + len(value))
		}
	}

	// Step 2 — close all current segments (flush and release file handles):
	//
	for _, seg := range p.segments {
	    seg.Close()
	}
	//
	// Step 3 — write surviving records into a new temp segment:
	//
	tmpPath := p.segmentPath(0) + ".tmp"
	newSeg, err := NewSegment(tmpPath, 0)
	if err != nil { return err }
	
	for _, rec := range latest {
	    if _, err := newSeg.Append(rec.key, rec.value); err != nil {
	        newSeg.Close()
	        os.Remove(tmpPath)
	        return err
	    }
	}
	newSeg.Close()
	

	// Step 4 — delete old segment files:
	//
	for _, seg := range p.segments {
	    os.Remove(seg.path)
	}
	//
	// Step 5 — move the temp file to the canonical segment-0 path:
	//
	finalPath := p.segmentPath(0)
	if err := os.Rename(tmpPath, finalPath); err != nil { return err }
	
	// Step 6 — reload the partition's segments slice with just the new segment:
	//
	reloaded, err := NewSegment(finalPath, 0)
	if err != nil { return err }
	p.segments = []*Segment{reloaded}
	
	return nil

}

