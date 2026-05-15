package broker

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const DefaultMaxSegmentSize uint64 = 100 * 1024 * 1024 // 100 MB

// Partition manages an ordered list of Segments for one topic-partition.
// The last segment is always the active (writable) one; the rest are sealed.
//
// On disk, a partition is a directory of .log files named by baseOffset:
//
//	00000000000000000000.log
//	00000000000000000512.log
//	...
type Partition struct {
	dir      string
	maxSize  uint64
	segments []*Segment // ordered by baseOffset, last one is active
}

// NewPartition opens or creates a partition at dir.
// It discovers any existing .log files and loads them in order.
func NewPartition(dir string, maxSize uint64) (*Partition, error) {
	// os.MkdirAll is like `mkdir -p`: creates dir and any missing parents.
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create partition dir %s: %w", dir, err)
	}

	p := &Partition{dir: dir, maxSize: maxSize}

	// filepath.Glob returns all files matching the pattern.
	// sort.Strings puts them in lexicographic order — which equals
	// chronological order because of our zero-padded naming convention.
	files, err := filepath.Glob(filepath.Join(dir, "*.log"))
	if err != nil {
		return nil, err
	}
	sort.Strings(files)

	for _, f := range files {
		// Filename is "00000000000000000512.log" — strip extension, parse as uint64.
		base := strings.TrimSuffix(filepath.Base(f), ".log")
		baseOffset, err := strconv.ParseUint(base, 10, 64)
		if err != nil {
			continue // skip any files we didn't create
		}
		seg, err := NewSegment(f, baseOffset)
		if err != nil {
			return nil, err
		}
		p.segments = append(p.segments, seg)
	}

	// Fresh partition with no existing files — create the first segment.
	if len(p.segments) == 0 {
		if err := p.roll(); err != nil {
			return nil, err
		}
	}

	return p, nil
}

// active returns the segment currently being written to (always the last one).
func (p *Partition) active() *Segment {
	return p.segments[len(p.segments)-1]
}

// segmentPath builds the file path for a segment with the given baseOffset.
func (p *Partition) segmentPath(baseOffset uint64) string {
	return filepath.Join(p.dir, fmt.Sprintf("%020d.log", baseOffset))
}

// roll seals the current active segment and opens a new one whose baseOffset
// starts exactly where the previous segment ended.
func (p *Partition) roll() error {
	var baseOffset uint64
	if len(p.segments) > 0 {
		// New segment starts right after the last byte of the current one.
		baseOffset = p.active().nextOffset
	}
	seg, err := NewSegment(p.segmentPath(baseOffset), baseOffset)
	if err != nil {
		return fmt.Errorf("roll at offset %d: %w", baseOffset, err)
	}
	// append() grows the slice and reassigns it.
	p.segments = append(p.segments, seg)
	return nil
}

// Append writes data to the partition. It rolls to a new segment if the
// active one has hit maxSize, then delegates to the active segment.
func (p *Partition) Append(data []byte) (uint64, error) {
	if p.active().Size() >= p.maxSize {
		if err := p.roll(); err != nil { return 0, err }
	}

	return p.active().Append(data)
	
}

// findSegment returns the segment that owns the given absolute byte offset.
func (p *Partition) findSegment(offset uint64) (*Segment, error) {
	for i := len(p.segments) - 1; i >= 0; i-- {
		if p.segments[i].baseOffset <= offset {
			return p.segments[i], nil
		}
	}

	return nil, fmt.Errorf("no segment for offset %d", offset)

}

// ReadAt reads the message at the given absolute offset.
func (p *Partition) ReadAt(offset uint64) ([]byte, error) {
	seg, err := p.findSegment(offset)
	if err != nil {
		return nil, err
	}
	return seg.ReadAt(offset)
}

// Close closes all segments.
func (p *Partition) Close() error {
	for _, seg := range p.segments {
		if err := seg.Close(); err != nil {
			return err
		}
	}
	return nil
}
