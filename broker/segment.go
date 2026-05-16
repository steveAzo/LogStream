package broker

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// recordHeaderSize is the number of bytes used to store the length of one field.
// Each record has two length-prefixed fields (key and value), so 2 × 4 bytes total overhead.
const recordHeaderSize = 4

// Segment is a single append-only log file on disk.
// Record format:
//
//	┌──────────────────┬──────────────┬──────────────────┬───────────────┐
//	│  4 bytes uint32  │  K bytes     │  4 bytes uint32  │  V bytes      │
//	│  key length      │  key bytes   │  value length    │  value bytes  │
//	└──────────────────┴──────────────┴──────────────────┴───────────────┘
type Segment struct {
	file       *os.File
	path       string // stored so compaction can delete the file
	baseOffset uint64
	nextOffset uint64
}

func NewSegment(path string, baseOffset uint64) (*Segment, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("open segment %s: %w", path, err)
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("stat segment %s: %w", path, err)
	}

	return &Segment{
		file:       f,
		path:       path,
		baseOffset: baseOffset,
		nextOffset: baseOffset + uint64(info.Size()),
	}, nil
}

// Append writes a key+value record to the segment.
// Returns the absolute byte offset where this record starts.
func (s *Segment) Append(key, value []byte) (uint64, error) {
	offset := s.nextOffset

	// write key length + key bytes
	kHeader := make([]byte, recordHeaderSize)
	binary.BigEndian.PutUint32(kHeader, uint32(len(key)))
	if _, err := s.file.Write(kHeader); err != nil {
		return 0, err
	}
	if _, err := s.file.Write(key); err != nil {
		return 0, err
	}

	// write value length + value bytes
	vHeader := make([]byte, recordHeaderSize)
	binary.BigEndian.PutUint32(vHeader, uint32(len(value)))
	if _, err := s.file.Write(vHeader); err != nil {
		return 0, err
	}
	if _, err := s.file.Write(value); err != nil {
		return 0, err
	}

	s.nextOffset += uint64(2*recordHeaderSize + len(key) + len(value))
	return offset, nil
}

// ReadAt reads the key+value record at the given absolute byte offset.
func (s *Segment) ReadAt(offset uint64) (key, value []byte, err error) {
	pos := int64(offset - s.baseOffset)
	if _, err = s.file.Seek(pos, io.SeekStart); err != nil {
		return
	}

	// read key
	kHeader := make([]byte, recordHeaderSize)
	if _, err = io.ReadFull(s.file, kHeader); err != nil {
		return
	}
	key = make([]byte, binary.BigEndian.Uint32(kHeader))
	if len(key) > 0 {
		if _, err = io.ReadFull(s.file, key); err != nil {
			return
		}
	}

	// read value
	vHeader := make([]byte, recordHeaderSize)
	if _, err = io.ReadFull(s.file, vHeader); err != nil {
		return
	}
	value = make([]byte, binary.BigEndian.Uint32(vHeader))
	if _, err = io.ReadFull(s.file, value); err != nil {
		return
	}
	return
}

// Size returns the number of bytes this segment currently holds.
func (s *Segment) Size() uint64 {
	return s.nextOffset - s.baseOffset
}

// Close closes the underlying file.
func (s *Segment) Close() error {
	return s.file.Close()
}
