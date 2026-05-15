package broker

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// recordHeaderSize is how many bytes we use to store the length of each message.
// 4 bytes = uint32 = max message size of ~4GB, plenty for us.
const recordHeaderSize = 4

// Segment is a single append-only log file on disk.
//
// Think of it as one chapter in a book. When it gets too big (Phase 2), we
// close it and start a new chapter. Each chapter (segment) has a baseOffset
// — the absolute byte position where this file's content starts in the
// "virtual" infinite log.
//
// For the first segment, baseOffset is 0.
// If segment 1 grows to 500 bytes and we roll, segment 2's baseOffset is 500.
type Segment struct {
	file       *os.File
	baseOffset uint64 // absolute byte offset of the first byte in this file
	nextOffset uint64 // absolute byte offset where the next message will land
}

// NewSegment opens or creates a segment file at path.
//
// baseOffset is the absolute offset this segment starts at (0 for the first
// segment; for later segments it equals the total bytes written so far).
func NewSegment(path string, baseOffset uint64) (*Segment, error) {
	// os.O_CREATE  — create the file if it doesn't exist
	// os.O_RDWR    — we need both read and write access
	// 0644         — file permissions: owner read/write, everyone else read
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("open segment %s: %w", path, err)
	}

	// If we're reopening an existing segment (e.g. after a restart), the file
	// already has data. Stat tells us how big it is so we can set nextOffset
	// correctly and resume appending without overwriting anything.
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("stat segment %s: %w", path, err)
	}

	return &Segment{
		file:       f,
		baseOffset: baseOffset,
		nextOffset: baseOffset + uint64(info.Size()),
	}, nil
}

// Append writes data to the end of the segment and returns the offset at
// which the message was written. The caller stores this offset so they can
// call ReadAt later to get the message back.
//
// On-disk format per record:
//
//	┌──────────────────┬──────────────────────────────┐
//	│  4 bytes uint32  │  N bytes                     │
//	│  message length  │  message data                │
//	└──────────────────┴──────────────────────────────┘
func (s *Segment) Append(data []byte) (offset uint64, err error) {
	offset = s.nextOffset

	header := make([]byte, recordHeaderSize)
	binary.BigEndian.PutUint32(header, uint32(len(data)))

	_, err = s.file.Write(header)
	if err != nil {
		return 0, err
	}

	_, err = s.file.Write(data)
	if err != nil {
		return 0, err
	}

	s.nextOffset += uint64(recordHeaderSize + len(data))
	return offset, nil 
}

// ReadAt reads back the message that was written at the given absolute byte
// offset. Pass in the offset returned by Append.
func (s *Segment) ReadAt(offset uint64) ([]byte, error) {
	pos := int64(offset - s.baseOffset)

	_, err := s.file.Seek(pos, io.SeekStart)
	if err != nil {
		return nil, err
	} 

	header := make([]byte, recordHeaderSize)
	_, err = io.ReadFull(s.file, header)
	if err != nil {
		return nil, err
	}

	msgLen := binary.BigEndian.Uint32(header)
	buf := make([]byte, msgLen)
	_, err = io.ReadFull(s.file, buf)
	if err != nil {
		return nil, err
	}
	

	return buf, nil 
}

// Size returns how many bytes this segment currently holds.
func (s *Segment) Size() uint64 {
	return s.nextOffset - s.baseOffset
}

// Close flushes and closes the underlying file.
func (s *Segment) Close() error {
	return s.file.Close()
}
