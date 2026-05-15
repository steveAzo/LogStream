package broker

import (
	"os"
	"testing"
)

// Run these tests with: go test ./broker/...

func TestSegmentAppendAndRead(t *testing.T) {
	// Create a temp file for the test — cleaned up automatically after.
	f, err := os.CreateTemp("", "segment-*.log")
	if err != nil {
		t.Fatal(err)
	}
	path := f.Name()
	f.Close()
	defer os.Remove(path)

	seg, err := NewSegment(path, 0)
	if err != nil {
		t.Fatalf("NewSegment: %v", err)
	}
	defer seg.Close()

	// Append three messages and remember their offsets.
	messages := []string{"hello", "world", "logstream"}
	offsets := make([]uint64, len(messages))

	for i, msg := range messages {
		off, err := seg.Append([]byte(msg))
		if err != nil {
			t.Fatalf("Append %q: %v", msg, err)
		}
		offsets[i] = off
	}

	// Read each message back by its offset — order shouldn't matter.
	for i, msg := range messages {
		got, err := seg.ReadAt(offsets[i])
		if err != nil {
			t.Fatalf("ReadAt offset %d: %v", offsets[i], err)
		}
		if string(got) != msg {
			t.Errorf("offset %d: got %q, want %q", offsets[i], got, msg)
		}
	}
}

func TestSegmentOffsetProgresses(t *testing.T) {
	f, _ := os.CreateTemp("", "segment-*.log")
	path := f.Name()
	f.Close()
	defer os.Remove(path)

	seg, _ := NewSegment(path, 0)
	defer seg.Close()

	off1, _ := seg.Append([]byte("a"))   // 4 + 1 = 5 bytes
	off2, _ := seg.Append([]byte("bb"))  // 4 + 2 = 6 bytes

	if off1 != 0 {
		t.Errorf("first offset should be 0, got %d", off1)
	}
	if off2 != 5 {
		t.Errorf("second offset should be 5, got %d", off2)
	}
	if seg.Size() != 11 {
		t.Errorf("size should be 11, got %d", seg.Size())
	}
}
