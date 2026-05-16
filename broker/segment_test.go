package broker

import (
	"os"
	"testing"
)

func TestSegmentAppendAndRead(t *testing.T) {
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

	messages := []string{"hello", "world", "logstream"}
	offsets := make([]uint64, len(messages))

	for i, msg := range messages {
		off, err := seg.Append([]byte("key"), []byte(msg))
		if err != nil {
			t.Fatalf("Append %q: %v", msg, err)
		}
		offsets[i] = off
	}

	for i, msg := range messages {
		_, got, err := seg.ReadAt(offsets[i])
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

	// nil key: 4 (key len) + 0 (key) + 4 (val len) + 1 (val "a") = 9 bytes
	off1, _ := seg.Append(nil, []byte("a"))
	// nil key: 4 + 0 + 4 + 2 = 10 bytes
	off2, _ := seg.Append(nil, []byte("bb"))

	if off1 != 0 {
		t.Errorf("first offset should be 0, got %d", off1)
	}
	if off2 != 9 {
		t.Errorf("second offset should be 9, got %d", off2)
	}
	if seg.Size() != 19 {
		t.Errorf("size should be 19, got %d", seg.Size())
	}
}
