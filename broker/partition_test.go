package broker

import (
	"fmt"
	"os"
	"testing"
)

func TestPartitionAppendAndRead(t *testing.T) {
	dir, err := os.MkdirTemp("", "partition-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// nil key: 4+0+4+N bytes per record.
	// "hello" = 9 bytes, "world" = 9 bytes → 18 bytes triggers roll at maxSize=20.
	p, err := NewPartition(dir, 20)
	if err != nil {
		t.Fatalf("NewPartition: %v", err)
	}
	defer p.Close()

	messages := []string{"hello", "world", "foo", "bar", "baz"}
	offsets := make([]uint64, len(messages))

	for i, msg := range messages {
		off, err := p.Append(nil, []byte(msg))
		if err != nil {
			t.Fatalf("Append %q: %v", msg, err)
		}
		offsets[i] = off
	}

	if len(p.segments) < 2 {
		t.Errorf("expected multiple segments after rolling, got %d", len(p.segments))
	}

	for i, msg := range messages {
		_, got, err := p.ReadAt(offsets[i])
		if err != nil {
			t.Fatalf("ReadAt offset %d: %v", offsets[i], err)
		}
		if string(got) != msg {
			t.Errorf("offset %d: got %q, want %q", offsets[i], got, msg)
		}
	}
}

func TestPartitionSurvivesRestart(t *testing.T) {
	dir, err := os.MkdirTemp("", "partition-restart-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	offsets := make([]uint64, 3)

	p, _ := NewPartition(dir, DefaultMaxSegmentSize)
	for i := 0; i < 3; i++ {
		off, err := p.Append(nil, []byte(fmt.Sprintf("msg-%d", i)))
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		offsets[i] = off
	}
	p.Close()

	p2, err := NewPartition(dir, DefaultMaxSegmentSize)
	if err != nil {
		t.Fatalf("reopen partition: %v", err)
	}
	defer p2.Close()

	for i := 0; i < 3; i++ {
		_, got, err := p2.ReadAt(offsets[i])
		if err != nil {
			t.Fatalf("ReadAt after restart: %v", err)
		}
		want := fmt.Sprintf("msg-%d", i)
		if string(got) != want {
			t.Errorf("got %q, want %q", got, want)
		}
	}
}
