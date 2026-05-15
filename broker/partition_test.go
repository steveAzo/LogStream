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

	// maxSize of 20 bytes forces a roll after every few messages.
	// "hello" = 4 (header) + 5 (data) = 9 bytes
	// Two messages = 18 bytes, third message triggers roll.
	p, err := NewPartition(dir, 20)
	if err != nil {
		t.Fatalf("NewPartition: %v", err)
	}
	defer p.Close()

	messages := []string{"hello", "world", "foo", "bar", "baz"}
	offsets := make([]uint64, len(messages))

	for i, msg := range messages {
		off, err := p.Append([]byte(msg))
		if err != nil {
			t.Fatalf("Append %q: %v", msg, err)
		}
		offsets[i] = off
	}

	// Should have rolled at least once with maxSize=20.
	if len(p.segments) < 2 {
		t.Errorf("expected multiple segments after rolling, got %d", len(p.segments))
	}

	// Read each message back by its offset — including across segment boundaries.
	for i, msg := range messages {
		got, err := p.ReadAt(offsets[i])
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

	// Write 3 messages then close.
	p, _ := NewPartition(dir, defaultMaxSegmentSize)
	for i := 0; i < 3; i++ {
		off, err := p.Append([]byte(fmt.Sprintf("msg-%d", i)))
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		offsets[i] = off
	}
	p.Close()

	// Reopen — simulates a process restart.
	p2, err := NewPartition(dir, defaultMaxSegmentSize)
	if err != nil {
		t.Fatalf("reopen partition: %v", err)
	}
	defer p2.Close()

	for i := 0; i < 3; i++ {
		got, err := p2.ReadAt(offsets[i])
		if err != nil {
			t.Fatalf("ReadAt after restart: %v", err)
		}
		want := fmt.Sprintf("msg-%d", i)
		if string(got) != want {
			t.Errorf("got %q, want %q", got, want)
		}
	}
}
