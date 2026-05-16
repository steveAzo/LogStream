package broker_test

import (
	"os"
	"testing"

	"LogStream/broker"
)

func TestCompactKeepsLatestPerKey(t *testing.T) {
	dir, err := os.MkdirTemp("", "compact-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	topic, err := broker.NewTopic(dir, 1, broker.DefaultMaxSegmentSize)
	if err != nil {
		t.Fatalf("NewTopic: %v", err)
	}

	writes := []struct {
		key, value string
	}{
		{"user:1", "alice"},
		{"user:2", "bob"},
		{"user:1", "alice-v2"}, // overwrites user:1
		{"user:3", "carol"},
		{"user:2", "bob-v2"}, // overwrites user:2
	}

	for _, w := range writes {
		if _, _, err := topic.Append([]byte(w.key), []byte(w.value)); err != nil {
			t.Fatalf("Append %q: %v", w.key, err)
		}
	}

	if err := broker.Compact(topic, 0); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	p := topic.PartitionAt(0)
	if len(p.Segments()) != 1 {
		t.Errorf("expected 1 segment after compaction, got %d", len(p.Segments()))
	}

	// Scan the compacted segment and collect all records.
	seg := p.Segments()[0]
	got := make(map[string]string)
	offset := seg.BaseOffset()
	for offset < seg.NextOffset() {
		key, value, err := seg.ReadAt(offset)
		if err != nil {
			t.Fatalf("ReadAt %d: %v", offset, err)
		}
		got[string(key)] = string(value)
		offset += uint64(2*broker.RecordHeaderSize + len(key) + len(value))
	}

	want := map[string]string{
		"user:1": "alice-v2",
		"user:2": "bob-v2",
		"user:3": "carol",
	}

	for key, wantVal := range want {
		if gotVal, ok := got[key]; !ok {
			t.Errorf("key %q missing after compaction", key)
		} else if gotVal != wantVal {
			t.Errorf("key %q: got %q, want %q", key, gotVal, wantVal)
		}
	}

	if len(got) != len(want) {
		t.Errorf("got %d records after compaction, want %d", len(got), len(want))
	}
}
