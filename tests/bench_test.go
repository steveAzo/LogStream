package broker_test

import (
	"os"
	"testing"

	"LogStream/broker"
)

// payload simulates a realistic log message (~128 bytes).
var payload = []byte(`{"user_id":"u_8f3a92","event":"page_view","path":"/dashboard","ts":1716057600}`)

// BenchmarkAppendWithSync measures append throughput with fsync after every write.
// This is the durability guarantee — every message is on disk before we return.
func BenchmarkAppendWithSync(b *testing.B) {
	f, _ := os.CreateTemp("", "bench-sync-*.log")
	path := f.Name()
	f.Close()
	defer os.Remove(path)

	seg, err := broker.NewSegment(path, 0)
	if err != nil {
		b.Fatal(err)
	}
	defer seg.Close()

	b.SetBytes(int64(len(payload)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := seg.Append(nil, payload); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkAppendNoSync measures the throughput ceiling without fsync.
// Shows what we give up for durability — and why Kafka uses replication instead.
func BenchmarkAppendNoSync(b *testing.B) {
	f, _ := os.CreateTemp("", "bench-nosync-*.log")
	path := f.Name()
	f.Close()
	defer os.Remove(path)

	seg, err := broker.NewSegmentNoSync(path, 0)
	if err != nil {
		b.Fatal(err)
	}
	defer seg.Close()

	b.SetBytes(int64(len(payload)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := seg.Append(nil, payload); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkReadAt measures random-read throughput — seeking to a known offset.
func BenchmarkReadAt(b *testing.B) {
	f, _ := os.CreateTemp("", "bench-read-*.log")
	path := f.Name()
	f.Close()
	defer os.Remove(path)

	seg, _ := broker.NewSegmentNoSync(path, 0)
	defer seg.Close()

	const n = 10_000
	offsets := make([]uint64, n)
	for i := range offsets {
		off, err := seg.Append(nil, payload)
		if err != nil {
			b.Fatal(err)
		}
		offsets[i] = off
	}

	b.SetBytes(int64(len(payload)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, _, err := seg.ReadAt(offsets[i%n]); err != nil {
			b.Fatal(err)
		}
	}
}
