# LogStream

A log-based message broker built from scratch in Go. Persistent, append-only, partitioned, replicated — a minimal Kafka implementation built to understand why these systems are designed the way they are.

Built while reading *Designing Data-Intensive Applications* (chapters 3, 4, 11).

---

## What it is

LogStream is an HTTP message broker backed by append-only segment files on disk. Producers write messages to a topic; consumers read them back by offset. The design mirrors Kafka's core architecture at a scale that fits in your head.

```
Producer
   │
   ▼
HTTP API (Leader :8080) ─────────────────────────────────────
   │                    │ replication fanout                 │
   │              Follower :8081                      Follower :8082
   ▼
Topic
   │  (FNV key routing)
   ├── Partition 0
   │      ├── 00000000000000000000.log
   │      └── 00000000000000000512.log  ← sealed, rolled at 100MB
   ├── Partition 1
   │      └── 00000000000000000000.log
   └── Partition 2
          └── 00000000000000000000.log
               │
               └── Record format:
                   [4B key len][key bytes][4B val len][val bytes]
```

---

## Why it's built this way

### Append-only writes

Every write is a sequential append to the end of the active segment file. No seeking, no in-place updates. This is the core insight from DDIA Chapter 3: sequential I/O on both HDDs and SSDs dramatically outperforms random writes because:
- No seek time (HDD) or erase-before-write cycles (SSD)
- The OS page cache and disk write buffers are optimised for sequential patterns
- Crash recovery is simpler — a partial write at the end of the file is the only failure case

The benchmark makes this concrete (see below).

### Length-prefix framing

Each record is prefixed with a 4-byte length field for both key and value. Without framing, there is no way to know where one record ends and the next begins when reading back from disk. This is the same approach used by most binary protocols (Kafka, gRPC, PostgreSQL WAL).

### Byte-offset addressing

Every message is identified by its absolute byte position in the segment file. `ReadAt(offset)` seeks directly to that position — O(1) regardless of how many messages exist. Consumers store the offset of the last message they processed. To resume, they seek to `offset + recordSize` and continue reading forward.

### Segment rolling

A single ever-growing file makes compaction impossible and crash recovery expensive. LogStream caps each segment at 100MB. Sealed segments are read-only; only the active (last) segment is written to. Segments are named by their base offset (`00000000000000000512.log`) so lexicographic sort equals chronological order — no index needed on restart.

### Partition routing

Each topic has 3 partitions. Messages with a key are routed via FNV-1a hash: `partition = hash(key) % N`. Same key always lands in the same partition, guaranteeing per-key ordering. Kafka uses the same algorithm. Messages without a key go to partition 0.

### Consumer offset tracking

Consumers commit their position (`topic + partition → offset`) to an `OffsetStore` persisted as a JSON file per consumer group. On restart, a consumer fetches its last committed offset and resumes from there — at-least-once delivery semantics (the message at the committed offset may be reprocessed after a crash).

### Log compaction

For topics where only the latest value per key matters (user profiles, configuration, CDC), compaction reduces storage by scanning all segments, keeping only the most recent record per key, and rewriting a single clean segment. The algorithm is O(n) in the number of records. After compaction, old offsets are invalidated — a known trade-off documented in DDIA Chapter 11.

### fsync and the durability trade-off

`file.Sync()` is called after every `Append`, forcing the OS to flush its page cache to physical storage before returning. Without it, a crash in the window between `Write()` and the OS flush silently loses data. The cost is real:

```
BenchmarkAppendWithSync    ~1,021,263 ns/op    ~979 msg/sec     (fsync after every write)
BenchmarkAppendNoSync      ~11,402 ns/op       ~87,700 msg/sec  (OS decides when to flush)
BenchmarkReadAt            ~7,915 ns/op        ~126,000 reads/sec

Hardware: Intel i3-1125G4 @ 2.00GHz, Windows 11
```

**The 90× gap is why Kafka defaults to no per-message fsync.** Replicating to N nodes over a network (~1ms round trip) costs the same as one fsync but gives fault tolerance instead of just durability. LogStream implements both: fsync on each node for single-node correctness, plus synchronous replication to followers before the leader acknowledges the write.

### Leader-follower replication

On a write, the leader appends to its own log then fans out to all followers concurrently using goroutines. It waits for every follower to acknowledge before returning success to the producer. This is synchronous replication — the strongest consistency guarantee, at the cost of latency equal to the slowest follower.

Each node has its own data directory (`data-<port>/`) so multiple instances can run on the same machine. Followers expose a `/internal/replicate` endpoint that writes directly to the specified partition, bypassing key routing — the leader already made that decision.

---

## API

### Produce

```
POST /topics/{topic}/messages
Content-Type: application/json

{"key": "user-123", "value": "clicked checkout"}
```

```json
{"partition": 1, "offset": 47}
```

`key` is optional. If omitted, message routes to partition 0. Store the returned `partition` and `offset` — they are required to read this message back.

### Consume

```
GET /topics/{topic}/messages?partition=1&offset=47
```

```json
{"partition": 1, "offset": 47, "key": "user-123", "value": "clicked checkout"}
```

### Commit offset

```
POST /groups/{group}/offsets/{topic}/{partition}
Content-Type: application/json

{"offset": 47}
```

`204 No Content`

### Get committed offset

```
GET /groups/{group}/offsets/{topic}/{partition}
```

```json
{"offset": 47, "exists": true}
```

### Compact a partition

```
POST /topics/{topic}/partitions/{partition}/compact
```

```json
{"compacted": true}
```

---

## Running locally

### Single node

```bash
git clone https://github.com/steveAzo/logstream
cd logstream
go run .
# Listens on :8080, data written to ./data-8080/
```

### Three-node cluster (leader + 2 followers)

```bash
# Terminal 1 — leader
go run . -addr=:8080 -role=leader -followers=:8081,:8082

# Terminal 2 — follower 1
go run . -addr=:8081 -role=follower

# Terminal 3 — follower 2
go run . -addr=:8082 -role=follower
```

Produce to the leader; read from any node at the same partition and offset.

Requirements: Go 1.22+

---

## Running tests and benchmarks

```bash
# Unit tests (segment, partition, compaction)
go test ./tests/

# Benchmarks (fsync vs no-fsync vs reads)
go test -bench=Benchmark -benchtime=5s -benchmem ./tests/
```

---

## Project layout

```
broker/         core storage engine (segment, partition, topic, compaction, replication)
api/            HTTP layer — routes and handlers
tests/          all tests and benchmarks (external test package, broker_test)
main.go         entry point — flag parsing, node role wiring
```

---

## Roadmap

- [ ] Batch produce — amortise fsync cost across multiple messages
- [ ] Binary protocol (replace HTTP+JSON with length-prefixed TCP frames)
