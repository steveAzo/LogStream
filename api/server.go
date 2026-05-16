package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"strconv"
	"sync"

	"LogStream/broker"
)

// Server holds all live topics and offset stores, and handles HTTP requests.
type Server struct {
	dataDir string
	mu      sync.Mutex
	topics  map[string]*broker.Topic       // topic name → Topic
	groups  map[string]*broker.OffsetStore // consumer group name → OffsetStore
}

func NewServer(dataDir string) *Server {
	return &Server{
		dataDir: dataDir,
		topics:  make(map[string]*broker.Topic),
		groups:  make(map[string]*broker.OffsetStore),
	}
}

func (s *Server) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /topics/{topic}/messages", s.handleProduce)
	mux.HandleFunc("GET /topics/{topic}/messages", s.handleConsume)
	mux.HandleFunc("POST /groups/{group}/offsets/{topic}/{partition}", s.handleCommitOffset)
	mux.HandleFunc("GET /groups/{group}/offsets/{topic}/{partition}", s.handleGetOffset)
	mux.HandleFunc("POST /topics/{topic}/partitions/{partition}/compact", s.handleCompact)
}

func (s *Server) getOrCreateTopic(name string) (*broker.Topic, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if t, ok := s.topics[name]; ok {
		return t, nil
	}

	dir := filepath.Join(s.dataDir, name)
	t, err := broker.NewTopic(dir, broker.DefaultNumPartitions, broker.DefaultMaxSegmentSize)
	if err != nil {
		return nil, fmt.Errorf("create topic %q: %w", name, err)
	}
	s.topics[name] = t
	return t, nil
}

func (s *Server) getOrCreateOffsetStore(group string) (*broker.OffsetStore, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if store, ok := s.groups[group]; ok {
		return store, nil
	}

	path := filepath.Join(s.dataDir, "__offsets", group+".json")
	store, err := broker.NewOffsetStore(path)
	if err != nil {
		return nil, fmt.Errorf("create offset store for group %q: %w", group, err)
	}
	s.groups[group] = store
	return store, nil
}

func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// handleProduce handles: POST /topics/{topic}/messages
// Request:  {"key": "user-123", "value": "hello"}   ← key is optional
// Response: {"partition": 1, "offset": 0}
func (s *Server) handleProduce(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("topic")
	t, err := s.getOrCreateTopic(name)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}

	var req struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, 400, "invalid JSON")
		return
	}

	partition, offset, err := t.Append([]byte(req.Key), []byte(req.Value))
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(struct {
		Partition int    `json:"partition"`
		Offset    uint64 `json:"offset"`
	}{Partition: partition, Offset: offset})
}

// handleConsume handles: GET /topics/{topic}/messages?partition=1&offset=0
// Response: {"partition": 1, "offset": 0, "value": "hello"}
func (s *Server) handleConsume(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("topic")
	s.mu.Lock()
	t, ok := s.topics[name]
	s.mu.Unlock()
	if !ok {
		writeError(w, 404, "topic not found")
		return
	}

	partition := 0
	if raw := r.URL.Query().Get("partition"); raw != "" {
		p, err := strconv.Atoi(raw)
		if err != nil {
			writeError(w, 400, "invalid partition")
			return
		}
		partition = p
	}

	raw := r.URL.Query().Get("offset")
	offset, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		writeError(w, 400, "invalid offset")
		return
	}

	key, value, err := t.ReadAt(partition, offset)
	if err != nil {
		writeError(w, 404, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(struct {
		Partition int    `json:"partition"`
		Offset    uint64 `json:"offset"`
		Key       string `json:"key"`
		Value     string `json:"value"`
	}{Partition: partition, Offset: offset, Key: string(key), Value: string(value)})
}

// handleCommitOffset handles: POST /groups/{group}/offsets/{topic}/{partition}
// Request body: {"offset": 47}
func (s *Server) handleCommitOffset(w http.ResponseWriter, r *http.Request) {
	group := r.PathValue("group")
	topic := r.PathValue("topic")
	partition, err := strconv.Atoi(r.PathValue("partition"))
	if err != nil {
		writeError(w, 400, "invalid partition")
		return
	}

	store, err := s.getOrCreateOffsetStore(group)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}

	var req struct {
		Offset uint64 `json:"offset"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, 400, "invalid JSON")
		return
	}

	if err := store.Commit(topic, partition, req.Offset); err != nil {
		writeError(w, 500, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleGetOffset handles: GET /groups/{group}/offsets/{topic}/{partition}
// Response: {"offset": 47, "exists": true}
func (s *Server) handleGetOffset(w http.ResponseWriter, r *http.Request) {
	group := r.PathValue("group")
	topic := r.PathValue("topic")
	partition, err := strconv.Atoi(r.PathValue("partition"))
	if err != nil {
		writeError(w, 400, "invalid partition")
		return
	}

	store, err := s.getOrCreateOffsetStore(group)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}

	offset, exists := store.Get(topic, partition)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(struct {
		Offset uint64 `json:"offset"`
		Exists bool   `json:"exists"`
	}{Offset: offset, Exists: exists})
}

// handleCompact handles: POST /topics/{topic}/partitions/{partition}/compact
// Triggers log compaction on a single partition — keeps only latest value per key.
func (s *Server) handleCompact(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("topic")
	s.mu.Lock()
	t, ok := s.topics[name]
	s.mu.Unlock()
	if !ok {
		writeError(w, 404, "topic not found")
		return
	}

	partIdx, err := strconv.Atoi(r.PathValue("partition"))
	if err != nil {
		writeError(w, 400, "invalid partition")
		return
	}

	if err := broker.Compact(t, partIdx); err != nil {
		writeError(w, 500, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"compacted": true})
}
