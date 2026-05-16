package broker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// Replicator fans out writes from the leader to all follower nodes.
// Each Replicate call blocks until every follower has acknowledged the write.
type Replicator struct {
	followers []string     // e.g. [":8081", ":8082"]
	client    *http.Client // shared client with timeout
}

func NewReplicator(followers []string) *Replicator {
	return &Replicator{
		followers: followers,
		client:    &http.Client{Timeout: 5 * time.Second},
	}
}

// replicateRequest is the JSON body sent to each follower's internal endpoint.
type replicateRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Replicate sends key+value to all followers for the given topic/partition.
// It fans out concurrently and waits for ALL acks before returning.
// If any follower fails, an error is returned — the write is considered failed.
func (r *Replicator) Replicate(topic string, partition int, key, value []byte) error {

	if len(r.followers) == 0 { return nil }

	body, err := json.Marshal(replicateRequest{Key: string(key), Value: string(value)})
	if err != nil { return err }
	
	errs := make(chan error, len(r.followers))
	var wg sync.WaitGroup 
	wg.Add(len(r.followers))
	for _, addr := range r.followers {
		addr := addr 
		go func() {
			defer wg.Done()
			url := fmt.Sprintf("http://%s/internal/replicate/%s/%d", addr, topic, partition)
			resp, err := r.client.Post(url, "application/json", bytes.NewReader(body))
			if err != nil { errs <- err; return }
			resp.Body.Close()
			if resp.StatusCode != http.StatusNoContent {
				errs <- fmt.Errorf("follower %s returned %d", addr, resp.StatusCode)
			}
		}()
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil { return err }
	}

	return nil 
	
}
