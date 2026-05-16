package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"

	"LogStream/api"
	"LogStream/broker"
)

func main() {
	// flag.String/flag.Bool register CLI flags and return pointers.
	// flag.Parse() reads os.Args and fills them in.
	addr        := flag.String("addr", ":8080", "address to listen on")
	role        := flag.String("role", "leader", "node role: leader or follower")
	followersRaw := flag.String("followers", "", "comma-separated follower addresses, e.g. :8081,:8082")
	flag.Parse()

	// Each node gets its own data directory so you can run 3 instances on
	// the same machine without them stomping on each other's segment files.
	// Strip the leading colon from the port: ":8080" → "data-8080"
	dataDir := "./data-" + strings.TrimPrefix(*addr, ":")
	if err := os.MkdirAll(dataDir+"/__offsets", 0755); err != nil {
		fmt.Fprintf(os.Stderr, "failed to create data dir: %v\n", err)
		os.Exit(1)
	}

	srv := api.NewServer(dataDir)

	// Only the leader gets a replicator. Followers just expose the
	// /internal/replicate endpoint and write whatever the leader sends.
	if *role == "leader" && *followersRaw != "" {
		followers := strings.Split(*followersRaw, ",")
		srv.SetReplicator(broker.NewReplicator(followers))
		fmt.Printf("replication enabled → followers: %v\n", followers)
	}

	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)

	fmt.Printf("LogStream [%s] listening on %s\n", *role, *addr)
	if err := http.ListenAndServe(*addr, mux); err != nil {
		fmt.Fprintf(os.Stderr, "server error: %v\n", err)
		os.Exit(1)
	}
}
