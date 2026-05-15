package main

import (
	"fmt"
	"net/http"
	"os"

	"LogStream/api"
)

func main() {
	dataDir := "./data"
	if err := os.MkdirAll(dataDir+"/__offsets", 0755); err != nil {
		fmt.Fprintf(os.Stderr, "failed to create data dir: %v\n", err)
		os.Exit(1)
	}

	srv := api.NewServer(dataDir)
	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)

	fmt.Println("LogStream listening on :8080")
	if err := http.ListenAndServe(":8080", mux); err != nil {
		fmt.Fprintf(os.Stderr, "server error: %v\n", err)
		os.Exit(1)
	}
}
