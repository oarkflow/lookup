package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"time"

	"github.com/oarkflow/squealx"
	"github.com/oarkflow/squealx/connection"

	v1 "github.com/oarkflow/lookup"
)

func mai1n() {
	manager := v1.NewManager()
	manager.StartHTTP(":8080")
}

func main() {
	log.Println("Starting main function")

	db, _, err := connection.FromConfig(squealx.Config{
		Host:     "localhost",
		Port:     5432,
		Driver:   "postgres",
		Username: "postgres",
		Password: "postgres",
		Database: "clear_dev",
	})
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	// Create index with increased workers and cache capacity.
	index := v1.NewIndex("test-filter",
		v1.WithIndexFieldsExcept("is_active", "created_by", "created_at", "updated_by", "updated_at", "deleted_at"),
	)

	// Register RPC service for distributed add/search.
	rpcServer := &v1.RPCServer{Index: index}
	if err := rpc.Register(rpcServer); err != nil {
		log.Fatal(err)
	}
	listener, err := net.Listen("tcp", ":8082") // adjust port to this node's TCP address
	if err != nil {
		log.Fatal(err)
	}
	go rpc.Accept(listener)
	log.Println("RPC server started on :8082")

	// Build index from database.
	query := "SELECT * FROM charge_master"
	log.Println("Starting index build from DB...")
	start := time.Now()
	err = squealx.SelectEach(db, func(row map[string]any) error {
		index.AddDocument(row)
		return nil
	}, query)
	if err != nil {
		log.Fatalf("index build error: %v", err)
	}
	log.Printf("Built index for %d docs in %s\n", index.TotalDocs, time.Since(start))

	req := v1.Request{
		Query: "RECENT",
		Page:  1,
		Size:  2,
	}
	log.Println("Performing search...")
	searchStart := time.Now()
	page, err := index.Search(ctx, req)
	if err != nil {
		log.Fatalf("Search error: %v", err)
	}
	log.Printf("Found %d docs (page %d/%d) in %s\n", page.Total, page.Page, page.TotalPages, time.Since(searchStart))
	fmt.Printf("%+v", page.Items)

	// Example: Search by field using filters
	req2 := v1.Request{
		Filters: []v1.Filter{
			{Field: "status", Operator: "=", Value: "ACTIVE"},
		},
		Page: 1,
		Size: 5,
	}
	page2, err := index.Search(ctx, req2)
	if err != nil {
		log.Printf("Field search error: %v", err)
	}
	fmt.Println("Field search results:", page2.Items)

	log.Println("Main function completed")
}
