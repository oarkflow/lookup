package main

import (
	"context"
	"fmt"
	"log"
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
	index := v1.NewIndex("test-filter", v1.WithIndexFieldsExcept("is_active", "status", "created_by", "created_at", "updated_by", "updated_at", "deleted_at"))
	query := "SELECT * FROM charge_master"
	start := time.Now()
	err = index.BuildFromDatabase(ctx, v1.DBRequest{DB: db, Query: query})
	if err != nil {
		log.Fatalf("index build error: %v", err)
	}
	fmt.Printf("Built index for %d docs in %s\n", index.TotalDocs, time.Since(start))
	req := v1.Request{
		Query: "ARTHROCENTESIS",
	}

	searchStart := time.Now()
	page, err := index.Search(ctx, req)
	if err != nil {
		log.Fatalf("Search error: %v", err)
	}
	fmt.Printf("Found %d docs (page %d/%d) in %s\n", page.Total, page.Page, page.TotalPages, time.Since(searchStart))
	fmt.Println(fmt.Sprintf("%+v", page.Items))
}
