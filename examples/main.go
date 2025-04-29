package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/oarkflow/filters"
	"github.com/oarkflow/json"

	v1 "github.com/oarkflow/lookup"
)

func mai1n() {
	manager := v1.NewManager()
	manager.StartHTTP(":8080")
}

func main() {
	// Initialize and build the index
	ctx := context.Background()
	index := v1.NewIndex("test-filter")
	jsonFile := "/Users/sujit/Sites/oarkflow2/search/examples/charge_master.json"
	start := time.Now()
	err := index.Build(ctx, jsonFile)
	if err != nil {
		log.Fatalf("Index build error: %v", err)
	}
	fmt.Printf("Built index for %d docs in %s\n", index.TotalDocs, time.Since(start))
	req := v1.Request{
		Query: "9560020",
		Exact: true,
		Filters: []v1.Filter{
			{Field: "charge_amt", Operator: filters.GreaterThanEqual, Value: 100},
			{Field: "charge_type", Operator: filters.Equal, Value: "ED_FACILITY"},
		},
	}

	searchStart := time.Now()
	page, err := index.Search(ctx, req)
	if err != nil {
		log.Fatalf("Search error: %v", err)
	}
	fmt.Printf("Found %d docs (page %d/%d) in %s\n", page.Total, page.Page, page.TotalPages, time.Since(searchStart))
	for _, sd := range page.Results {
		rec, _ := index.GetDocument(sd.DocID)
		bt, _ := json.Marshal(rec)
		fmt.Printf("DocID:%d Score:%.4f Data:%s\n", sd.DocID, sd.Score, string(bt))
	}
}
