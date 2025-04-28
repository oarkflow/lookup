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

func main() {
	manager := v1.NewManager()
	manager.StartHTTP(":8080")
}

func mai1n() {
	// Initialize and build the index
	ctx := context.Background()
	index := v1.NewIndex("test-filter")
	jsonFile := "charge_master.json"
	start := time.Now()
	err := index.Build(ctx, jsonFile)
	if err != nil {
		log.Fatalf("Index build error: %v", err)
	}
	fmt.Printf("Built index for %d docs in %s\n", index.TotalDocs, time.Since(start))

	// 1) TermQuery example (fuzzy)
	// termQ := v1.NewTermQuery("9560012", true, 1)

	// 2) Define Filters: e.g., charge_amount >= 100 AND charge_type = "service"
	conditions := []filters.Condition{
		&filters.Filter{Field: "charge_amt", Operator: filters.GreaterThanEqual, Value: 100},
		&filters.Filter{Field: "charge_type", Operator: filters.Equal, Value: "ED_FACILITY"},
	}

	// 3) Combine into a FilterQuery
	fq := v1.NewFilterQuery(nil, filters.AND, false, conditions...)

	// 4) SearchParams (with sorting and pagination)
	params := v1.SearchParams{
		Page:    1,
		PerPage: 1,
		SortFields: []v1.SortField{{
			Field: "charge_type",
		}},
	}

	searchStart := time.Now()
	page, err := index.Search(ctx, fq, params)
	if err != nil {
		log.Fatalf("Search error: %v", err)
	}
	fmt.Printf("Found %d docs (page %d/%d) in %s\n", page.Total, page.Page, page.TotalPages, time.Since(searchStart))
	for _, sd := range page.Results {
		rec, _ := index.GetDocument(sd.DocID)
		bt, _ := json.Marshal(rec)
		fmt.Printf("DocID:%d Score:%.4f Data:%s\n", sd.DocID, sd.Score, string(bt))
	}

	searchStart = time.Now()
	page, err = index.Search(ctx, fq, params)
	if err != nil {
		log.Fatalf("Search error: %v", err)
	}
	fmt.Printf("Found %d docs (page %d/%d) in %s\n", page.Total, page.Page, page.TotalPages, time.Since(searchStart))
	for _, sd := range page.Results {
		rec, _ := index.GetDocument(sd.DocID)
		fmt.Println(rec)
		bt, _ := json.Marshal(rec)
		fmt.Printf("DocID:%d Score:%.4f Data:%s\n", sd.DocID, sd.Score, string(bt))
	}
}
