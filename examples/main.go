package main

import (
	"fmt"
	"runtime"
	"strconv"
	"time"

	"github.com/oarkflow/lookup"
)

func formatBytes(bytes uint64) string {
	const (
		KB = 1 << 10
		MB = 1 << 20
		GB = 1 << 30
		TB = 1 << 40
	)

	switch {
	case bytes >= TB:
		return fmt.Sprintf("%.2f TB", float64(bytes)/TB)
	case bytes >= GB:
		return fmt.Sprintf("%.2f GB", float64(bytes)/GB)
	case bytes >= MB:
		return fmt.Sprintf("%.2f MB", float64(bytes)/MB)
	case bytes >= KB:
		return fmt.Sprintf("%.2f KB", float64(bytes)/KB)
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}

func monitorResources(fn func()) {
	// Force garbage collection to get more accurate baseline
	runtime.GC()

	var memStatsBefore, memStatsAfter runtime.MemStats
	runtime.ReadMemStats(&memStatsBefore)

	start := time.Now()
	fn()
	elapsed := time.Since(start)

	runtime.ReadMemStats(&memStatsAfter)

	before := formatBytes(memStatsBefore.Alloc)
	after := formatBytes(memStatsAfter.Alloc)
	fmt.Printf("\nFunction resource usage:\n")
	fmt.Printf("Memory before: %s\n", before)
	fmt.Printf("Memory after : %s\n", after)
	fmt.Printf("Total memory used: %s\n", formatBytes(memStatsAfter.Alloc-memStatsBefore.Alloc))
	fmt.Printf("Execution time: %v\n", elapsed)
}

type Person struct {
	Name       string
	Occupation string
	Email      string
}

func main() {
	leStr := lookup.NewLookupEngine[int, string](3, 1024, 3)
	leStr.StartBackgroundCleaner(1*time.Minute, 5*time.Minute)
	for _, k := range []int{10, 20, 5, 6, 12, 30, 7, 17} {
		leStr.Insert(k, "Autocomplete record "+strconv.Itoa(k))
	}
	fmt.Println("String Records:")
	for _, r := range leStr.InOrderTraversal() {
		fmt.Printf("Key: %v, Value: %v\n", r.Key, r.Value)
	}
	fmt.Println("Keyword Search for 'autocomplete':")
	for _, r := range leStr.KeywordSearch("autocomplete") {
		fmt.Printf("Key: %v, Value: %v\n", r.Key, r.Value)
	}
	fmt.Println("Fuzzy Search for 'autocomplet' (threshold 1):")
	for _, r := range leStr.FuzzySearch("autocomplet", 1) {
		fmt.Printf("Key: %v, Value: %v\n", r.Key, r.Value)
	}
	q := lookup.Query{Keywords: []string{"autocomplete"}, Fuzzy: map[string]int{"autocomplet": 1}, Page: 1, Size: 10, Sort: "asc"}
	fmt.Println("Combined Query Search:")
	for _, r := range leStr.Query(q) {
		fmt.Printf("Key: %v, Value: %v\n", r.Key, r.Value)
	}
	leMap := lookup.NewLookupEngine[int, map[string]string](3, 1024, 3)
	leMap.Insert(101, map[string]string{"title": "Map Record One", "desc": "This is the first map record"})
	leMap.Insert(102, map[string]string{"title": "Map Record Two", "desc": "Second record in a map"})
	leMap.Insert(103, map[string]string{"title": "Another Map", "desc": "Map record three"})
	fmt.Println("\nMap Records:")
	for _, r := range leMap.InOrderTraversal() {
		fmt.Printf("Key: %v, Value: %v\n", r.Key, r.Value)
	}
	fmt.Println("Keyword Search for 'record':")
	for _, r := range leMap.KeywordSearch("record") {
		fmt.Printf("Key: %v, Value: %v\n", r.Key, r.Value)
	}
	fmt.Println("Fuzzy Search for 'map' (threshold 0):")
	for _, r := range leMap.FuzzySearch("map", 0) {
		fmt.Printf("Key: %v, Value: %v\n", r.Key, r.Value)
	}
	leStruct := lookup.NewLookupEngine[int, Person](3, 1024, 3)
	leStruct.Insert(201, Person{"Alice", "Engineer", "alice@example.com"})
	leStruct.Insert(202, Person{"Bob", "Artist", "bob@example.com"})
	leStruct.Insert(203, Person{"Charlie", "Doctor", "charlie@example.com"})
	fmt.Println("\nStruct Records:")
	for _, r := range leStruct.InOrderTraversal() {
		fmt.Printf("Key: %v, Value: %+v\n", r.Key, r.Value)
	}
	fmt.Println("Keyword Search for 'doctor':")
	for _, r := range leStruct.KeywordSearch("doctor") {
		fmt.Printf("Key: %v, Value: %+v\n", r.Key, r.Value)
	}
	fmt.Println("Fuzzy Search for 'Alic' (threshold 1):")
	for _, r := range leStruct.FuzzySearch("Alic", 1) {
		fmt.Printf("Key: %v, Value: %+v\n", r.Key, r.Value)
	}

	// New block: generate 1 million []map[string]any records and apply searches/lookup.
	leAny := lookup.NewLookupEngine[int, map[string]any](3, 1024, 3)
	const recordsCount = 1000000
	var pairs []lookup.KeyValuePair[int, map[string]any]
	for i := 1; i <= recordsCount; i++ {
		rec := map[string]any{
			"Field1":  fmt.Sprintf("Record %d", i),
			"Field2":  fmt.Sprintf("Data %d", i),
			"Field3":  fmt.Sprintf("Value %d", i),
			"Field4":  fmt.Sprintf("Info %d", i),
			"Field5":  fmt.Sprintf("Detail %d", i),
			"Field6":  fmt.Sprintf("Note %d", i),
			"Field7":  fmt.Sprintf("Extra %d", i),
			"Field8":  fmt.Sprintf("Meta %d", i),
			"Field9":  fmt.Sprintf("Param %d", i),
			"Field10": fmt.Sprintf("Item %d", i),
		}
		pairs = append(pairs, lookup.KeyValuePair[int, map[string]any]{Key: i, Value: rec})
	}
	start := time.Now()
	monitorResources(func() {
		leAny.BulkInsert(pairs)
	})
	fmt.Printf("Inserted %d records in %v\n", recordsCount, time.Since(start))
	fmt.Println("\nMap[string]any Records Keyword Search for 'record':")
	results := leAny.KeywordSearch("record")
	fmt.Printf("Found %d records matching 'record'\n", len(results))

	fmt.Println("Fuzzy Search for 'Recor' (threshold 1):")
	results = leAny.FuzzySearch("Recor", 1)
	fmt.Printf("Found %d records matching fuzzy 'Recor'\n", len(results))

	// === New Examples for MultiQuery ===
	termQuery := lookup.TermQuery{Term: "autocomplete"}
	fmt.Println("\nMultiQuery Example - TermQuery for 'autocomplete':")
	for _, r := range leStr.MultiQuery(termQuery) {
		fmt.Printf("Key: %v, Value: %v\n", r.Key, r.Value)
	}

	boolQuery := lookup.BooleanQuery{
		Must:    []any{lookup.TermQuery{Term: "map"}},
		MustNot: []any{lookup.TermQuery{Term: "first"}},
		Should:  []any{lookup.KeyValueQuery{Field: "title", Value: "Record"}},
	}
	start = time.Now()
	fmt.Println("\nMultiQuery Example - BooleanQuery:")
	for _, r := range leMap.MultiQuery(boolQuery) {
		fmt.Printf("Key: %v, Value: %v\n", r.Key, r.Value)
	}
	fmt.Printf("BooleanQuery took %v\n", time.Since(start))
	kvQuery := lookup.KeyValueQuery{Field: "Occupation", Value: "Engineer"}

	start = time.Now()
	fmt.Println("\nMultiQuery Example - KeyValueQuery:")
	for _, r := range leStruct.MultiQuery(kvQuery) {
		fmt.Printf("Key: %v, Value: %+v\n", r.Key, r.Value)
	}
	fmt.Printf("KeyValueQuery took %v\n", time.Since(start))
}
