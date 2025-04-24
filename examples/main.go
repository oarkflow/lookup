package main

import (
	"fmt"
	"runtime"
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

	// New block: generate 1 million []map[string]any records and apply searches/lookup.
	leAny := lookup.NewLookupEngine[int, map[string]any](3, 1024, 3)
	const recordsCount = 1000000
	start := time.Now()
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
		pair := lookup.KeyValuePair[int, map[string]any]{Key: i, Value: rec}
		leAny.Insert(pair.Key, pair.Value)
	}
	fmt.Println(leAny.TotalRecords())
	fmt.Printf("Inserted %d records in %v\n", recordsCount, time.Since(start))
	fmt.Println("\nMap[string]any Records Keyword Search for 'record':")
	results := leAny.KeywordSearch("record")
	fmt.Printf("Found %d records matching 'record'\n", len(results))

	fmt.Println("Fuzzy Search for 'Recor' (threshold 1):")
	results = leAny.FuzzySearch("Recor", 1)
	fmt.Printf("Found %d records matching fuzzy 'Recor'\n", len(results))

}
