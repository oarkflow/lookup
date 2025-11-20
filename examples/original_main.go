package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/oarkflow/lookup"
)

func mainOriginal() {
	// Create enhanced index with all features enabled
	fmt.Println("🚀 Creating Enhanced Full-Text Search Engine...")

	index := lookup.NewEnhancedIndex("demo_index",
		lookup.WithNumOfWorkers(4),
		lookup.WithCacheCapacity(1000),
		lookup.WithCacheExpiry(5*time.Minute),
	)
	defer index.Close()

	// Enable advanced features
	index.EnableStemming(true)
	index.EnableMetrics(true)

	// Sample dataset
	documents := []lookup.GenericRecord{
		{
			"id":          1,
			"title":       "Introduction to Machine Learning",
			"content":     "Machine learning is a subset of artificial intelligence that focuses on algorithms",
			"category":    "AI",
			"author":      "John Smith",
			"publishDate": "2024-01-15",
			"rating":      4.5,
		},
		{
			"id":          2,
			"title":       "Advanced Go Programming Techniques",
			"content":     "Learn advanced programming patterns and best practices in Go language",
			"category":    "Programming",
			"author":      "Jane Doe",
			"publishDate": "2024-02-20",
			"rating":      4.8,
		},
		{
			"id":          3,
			"title":       "Database Design and Optimization",
			"content":     "Comprehensive guide to designing efficient database systems and query optimization",
			"category":    "Database",
			"author":      "Bob Johnson",
			"publishDate": "2024-03-10",
			"rating":      4.2,
		},
		{
			"id":          4,
			"title":       "Full-Text Search Engines",
			"content":     "Building high-performance search engines with advanced indexing techniques",
			"category":    "Programming",
			"author":      "Alice Wilson",
			"publishDate": "2024-04-05",
			"rating":      4.7,
		},
		{
			"id":          5,
			"title":       "Machine Learning Algorithms",
			"content":     "Deep dive into various machine learning algorithms and their applications",
			"category":    "AI",
			"author":      "Charlie Brown",
			"publishDate": "2024-05-12",
			"rating":      4.4,
		},
	}

	// Build the index
	fmt.Println("📚 Indexing documents...")
	ctx := context.Background()
	if err := index.Build(ctx, documents); err != nil {
		log.Fatal("Failed to build index:", err)
	}

	// Build autocomplete
	index.BuildAutoComplete()

	fmt.Printf("✅ Successfully indexed %d documents\n\n", len(documents))

	// Demonstrate basic search with highlights
	fmt.Println("🔍 Basic Search with Highlights:")
	result, err := index.SearchWithHighlights(ctx, lookup.Request{
		Query: "machine learning",
		Size:  3,
	})
	if err != nil {
		log.Fatal("Search failed:", err)
	}

	fmt.Printf("Found %d results for 'machine learning':\n", result.Result.Total)
	for i, item := range result.Items {
		fmt.Printf("  %d. %s (Rating: %.1f)\n", i+1,
			item.Record["title"], item.Record["rating"])
		if highlights, ok := item.Highlights["content"]; ok {
			fmt.Printf("     Content: %s\n", highlights)
		}
	}
	fmt.Println()

	// Demonstrate autocomplete
	fmt.Println("💡 Autocomplete Suggestions:")
	suggestions := index.GetSuggestions("prog", 5)
	fmt.Printf("Suggestions for 'prog': %v\n\n", suggestions)

	// Demonstrate advanced query processing
	fmt.Println("🧠 Advanced Query Processing:")
	processor := lookup.NewQueryProcessor()
	processor.EnableStemming(true)
	processor.AddSynonym("ai", []string{"artificial intelligence", "machine learning"})

	// Boolean query
	booleanQuery, err := processor.ParseAdvancedQuery("programming AND optimization")
	if err == nil {
		booleanResults := booleanQuery.Evaluate(index.Index)
		fmt.Printf("Boolean query 'programming AND optimization' found %d documents\n",
			len(booleanResults))
	}

	// Phrase query
	phraseQuery, err := processor.ParseAdvancedQuery(`"machine learning"`)
	if err == nil {
		phraseResults := phraseQuery.Evaluate(index.Index)
		fmt.Printf("Phrase query '\"machine learning\"' found %d documents\n",
			len(phraseResults))
	}

	// Field-specific query
	fieldQuery, err := processor.ParseAdvancedQuery("category:Programming")
	if err == nil {
		fieldResults := fieldQuery.Evaluate(index.Index)
		fmt.Printf("Field query 'category:Programming' found %d documents\n",
			len(fieldResults))
	}
	fmt.Println()

	// Demonstrate query builder
	fmt.Println("🏗️  Query Builder:")
	builder := lookup.NewQueryBuilder()
	complexQuery := builder.
		Term("programming").
		And().
		Range("rating", 4.0, 5.0).
		Build()

	if complexQuery != nil {
		builderResults := complexQuery.Evaluate(index.Index)
		fmt.Printf("Complex query (programming + rating 4.0-5.0) found %d documents\n",
			len(builderResults))
	}
	fmt.Println()

	// Demonstrate fuzzy search
	fmt.Println("🔮 Fuzzy Search:")
	fuzzyQuery := lookup.NewFuzzyQuery("machne", 2) // Misspelled "machine"
	fuzzyResults := fuzzyQuery.Evaluate(index.Index)
	fmt.Printf("Fuzzy search for 'machne' (2 edits) found %d documents\n",
		len(fuzzyResults))
	fmt.Println()

	// Demonstrate performance monitoring
	fmt.Println("📊 Performance Metrics:")
	stats := index.GetQueryStats()
	fmt.Printf("Total queries executed: %d\n", len(stats))
	if len(stats) > 0 {
		avgLatency := index.GetAverageLatency()
		fmt.Printf("Average query latency: %v\n", avgLatency)
	}

	// Popular queries
	popular := index.GetPopularQueries(3)
	fmt.Printf("Popular queries: %v\n", popular)
	fmt.Println()

	// Health check
	fmt.Println("🏥 Index Health Check:")
	health := index.IndexHealthCheck()
	fmt.Printf("Status: %s\n", health["status"])
	fmt.Printf("Total documents: %d\n", health["total_documents"])
	fmt.Printf("Total terms: %d\n", health["total_terms"])
	fmt.Printf("Average document length: %.2f\n", health["avg_doc_length"])
	fmt.Println()

	// Demonstrate performance optimizations
	fmt.Println("⚡ Performance Optimizations:")

	// Bloom filter example
	bf := lookup.NewBloomFilter(1000, 0.01)
	testTerms := []string{"machine", "learning", "programming", "database"}
	for _, term := range testTerms {
		bf.Add([]byte(term))
	}

	fmt.Printf("Bloom filter test - 'machine' might exist: %v\n",
		bf.MightContain([]byte("machine")))
	fmt.Printf("Bloom filter test - 'xyz' might exist: %v\n",
		bf.MightContain([]byte("xyz")))

	// Compressed index example
	ci := lookup.NewCompressedIndex()
	postings := []lookup.Posting{
		{DocID: 1, Frequency: 5},
		{DocID: 3, Frequency: 2},
		{DocID: 5, Frequency: 8},
	}
	ci.CompressPostings("example", postings)
	decompressed := ci.DecompressPostings("example")
	fmt.Printf("Compressed %d postings, decompressed %d postings\n",
		len(postings), len(decompressed))
	fmt.Println()

	// Add a new document in real-time
	fmt.Println("🔄 Real-time Document Addition:")
	newDoc := lookup.GenericRecord{
		"id":       6,
		"title":    "Real-time Search Updates",
		"content":  "Adding documents to the search index in real-time",
		"category": "Programming",
		"author":   "Live User",
		"rating":   4.0,
	}

	if err := index.AddDocument(newDoc); err != nil {
		log.Printf("failed to add new document: %v", err)
	}

	// Wait a moment for the document to be processed
	time.Sleep(100 * time.Millisecond)

	// Search for the new document
	liveResult, err := index.Search(ctx, lookup.Request{
		Query: "real-time",
		Size:  5,
	})
	if err == nil {
		fmt.Printf("Search for 'real-time' found %d documents (including new one)\n",
			liveResult.Total)
	}

	fmt.Println("\n🎉 Demo completed successfully!")
	fmt.Println("📖 Check the README.md for comprehensive documentation and more examples.")
}
