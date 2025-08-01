package lookup

import (
	"context"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/oarkflow/lookup/utils"
)

// EnhancedIndex provides additional features for the search engine
type EnhancedIndex struct {
	*Index
	autoComplete   *TrieNode
	queryLog       []QueryStats
	queryLogMutex  sync.RWMutex
	enableStemming bool
	enableMetrics  bool
}

// QueryStats tracks query performance
type QueryStats struct {
	Query     string
	Timestamp time.Time
	Latency   time.Duration
	Results   int
}

// TrieNode represents a node in the autocomplete trie
type TrieNode struct {
	children map[rune]*TrieNode
	isEnd    bool
	count    int
}

// NewEnhancedIndex creates an enhanced index with additional features
func NewEnhancedIndex(id string, opts ...Options) *EnhancedIndex {
	baseIndex := NewIndex(id, opts...)
	return &EnhancedIndex{
		Index:         baseIndex,
		autoComplete:  &TrieNode{children: make(map[rune]*TrieNode)},
		queryLog:      make([]QueryStats, 0, 1000),
		enableMetrics: true,
	}
}

// EnableStemming turns on stemming for tokenization
func (ei *EnhancedIndex) EnableStemming(enable bool) {
	ei.enableStemming = enable
}

// EnableMetrics turns on query metrics collection
func (ei *EnhancedIndex) EnableMetrics(enable bool) {
	ei.enableMetrics = enable
}

// BuildAutoComplete builds the autocomplete trie from indexed terms
func (ei *EnhancedIndex) BuildAutoComplete() {
	ei.RLock()
	defer ei.RUnlock()

	for term := range ei.index {
		ei.insertToTrie(term)
	}
}

// insertToTrie inserts a term into the autocomplete trie
func (ei *EnhancedIndex) insertToTrie(word string) {
	node := ei.autoComplete
	for _, char := range word {
		if node.children[char] == nil {
			node.children[char] = &TrieNode{children: make(map[rune]*TrieNode)}
		}
		node = node.children[char]
		node.count++
	}
	node.isEnd = true
}

// GetSuggestions returns autocomplete suggestions for a prefix
func (ei *EnhancedIndex) GetSuggestions(prefix string, maxSuggestions int) []string {
	prefix = utils.ToLower(prefix)
	node := ei.autoComplete

	// Navigate to the prefix
	for _, char := range prefix {
		if node.children[char] == nil {
			return []string{}
		}
		node = node.children[char]
	}

	// Collect all words with this prefix
	suggestions := make([]string, 0, maxSuggestions)
	ei.collectWords(node, prefix, &suggestions, maxSuggestions)

	// Sort by frequency (count)
	sort.Slice(suggestions, func(i, j int) bool {
		return ei.getTermFrequency(suggestions[i]) > ei.getTermFrequency(suggestions[j])
	})

	return suggestions
}

// collectWords recursively collects words from trie
func (ei *EnhancedIndex) collectWords(node *TrieNode, prefix string, suggestions *[]string, maxSuggestions int) {
	if len(*suggestions) >= maxSuggestions {
		return
	}

	if node.isEnd {
		*suggestions = append(*suggestions, prefix)
	}

	for char, child := range node.children {
		ei.collectWords(child, prefix+string(char), suggestions, maxSuggestions)
	}
}

// getTermFrequency returns the frequency of a term across all documents
func (ei *EnhancedIndex) getTermFrequency(term string) int {
	ei.RLock()
	defer ei.RUnlock()

	if postings, ok := ei.index[term]; ok {
		totalFreq := 0
		for _, posting := range postings {
			totalFreq += posting.Frequency
		}
		return totalFreq
	}
	return 0
}

// SearchWithHighlights performs search and returns results with highlighted terms
func (ei *EnhancedIndex) SearchWithHighlights(ctx context.Context, req Request) (*HighlightedResult, error) {
	start := time.Now()

	// Perform regular search
	result, err := ei.Search(ctx, req)
	if err != nil {
		return nil, err
	}

	// Add highlights
	highlightedItems := make([]HighlightedRecord, 0, len(result.Items))
	queryTerms := ei.extractQueryTerms(req.Query)

	for _, item := range result.Items {
		highlighted := ei.highlightRecord(item, queryTerms)
		highlightedItems = append(highlightedItems, highlighted)
	}

	latency := time.Since(start)

	// Log query stats if metrics are enabled
	if ei.enableMetrics {
		ei.logQuery(req.Query, latency, result.Total)
	}

	return &HighlightedResult{
		Items:  highlightedItems,
		Result: *result,
	}, nil
}

// extractQueryTerms extracts terms from a query string
func (ei *EnhancedIndex) extractQueryTerms(query string) []string {
	if ei.enableStemming {
		return utils.TokenizeWithStemming(query)
	}
	return utils.Tokenize(query)
}

// highlightRecord highlights query terms in a record
func (ei *EnhancedIndex) highlightRecord(record GenericRecord, queryTerms []string) HighlightedRecord {
	highlighted := HighlightedRecord{
		Record:     record,
		Highlights: make(map[string]string),
	}

	for field, value := range record {
		if strValue, ok := value.(string); ok {
			highlightedValue := ei.highlightText(strValue, queryTerms)
			if highlightedValue != strValue {
				highlighted.Highlights[field] = highlightedValue
			}
		}
	}

	return highlighted
}

// highlightText highlights query terms in text
func (ei *EnhancedIndex) highlightText(text string, queryTerms []string) string {
	result := text
	for _, term := range queryTerms {
		// Simple case-insensitive highlighting
		termLower := utils.ToLower(term)
		resultLower := utils.ToLower(result)

		if strings.Contains(resultLower, termLower) {
			// Find all occurrences and highlight them
			highlighted := strings.ReplaceAll(result, term, "<mark>"+term+"</mark>")
			// Also try different cases
			highlighted = strings.ReplaceAll(highlighted, strings.Title(term), "<mark>"+strings.Title(term)+"</mark>")
			highlighted = strings.ReplaceAll(highlighted, strings.ToUpper(term), "<mark>"+strings.ToUpper(term)+"</mark>")
			result = highlighted
		}
	}
	return result
}

// logQuery logs query statistics
func (ei *EnhancedIndex) logQuery(query string, latency time.Duration, results int) {
	ei.queryLogMutex.Lock()
	defer ei.queryLogMutex.Unlock()

	stat := QueryStats{
		Query:     query,
		Timestamp: time.Now(),
		Latency:   latency,
		Results:   results,
	}

	ei.queryLog = append(ei.queryLog, stat)

	// Keep only last 1000 queries
	if len(ei.queryLog) > 1000 {
		ei.queryLog = ei.queryLog[len(ei.queryLog)-1000:]
	}
}

// GetQueryStats returns query statistics
func (ei *EnhancedIndex) GetQueryStats() []QueryStats {
	ei.queryLogMutex.RLock()
	defer ei.queryLogMutex.RUnlock()

	// Return a copy to avoid race conditions
	stats := make([]QueryStats, len(ei.queryLog))
	copy(stats, ei.queryLog)
	return stats
}

// GetPopularQueries returns the most popular queries
func (ei *EnhancedIndex) GetPopularQueries(limit int) []string {
	ei.queryLogMutex.RLock()
	defer ei.queryLogMutex.RUnlock()

	queryCount := make(map[string]int)
	for _, stat := range ei.queryLog {
		queryCount[stat.Query]++
	}

	type queryFreq struct {
		query string
		count int
	}

	var queries []queryFreq
	for query, count := range queryCount {
		queries = append(queries, queryFreq{query, count})
	}

	sort.Slice(queries, func(i, j int) bool {
		return queries[i].count > queries[j].count
	})

	result := make([]string, 0, min(limit, len(queries)))
	for i := 0; i < min(limit, len(queries)); i++ {
		result = append(result, queries[i].query)
	}

	return result
}

// GetAverageLatency returns the average query latency
func (ei *EnhancedIndex) GetAverageLatency() time.Duration {
	ei.queryLogMutex.RLock()
	defer ei.queryLogMutex.RUnlock()

	if len(ei.queryLog) == 0 {
		return 0
	}

	var total time.Duration
	for _, stat := range ei.queryLog {
		total += stat.Latency
	}

	return total / time.Duration(len(ei.queryLog))
}

// HighlightedRecord represents a search result with highlights
type HighlightedRecord struct {
	Record     GenericRecord     `json:"record"`
	Highlights map[string]string `json:"highlights"`
}

// HighlightedResult represents search results with highlights
type HighlightedResult struct {
	Items  []HighlightedRecord `json:"items"`
	Result Result              `json:"result"`
}

// IndexHealthCheck performs basic health checks on the index
func (ei *EnhancedIndex) IndexHealthCheck() map[string]interface{} {
	ei.RLock()
	defer ei.RUnlock()

	health := map[string]interface{}{
		"status":            "healthy",
		"total_documents":   ei.TotalDocs,
		"total_terms":       len(ei.index),
		"avg_doc_length":    ei.AvgDocLength,
		"indexing_progress": ei.indexingInProgress,
		"cache_size":        len(ei.searchCache),
		"memory_capacity":   ei.MemoryCapacity,
	}

	// Check for potential issues
	issues := []string{}

	if ei.TotalDocs == 0 {
		issues = append(issues, "no_documents_indexed")
		health["status"] = "warning"
	}

	if len(ei.index) == 0 {
		issues = append(issues, "empty_inverted_index")
		health["status"] = "error"
	}

	if ei.indexingInProgress {
		issues = append(issues, "indexing_in_progress")
	}

	if len(issues) > 0 {
		health["issues"] = issues
	}

	return health
}

// Optimize performs index optimization
func (ei *EnhancedIndex) Optimize() error {
	log.Println("Starting index optimization...")

	// Track optimization metrics
	startTime := time.Now()
	originalTerms := 0
	optimizedTerms := 0

	// Lock the base index for optimization
	ei.Index.Lock()
	defer ei.Index.Unlock()

	// 1. Compress posting lists and remove empty entries
	for term, postings := range ei.Index.index {
		originalTerms++

		if len(postings) == 0 {
			delete(ei.Index.index, term)
			continue
		}

		// Sort postings by document ID for better compression
		sort.Slice(postings, func(i, j int) bool {
			return postings[i].DocID < postings[j].DocID
		})

		// Remove duplicate entries
		uniquePostings := make([]Posting, 0, len(postings))
		var lastDocID int64 = -1
		for _, posting := range postings {
			if posting.DocID != lastDocID {
				uniquePostings = append(uniquePostings, posting)
				lastDocID = posting.DocID
			}
		}

		ei.Index.index[term] = uniquePostings
		optimizedTerms++
	}

	// 2. Clean up document storage - count valid documents
	validDocCount := 0
	ei.Index.documents.ForEach(func(docID int64, doc GenericRecord) bool {
		if doc != nil {
			validDocCount++
		}
		return true
	})

	// 3. Rebuild autocomplete trie for better performance
	ei.BuildAutoComplete()

	// 4. Update metrics
	optimizationTime := time.Since(startTime)
	removedTerms := originalTerms - optimizedTerms

	log.Printf("Index optimization completed in %v", optimizationTime)
	log.Printf("Processed %d terms, removed %d empty terms", originalTerms, removedTerms)
	log.Printf("Valid document count: %d", validDocCount)

	return nil
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
