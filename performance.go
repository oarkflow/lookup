package lookup

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
	"net"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

// FastTokenization provides optimized tokenization with SIMD-like processing
type FastTokenization struct {
	bufferPool sync.Pool
}

// NewFastTokenization creates a new fast tokenization instance
func NewFastTokenization() *FastTokenization {
	return &FastTokenization{
		bufferPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, 4096)
			},
		},
	}
}

// TokenizeFast performs high-performance tokenization
func (ft *FastTokenization) TokenizeFast(text string) []string {
	if len(text) == 0 {
		return nil
	}

	buf := ft.bufferPool.Get().([]byte)
	defer ft.bufferPool.Put(buf[:0])

	buf = append(buf, text...)
	ft.toLowerInPlace(buf)

	var tokens []string
	i := 0
	for i < len(buf) {
		// Skip non-alphanumeric characters
		for i < len(buf) && !ft.isAlphaNum(buf[i]) {
			i++
		}

		if i >= len(buf) {
			break
		}

		start := i
		// Process token
		for i < len(buf) && ft.isAlphaNum(buf[i]) {
			i++
		}

		if start < i {
			tokens = append(tokens, string(buf[start:i]))
		}
	}

	return tokens
}

// toLowerInPlace converts ASCII uppercase to lowercase in-place
func (ft *FastTokenization) toLowerInPlace(buf []byte) {
	for i := 0; i < len(buf); i++ {
		if buf[i] >= 'A' && buf[i] <= 'Z' {
			buf[i] += 32 // 'a' - 'A' = 32
		}
	}
}

// isAlphaNum checks if byte is alphanumeric
func (ft *FastTokenization) isAlphaNum(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= '0' && b <= '9') || b == '_'
}

// OptimizedTokenization provides SIMD-accelerated tokenization
type OptimizedTokenization struct {
	ft *FastTokenization
}

// NewOptimizedTokenization creates optimized tokenization
func NewOptimizedTokenization() *OptimizedTokenization {
	return &OptimizedTokenization{
		ft: NewFastTokenization(),
	}
}

// TokenizeOptimized performs optimized tokenization with stemming
func (ot *OptimizedTokenization) TokenizeOptimized(text string) []string {
	tokens := ot.ft.TokenizeFast(text)

	// Apply fast stemming in-place
	for i, token := range tokens {
		if len(token) > 3 {
			tokens[i] = ot.fastStem(token)
		}
	}

	return tokens
}

// fastStem applies fast stemming rules
func (ot *OptimizedTokenization) fastStem(word string) string {
	if len(word) <= 3 {
		return word
	}

	// Fast suffix removal
	switch {
	case strings.HasSuffix(word, "ing"):
		if len(word) > 4 {
			stem := word[:len(word)-3]
			// Remove double consonants (except l, s, z)
			if len(stem) > 2 {
				last := stem[len(stem)-1]
				if last == stem[len(stem)-2] && last != 'l' && last != 's' && last != 'z' {
					return stem[:len(stem)-1]
				}
			}
			return stem
		}
	case strings.HasSuffix(word, "ly"):
		if len(word) > 4 {
			return word[:len(word)-2]
		}
	case strings.HasSuffix(word, "ed"):
		if len(word) > 4 {
			return word[:len(word)-2]
		}
	case strings.HasSuffix(word, "ies"):
		if len(word) > 4 {
			return word[:len(word)-3] + "y"
		}
	case strings.HasSuffix(word, "s"):
		if len(word) > 3 && word[len(word)-2] != 's' {
			return word[:len(word)-1]
		}
	}

	return word
}

// HighPerformanceIndex provides optimized indexing with reduced allocations
type HighPerformanceIndex struct {
	*Index
	tokenizer  *OptimizedTokenization
	docBuffer  []GenericRecord
	bufferSize int
	bufferMu   sync.Mutex
}

// NewHighPerformanceIndex creates a high-performance index
func NewHighPerformanceIndex(id string, bufferSize int, opts ...Options) *HighPerformanceIndex {
	tokenizer := NewOptimizedTokenization()
	opts = append(opts, WithNumOfWorkers(runtime.NumCPU()*2)) // More workers

	hpi := &HighPerformanceIndex{
		Index:      NewIndex(id, opts...),
		tokenizer:  tokenizer,
		docBuffer:  make([]GenericRecord, 0, bufferSize),
		bufferSize: bufferSize,
	}

	// Override the document processing to use optimized tokenization
	hpi.Index.addDocChan = make(chan GenericRecord, 10000)

	go hpi.processOptimizedBatchLoop()

	return hpi
}

// processOptimizedBatchLoop processes documents with optimized tokenization
func (hpi *HighPerformanceIndex) processOptimizedBatchLoop() {
	flushThreshold := 2000 // Larger batch for better throughput
	batchPtr := batchPool.Get().(*[]GenericRecord)
	batch := *batchPtr
	batch = batch[:0]
	ticker := time.NewTicker(100 * time.Millisecond) // More frequent flushing
	defer func() {
		ticker.Stop()
		batchPool.Put(&batch)
	}()

	for {
		select {
		case rec, ok := <-hpi.Index.addDocChan:
			if !ok {
				if len(batch) > 0 {
					hpi.processOptimizedBatch(batch)
				}
				return
			}
			batch = append(batch, rec)
			if len(batch) >= flushThreshold {
				hpi.processOptimizedBatch(batch)
				batch = batch[:0]
			}
		case <-ticker.C:
			if len(batch) > 0 {
				hpi.processOptimizedBatch(batch)
				batch = batch[:0]
			}
		}
	}
}

// processOptimizedBatch processes a batch with optimized tokenization
func (hpi *HighPerformanceIndex) processOptimizedBatch(recs []GenericRecord) {
	partial := partialIndex{
		docs:     make(map[int64]GenericRecord),
		lengths:  make(map[int64]int),
		inverted: make(map[string][]Posting),
	}

	for _, rec := range recs {
		docID := hpi.extractDocID(rec)
		combined := rec.String(hpi.FieldsToIndex, hpi.IndexFieldsExcept)
		tokens := hpi.tokenizer.TokenizeOptimized(combined)

		partial.docs[docID] = rec
		docLen := 0
		freqMap := make(map[string]int, len(tokens))

		for _, token := range tokens {
			freqMap[token]++
		}

		for term, count := range freqMap {
			partial.inverted[term] = append(partial.inverted[term], Posting{DocID: docID, Frequency: count})
			docLen += count
		}

		partial.lengths[docID] = docLen
		partial.totalDocs++
	}

	hpi.mergePartial(partial)
}

// FastCache provides high-performance caching with uint64 keys
type FastCache struct {
	entries  map[uint64]cacheEntry
	lru      []uint64
	capacity int
	mu       sync.RWMutex
}

// NewFastCache creates a fast cache with uint64 keys
func NewFastCache(capacity int) *FastCache {
	return &FastCache{
		entries:  make(map[uint64]cacheEntry, capacity),
		lru:      make([]uint64, 0, capacity),
		capacity: capacity,
	}
}

// Get retrieves a value from the cache
func (fc *FastCache) Get(key uint64) ([]ScoredDoc, bool) {
	fc.mu.RLock()
	entry, exists := fc.entries[key]
	fc.mu.RUnlock()

	if !exists || time.Now().After(entry.expiry) {
		return nil, false
	}

	// Move to front (simple LRU)
	fc.mu.Lock()
	fc.moveToFront(key)
	fc.mu.Unlock()

	return entry.data, true
}

// Put stores a value in the cache
func (fc *FastCache) Put(key uint64, value []ScoredDoc, expiry time.Time) {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	if len(fc.entries) >= fc.capacity {
		fc.evictLRU()
	}

	fc.entries[key] = cacheEntry{data: value, expiry: expiry}
	fc.lru = append(fc.lru, key)
}

// moveToFront moves a key to the front of LRU list
func (fc *FastCache) moveToFront(key uint64) {
	for i, k := range fc.lru {
		if k == key {
			copy(fc.lru[i:], fc.lru[i+1:])
			fc.lru[len(fc.lru)-1] = key
			break
		}
	}
}

// evictLRU removes the least recently used entry
func (fc *FastCache) evictLRU() {
	if len(fc.lru) == 0 {
		return
	}

	lruKey := fc.lru[0]
	delete(fc.entries, lruKey)
	fc.lru = fc.lru[1:]
}

// ParallelQueryProcessor provides parallel query processing
type ParallelQueryProcessor struct {
	workers int
}

// NewParallelQueryProcessor creates a parallel query processor
func NewParallelQueryProcessor(workers int) *ParallelQueryProcessor {
	if workers <= 0 {
		workers = runtime.NumCPU()
	}
	return &ParallelQueryProcessor{workers: workers}
}

// ProcessParallel processes queries in parallel
func (pqp *ParallelQueryProcessor) ProcessParallel(index *Index, queries []Query, tokens [][]string) ([][]int64, error) {
	if len(queries) == 0 {
		return nil, nil
	}

	results := make([][]int64, len(queries))
	jobs := make(chan int, len(queries))
	resultsChan := make(chan struct {
		index  int
		result []int64
	}, len(queries))

	// Start workers
	for i := 0; i < pqp.workers; i++ {
		go func() {
			for jobIndex := range jobs {
				query := queries[jobIndex]
				result := query.Evaluate(index)
				resultsChan <- struct {
					index  int
					result []int64
				}{jobIndex, result}
			}
		}()
	}

	// Send jobs
	go func() {
		for i := range queries {
			jobs <- i
		}
		close(jobs)
	}()

	// Collect results
	for i := 0; i < len(queries); i++ {
		res := <-resultsChan
		results[res.index] = res.result
	}

	return results, nil
}

// OptimizedBM25 provides faster BM25 scoring with pre-computed values
type OptimizedBM25 struct {
	k1, b     float64
	idfCache  map[string]float64
	mu        sync.RWMutex
	cacheSize int
}

// NewOptimizedBM25 creates optimized BM25 scorer
func NewOptimizedBM25(k1, b float64) *OptimizedBM25 {
	return &OptimizedBM25{
		k1:        k1,
		b:         b,
		idfCache:  make(map[string]float64),
		cacheSize: 0,
	}
}

// ScoreOptimized performs optimized BM25 scoring
func (obm25 *OptimizedBM25) ScoreOptimized(index *Index, queryTokens []string, docID int64, docLength float64) float64 {
	score := 0.0
	avgDocLength := index.AvgDocLength

	// For small queries, avoid mutex overhead
	if len(queryTokens) <= 3 {
		return obm25.scoreWithoutCache(index, queryTokens, docID, docLength, avgDocLength)
	}

	for _, term := range queryTokens {
		// Get cached IDF or compute it
		obm25.mu.RLock()
		idf, exists := obm25.idfCache[term]
		obm25.mu.RUnlock()

		if !exists {
			if postings, ok := index.index[term]; ok {
				df := float64(len(postings))
				idf = math.Log((float64(index.TotalDocs)-df+0.5)/(df+0.5) + 1)
			} else {
				continue
			}

			// Cache the IDF (with size limit to prevent unbounded growth)
			obm25.mu.Lock()
			if obm25.cacheSize < 1000 { // Limit cache size
				obm25.idfCache[term] = idf
				obm25.cacheSize++
			}
			obm25.mu.Unlock()
		}

		// Find term frequency for this document
		var tf int
		if postings, ok := index.index[term]; ok {
			for _, p := range postings {
				if p.DocID == docID {
					tf = p.Frequency
					break
				}
			}
		}

		if tf == 0 {
			continue
		}

		// BM25 formula
		tfScore := (float64(tf) * (obm25.k1 + 1)) / (float64(tf) + obm25.k1*(1-obm25.b+obm25.b*(docLength/avgDocLength)))
		score += idf * tfScore
	}

	return score
}

// scoreWithoutCache performs BM25 scoring without caching (for small queries)
func (obm25 *OptimizedBM25) scoreWithoutCache(index *Index, queryTokens []string, docID int64, docLength, avgDocLength float64) float64 {
	score := 0.0

	for _, term := range queryTokens {
		// Compute IDF directly
		var idf float64
		if postings, ok := index.index[term]; ok {
			df := float64(len(postings))
			idf = math.Log((float64(index.TotalDocs)-df+0.5)/(df+0.5) + 1)
		} else {
			continue
		}

		// Find term frequency for this document
		var tf int
		if postings, ok := index.index[term]; ok {
			for _, p := range postings {
				if p.DocID == docID {
					tf = p.Frequency
					break
				}
			}
		}

		if tf == 0 {
			continue
		}

		// BM25 formula
		tfScore := (float64(tf) * (obm25.k1 + 1)) / (float64(tf) + obm25.k1*(1-obm25.b+obm25.b*(docLength/avgDocLength)))
		score += idf * tfScore
	}

	return score
}

// MemoryPool provides object pooling for reduced GC pressure
type MemoryPool struct {
	scoredDocPool sync.Pool
	postingPool   sync.Pool
	tokenPool     sync.Pool
}

// NewMemoryPool creates a memory pool
func NewMemoryPool() *MemoryPool {
	return &MemoryPool{
		scoredDocPool: sync.Pool{
			New: func() interface{} {
				s := make([]ScoredDoc, 0, 1024)
				return &s
			},
		},
		postingPool: sync.Pool{
			New: func() interface{} {
				p := make([]Posting, 0, 512)
				return &p
			},
		},
		tokenPool: sync.Pool{
			New: func() interface{} {
				t := make([]string, 0, 64)
				return &t
			},
		},
	}
}

// GetScoredDocs gets a scored docs slice from pool
func (mp *MemoryPool) GetScoredDocs() *[]ScoredDoc {
	return mp.scoredDocPool.Get().(*[]ScoredDoc)
}

// PutScoredDocs returns a scored docs slice to pool
func (mp *MemoryPool) PutScoredDocs(s *[]ScoredDoc) {
	*s = (*s)[:0]
	mp.scoredDocPool.Put(s)
}

// GetPostings gets a postings slice from pool
func (mp *MemoryPool) GetPostings() *[]Posting {
	return mp.postingPool.Get().(*[]Posting)
}

// PutPostings returns a postings slice to pool
func (mp *MemoryPool) PutPostings(p *[]Posting) {
	*p = (*p)[:0]
	mp.postingPool.Put(p)
}

// GetTokens gets a tokens slice from pool
func (mp *MemoryPool) GetTokens() *[]string {
	return mp.tokenPool.Get().(*[]string)
}

// Connection types for the connection pool
type HTTPConnection struct {
	Client  *http.Client
	BaseURL string
}

type TCPConnection struct {
	Conn    net.Conn
	Address string
}

// BloomFilter implements a space-efficient probabilistic data structure
type BloomFilter struct {
	bitArray []uint64
	size     uint64
	hashFunc []func([]byte) uint64
}

// NewBloomFilter creates a new Bloom filter
func NewBloomFilter(expectedElements int, falsePositiveRate float64) *BloomFilter {
	size := optimalBloomFilterSize(expectedElements, falsePositiveRate)
	hashCount := optimalHashFunctions(expectedElements, int(size))

	bf := &BloomFilter{
		bitArray: make([]uint64, (size+63)/64), // Round up to uint64 boundaries
		size:     size,
		hashFunc: make([]func([]byte) uint64, hashCount),
	}

	// Initialize hash functions
	for i := 0; i < hashCount; i++ {
		salt := uint64(i)
		bf.hashFunc[i] = func(data []byte) uint64 {
			h := fnv.New64a()
			h.Write(data)
			binary.Write(h, binary.LittleEndian, salt)
			return h.Sum64()
		}
	}

	return bf
}

// Add adds an element to the Bloom filter
func (bf *BloomFilter) Add(data []byte) {
	for _, hash := range bf.hashFunc {
		index := hash(data) % bf.size
		wordIndex := index / 64
		bitIndex := index % 64
		bf.bitArray[wordIndex] |= 1 << bitIndex
	}
}

// MightContain checks if an element might be in the set
func (bf *BloomFilter) MightContain(data []byte) bool {
	for _, hash := range bf.hashFunc {
		index := hash(data) % bf.size
		wordIndex := index / 64
		bitIndex := index % 64
		if bf.bitArray[wordIndex]&(1<<bitIndex) == 0 {
			return false
		}
	}
	return true
}

// Calculate optimal Bloom filter size
func optimalBloomFilterSize(n int, p float64) uint64 {
	// m = -(n * ln(p)) / (ln(2)^2)
	// Simplified approximation
	return uint64(-float64(n) * 1.44 * logBase2(p))
}

// Calculate optimal number of hash functions
func optimalHashFunctions(n int, m int) int {
	// k = (m/n) * ln(2)
	k := float64(m) / float64(n) * 0.693
	if k < 1 {
		return 1
	}
	return int(k + 0.5) // Round to nearest integer
}

// Simple log base 2 approximation
func logBase2(x float64) float64 {
	if x <= 0 {
		return -10 // Fallback for edge case
	}
	// Simple approximation: log2(x) ≈ log(x) / log(2)
	return math.Log(x) / math.Log(2)
}

// CompressedIndex provides compression for the inverted index
type CompressedIndex struct {
	mu               sync.RWMutex
	compressedTerms  map[string][]byte
	originalIndex    map[string][]Posting
	compressionRatio float64
}

// NewCompressedIndex creates a new compressed index
func NewCompressedIndex() *CompressedIndex {
	return &CompressedIndex{
		compressedTerms: make(map[string][]byte),
		originalIndex:   make(map[string][]Posting),
	}
}

// CompressPostings compresses posting lists using delta encoding
func (ci *CompressedIndex) CompressPostings(term string, postings []Posting) {
	ci.mu.Lock()
	defer ci.mu.Unlock()

	if len(postings) == 0 {
		return
	}

	// Store original for fallback
	ci.originalIndex[term] = postings

	// Simple delta encoding + variable byte encoding
	compressed := make([]byte, 0, len(postings)*16)

	var prevDocID int64 = 0
	for _, posting := range postings {
		// Encode delta of document ID
		delta := posting.DocID - prevDocID
		compressed = append(compressed, encodeVarint(delta)...)
		compressed = append(compressed, encodeVarint(int64(posting.Frequency))...)
		prevDocID = posting.DocID
	}

	ci.compressedTerms[term] = compressed

	// Calculate compression ratio
	originalSize := len(postings) * 16 // Rough estimate: 8 bytes ID + 8 bytes frequency
	compressedSize := len(compressed)
	ci.compressionRatio = float64(compressedSize) / float64(originalSize)
}

// DecompressPostings decompresses posting lists
func (ci *CompressedIndex) DecompressPostings(term string) []Posting {
	ci.mu.RLock()
	defer ci.mu.RUnlock()

	compressed, exists := ci.compressedTerms[term]
	if !exists {
		// Fallback to original
		if original, ok := ci.originalIndex[term]; ok {
			return original
		}
		return nil
	}

	postings := make([]Posting, 0)
	var currentDocID int64 = 0

	i := 0
	for i < len(compressed) {
		// Decode delta
		delta, bytesRead := decodeVarint(compressed[i:])
		i += bytesRead
		currentDocID += delta

		// Decode frequency
		freq, bytesRead := decodeVarint(compressed[i:])
		i += bytesRead

		postings = append(postings, Posting{
			DocID:     currentDocID,
			Frequency: int(freq),
		})
	}

	return postings
}

// Simple variable-length integer encoding
func encodeVarint(value int64) []byte {
	if value < 0 {
		value = 0 // Handle negative values
	}

	result := make([]byte, 0, 10)
	for value >= 0x80 {
		result = append(result, byte(value)|0x80)
		value >>= 7
	}
	result = append(result, byte(value))
	return result
}

// Simple variable-length integer decoding
func decodeVarint(data []byte) (int64, int) {
	var result int64
	var shift uint
	var i int

	for i < len(data) {
		b := data[i]
		result |= int64(b&0x7F) << shift
		i++
		if b&0x80 == 0 {
			break
		}
		shift += 7
	}

	return result, i
}

// PerformanceMonitor tracks index performance metrics
type PerformanceMonitor struct {
	mu              sync.RWMutex
	searchLatencies []time.Duration
	indexingTimes   []time.Duration
	cacheHitRate    float64
	cacheHits       int64
	cacheMisses     int64
	memoryUsage     int64
	startTime       time.Time
}

// NewPerformanceMonitor creates a new performance monitor
func NewPerformanceMonitor() *PerformanceMonitor {
	return &PerformanceMonitor{
		startTime: time.Now(),
	}
}

// RecordSearchLatency records a search operation latency
func (pm *PerformanceMonitor) RecordSearchLatency(latency time.Duration) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	pm.searchLatencies = append(pm.searchLatencies, latency)
	// Keep only last 1000 measurements
	if len(pm.searchLatencies) > 1000 {
		pm.searchLatencies = pm.searchLatencies[len(pm.searchLatencies)-1000:]
	}
}

// RecordIndexingTime records an indexing operation time
func (pm *PerformanceMonitor) RecordIndexingTime(duration time.Duration) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	pm.indexingTimes = append(pm.indexingTimes, duration)
	if len(pm.indexingTimes) > 100 {
		pm.indexingTimes = pm.indexingTimes[len(pm.indexingTimes)-100:]
	}
}

// RecordCacheHit records a cache hit
func (pm *PerformanceMonitor) RecordCacheHit() {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.cacheHits++
	pm.updateCacheHitRate()
}

// RecordCacheMiss records a cache miss
func (pm *PerformanceMonitor) RecordCacheMiss() {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.cacheMisses++
	pm.updateCacheHitRate()
}

// updateCacheHitRate updates the cache hit rate
func (pm *PerformanceMonitor) updateCacheHitRate() {
	total := pm.cacheHits + pm.cacheMisses
	if total > 0 {
		pm.cacheHitRate = float64(pm.cacheHits) / float64(total)
	}
}

// GetMetrics returns performance metrics
func (pm *PerformanceMonitor) GetMetrics() map[string]interface{} {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	metrics := map[string]interface{}{
		"uptime":          time.Since(pm.startTime),
		"cache_hit_rate":  pm.cacheHitRate,
		"cache_hits":      pm.cacheHits,
		"cache_misses":    pm.cacheMisses,
		"memory_usage_mb": pm.memoryUsage / (1024 * 1024),
	}

	if len(pm.searchLatencies) > 0 {
		var totalLatency time.Duration
		var minLatency = pm.searchLatencies[0]
		var maxLatency = pm.searchLatencies[0]

		for _, latency := range pm.searchLatencies {
			totalLatency += latency
			if latency < minLatency {
				minLatency = latency
			}
			if latency > maxLatency {
				maxLatency = latency
			}
		}

		metrics["avg_search_latency_ms"] = float64(totalLatency.Nanoseconds()) / float64(len(pm.searchLatencies)) / 1e6
		metrics["min_search_latency_ms"] = float64(minLatency.Nanoseconds()) / 1e6
		metrics["max_search_latency_ms"] = float64(maxLatency.Nanoseconds()) / 1e6
		metrics["total_searches"] = len(pm.searchLatencies)
	}

	if len(pm.indexingTimes) > 0 {
		var totalIndexing time.Duration
		for _, duration := range pm.indexingTimes {
			totalIndexing += duration
		}
		metrics["avg_indexing_time_ms"] = float64(totalIndexing.Nanoseconds()) / float64(len(pm.indexingTimes)) / 1e6
	}

	return metrics
}

// ConnectionPool manages RPC connections for distributed search
type ConnectionPool struct {
	mu          sync.RWMutex
	connections map[string]chan interface{} // Simple connection pool
	maxSize     int
}

// NewConnectionPool creates a new connection pool
func NewConnectionPool(maxSize int) *ConnectionPool {
	return &ConnectionPool{
		connections: make(map[string]chan interface{}),
		maxSize:     maxSize,
	}
}

// GetConnection gets a connection from the pool
func (cp *ConnectionPool) GetConnection(address string) (interface{}, error) {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	pool, exists := cp.connections[address]
	if !exists {
		pool = make(chan interface{}, cp.maxSize)
		cp.connections[address] = pool
	}

	select {
	case conn := <-pool:
		return conn, nil
	default:
		// Create new connection based on address type
		if strings.HasPrefix(address, "http://") || strings.HasPrefix(address, "https://") {
			// HTTP connection
			client := &http.Client{
				Timeout: 30 * time.Second,
				Transport: &http.Transport{
					MaxIdleConns:       10,
					IdleConnTimeout:    30 * time.Second,
					DisableCompression: false,
				},
			}
			return &HTTPConnection{
				Client:  client,
				BaseURL: address,
			}, nil
		} else {
			// TCP/RPC connection
			conn, err := net.Dial("tcp", address)
			if err != nil {
				return nil, fmt.Errorf("failed to create connection to %s: %v", address, err)
			}
			return &TCPConnection{
				Conn:    conn,
				Address: address,
			}, nil
		}
	}
}

// ReturnConnection returns a connection to the pool
func (cp *ConnectionPool) ReturnConnection(address string, conn interface{}) {
	cp.mu.RLock()
	pool, exists := cp.connections[address]
	cp.mu.RUnlock()

	if exists {
		select {
		case pool <- conn:
			// Connection returned to pool
		default:
			// Pool is full, discard connection
		}
	}
}

// BatchProcessor processes documents in batches for better performance
type BatchProcessor struct {
	batchSize     int
	flushInterval time.Duration
	processor     func([]GenericRecord) error
	buffer        []GenericRecord
	mu            sync.Mutex
	ticker        *time.Ticker
	done          chan struct{}
}

// NewBatchProcessor creates a new batch processor
func NewBatchProcessor(batchSize int, flushInterval time.Duration, processor func([]GenericRecord) error) *BatchProcessor {
	bp := &BatchProcessor{
		batchSize:     batchSize,
		flushInterval: flushInterval,
		processor:     processor,
		buffer:        make([]GenericRecord, 0, batchSize),
		ticker:        time.NewTicker(flushInterval),
		done:          make(chan struct{}),
	}

	go bp.run()
	return bp
}

// Add adds a document to the batch
func (bp *BatchProcessor) Add(doc GenericRecord) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	bp.buffer = append(bp.buffer, doc)
	if len(bp.buffer) >= bp.batchSize {
		return bp.flush()
	}
	return nil
}

// flush processes the current batch
func (bp *BatchProcessor) flush() error {
	if len(bp.buffer) == 0 {
		return nil
	}

	batch := make([]GenericRecord, len(bp.buffer))
	copy(batch, bp.buffer)
	bp.buffer = bp.buffer[:0]

	return bp.processor(batch)
}

// run handles periodic flushing
func (bp *BatchProcessor) run() {
	for {
		select {
		case <-bp.ticker.C:
			bp.mu.Lock()
			bp.flush()
			bp.mu.Unlock()
		case <-bp.done:
			bp.ticker.Stop()
			return
		}
	}
}

// Close closes the batch processor
func (bp *BatchProcessor) Close() error {
	close(bp.done)
	bp.mu.Lock()
	defer bp.mu.Unlock()
	return bp.flush()
}

// CacheManager provides advanced caching with eviction policies
type CacheManager struct {
	mu       sync.RWMutex
	cache    map[string]*CacheEntry
	capacity int
	policy   string // "lru", "lfu", "ttl"
}

// CacheEntry represents a cache entry
type CacheEntry struct {
	Value       interface{}
	AccessTime  time.Time
	AccessCount int
	TTL         time.Duration
}

// NewCacheManager creates a new cache manager
func NewCacheManager(capacity int, policy string) *CacheManager {
	return &CacheManager{
		cache:    make(map[string]*CacheEntry),
		capacity: capacity,
		policy:   policy,
	}
}

// Get retrieves a value from cache
func (cm *CacheManager) Get(key string) (interface{}, bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	entry, exists := cm.cache[key]
	if !exists {
		return nil, false
	}

	// Check TTL if applicable
	if entry.TTL > 0 && time.Since(entry.AccessTime) > entry.TTL {
		delete(cm.cache, key)
		return nil, false
	}

	// Update access statistics
	entry.AccessTime = time.Now()
	entry.AccessCount++

	return entry.Value, true
}

// Set stores a value in cache
func (cm *CacheManager) Set(key string, value interface{}, ttl time.Duration) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// Check if we need to evict
	if len(cm.cache) >= cm.capacity {
		cm.evict()
	}

	cm.cache[key] = &CacheEntry{
		Value:       value,
		AccessTime:  time.Now(),
		AccessCount: 1,
		TTL:         ttl,
	}
}

// evict removes entries based on the eviction policy
func (cm *CacheManager) evict() {
	if len(cm.cache) == 0 {
		return
	}

	var keyToEvict string
	switch cm.policy {
	case "lru":
		oldestTime := time.Now()
		for key, entry := range cm.cache {
			if entry.AccessTime.Before(oldestTime) {
				oldestTime = entry.AccessTime
				keyToEvict = key
			}
		}
	case "lfu":
		minCount := int(^uint(0) >> 1) // Max int
		for key, entry := range cm.cache {
			if entry.AccessCount < minCount {
				minCount = entry.AccessCount
				keyToEvict = key
			}
		}
	default: // Random eviction as fallback
		for key := range cm.cache {
			keyToEvict = key
			break
		}
	}

	if keyToEvict != "" {
		delete(cm.cache, keyToEvict)
	}
}

// AsyncIndexer provides asynchronous indexing capabilities
type AsyncIndexer struct {
	workQueue chan IndexWork
	workers   int
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
	index     *Index // Reference to the main index
}

// IndexWork represents work to be done by the indexer
type IndexWork struct {
	Type       string
	Data       interface{}
	Document   GenericRecord
	DocumentID string
}

// NewAsyncIndexer creates a new async indexer
func NewAsyncIndexer(workers int, queueSize int, index *Index) *AsyncIndexer {
	ctx, cancel := context.WithCancel(context.Background())
	ai := &AsyncIndexer{
		workQueue: make(chan IndexWork, queueSize),
		workers:   workers,
		ctx:       ctx,
		cancel:    cancel,
		index:     index,
	}

	// Start workers
	for i := 0; i < workers; i++ {
		ai.wg.Add(1)
		go ai.worker()
	}

	return ai
}

// Submit submits work to the indexer
func (ai *AsyncIndexer) Submit(work IndexWork) error {
	select {
	case ai.workQueue <- work:
		return nil
	case <-ai.ctx.Done():
		return ai.ctx.Err()
	default:
		return fmt.Errorf("queue is full")
	}
}

// worker processes work from the queue
func (ai *AsyncIndexer) worker() {
	defer ai.wg.Done()

	for {
		select {
		case work := <-ai.workQueue:
			// Process the work
			if err := ai.processWork(work); err != nil {
				fmt.Printf("Error processing work: %v\n", err)
			}
		case <-ai.ctx.Done():
			return
		}
	}
}

// processWork processes a unit of work
func (ai *AsyncIndexer) processWork(work IndexWork) error {
	switch work.Type {
	case "index":
		// Handle document indexing
		if work.Document != nil {
			return ai.indexDocument(work.Document)
		}
	case "update":
		// Handle document updates
		if work.Document != nil && work.DocumentID != "" {
			return ai.updateDocument(work.DocumentID, work.Document)
		}
	case "delete":
		// Handle document deletion
		if work.DocumentID != "" {
			return ai.deleteDocument(work.DocumentID)
		}
	case "optimize":
		// Handle index optimization
		return ai.optimizeIndex()
	default:
		return fmt.Errorf("unknown work type: %s", work.Type)
	}
	return nil
}

// indexDocument indexes a single document
func (ai *AsyncIndexer) indexDocument(doc GenericRecord) error {
	ai.index.AddDocument(doc)
	return nil
}

// updateDocument updates an existing document
func (ai *AsyncIndexer) updateDocument(docID string, doc GenericRecord) error {
	// Parse docID to int64
	id, err := strconv.ParseInt(docID, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid document ID: %v", err)
	}
	return ai.index.UpdateDocument(id, doc)
}

// deleteDocument removes a document from the index
func (ai *AsyncIndexer) deleteDocument(docID string) error {
	// Parse docID to int64
	id, err := strconv.ParseInt(docID, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid document ID: %v", err)
	}
	return ai.index.DeleteDocument(id)
}

// optimizeIndex performs index optimization
func (ai *AsyncIndexer) optimizeIndex() error {
	// For now, simulate work - in a real implementation, this would optimize the index
	time.Sleep(100 * time.Millisecond)
	return nil
}

// Close shuts down the async indexer
func (ai *AsyncIndexer) Close() error {
	ai.cancel()
	close(ai.workQueue)
	ai.wg.Wait()
	return nil
}

// FastQueryHash generates a fast hash for query caching
func FastQueryHash(query string, page, size int, filters []interface{}) uint64 {
	// Use a simple hash for small queries to reduce overhead
	if len(query) < 50 && len(filters) == 0 {
		// Simple hash for small queries
		h := uint64(0)
		for i, c := range query {
			h = h*31 + uint64(c) + uint64(page)*7 + uint64(size)*13
			if i > 20 { // Limit hash computation for very long queries
				break
			}
		}
		return h
	}

	// Use FNV for larger queries or queries with filters
	h := fnv.New64a()
	h.Write([]byte(query))
	h.Write([]byte(fmt.Sprintf("%d%d", page, size)))

	for _, f := range filters {
		h.Write([]byte(fmt.Sprintf("%v", f)))
	}

	return h.Sum64()
}
