package lookup

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/oarkflow/filters"
	"github.com/oarkflow/json"
)

// Manager provides advanced index management
type Manager struct {
	indexes      map[string]*EnhancedIndex
	indexStats   map[string]*IndexStats
	mutex        sync.RWMutex
	requestQueue chan *ManagerRequest
	workers      []*ManagerWorker
	shutdown     chan struct{}
	wg           sync.WaitGroup
	config       *ManagerConfig
}

type Filter struct {
	Field    string           `json:"field"`
	Operator filters.Operator `json:"operator"`
	Value    any              `json:"value"`
	Reverse  bool             `json:"reverse"`
	Lookup   *filters.Lookup  `json:"lookup"`
}

type Request struct {
	Filters        []Filter      `json:"filters"`
	Rule           *filters.Rule `json:"rule"`
	Query          string        `json:"q" query:"q"`
	Condition      string        `json:"condition"`
	Match          string        `json:"m" query:"m"`
	Offset         int           `json:"o" query:"o"`
	Size           int           `json:"s" query:"s"`
	SortField      string        `json:"sort_field" query:"sort_field"`
	SortOrder      string        `json:"sort_order" query:"sort_order"`
	Page           int           `json:"p" query:"p"`
	Reverse        bool          `json:"reverse" query:"reverse"`
	Exact          bool          `json:"exact" query:"exact"`
	Fuzzy          bool          `json:"fuzzy" query:"fuzzy"`
	FuzzyThreshold int           `json:"fuzzy_threshold" query:"fuzzy_threshold"`
	SearchType     string        `json:"search_type" query:"search_type"` // "fuzzy", "exact", "phrase"
}

func (r Request) Checksum() (uint64, error) {
	tmp := r
	condStrs := make([]string, len(tmp.Filters))
	for i, c := range tmp.Filters {
		b, err := json.Marshal(c)
		if err != nil {
			return 0, fmt.Errorf("marshaling filter condition: %w", err)
		}
		condStrs[i] = string(b)
	}
	sort.Strings(condStrs)
	canon := struct {
		Filters        []string `json:"filters"`
		Query          string   `json:"q"`
		Condition      string   `json:"condition"`
		Match          string   `json:"m"`
		Offset         int      `json:"o"`
		Size           int      `json:"s"`
		SortField      string   `json:"sort_field"`
		SortOrder      string   `json:"sort_order"`
		Page           int      `json:"p"`
		Reverse        bool     `json:"reverse"`
		Exact          bool     `json:"exact"`
		Fuzzy          bool     `json:"fuzzy"`
		FuzzyThreshold int      `json:"fuzzy_threshold"`
		SearchType     string   `json:"search_type"`
	}{
		Filters:        condStrs,
		Query:          tmp.Query,
		Condition:      tmp.Condition,
		Match:          tmp.Match,
		Offset:         tmp.Offset,
		Size:           tmp.Size,
		SortField:      tmp.SortField,
		SortOrder:      tmp.SortOrder,
		Page:           tmp.Page,
		Reverse:        tmp.Reverse,
		Exact:          tmp.Exact,
		Fuzzy:          tmp.Fuzzy,
		FuzzyThreshold: tmp.FuzzyThreshold,
		SearchType:     tmp.SearchType,
	}
	payload, err := json.Marshal(canon)
	if err != nil {
		return 0, fmt.Errorf("marshaling canonical request: %w", err)
	}
	return xxhash.Sum64(payload), nil
}

// ManagerConfig holds configuration for the manager
type ManagerConfig struct {
	MaxWorkers           int           `json:"max_workers"`
	RequestQueueSize     int           `json:"request_queue_size"`
	AutoOptimizeInterval time.Duration `json:"auto_optimize_interval"`
	HealthCheckInterval  time.Duration `json:"health_check_interval"`
	PersistenceEnabled   bool          `json:"persistence_enabled"`
	PersistencePath      string        `json:"persistence_path"`
	CacheSize            int           `json:"cache_size"`
	CacheExpiry          time.Duration `json:"cache_expiry"`
}

// IndexStats tracks performance metrics for each index
type IndexStats struct {
	TotalQueries   int64         `json:"total_queries"`
	AverageLatency time.Duration `json:"average_latency"`
	LastAccessed   time.Time     `json:"last_accessed"`
	DocumentCount  int           `json:"document_count"`
	TermCount      int           `json:"term_count"`
	ErrorCount     int64         `json:"error_count"`
	LastOptimized  time.Time     `json:"last_optimized"`
}

// ManagerRequest represents a request to be processed
type ManagerRequest struct {
	Type      string      `json:"type"`
	IndexName string      `json:"index_name"`
	Data      interface{} `json:"data"`
	Response  chan *ManagerResponse
	Timestamp time.Time `json:"timestamp"`
}

// ManagerResponse represents the response from processing a request
type ManagerResponse struct {
	Success bool          `json:"success"`
	Data    interface{}   `json:"data"`
	Error   string        `json:"error,omitempty"`
	Latency time.Duration `json:"latency"`
}

// ManagerWorker processes index requests
type ManagerWorker struct {
	id      int
	manager *Manager
	stop    chan struct{}
}

// NewManager creates a new manager
func NewManager(config *ManagerConfig) *Manager {
	if config == nil {
		config = &ManagerConfig{
			MaxWorkers:           8,
			RequestQueueSize:     10000,
			AutoOptimizeInterval: 1 * time.Hour,
			HealthCheckInterval:  5 * time.Minute,
			PersistenceEnabled:   true,
			PersistencePath:      "./data/indexes",
			CacheSize:            10000,
			CacheExpiry:          1 * time.Hour,
		}
	}

	manager := &Manager{
		indexes:      make(map[string]*EnhancedIndex),
		indexStats:   make(map[string]*IndexStats),
		requestQueue: make(chan *ManagerRequest, config.RequestQueueSize),
		shutdown:     make(chan struct{}),
		config:       config,
	}

	// Start workers
	manager.startWorkers()

	return manager
}

// startWorkers initializes and starts worker goroutines
func (m *Manager) startWorkers() {
	m.workers = make([]*ManagerWorker, m.config.MaxWorkers)
	for i := 0; i < m.config.MaxWorkers; i++ {
		worker := &ManagerWorker{
			id:      i,
			manager: m,
			stop:    make(chan struct{}),
		}
		m.workers[i] = worker
		m.wg.Add(1)
		go worker.run()
	}
}

// CreateIndex creates a new enhanced index
func (m *Manager) CreateIndex(name string, options ...Options) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if _, exists := m.indexes[name]; exists {
		return fmt.Errorf("index %s already exists", name)
	}

	enhancedOptions := append(options,
		WithCacheCapacity(m.config.CacheSize),
		WithCacheExpiry(m.config.CacheExpiry),
		WithNumOfWorkers(4),
	)

	index := NewEnhancedIndex(name, enhancedOptions...)
	index.EnableStemming(true)
	index.EnableMetrics(true)

	m.indexes[name] = index
	m.indexStats[name] = &IndexStats{
		LastAccessed: time.Now(),
	}

	log.Printf("Created enhanced index: %s", name)
	return nil
}

// GetIndex retrieves an index by name
func (m *Manager) GetIndex(name string) (*EnhancedIndex, bool) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	index, ok := m.indexes[name]
	if ok && m.indexStats[name] != nil {
		m.indexStats[name].LastAccessed = time.Now()
	}
	return index, ok
}

// DeleteIndex removes an index
func (m *Manager) DeleteIndex(name string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	index, exists := m.indexes[name]
	if !exists {
		return fmt.Errorf("index %s not found", name)
	}

	if err := index.Close(); err != nil {
		log.Printf("Warning: Error closing index %s: %v", name, err)
	}

	delete(m.indexes, name)
	delete(m.indexStats, name)

	log.Printf("Deleted index: %s", name)
	return nil
}

// ListIndexes returns a list of all index names with stats
func (m *Manager) ListIndexes() map[string]*IndexStats {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	result := make(map[string]*IndexStats)
	for name, stats := range m.indexStats {
		if index, exists := m.indexes[name]; exists {
			stats.DocumentCount = index.TotalDocs
			stats.TermCount = len(index.Index.index)
		}
		result[name] = stats
	}
	return result
}

// ProcessRequest handles index requests asynchronously
func (m *Manager) ProcessRequest(req *ManagerRequest) *ManagerResponse {
	req.Response = make(chan *ManagerResponse, 1)
	req.Timestamp = time.Now()

	select {
	case m.requestQueue <- req:
		return <-req.Response
	case <-time.After(30 * time.Second):
		return &ManagerResponse{
			Success: false,
			Error:   "request timeout",
			Latency: 30 * time.Second,
		}
	}
}

// Build indexes documents from various sources
func (m *Manager) Build(ctx context.Context, indexName string, source interface{}) error {
	req := &ManagerRequest{
		Type:      "build",
		IndexName: indexName,
		Data:      source,
	}

	response := m.ProcessRequest(req)
	if !response.Success {
		return fmt.Errorf("build failed: %s", response.Error)
	}
	return nil
}

// Search performs high-performance search
func (m *Manager) Search(ctx context.Context, indexName string, query Request) (*Result, error) {
	req := &ManagerRequest{
		Type:      "search",
		IndexName: indexName,
		Data:      query,
	}

	response := m.ProcessRequest(req)
	if !response.Success {
		return nil, fmt.Errorf("search failed: %s", response.Error)
	}

	result, ok := response.Data.(*Result)
	if !ok {
		return nil, fmt.Errorf("invalid search response type")
	}

	return result, nil
}

// Close gracefully shuts down the manager
func (m *Manager) Close() error {
	log.Println("Shutting down HighPerformanceManager...")

	// Stop workers
	for _, worker := range m.workers {
		close(worker.stop)
	}

	// Wait for all goroutines to finish
	m.wg.Wait()

	// Close all indexes
	m.mutex.Lock()
	defer m.mutex.Unlock()

	for name, index := range m.indexes {
		if err := index.Close(); err != nil {
			log.Printf("Error closing index %s: %v", name, err)
		}
	}

	log.Println("HighPerformanceManager shutdown complete")
	return nil
}

// Worker implementation
func (w *ManagerWorker) run() {
	defer w.manager.wg.Done()

	for {
		select {
		case req := <-w.manager.requestQueue:
			w.processRequest(req)
		case <-w.stop:
			return
		case <-w.manager.shutdown:
			return
		}
	}
}

func (w *ManagerWorker) processRequest(req *ManagerRequest) {
	startTime := time.Now()
	response := &ManagerResponse{
		Success: true,
		Latency: 0,
	}

	defer func() {
		response.Latency = time.Since(startTime)
		req.Response <- response

		// Update stats
		if stats, exists := w.manager.indexStats[req.IndexName]; exists {
			stats.TotalQueries++
			if stats.TotalQueries == 1 {
				stats.AverageLatency = response.Latency
			} else {
				stats.AverageLatency = (stats.AverageLatency + response.Latency) / 2
			}
			if !response.Success {
				stats.ErrorCount++
			}
		}
	}()

	index, exists := w.manager.GetIndex(req.IndexName)
	if !exists {
		response.Success = false
		response.Error = fmt.Sprintf("index %s not found", req.IndexName)
		return
	}

	switch req.Type {
	case "build":
		if err := w.handleBuild(index, req.Data); err != nil {
			response.Success = false
			response.Error = err.Error()
		}
	case "search":
		result, err := w.handleSearch(index, req.Data)
		if err != nil {
			response.Success = false
			response.Error = err.Error()
		} else {
			response.Data = result
		}
	default:
		response.Success = false
		response.Error = fmt.Sprintf("unknown request type: %s", req.Type)
	}
}

func (w *ManagerWorker) handleBuild(index *EnhancedIndex, data interface{}) error {
	ctx := context.Background()

	switch source := data.(type) {
	case []GenericRecord:
		return index.Build(ctx, source)
	default:
		return fmt.Errorf("unsupported build source type: %T", data)
	}
}

func (w *ManagerWorker) handleSearch(index *EnhancedIndex, data interface{}) (*Result, error) {
	query, ok := data.(Request)
	if !ok {
		return nil, fmt.Errorf("invalid search query type: %T", data)
	}

	ctx := context.Background()
	return index.Search(ctx, query)
}

// StartAdvancedHTTPServer starts an enhanced HTTP server
func (m *Manager) StartAdvancedHTTPServer(addr string) {
	// Serve static files for the UI
	http.Handle("/", http.FileServer(http.Dir("./static")))

	// API endpoints
	http.HandleFunc("/api/indexes", m.handleIndexes)
	http.HandleFunc("/api/index/create", m.handleCreateIndex)
	http.HandleFunc("/api/index/", m.handleIndexOperations)
	http.HandleFunc("/api/search/", m.handleSearch)
	http.HandleFunc("/api/metrics", m.handleMetrics)

	log.Printf("Enhanced HTTP server listening on http://%s", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}

func (m *Manager) handleIndexes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	indexes := m.ListIndexes()
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	if err := enc.Encode(indexes); err != nil {
		http.Error(w, "Failed to encode indexes", http.StatusInternalServerError)
		return
	}
}

func (m *Manager) handleCreateIndex(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// For now, create a simple index
	indexName := r.URL.Query().Get("name")
	if indexName == "" {
		http.Error(w, "Index name required", http.StatusBadRequest)
		return
	}

	if err := m.CreateIndex(indexName); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusCreated)
	w.Header().Set("Content-Type", "application/json")
	resp := struct {
		Status  string `json:"status"`
		Message string `json:"message"`
	}{
		Status:  "success",
		Message: fmt.Sprintf("Index %s created", indexName),
	}
	enc := json.NewEncoder(w)
	if err := enc.Encode(resp); err != nil {
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
		return
	}
}

func (m *Manager) handleIndexOperations(w http.ResponseWriter, r *http.Request) {
	// Extract index name from path
	path := r.URL.Path[len("/api/index/"):]
	if path == "" {
		http.Error(w, "Index name required", http.StatusBadRequest)
		return
	}

	// Check if this is a data endpoint
	if strings.HasSuffix(path, "/data") {
		indexName := strings.TrimSuffix(path, "/data")
		m.handleIndexData(w, r, indexName)
		return
	}

	// Check if this is a document endpoint
	if strings.Contains(path, "/document") {
		parts := strings.Split(path, "/document")
		if len(parts) == 2 {
			indexName := parts[0]
			documentPath := strings.TrimPrefix(parts[1], "/")
			m.handleDocumentOperations(w, r, indexName, documentPath)
			return
		}
	}

	switch r.Method {
	case http.MethodDelete:
		if err := m.DeleteIndex(path); err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		resp := struct {
			Status  string `json:"status"`
			Message string `json:"message"`
		}{
			Status:  "success",
			Message: fmt.Sprintf("Index %s deleted", path),
		}
		enc := json.NewEncoder(w)
		if err := enc.Encode(resp); err != nil {
			http.Error(w, "Failed to encode response", http.StatusInternalServerError)
			return
		}
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (m *Manager) handleSearch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract index name from path
	path := r.URL.Path[len("/api/search/"):]
	if path == "" {
		http.Error(w, "Index name required", http.StatusBadRequest)
		return
	}

	query := r.URL.Query().Get("q")
	if query == "" {
		http.Error(w, "Query required", http.StatusBadRequest)
		return
	}

	// Parse search parameters
	size := 10
	if s := r.URL.Query().Get("size"); s != "" {
		if parsed, err := strconv.Atoi(s); err == nil {
			size = parsed
		}
	}

	searchType := r.URL.Query().Get("search_type")
	if searchType == "" {
		searchType = "fuzzy" // default to fuzzy
	}

	fuzzyThreshold := 2 // default threshold
	if t := r.URL.Query().Get("fuzzy_threshold"); t != "" {
		if parsed, err := strconv.Atoi(t); err == nil {
			fuzzyThreshold = parsed
		}
	}

	req := Request{
		Query:          query,
		Size:           size,
		SearchType:     searchType,
		Fuzzy:          searchType == "fuzzy",
		FuzzyThreshold: fuzzyThreshold,
		Exact:          searchType == "exact",
	}

	result, err := m.Search(r.Context(), path, req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	resp := struct {
		Total int             `json:"total"`
		Items []GenericRecord `json:"items"`
	}{
		Total: result.Total,
		Items: result.Items,
	}
	enc := json.NewEncoder(w)
	if err := enc.Encode(resp); err != nil {
		http.Error(w, "Failed to encode search result", http.StatusInternalServerError)
		return
	}
}

func (m *Manager) handleIndexData(w http.ResponseWriter, r *http.Request, indexName string) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	index, exists := m.GetIndex(indexName)
	if !exists {
		http.Error(w, "Index not found", http.StatusNotFound)
		return
	}

	// Parse pagination parameters
	page := 1
	if p := r.URL.Query().Get("page"); p != "" {
		if parsed, err := strconv.Atoi(p); err == nil && parsed > 0 {
			page = parsed
		}
	}

	size := 20
	if s := r.URL.Query().Get("size"); s != "" {
		if parsed, err := strconv.Atoi(s); err == nil && parsed > 0 {
			size = parsed
		}
	}

	// Collect all documents from the index
	var allDocs []GenericRecord
	index.documents.ForEach(func(docID int64, doc GenericRecord) bool {
		allDocs = append(allDocs, doc)
		return true
	})

	// Apply simple filtering if specified
	filteredDocs := allDocs
	if filtersParam := r.URL.Query().Get("filters"); filtersParam != "" {
		var filters []map[string]interface{}
		if err := json.Unmarshal([]byte(filtersParam), &filters); err == nil {
			filteredDocs = m.applyFilters(allDocs, filters)
		}
	}

	// Calculate pagination
	total := len(filteredDocs)
	offset := (page - 1) * size
	end := offset + size

	if offset > total {
		offset = total
	}
	if end > total {
		end = total
	}

	var pageItems []GenericRecord
	if offset < total {
		pageItems = filteredDocs[offset:end]
	}

	w.Header().Set("Content-Type", "application/json")

	resp := struct {
		Items      []GenericRecord `json:"items"`
		Total      int             `json:"total"`
		Page       int             `json:"page"`
		Size       int             `json:"size"`
		TotalPages int             `json:"total_pages"`
	}{
		Items:      pageItems,
		Total:      total,
		Page:       page,
		Size:       size,
		TotalPages: (total + size - 1) / size,
	}

	enc := json.NewEncoder(w)
	if err := enc.Encode(resp); err != nil {
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
		return
	}
}

func (m *Manager) applyFilters(docs []GenericRecord, filters []map[string]interface{}) []GenericRecord {
	var filtered []GenericRecord

	for _, doc := range docs {
		matches := true
		for _, filter := range filters {
			field, _ := filter["field"].(string)
			operator, _ := filter["operator"].(string)
			value, _ := filter["value"].(string)

			if !m.matchesFilter(doc, field, operator, value) {
				matches = false
				break
			}
		}
		if matches {
			filtered = append(filtered, doc)
		}
	}

	return filtered
}

func (m *Manager) matchesFilter(doc GenericRecord, field, operator, value string) bool {
	var docValue interface{}
	var ok bool

	if field == "all" || field == "" {
		// Search in all fields
		for _, v := range doc {
			if m.matchesFilterValue(v, operator, value) {
				return true
			}
		}
		return false
	}

	docValue, ok = doc[field]
	if !ok {
		return false
	}

	return m.matchesFilterValue(docValue, operator, value)
}

func (m *Manager) matchesFilterValue(docValue interface{}, operator, value string) bool {
	docStr := fmt.Sprintf("%v", docValue)
	docStrLower := strings.ToLower(docStr)
	valueLower := strings.ToLower(value)

	switch operator {
	case "contains":
		return strings.Contains(docStrLower, valueLower)
	case "equals":
		return docStrLower == valueLower
	case "starts_with":
		return strings.HasPrefix(docStrLower, valueLower)
	case "ends_with":
		return strings.HasSuffix(docStrLower, valueLower)
	case "not_equals":
		return docStrLower != valueLower
	default:
		return strings.Contains(docStrLower, valueLower)
	}
}

func (m *Manager) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	stats := m.ListIndexes()
	w.Header().Set("Content-Type", "application/json")

	totalDocs := 0
	totalQueries := int64(0)
	for _, stat := range stats {
		totalDocs += stat.DocumentCount
		totalQueries += stat.TotalQueries
	}

	resp := struct {
		TotalIndexes   int   `json:"total_indexes"`
		TotalDocuments int   `json:"total_documents"`
		TotalQueries   int64 `json:"total_queries"`
	}{
		TotalIndexes:   len(stats),
		TotalDocuments: totalDocs,
		TotalQueries:   totalQueries,
	}
	enc := json.NewEncoder(w)
	if err := enc.Encode(resp); err != nil {
		http.Error(w, "Failed to encode metrics", http.StatusInternalServerError)
		return
	}
}

func (m *Manager) handleDocumentOperations(w http.ResponseWriter, r *http.Request, indexName, documentPath string) {
	index, exists := m.GetIndex(indexName)
	if !exists {
		http.Error(w, "Index not found", http.StatusNotFound)
		return
	}

	switch r.Method {
	case http.MethodPost:
		// Create new document
		m.createDocument(w, r, index, indexName)
	case http.MethodPut:
		// Update existing document
		m.updateDocument(w, r, index, indexName, documentPath)
	case http.MethodDelete:
		// Delete document
		m.deleteDocument(w, r, index, indexName, documentPath)
	case http.MethodGet:
		// Get specific document
		m.getDocument(w, r, index, indexName, documentPath)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (m *Manager) createDocument(w http.ResponseWriter, r *http.Request, index *EnhancedIndex, indexName string) {
	var document GenericRecord
	if err := json.NewDecoder(r.Body).Decode(&document); err != nil {
		http.Error(w, "Invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Add the document to the index
	index.AddDocument(document)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)

	resp := struct {
		Status  string `json:"status"`
		Message string `json:"message"`
	}{
		Status:  "success",
		Message: "Document created successfully",
	}

	enc := json.NewEncoder(w)
	if err := enc.Encode(resp); err != nil {
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
		return
	}
}

func (m *Manager) updateDocument(w http.ResponseWriter, r *http.Request, index *EnhancedIndex, indexName, documentPath string) {
	// Parse document index from path
	docIndex, err := strconv.Atoi(documentPath)
	if err != nil {
		http.Error(w, "Invalid document index", http.StatusBadRequest)
		return
	}

	var updatedDocument GenericRecord
	if err := json.NewDecoder(r.Body).Decode(&updatedDocument); err != nil {
		http.Error(w, "Invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Update the document using the new method
	if err := index.UpdateDocument(docIndex, updatedDocument); err != nil {
		http.Error(w, "Failed to update document: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	resp := struct {
		Status  string `json:"status"`
		Message string `json:"message"`
	}{
		Status:  "success",
		Message: "Document updated successfully",
	}

	enc := json.NewEncoder(w)
	if err := enc.Encode(resp); err != nil {
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
		return
	}
}

func (m *Manager) deleteDocument(w http.ResponseWriter, r *http.Request, index *EnhancedIndex, indexName, documentPath string) {
	// Parse document index from path
	docIndex, err := strconv.Atoi(documentPath)
	if err != nil {
		http.Error(w, "Invalid document index", http.StatusBadRequest)
		return
	}

	// Delete the document using the new method
	if err := index.DeleteDocument(docIndex); err != nil {
		http.Error(w, "Failed to delete document: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	resp := struct {
		Status  string `json:"status"`
		Message string `json:"message"`
	}{
		Status:  "success",
		Message: "Document deleted successfully",
	}

	enc := json.NewEncoder(w)
	if err := enc.Encode(resp); err != nil {
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
		return
	}
}

func (m *Manager) getDocument(w http.ResponseWriter, r *http.Request, index *EnhancedIndex, indexName, documentPath string) {
	// Parse document index from path
	docIndex, err := strconv.Atoi(documentPath)
	if err != nil {
		http.Error(w, "Invalid document index", http.StatusBadRequest)
		return
	}

	// Collect documents to find the one at the specified index
	var docs []GenericRecord
	index.documents.ForEach(func(docID int64, doc GenericRecord) bool {
		docs = append(docs, doc)
		return true
	})

	if docIndex < 0 || docIndex >= len(docs) {
		http.Error(w, "Document not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	if err := enc.Encode(docs[docIndex]); err != nil {
		http.Error(w, "Failed to encode document", http.StatusInternalServerError)
		return
	}
}
