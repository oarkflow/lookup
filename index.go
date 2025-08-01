package lookup

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/goccy/go-reflect"
	"github.com/oarkflow/filters"
	"github.com/oarkflow/json"
	"github.com/oarkflow/squealx"
	"github.com/oarkflow/squealx/connection"

	"github.com/oarkflow/lookup/utils"
)

var DefaultPath = "lookup"

type GenericRecord map[string]any

func (rec GenericRecord) String(fieldsToIndex []string, except []string) string {
	keys := make([]string, 0, len(rec))
	for k := range rec {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		if (len(fieldsToIndex) > 0 && !slices.Contains(fieldsToIndex, k)) || (len(except) > 0 && slices.Contains(except, k)) {
			continue
		}
		switch val := rec[k].(type) {
		case string:
			parts[i] = val
		case json.Number:
			if i, err := val.Int64(); err == nil {
				parts[i] = strconv.FormatInt(i, 10)
			} else if f, err := val.Float64(); err == nil {
				parts[i] = strconv.FormatFloat(f, 'f', -1, 64)
			}
		case float64:
			parts[i] = strconv.FormatFloat(val, 'f', -1, 64)
		case int:
			parts[i] = strconv.Itoa(val)
		case int64:
			parts[i] = strconv.FormatInt(val, 10)
		case bool:
			parts[i] = strconv.FormatBool(val)
		case time.Time:
			if val.Hour() == 0 && val.Minute() == 0 && val.Second() == 0 && val.Nanosecond() == 0 {
				parts[i] = val.Format("2006-01-02")
			} else {
				parts[i] = val.Format("2006-01-02T15:04:05.000000-0700")
			}
		default:
			parts[i] = utils.ToString(val)
		}
	}
	return strings.Join(parts, " ")
}

func (rec GenericRecord) getFrequency(fieldsToIndex []string, except []string) map[string]int {
	combined := utils.ToLower(rec.String(fieldsToIndex, except))
	var tokens []string
	// Example: switch to Unicode tokenization if needed
	// tokens = utils.TokenizeUnicode(combined)
	tokens = utils.Tokenize(combined)
	freq := make(map[string]int, len(tokens))
	for _, t := range tokens {
		freq[t]++
	}
	return freq
}

type Posting struct {
	DocID     int64
	Frequency int
}

type ScoredDoc struct {
	DocID int64
	Score float64
}

type cacheEntry struct {
	data   []ScoredDoc
	expiry time.Time
}

type Index struct {
	sync.RWMutex
	ID                 string
	TotalDocs          int
	AvgDocLength       float64
	MemoryCapacity     int
	NumWorkers         int
	FieldsToIndex      []string
	IndexFieldsExcept  []string
	defaultSortField   *SortField
	index              map[string][]Posting
	docLength          map[int64]int
	documents          *BPTree[int64, GenericRecord]
	order              int
	storage            string
	indexingInProgress bool
	searchCache        map[string]cacheEntry
	cacheExpiry        time.Duration
	reset              bool
	addDocChan         chan GenericRecord // new channel for individual document additions
	Distributed        bool
	Peers              []string
	DocIDField         string        // enhancement: allow custom doc ID field
	closed             chan struct{} // enhancement: for graceful shutdown
}

type DBConfig struct {
	DBType  string `json:"type,omitempty"`
	DBHost  string `json:"host,omitempty"`
	DBPort  int    `json:"port,omitempty"`
	DBUser  string `json:"user,omitempty"`
	DBPass  string `json:"password,omitempty"`
	DBName  string `json:"database,omitempty"`
	DBQuery string `json:"query,omitempty"`
}

type IndexRequest struct {
	Path     string          `json:"path"`
	Data     []GenericRecord `json:"data"`
	Database *DBConfig       `json:"database,omitempty"` // enhancement: allow database config

}

type Options func(*Index)

func WithNumOfWorkers(numOfWorkers int) Options {
	return func(index *Index) {
		index.NumWorkers = numOfWorkers
	}
}

func WithFieldsToIndex(fieldsToIndex ...string) Options {
	return func(index *Index) {
		index.FieldsToIndex = fieldsToIndex
	}
}

func WithIndexFieldsExcept(except ...string) Options {
	return func(index *Index) {
		index.IndexFieldsExcept = except
	}
}

func WithDefaultSortField(field string, descending bool) Options {
	return func(index *Index) {
		index.defaultSortField = &SortField{Field: field, Descending: descending}
	}
}

func WithOrder(order int) Options {
	return func(index *Index) {
		index.order = order
	}
}

func WithCacheCapacity(capacity int) Options {
	return func(index *Index) {
		index.MemoryCapacity = capacity
	}
}

func WithStorage(storage string) Options {
	return func(index *Index) {
		index.storage = storage
	}
}

func WithReset(reset bool) Options {
	return func(index *Index) {
		index.reset = reset
	}
}

func WithCacheExpiry(dur time.Duration) Options {
	return func(index *Index) {
		index.cacheExpiry = dur
	}
}

func WithDistributed() Options {
	return func(index *Index) {
		index.Distributed = true
	}
}

func WithPeers(peers ...string) Options {
	return func(index *Index) {
		index.Peers = peers
	}
}

// Option to set custom doc ID field
func WithDocIDField(field string) Options {
	return func(index *Index) {
		index.DocIDField = field
	}
}

var scoredDocPool = sync.Pool{
	New: func() any {
		// Preallocate a slice with some capacity for scoring
		s := make([]ScoredDoc, 0, 1024)
		return &s
	},
}

var batchPool = sync.Pool{
	New: func() any {
		// Preallocate a slice with some capacity for batching
		b := make([]GenericRecord, 0, 1000)
		return &b
	},
}

func NewIndex(id string, opts ...Options) *Index {
	runtime.GOMAXPROCS(runtime.NumCPU()) // maximize CPU usage
	os.MkdirAll(DefaultPath, 0755)
	storagePath := filepath.Join(DefaultPath, "storage-"+id+".dat")
	index := &Index{
		ID:             id,
		NumWorkers:     runtime.NumCPU(), // maximize parallelism
		index:          make(map[string][]Posting),
		docLength:      make(map[int64]int),
		order:          3,
		storage:        storagePath,
		MemoryCapacity: 1000,
		searchCache:    make(map[string]cacheEntry),
		cacheExpiry:    time.Minute,
		closed:         make(chan struct{}),
	}
	for _, opt := range opts {
		opt(index)
	}
	if index.reset {
		os.Remove(storagePath)
	}
	index.documents = NewBPTree[int64, GenericRecord](index.order, index.storage, index.MemoryCapacity)
	index.addDocChan = make(chan GenericRecord, 10000) // larger buffer for high throughput
	go index.processAddDocLoop()
	index.startCacheCleanup()
	return index
}

// Enhancement: Close method to shut down goroutines
func (index *Index) Close() error {
	close(index.addDocChan)
	close(index.closed)
	return index.documents.Close()
}

func (index *Index) FuzzySearch(term string, threshold int) []string {
	index.RLock()
	defer index.RUnlock()
	var results []string
	for token := range index.index {
		if utils.BoundedLevenshtein(term, token, threshold) <= threshold {
			results = append(results, token)
		}
	}
	return results
}

// New helper function: processAddDocLoop batches individual docs
func (index *Index) processAddDocLoop() {
	flushThreshold := 1000 // larger batch for throughput
	batchPtr := batchPool.Get().(*[]GenericRecord)
	batch := *batchPtr
	batch = batch[:0]
	ticker := time.NewTicker(200 * time.Millisecond) // flush more frequently
	defer func() {
		ticker.Stop()
		batchPool.Put(&batch)
	}()
	for {
		select {
		case rec, ok := <-index.addDocChan:
			if !ok {
				if len(batch) > 0 {
					index.processBatch(batch)
				}
				return
			}
			batch = append(batch, rec)
			if len(batch) >= flushThreshold {
				index.processBatch(batch)
				batch = batch[:0]
			}
		case <-ticker.C:
			if len(batch) > 0 {
				index.processBatch(batch)
				batch = batch[:0]
			}
		}
	}
}

// Helper function to extract or generate docID
func (index *Index) extractDocID(rec GenericRecord) int64 {
	if index.DocIDField != "" {
		if v, ok := rec[index.DocIDField]; ok {
			if id, err := strconv.ParseInt(utils.ToString(v), 10, 64); err == nil {
				return id
			}
		}
	}
	return utils.NewID().Int64()
}

// Helper function to merge a batch of documents
func (index *Index) processBatch(recs []GenericRecord) {
	partial := partialIndex{
		docs:     make(map[int64]GenericRecord),
		lengths:  make(map[int64]int),
		inverted: make(map[string][]Posting),
	}
	for _, rec := range recs {
		docID := index.extractDocID(rec) // enhancement: use custom docID
		freq := rec.getFrequency(index.FieldsToIndex, index.IndexFieldsExcept)
		partial.docs[docID] = rec
		docLen := 0
		for term, count := range freq {
			partial.inverted[term] = append(partial.inverted[term], Posting{DocID: docID, Frequency: count})
			docLen += count
		}
		partial.lengths[docID] = docLen
		partial.totalDocs++
	}
	index.mergePartial(partial)
}

// AddDocument that pushes docs to the channel for batching
func (index *Index) AddDocument(rec GenericRecord) {
	index.addDocChan <- rec
	if index.Distributed {
		go index.distributedAddDocument(rec)
	}
}

type partialIndex struct {
	docs      map[int64]GenericRecord
	lengths   map[int64]int
	inverted  map[string][]Posting
	totalDocs int
}

func (index *Index) mergePartial(partial partialIndex) {
	index.Lock()
	for id, rec := range partial.docs {
		index.documents.Insert(id, rec)
	}
	for id, length := range partial.lengths {
		index.docLength[id] = length
	}
	for term, postings := range partial.inverted {
		index.index[term] = append(index.index[term], postings...)
	}
	index.TotalDocs += partial.totalDocs
	index.Unlock()
}

type DBRequest struct {
	DB    *squealx.DB
	Query string
}

func (index *Index) BuildFromDatabase(ctx context.Context, req DBRequest, callbacks ...func(v GenericRecord) error) error {
	if req.DB == nil {
		return fmt.Errorf("no database provided")
	}
	if req.Query == "" {
		return fmt.Errorf("no query provided")
	}
	var data []map[string]any
	err := req.DB.Select(&data, req.Query)
	if err != nil {
		return err
	}
	return index.BuildFromRecords(ctx, data, callbacks...)
}

func (index *Index) BuildFromReader(ctx context.Context, r io.Reader, callbacks ...func(v GenericRecord) error) error {
	index.Lock()
	if index.indexingInProgress {
		index.Unlock()
		return fmt.Errorf("indexing already in progress")
	}
	index.indexingInProgress = true
	index.searchCache = make(map[string]cacheEntry)
	index.Unlock()
	defer func() {
		index.Lock()
		index.indexingInProgress = false
		index.Unlock()
	}()
	decoder := json.NewDecoder(r)
	decoder.UseNumber()
	tok, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("failed to read JSON token: %v", err)
	}
	d, ok := tok.(json.Delim)
	if !ok || d != '[' {
		return fmt.Errorf("invalid JSON array, expected '[' got %v", tok)
	}
	jobs := make(chan GenericRecord, 500)
	const flushThreshold = 100
	var wg sync.WaitGroup
	for w := 0; w < index.NumWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			partial := partialIndex{
				docs:     make(map[int64]GenericRecord),
				lengths:  make(map[int64]int),
				inverted: make(map[string][]Posting),
			}
			var localID, count int
			for rec := range jobs {
				if ctx.Err() != nil {
					return
				}
				localID++
				docID := utils.NewID().Int64()
				partial.docs[docID] = rec
				freq := rec.getFrequency(index.FieldsToIndex, index.IndexFieldsExcept)
				docLen := 0
				for term, cnt := range freq {
					partial.inverted[term] = append(partial.inverted[term], Posting{DocID: docID, Frequency: cnt})
					docLen += cnt
				}
				partial.lengths[docID] = docLen
				partial.totalDocs++
				count++
				if count >= flushThreshold {
					index.mergePartial(partial)
					partial = partialIndex{
						docs:     make(map[int64]GenericRecord),
						lengths:  make(map[int64]int),
						inverted: make(map[string][]Posting),
					}
					count = 0
				}
				for _, cb := range callbacks {
					if err := cb(rec); err != nil {
						log.Printf("callback error: %v", err)
					}
				}
			}
			if count > 0 {
				index.mergePartial(partial)
			}
		}(w)
	}
	// Updated job producer: check context and break out properly
	go func() {
		for decoder.More() {
			if ctx.Err() != nil {
				break
			}
			var rec GenericRecord
			if err := decoder.Decode(&rec); err != nil {
				log.Printf("Skipping invalid record: %v", err)
				continue
			}
			jobs <- rec
		}
		close(jobs)
	}()
	wg.Wait()
	index.Lock()
	index.update()
	index.Unlock()
	return nil
}

func (index *Index) Evaluate(tokens []string) []int64 {
	var docSet []int64
	for _, token := range tokens {
		if postings, ok := index.index[token]; ok {
			for _, p := range postings {
				docSet = append(docSet, p.DocID)
			}
		}
	}
	return docSet
}

func (index *Index) Build(ctx context.Context, input any, callbacks ...func(v GenericRecord) error) error {
	switch v := input.(type) {
	case string:
		trimmed := strings.TrimSpace(v)
		if strings.HasPrefix(trimmed, "[") {
			return index.BuildFromReader(ctx, strings.NewReader(v), callbacks...)
		}
		return index.BuildFromFile(ctx, v, callbacks...)
	case []byte:
		return index.BuildFromReader(ctx, bytes.NewReader(v), callbacks...)
	case io.Reader:
		return index.BuildFromReader(ctx, v, callbacks...)
	case DBRequest:
		return index.BuildFromDatabase(ctx, v, callbacks...)
	case []GenericRecord:
		return index.BuildFromRecords(ctx, v, callbacks...)
	case IndexRequest:
		// Database import support
		if v.Database != nil {
			db, _, err := connection.FromConfig(squealx.Config{
				Host:     v.Database.DBHost,
				Port:     v.Database.DBPort,
				Driver:   v.Database.DBType,
				Username: v.Database.DBUser,
				Password: v.Database.DBPass,
				Database: v.Database.DBName,
			})
			if err != nil {
				return fmt.Errorf("failed to connect to database: %v", err)
			}
			defer db.Close()
			return squealx.SelectEach(db, func(row map[string]any) error {
				index.AddDocument(row)
				return nil
			}, v.Database.DBQuery)
		}
		if v.Path != "" {
			return index.BuildFromFile(ctx, v.Path, callbacks...)
		}
		if len(v.Data) > 0 {
			return index.BuildFromRecords(ctx, v.Data, callbacks...)
		}
		return fmt.Errorf("no data, path, or database config provided")
	default:
		rv := reflect.ValueOf(v)
		if rv.Kind() == reflect.Slice {
			return index.BuildFromStruct(ctx, v, callbacks...)
		}
	}
	return fmt.Errorf("unsupported input type: %T", input)
}

func (index *Index) BuildFromFile(ctx context.Context, path string, callbacks ...func(v GenericRecord) error) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	return index.BuildFromReader(ctx, file, callbacks...)
}

func (index *Index) BuildFromRecords(ctx context.Context, records any, callbacks ...func(v GenericRecord) error) error {
	index.Lock()
	if index.indexingInProgress {
		index.Unlock()
		return fmt.Errorf("indexing already in progress")
	}
	index.indexingInProgress = true
	index.searchCache = make(map[string]cacheEntry)
	index.Unlock()
	jobs := make(chan GenericRecord, 50)
	const flushThreshold = 100
	var wg sync.WaitGroup
	for w := 0; w < index.NumWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			partial := partialIndex{
				docs:     make(map[int64]GenericRecord),
				lengths:  make(map[int64]int),
				inverted: make(map[string][]Posting),
			}
			var localID, count int
			for rec := range jobs {
				if ctx.Err() != nil {
					break
				}
				localID++
				docID := utils.NewID().Int64()
				partial.docs[docID] = rec
				freq := rec.getFrequency(index.FieldsToIndex, index.IndexFieldsExcept)
				docLen := 0
				for term, cnt := range freq {
					partial.inverted[term] = append(partial.inverted[term], Posting{DocID: docID, Frequency: cnt})
					docLen += cnt
				}
				partial.lengths[docID] = docLen
				partial.totalDocs++
				count++
				if count >= flushThreshold {
					index.mergePartial(partial)
					partial = partialIndex{
						docs:     make(map[int64]GenericRecord),
						lengths:  make(map[int64]int),
						inverted: make(map[string][]Posting),
					}
					count = 0
				}
				for _, cb := range callbacks {
					if err := cb(rec); err != nil {
						log.Printf("callback error: %v", err)
					}
				}
			}
			if count > 0 {
				index.mergePartial(partial)
			}
		}(w)
	}
	go func() {
		switch records := records.(type) {
		case []GenericRecord:
			for _, rec := range records {
				if ctx.Err() != nil {
					break
				}
				jobs <- rec
			}
		case []map[string]any:
			for _, rec := range records {
				if ctx.Err() != nil {
					break
				}
				jobs <- rec
			}
		}
		close(jobs)
	}()
	wg.Wait()
	index.Lock()
	index.update()
	index.Unlock()
	return nil
}

func (index *Index) BuildFromStruct(ctx context.Context, slice any, callbacks ...func(v GenericRecord) error) error {
	v := reflect.ValueOf(slice)
	if v.Kind() != reflect.Slice {
		return fmt.Errorf("not a slice")
	}
	var records []GenericRecord
	for i := 0; i < v.Len(); i++ {
		b, err := json.Marshal(v.Index(i).Interface())
		if err != nil {
			return fmt.Errorf("error marshalling element %d: %v", i, err)
		}
		var rec GenericRecord
		if err := json.Unmarshal(b, &rec); err != nil {
			return fmt.Errorf("error unmarshalling element %d: %v", i, err)
		}
		records = append(records, rec)
	}
	return index.BuildFromRecords(ctx, records, callbacks...)
}

func (index *Index) indexDoc(docID int64, rec GenericRecord, freq map[string]int) {
	index.documents.Insert(docID, rec)
	docLen := 0
	for t, count := range freq {
		index.index[t] = append(index.index[t], Posting{DocID: docID, Frequency: count})
		docLen += count
	}
	index.docLength[docID] = docLen
}

func (index *Index) update() {
	total := 0
	for _, l := range index.docLength {
		total += l
	}
	if index.TotalDocs > 0 {
		index.AvgDocLength = float64(total) / float64(index.TotalDocs)
	}
	index.indexingInProgress = false
}

func (index *Index) bm25Score(queryTokens []string, docID int64, k1, b float64) float64 {
	index.RLock()
	defer index.RUnlock()
	score := 0.0
	docLength := float64(index.docLength[docID])
	for _, term := range queryTokens {
		postings, ok := index.index[term]
		if !ok {
			continue
		}
		df := float64(len(postings))
		var tf int
		for _, p := range postings {
			if p.DocID == docID {
				tf = p.Frequency
				break
			}
		}
		if tf == 0 {
			continue
		}
		idf := math.Log((float64(index.TotalDocs)-df+0.5)/(df+0.5) + 1)
		tfScore := (float64(tf) * (k1 + 1)) / (float64(tf) + k1*(1-b+b*(docLength/index.AvgDocLength)))
		score += idf * tfScore
	}
	return score
}

type BM25 struct {
	K float64
	B float64
}

var defaultBM25 = BM25{K: 1.2, B: 0.75}

type Pagination struct {
	Page    int
	PerPage int
}

type SearchParams struct {
	Page       int
	PerPage    int
	BM25Params BM25
	SortFields []SortField
	Fields     []string
}

func (index *Index) Search(ctx context.Context, req Request) (*Result, error) {
	if index.indexingInProgress {
		return nil, fmt.Errorf("indexing in progress; please try again later")
	}
	start := time.Now()
	if index.Distributed {
		return index.distributedSearch(ctx, req)
	}
	results, err := index.SearchScoreDocs(ctx, req)
	if err != nil {
		return nil, err
	}
	var data []GenericRecord
	for _, sd := range results.Results {
		rec, ok := index.GetDocument(sd.DocID)
		if ok {
			record, ok := rec.(GenericRecord)
			if ok {
				data = append(data, record)
			}
		}
	}
	pagedData := &Result{
		Items:      data,
		Total:      results.Total,
		Page:       results.Page,
		PerPage:    results.PerPage,
		TotalPages: results.TotalPages,
		NextPage:   results.NextPage,
		PrevPage:   results.PrevPage,
		Latency:    fmt.Sprintf("%s", time.Since(start)),
	}
	return pagedData, nil
}

// Modified distributedSearch function with dial timeouts and shorter overall timeout.
func (index *Index) distributedSearch(ctx context.Context, req Request) (*Result, error) {
	localPage, err := index.SearchScoreDocs(ctx, req)
	if err != nil {
		return nil, err
	}
	allScores := localPage.Results
	var wg sync.WaitGroup
	resultCh := make(chan []ScoredDoc, len(index.Peers))
	peerTimeout := 500 * time.Millisecond // reduced per-peer dial timeout

	for _, peer := range index.Peers {
		wg.Add(1)
		go func(peerAddr string) {
			defer wg.Done()
			// Use DialTimeout instead of rpc.Dial.
			conn, err := net.DialTimeout("tcp", peerAddr, peerTimeout)
			if err != nil {
				return
			}
			client := rpc.NewClient(conn)
			defer client.Close()
			var reply RPCSearchResponse
			rpcReq := &RPCSearchRequest{Req: req}
			if err := client.Call("RPCServer.SearchRPC", rpcReq, &reply); err == nil {
				resultCh <- reply.Page.Results
			}
		}(peer)
	}
	go func() {
		wg.Wait()
		close(resultCh)
	}()
	// Reduce overall wait to 1 second.
	collectTimeout := time.After(1 * time.Second)
	for {
		select {
		case res, ok := <-resultCh:
			if !ok {
				goto DONE
			}
			allScores = append(allScores, res...)
		case <-collectTimeout:
			goto DONE
		}
	}
DONE:
	sort.Slice(allScores, func(i, j int) bool { return allScores[i].Score > allScores[j].Score })
	mergedPage := smartPaginate(allScores, req.Page, req.Size)
	var records []GenericRecord
	for _, sd := range mergedPage.Results {
		if rec, ok := index.GetDocument(sd.DocID); ok {
			if record, ok := rec.(GenericRecord); ok {
				records = append(records, record)
			}
		}
	}
	return &Result{
		Items:      records,
		Total:      mergedPage.Total,
		Page:       mergedPage.Page,
		PerPage:    mergedPage.PerPage,
		TotalPages: mergedPage.TotalPages,
		NextPage:   mergedPage.NextPage,
		PrevPage:   mergedPage.PrevPage,
	}, nil
}

func (index *Index) SearchScoreDocs(ctx context.Context, req Request) (Page, error) {
	if index.indexingInProgress {
		return Page{}, fmt.Errorf("indexing in progress; please try again later")
	}
	req.Match = "AND"
	if utils.ToLower(req.Match) == "any" {
		req.Match = "OR"
	}
	sortField := SortField{Field: req.SortField}
	if utils.ToLower(req.SortOrder) == "desc" {
		sortField.Descending = true
	}
	if sortField.Field == "" && index.defaultSortField != nil {
		sortField = *index.defaultSortField
	}
	if req.Page <= 0 {
		req.Page = 1
	}
	if req.Size <= 0 {
		req.Size = 10
	}
	params := SearchParams{
		Page:       req.Page,
		PerPage:    req.Size,
		SortFields: []SortField{sortField},
	}
	if len(req.Filters) == 0 && req.Query == "" {
		return Page{}, fmt.Errorf("no filters or query provided")
	}
	var query Query
	if len(req.Filters) > 0 {
		var fil []filters.Condition
		for _, f := range req.Filters {
			fil = append(fil, &filters.Filter{
				Field:    f.Field,
				Operator: f.Operator,
				Value:    f.Value,
				Reverse:  f.Reverse,
				Lookup:   f.Lookup,
			})
		}
		query = NewFilterQuery(nil, filters.Boolean(req.Match), req.Reverse, fil...)
	}
	if req.Query != "" {
		var q Query

		// Determine search type based on request parameters
		useFuzzy := req.Fuzzy || req.SearchType == "fuzzy"
		threshold := req.FuzzyThreshold
		if threshold <= 0 {
			threshold = 2 // default threshold
		}

		switch req.SearchType {
		case "phrase":
			q = NewPhraseQuery(req.Query, useFuzzy, threshold)
		case "exact":
			q = NewTermQuery(req.Query, false, 0) // exact search, no fuzzy
		case "fuzzy":
			fallthrough
		default:
			// Default to fuzzy search if not specified
			if strings.Contains(req.Query, " ") {
				q = NewPhraseQuery(req.Query, true, threshold)
			} else {
				q = NewTermQuery(req.Query, true, threshold)
			}
		}

		switch qry := query.(type) {
		case *FilterQuery:
			qry.Term = q
			query = qry
		case FilterQuery:
			qry.Term = q
			query = qry
		case nil:
			query = q
		}
	}
	intKey, err := req.Checksum()
	if err != nil {
		return Page{}, err
	}
	key := fmt.Sprint(intKey)
	queryTokens := query.Tokens()
	page := params.Page
	perPage := params.PerPage
	index.RLock()
	entry, found := index.searchCache[key]
	if found && time.Now().Before(entry.expiry) {
		cached := entry.data
		index.RUnlock()
		if page < 1 {
			page = 1
		}
		if perPage < 1 {
			perPage = 10
		}
		if len(params.SortFields) > 0 {
			index.sortData(cached, params.SortFields)
		} else {
			sort.Slice(cached, func(i, j int) bool {
				return cached[i].Score > cached[j].Score
			})
		}
		return smartPaginate(cached, page, perPage), nil
	}
	if index.indexingInProgress {
		index.RUnlock()
		return Page{}, fmt.Errorf("indexing in progress; please try again later")
	}
	index.RUnlock()
	bm25 := defaultBM25
	if params.BM25Params.K != 0 || params.BM25Params.B != 0 {
		bm25 = params.BM25Params
	}
	docIDs := query.Evaluate(index)
	if len(docIDs) == 0 {
		return smartPaginate(nil, page, perPage), nil
	}
	ch := make(chan int64, len(docIDs))
	for _, id := range docIDs {
		ch <- id
	}
	close(ch)
	scoredPtr := scoredDocPool.Get().(*[]ScoredDoc)
	scored := *scoredPtr
	scored = scored[:0]
	var wg sync.WaitGroup
	var mu sync.Mutex
	for i := 0; i < index.NumWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			localScored := make([]ScoredDoc, 0, 256)
			for docID := range ch {
				select {
				case <-ctx.Done():
					return
				default:
				}
				score := index.bm25Score(queryTokens, docID, bm25.K, bm25.B)
				localScored = append(localScored, ScoredDoc{DocID: docID, Score: score})
			}
			if len(localScored) > 0 {
				mu.Lock()
				scored = append(scored, localScored...)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	*scoredPtr = scored[:0] // reset before putting back
	scoredDocPool.Put(scoredPtr)
	if len(params.SortFields) > 0 {
		index.sortData(scored, params.SortFields)
	} else {
		sort.Slice(scored, func(i, j int) bool {
			return scored[i].Score > scored[j].Score
		})
	}
	if len(scored) > 0 {
		index.Lock()
		index.searchCache[key] = cacheEntry{data: scored, expiry: time.Now().Add(index.cacheExpiry)}
		index.Unlock()
	}
	return smartPaginate(scored, page, perPage), nil
}

func (index *Index) sortData(scored []ScoredDoc, fields []SortField) {
	sort.SliceStable(scored, func(i, j int) bool {
		docI, _ := index.documents.Search(scored[i].DocID)
		docJ, _ := index.documents.Search(scored[j].DocID)
		for _, field := range fields {
			valI, okI := docI[field.Field]
			valJ, okJ := docJ[field.Field]
			if !okI || !okJ {
				continue
			}
			cmp, err := utils.Compare(valI, valJ)
			if cmp == 0 || err != nil {
				continue
			}
			if field.Descending {
				return cmp > 0
			}
			return cmp < 0
		}
		return scored[i].Score > scored[j].Score
	})
}

type SortField struct {
	Field      string
	Descending bool
}

type Page struct {
	Results    []ScoredDoc
	Total      int
	Page       int
	PerPage    int
	TotalPages int
	NextPage   *int
	PrevPage   *int
}

type Result struct {
	Items      []GenericRecord `json:"items"`
	Total      int             `json:"total"`
	Page       int             `json:"page"`
	PerPage    int             `json:"per_page"`
	TotalPages int             `json:"total_pages"`
	NextPage   *int            `json:"next_page"`
	PrevPage   *int            `json:"prev_page"`
	Latency    string          `json:"latency"` // enhancement: add latency for performance tracking
}

func smartPaginate(docs []ScoredDoc, page, perPage int) Page {
	total := len(docs)
	if perPage < 1 {
		perPage = 10
	}
	if total == 0 {
		return Page{
			Results:    []ScoredDoc{},
			Total:      0,
			Page:       1,
			PerPage:    perPage,
			TotalPages: 0,
			NextPage:   nil,
			PrevPage:   nil,
		}
	}
	totalPages := (total + perPage - 1) / perPage
	if page < 1 {
		page = 1
	} else if page > totalPages {
		page = totalPages
	}
	start := (page - 1) * perPage
	end := start + perPage
	if end > total {
		end = total
	}
	var next, prev *int
	if page < totalPages {
		np := page + 1
		next = &np
	}
	if page > 1 {
		pp := page - 1
		prev = &pp
	}
	return Page{
		Results:    docs[start:end],
		Total:      total,
		Page:       page,
		PerPage:    perPage,
		TotalPages: totalPages,
		NextPage:   next,
		PrevPage:   prev,
	}
}

func (index *Index) UpdateDocument(docID int64, rec GenericRecord) error {
	index.Lock()
	index.searchCache = make(map[string]cacheEntry)
	oldRec, ok := index.documents.Search(docID)
	if !ok {
		index.Unlock()
		return fmt.Errorf("document %d does not exist", docID)
	}

	oldFreq := oldRec.getFrequency(index.FieldsToIndex, index.IndexFieldsExcept)
	for term := range oldFreq {
		if postings, exists := index.index[term]; exists {
			newPostings := postings[:0]
			for _, p := range postings {
				if p.DocID != docID {
					newPostings = append(newPostings, p)
				}
			}
			if len(newPostings) == 0 {
				delete(index.index, term)
			} else {
				index.index[term] = newPostings
			}
		}
		index.docLength[docID] -= oldFreq[term]
	}
	index.documents.Insert(docID, rec)
	newFreq := rec.getFrequency(index.FieldsToIndex, index.IndexFieldsExcept)
	docLen := 0
	for term, count := range newFreq {
		docLen += count
		index.index[term] = append(index.index[term], Posting{DocID: docID, Frequency: count})
	}
	index.docLength[docID] = docLen
	index.update()
	index.Unlock()
	return nil
}

func (index *Index) DeleteDocument(docID int64) error {
	index.Lock()
	index.searchCache = make(map[string]cacheEntry)
	rec, ok := index.documents.Search(docID)
	if !ok {
		index.Unlock()
		return fmt.Errorf("document %d does not exist", docID)
	}
	freq := rec.getFrequency(index.FieldsToIndex, index.IndexFieldsExcept)
	for term := range freq {
		if postings, exists := index.index[term]; exists {
			newPostings := postings[:0]
			for _, p := range postings {
				if p.DocID != docID {
					newPostings = append(newPostings, p)
				}
			}
			if len(newPostings) == 0 {
				delete(index.index, term)
			} else {
				index.index[term] = newPostings
			}
		}
	}
	index.documents.Delete(docID)
	delete(index.docLength, docID)
	index.TotalDocs--
	index.update()
	index.Unlock()
	return nil
}

func (index *Index) GetDocument(id int64) (any, bool) {
	return index.documents.Search(id)
}

type QueryFunc func(index *Index) []int

func (f QueryFunc) Evaluate(index *Index) []int {
	return f(index)
}

func (index *Index) startCacheCleanup() {
	go func() {
		ticker := time.NewTicker(index.cacheExpiry)
		defer ticker.Stop()
		for range ticker.C {
			index.Lock()
			now := time.Now()
			for k, entry := range index.searchCache {
				if now.After(entry.expiry) {
					log.Printf("cache entry expired: %s", k)
					delete(index.searchCache, k)
				}
			}
			index.Unlock()
		}
	}()
}

// RPC types for distributed add and search.
type RPCAddRequest struct {
	Record GenericRecord
}

type RPCAddResponse struct{}

type RPCSearchRequest struct {
	Req Request
}

type RPCSearchResponse struct {
	Page Page
}

// RPCServer exposes Index methods over RPC.
type RPCServer struct {
	Index *Index
}

func (s *RPCServer) AddDocumentRPC(args *RPCAddRequest, reply *RPCAddResponse) error {
	s.Index.AddDocument(args.Record)
	return nil
}

func (s *RPCServer) SearchRPC(args *RPCSearchRequest, reply *RPCSearchResponse) error {
	page, err := s.Index.SearchScoreDocs(context.Background(), args.Req)
	if err != nil {
		return err
	}
	reply.Page = page
	return nil
}

// Modified distributedAddDocument with dial timeout.
func (index *Index) distributedAddDocument(rec GenericRecord) {
	req := &RPCAddRequest{Record: rec}
	timeout := 1 * time.Second
	for _, peer := range index.Peers {
		go func(peerAddr string) {
			conn, err := net.DialTimeout("tcp", peerAddr, timeout)
			if err != nil {
				return
			}
			client := rpc.NewClient(conn)
			defer client.Close()
			var reply RPCAddResponse
			_ = client.Call("RPCServer.AddDocumentRPC", req, &reply)
		}(peer)
	}
}

type indexPersistence struct {
	Index     map[string][]Posting `json:"index"`
	DocLength map[int64]int        `json:"doc_length"`
	TotalDocs int                  `json:"total_docs"`
}

// Enhancement: Serialization/deserialization for persistence
func (index *Index) SaveToDisk(path string) error {
	index.RLock()
	defer index.RUnlock()
	data := indexPersistence{
		Index:     index.index,
		DocLength: index.docLength,
		TotalDocs: index.TotalDocs,
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(&data)
}

func (index *Index) LoadFromDisk(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	var data indexPersistence
	dec := json.NewDecoder(f)
	if err := dec.Decode(&data); err != nil {
		return err
	}
	index.Lock()
	index.index = data.Index
	index.docLength = data.DocLength
	index.TotalDocs = data.TotalDocs
	index.Unlock()
	return nil
}

// Enhancement: Status/progress API stub
func (index *Index) Status() map[string]any {
	index.RLock()
	defer index.RUnlock()
	return map[string]any{
		"total_docs":           index.TotalDocs,
		"avg_doc_length":       index.AvgDocLength,
		"indexing_in_progress": index.indexingInProgress,
	}
}
