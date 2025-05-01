package lookup

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"math"
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

	"github.com/oarkflow/lookup/trie"
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
	tokens := utils.Tokenize(combined)
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
	tokenTrie          *trie.Trie // replaced index map with trie
	docLength          map[int64]int
	documents          *BPTree[int64, GenericRecord]
	order              int
	storage            string
	indexingInProgress bool
	searchCache        map[string]cacheEntry
	cacheExpiry        time.Duration
	reset              bool
}

type IndexRequest struct {
	Path string          `json:"path"`
	Data []GenericRecord `json:"data"`
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

func NewIndex(id string, opts ...Options) *Index {
	os.MkdirAll(DefaultPath, 0755)
	storagePath := filepath.Join(DefaultPath, "storage-"+id+".dat")
	index := &Index{
		ID:             id,
		NumWorkers:     runtime.NumCPU(),
		tokenTrie:      trie.NewTrie(), // use trie for token postings
		docLength:      make(map[int64]int),
		order:          3,
		storage:        storagePath,
		MemoryCapacity: 1000,
		searchCache:    make(map[string]cacheEntry),
		cacheExpiry:    time.Minute,
	}
	for _, opt := range opts {
		opt(index)
	}
	if index.reset {
		os.Remove(storagePath)
	}
	index.documents = NewBPTree[int64, GenericRecord](index.order, index.storage, index.MemoryCapacity)
	index.startCacheCleanup()
	return index
}

func (index *Index) FuzzySearch(term string, threshold int) []string {
	index.RLock()
	defer index.RUnlock()
	var results []string
	index.tokenTrie.Traverse(func(key string, value any) {
		if utils.BoundedLevenshtein(term, key, threshold) <= threshold {
			results = append(results, key)
		}
	})
	return results
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
	// merge postings from partial.inverted into tokenTrie
	for term, postings := range partial.inverted {
		var existing []Posting
		if val, ok := index.tokenTrie.Get(term); ok {
			existing = val.([]Posting)
		}
		existing = append(existing, postings...)
		index.tokenTrie.Insert(term, existing)
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
		if val, ok := index.tokenTrie.Get(token); ok {
			postings := val.([]Posting)
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
		if v.Path != "" {
			return index.BuildFromFile(ctx, v.Path, callbacks...)
		}
		if len(v.Data) > 0 {
			return index.BuildFromRecords(ctx, v.Data, callbacks...)
		}
		return fmt.Errorf("no data or path provided")
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
		val, ok := index.tokenTrie.Get(t)
		if ok {
			postings, can := val.([]Posting)
			if can {
				index.tokenTrie.Insert(t, append(postings, Posting{DocID: docID, Frequency: count}))
				docLen += count
			}
		}

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
	log.Println(fmt.Sprintf("Indexing completed for %s", index.ID))
}

func (index *Index) bm25Score(queryTokens []string, docID int64, k1, b float64) float64 {
	index.RLock()
	defer index.RUnlock()
	score := 0.0
	docLength := float64(index.docLength[docID])
	for _, term := range queryTokens {
		val, ok := index.tokenTrie.Get(term)
		if !ok {
			continue
		}
		postings := val.([]Posting)
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
	}
	return pagedData, nil
}

func (index *Index) SearchScoreDocs(ctx context.Context, req Request) (Page, error) {
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
		if strings.Contains(req.Query, " ") {
			q = NewPhraseQuery(req.Query, !req.Exact, 1)
		} else {
			q = NewTermQuery(req.Query, !req.Exact, 1)
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
	var (
		scored []ScoredDoc
		wg     sync.WaitGroup
		mu     sync.Mutex
	)
	docIDs := query.Evaluate(index)
	ch := make(chan int64, len(docIDs))
	for _, id := range docIDs {
		ch <- id
	}
	close(ch)
	for i := 0; i < index.NumWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for docID := range ch {
				select {
				case <-ctx.Done():
					return
				default:
				}
				score := index.bm25Score(queryTokens, docID, bm25.K, bm25.B)
				mu.Lock()
				scored = append(scored, ScoredDoc{DocID: docID, Score: score})
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
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
			cmp := utils.Compare(valI, valJ)
			if cmp == 0 {
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

func (index *Index) AddDocument(rec GenericRecord) {
	index.Lock()
	index.searchCache = make(map[string]cacheEntry)
	docID := utils.NewID().Int64()
	freq := rec.getFrequency(index.FieldsToIndex, index.IndexFieldsExcept)
	// insert each token posting into trie
	for term, count := range freq {
		var existing []Posting
		if val, ok := index.tokenTrie.Get(term); ok {
			existing = val.([]Posting)
		}
		existing = append(existing, Posting{DocID: docID, Frequency: count})
		index.tokenTrie.Insert(term, existing)
	}
	index.indexDoc(docID, rec, freq)
	index.TotalDocs++
	index.update()
	index.Unlock()
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
	// remove old postings from trie
	for term, oldCount := range oldFreq {
		if val, ok := index.tokenTrie.Get(term); ok {
			postings := val.([]Posting)
			newPostings := postings[:0]
			for _, p := range postings {
				if p.DocID != docID {
					newPostings = append(newPostings, p)
				}
			}
			index.tokenTrie.Insert(term, newPostings)
			index.docLength[docID] -= oldCount
		}
	}
	index.documents.Insert(docID, rec)
	newFreq := rec.getFrequency(index.FieldsToIndex, index.IndexFieldsExcept)
	docLen := 0
	for term, count := range newFreq {
		docLen += count
		var existing []Posting
		if val, ok := index.tokenTrie.Get(term); ok {
			existing = val.([]Posting)
		}
		existing = append(existing, Posting{DocID: docID, Frequency: count})
		index.tokenTrie.Insert(term, existing)
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
		if val, ok := index.tokenTrie.Get(term); ok {
			postings := val.([]Posting)
			newPostings := postings[:0]
			for _, p := range postings {
				if p.DocID != docID {
					newPostings = append(newPostings, p)
				}
			}
			index.tokenTrie.Insert(term, newPostings)
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
