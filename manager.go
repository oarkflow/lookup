package lookup

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"

	"github.com/oarkflow/filters"
	"github.com/oarkflow/json"

	"github.com/oarkflow/lookup/utils"
)

type Manager struct {
	indexes map[string]*Index
	mutex   sync.Mutex
}

func NewManager() *Manager {
	return &Manager{
		indexes: make(map[string]*Index),
	}
}

func (m *Manager) AddIndex(name string, index *Index) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.indexes[name] = index
}

func (m *Manager) GetIndex(name string) (*Index, bool) {
	m.mutex.Lock() // Replace with RLock if mutex becomes RWMutex; otherwise, minimal change
	defer m.mutex.Unlock()
	index, ok := m.indexes[name]
	return index, ok
}

func (m *Manager) DeleteIndex(name string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	delete(m.indexes, name)
}

func (m *Manager) ListIndexes() []string {
	m.mutex.Lock() // Replace with RLock if mutex becomes RWMutex
	defer m.mutex.Unlock()
	names := make([]string, 0, len(m.indexes))
	for name := range m.indexes {
		names = append(names, name)
	}
	return names
}

func (m *Manager) Build(ctx context.Context, name string, req any) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	index, ok := m.indexes[name]
	if !ok {
		return fmt.Errorf("index %s not found", name)
	}
	return index.Build(ctx, req)
}

func (m *Manager) Search(ctx context.Context, name string, req Request) (*Result, error) {
	m.mutex.Lock()
	index, ok := m.indexes[name]
	m.mutex.Unlock()
	if !ok {
		fmt.Printf("index %s not found\n", name)
		return nil, fmt.Errorf("index %s not found", name)
	}

	return index.Search(ctx, req)
}

type NewIndexRequest struct {
	ID string `json:"id"`
}

type Filter struct {
	Field    string           `json:"field"`
	Operator filters.Operator `json:"operator"`
	Value    any              `json:"value"`
	Reverse  bool             `json:"reverse"`
	Lookup   *filters.Lookup  `json:"lookup"`
}

type Request struct {
	Filters   []Filter      `json:"filters"`
	Rule      *filters.Rule `json:"rule"`
	Query     string        `json:"q" query:"q"`
	Condition string        `json:"condition"`
	Match     string        `json:"m" query:"m"`
	Offset    int           `json:"o" query:"o"`
	Size      int           `json:"s" query:"s"`
	SortField string        `json:"sort_field" query:"sort_field"`
	SortOrder string        `json:"sort_order" query:"sort_order"`
	Page      int           `json:"p" query:"p"`
	Reverse   bool          `json:"reverse" query:"reverse"`
	Exact     bool          `json:"exact" query:"exact"`
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
		Filters   []string `json:"filters"`
		Query     string   `json:"q"`
		Condition string   `json:"condition"`
		Match     string   `json:"m"`
		Offset    int      `json:"o"`
		Size      int      `json:"s"`
		SortField string   `json:"sort_field"`
		SortOrder string   `json:"sort_order"`
		Page      int      `json:"p"`
		Reverse   bool     `json:"reverse"`
		Exact     bool     `json:"exact"`
	}{
		Filters:   condStrs,
		Query:     tmp.Query,
		Condition: tmp.Condition,
		Match:     tmp.Match,
		Offset:    tmp.Offset,
		Size:      tmp.Size,
		SortField: tmp.SortField,
		SortOrder: tmp.SortOrder,
		Page:      tmp.Page,
		Reverse:   tmp.Reverse,
		Exact:     tmp.Exact,
	}
	payload, err := json.Marshal(canon)
	if err != nil {
		return 0, fmt.Errorf("marshaling canonical request: %w", err)
	}
	return xxhash.Sum64(payload), nil
}

var builtInFields = []string{"q", "m", "l", "f", "t", "o", "s", "exact", "p", "condition", "sort_field", "sort_order"}

func prepareQuery(r *http.Request) (Request, error) {
	var query Request
	extraMap := make(map[string]any)
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		return query, err
	}
	r.Body = io.NopCloser(bytes.NewReader(bodyBytes))
	if bodyBytes != nil && len(bodyBytes) > 0 {
		err = json.Unmarshal(bodyBytes, &query)
		if err != nil {
			return query, fmt.Errorf("error unmarshalling query: %v", err)
		}
		err = json.Unmarshal(bodyBytes, &extraMap)
		if err != nil {
			return query, fmt.Errorf("error unmarshalling extra: %v", err)
		}
	}
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	if q != "" {
		query.Query = q
	}
	var extra []Filter
	for k, v := range extraMap {
		if slices.Contains(builtInFields, k) {
			continue
		}
		vt := reflect.TypeOf(v).Kind()
		operator := filters.Equal
		if vt == reflect.Slice {
			operator = filters.In
		}
		extra = append(extra, Filter{
			Field:    k,
			Operator: operator,
			Value:    v,
		})
	}
	if len(extra) == 0 {
		rawQuery := r.URL.RawQuery
		extraFilters, err := filters.ParseQuery(rawQuery, builtInFields...)
		if err != nil {
			return query, err
		}
		for _, v := range extraFilters {
			extra = append(extra, Filter{
				Field:    v.Field,
				Operator: v.Operator,
				Value:    v.Value,
				Reverse:  v.Reverse,
				Lookup:   v.Lookup,
			})
		}
	}
	if extra != nil && query.Filters == nil {
		query.Filters = extra
	}
	condition := strings.TrimSpace(utils.ToLower(query.Condition))
	if condition != "" {
		rule, err := filters.ParseSQL(condition)
		if err != nil {
			return query, fmt.Errorf("error parsing condition: %v", err)
		}
		if rule != nil {
			query.Rule = rule
		}
	}
	return query, nil
}

func (m *Manager) StartHTTP(addr string) {
	http.HandleFunc("/index/add", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Unsupported method", http.StatusMethodNotAllowed)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, fmt.Sprintf("Error reading body: %v", err), http.StatusBadRequest)
			return
		}
		var req NewIndexRequest
		if err := json.Unmarshal(body, &req); err != nil {
			http.Error(w, fmt.Sprintf("Error unmarshalling request: %v", err), http.StatusBadRequest)
			return
		}
		if strings.TrimSpace(req.ID) == "" {
			http.Error(w, "index ID required in request body", http.StatusBadRequest)
			return
		}
		index := NewIndex(req.ID)
		m.AddIndex(req.ID, index)
		w.Write([]byte(fmt.Sprintf("index %s created successfully", req.ID)))
	})
	http.HandleFunc("/indexes", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Unsupported method", http.StatusMethodNotAllowed)
			return
		}
		indexes := m.ListIndexes()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(indexes)
	})
	http.HandleFunc("/{index}/build", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Unsupported method", http.StatusMethodNotAllowed)
			return
		}
		indexName := r.PathValue("index")
		if strings.TrimSpace(indexName) == "" {
			http.Error(w, "index name required in path", http.StatusBadRequest)
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, fmt.Sprintf("Error reading body: %v", err), http.StatusBadRequest)
			return
		}
		var req IndexRequest
		if err := json.Unmarshal(body, &req); err != nil {
			http.Error(w, fmt.Sprintf("Error unmarshalling request: %v", err), http.StatusBadRequest)
			return
		}
		if req.Path != "" {
			go func(indexName string, req IndexRequest) {
				err = m.Build(context.Background(), indexName, req)
				if err != nil {
					http.Error(w, fmt.Sprintf("Build error: %v", err), http.StatusInternalServerError)
					return
				}
			}(indexName, req)
			w.Write([]byte(fmt.Sprintf("Indexing started for %s with index name %s", req.Path, indexName)))
			return
		}
		err = m.Build(ctx, indexName, req)
		if err != nil {
			http.Error(w, fmt.Sprintf("Build error: %v", err), http.StatusInternalServerError)
			return
		}
		w.Write([]byte("index built successfully"))
	})
	http.HandleFunc("/{index}/search", func(w http.ResponseWriter, r *http.Request) {
		indexName := r.PathValue("index")
		if strings.TrimSpace(indexName) == "" {
			http.Error(w, "index name required in path", http.StatusBadRequest)
			return
		}
		req, err := prepareQuery(r)
		if err != nil {
			http.Error(w, fmt.Sprintf("Error preparing query: %v", err), http.StatusBadRequest)
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		results, err := m.Search(ctx, indexName, req)
		if err != nil {
			http.Error(w, fmt.Sprintf("Search error: %v", err), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(results)
	})

	log.Printf("HTTP server listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}
