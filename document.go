package lookup

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"sync"

	"github.com/oarkflow/json"

	"github.com/oarkflow/lookup/utils"
)

// Document represents a normalized record ready for indexing.
type Document interface {
	ID() int64
	Data() GenericRecord
}

// DocumentAdapter normalizes arbitrary inputs (string, map, struct) into Documents.
type DocumentAdapter interface {
	CanHandle(value any) bool
	Adapt(ctx context.Context, value any, cfg AdaptConfig) (Document, error)
}

// Analyzer defines the contract for transforming field values into tokens.
type Analyzer interface {
	Analyze(field string, value any) []Token
}

// Token represents a single analyzed term produced by an Analyzer.
type Token struct {
	Term      string
	Frequency int
}

// IndexWriter abstracts how analyzed documents are persisted into the inverted index.
type IndexWriter interface {
	Write(doc Document, analyzed map[string][]Token) error
}

// DocStore abstracts the persistent storage of raw documents.
type DocStore interface {
	Insert(doc Document) error
	Update(doc Document) error
	Delete(docID int64) error
	Get(docID int64) (GenericRecord, bool)
	ForEach(fn func(id int64, rec GenericRecord) bool)
}

// IDGenerator defines how document identifiers are produced when absent.
type IDGenerator interface {
	Next(source any) int64
}

// IDGeneratorFunc is a helper to turn functions into IDGenerator values.
type IDGeneratorFunc func(source any) int64

// Next implements IDGenerator.
func (f IDGeneratorFunc) Next(source any) int64 {
	return f(source)
}

var (
	errNoAdapter           = errors.New("lookup: no document adapter registered for value")
	defaultIDGenerator     = IDGeneratorFunc(func(any) int64 { return utils.NewID().Int64() })
	defaultAdapterRegistry = newAdapterRegistry()
)

// AdaptConfig carries knobs that influence how adapters interpret inputs.
type AdaptConfig struct {
	DocIDField   string
	DefaultField string
	IDGenerator  IDGenerator
}

func (cfg *AdaptConfig) applyDefaults() {
	if cfg.DefaultField == "" {
		cfg.DefaultField = "content"
	}
	if cfg.IDGenerator == nil {
		cfg.IDGenerator = defaultIDGenerator
	}
}

// AdaptOption mutates AdaptConfig.
type AdaptOption func(*AdaptConfig)

// WithDocIDField instructs adapters to pull the ID from a given field when possible.
func WithDocumentIDField(field string) AdaptOption {
	return func(cfg *AdaptConfig) {
		cfg.DocIDField = field
	}
}

// WithDefaultField sets the fallback field name when normalizing raw strings/bytes.
func WithDefaultField(field string) AdaptOption {
	return func(cfg *AdaptConfig) {
		cfg.DefaultField = field
	}
}

// WithIDGenerator overrides the default ID generator.
func WithIDGenerator(gen IDGenerator) AdaptOption {
	return func(cfg *AdaptConfig) {
		cfg.IDGenerator = gen
	}
}

// AdaptDocument converts any supported value into a Document using registered adapters.
func AdaptDocument(ctx context.Context, value any, opts ...AdaptOption) (Document, error) {
	cfg := &AdaptConfig{}
	for _, opt := range opts {
		opt(cfg)
	}
	cfg.applyDefaults()

	if doc, ok := value.(Document); ok {
		return doc, nil
	}

	if rec, ok := toGenericRecord(value); ok {
		return newDocument(rec, cfg), nil
	}

	adapter, ok := defaultAdapterRegistry.adapterFor(value)
	if !ok {
		return nil, fmt.Errorf("%w: %T", errNoAdapter, value)
	}
	return adapter.Adapt(ctx, value, *cfg)
}

// toGenericRecord attempts to coerce known map types into GenericRecord.
func toGenericRecord(value any) (GenericRecord, bool) {
	switch v := value.(type) {
	case GenericRecord:
		return v, true
	case map[string]any:
		rec := make(GenericRecord, len(v))
		for k, val := range v {
			rec[k] = val
		}
		return rec, true
	default:
		return nil, false
	}
}

// adapterRegistry keeps a prioritized list of adapters.
type adapterRegistry struct {
	mu       sync.RWMutex
	adapters []DocumentAdapter
}

func newAdapterRegistry() *adapterRegistry {
	return &adapterRegistry{adapters: make([]DocumentAdapter, 0, 8)}
}

func (ar *adapterRegistry) register(adapter DocumentAdapter) {
	ar.mu.Lock()
	defer ar.mu.Unlock()
	ar.adapters = append(ar.adapters, adapter)
}

func (ar *adapterRegistry) adapterFor(value any) (DocumentAdapter, bool) {
	ar.mu.RLock()
	defer ar.mu.RUnlock()
	for _, adapter := range ar.adapters {
		if adapter.CanHandle(value) {
			return adapter, true
		}
	}
	return nil, false
}

// RegisterDocumentAdapter adds a new adapter globally.
func RegisterDocumentAdapter(adapter DocumentAdapter) {
	defaultAdapterRegistry.register(adapter)
}

type baseDocument struct {
	id   int64
	data GenericRecord
}

func (d baseDocument) ID() int64 {
	return d.id
}

func (d baseDocument) Data() GenericRecord {
	return d.data
}

func newDocument(rec GenericRecord, cfg *AdaptConfig) Document {
	return baseDocument{
		id:   extractDocID(rec, cfg),
		data: rec,
	}
}

func extractDocID(rec GenericRecord, cfg *AdaptConfig) int64 {
	if cfg.DocIDField != "" {
		if raw, ok := rec[cfg.DocIDField]; ok {
			if id, err := strconv.ParseInt(utils.ToString(raw), 10, 64); err == nil {
				return id
			}
		}
	}
	return cfg.IDGenerator.Next(rec)
}

// ------------------- Default Adapters -------------------

type StringAdapter struct{}

func (StringAdapter) CanHandle(value any) bool {
	switch value.(type) {
	case string, []byte:
		return true
	default:
		return false
	}
}

func (StringAdapter) Adapt(_ context.Context, value any, cfg AdaptConfig) (Document, error) {
	cfg.applyDefaults()
	var text string
	switch v := value.(type) {
	case string:
		text = v
	case []byte:
		text = string(v)
	default:
		return nil, fmt.Errorf("string adapter cannot handle %T", value)
	}
	rec := GenericRecord{cfg.DefaultField: text}
	return newDocument(rec, &cfg), nil
}

type MapAdapter struct{}

func (MapAdapter) CanHandle(value any) bool {
	if value == nil {
		return false
	}
	t := reflect.TypeOf(value)
	if t.Kind() != reflect.Map {
		return false
	}
	return t.Key().Kind() == reflect.String
}

func (MapAdapter) Adapt(_ context.Context, value any, cfg AdaptConfig) (Document, error) {
	cfg.applyDefaults()
	rv := reflect.ValueOf(value)
	if rv.Kind() != reflect.Map {
		return nil, fmt.Errorf("map adapter requires map, got %T", value)
	}
	rec := make(GenericRecord, rv.Len())
	iter := rv.MapRange()
	for iter.Next() {
		rec[iter.Key().String()] = iter.Value().Interface()
	}
	return newDocument(rec, &cfg), nil
}

type StructAdapter struct{}

func (StructAdapter) CanHandle(value any) bool {
	if value == nil {
		return false
	}
	t := reflect.TypeOf(value)
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return t.Kind() == reflect.Struct
}

func (StructAdapter) Adapt(_ context.Context, value any, cfg AdaptConfig) (Document, error) {
	cfg.applyDefaults()
	data, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("struct adapter marshal error: %w", err)
	}
	var rec GenericRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		return nil, fmt.Errorf("struct adapter unmarshal error: %w", err)
	}
	return newDocument(rec, &cfg), nil
}

func init() {
	RegisterDocumentAdapter(StringAdapter{})
	RegisterDocumentAdapter(MapAdapter{})
	RegisterDocumentAdapter(StructAdapter{})
}
