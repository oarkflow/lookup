package lookup

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"
)

type Ordered interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~float32 | ~float64 | ~string
}

type Record any

type BPlusTree[K Ordered, V Record] struct {
	order int
	root  *bTreeNode[K, V]
	mu    sync.RWMutex
}

type bTreeNode[K Ordered, V Record] struct {
	mu       sync.RWMutex
	keys     []K
	children []*bTreeNode[K, V]
	values   []V
	leaf     bool
	next     *bTreeNode[K, V]
}

func newNode[K Ordered, V Record](order int, leaf bool) *bTreeNode[K, V] {
	node := &bTreeNode[K, V]{leaf: leaf, next: nil}
	if leaf {
		node.keys = make([]K, 0, order-1)
		node.values = make([]V, 0, order-1)
	} else {
		node.keys = make([]K, 0, order-1)
		node.children = make([]*bTreeNode[K, V], 0, order)
	}
	return node
}

func NewBPlusTree[K Ordered, V Record](order int) *BPlusTree[K, V] {
	if order < 3 {
		panic("B+ tree order must be at least 3")
	}
	return &BPlusTree[K, V]{
		order: order,
		root:  newNode[K, V](order, true),
	}
}

func (tree *BPlusTree[K, V]) Insert(key K, value V) {
	tree.mu.Lock()
	defer tree.mu.Unlock()
	root := tree.root
	if len(root.keys) == tree.order {
		newRoot := newNode[K, V](tree.order, false)
		newRoot.children = append(newRoot.children, root)
		tree.splitChild(newRoot, 0, root)
		tree.root = newRoot
	}
	tree.insertNonFull(tree.root, key, value)
}

func (tree *BPlusTree[K, V]) insertNonFull(node *bTreeNode[K, V], key K, value V) {
	if node.leaf {
		idx := sort.Search(len(node.keys), func(i int) bool { return node.keys[i] >= key })
		if idx < len(node.keys) && node.keys[idx] == key {
			node.values[idx] = value
			return
		}
		node.keys = append(node.keys, key)
		node.values = append(node.values, value)
		copy(node.keys[idx+1:], node.keys[idx:])
		copy(node.values[idx+1:], node.values[idx:])
		node.keys[idx] = key
		node.values[idx] = value
		return
	}
	idx := sort.Search(len(node.keys), func(i int) bool { return key < node.keys[i] })
	child := node.children[idx]
	if len(child.keys) == tree.order {
		tree.splitChild(node, idx, child)
		if key >= node.keys[idx] {
			idx++
		}
	}
	tree.insertNonFull(node.children[idx], key, value)
}

func (tree *BPlusTree[K, V]) splitChild(parent *bTreeNode[K, V], idx int, child *bTreeNode[K, V]) {
	mid := tree.order / 2
	if child.leaf {
		newLeaf := newNode[K, V](tree.order, true)
		newLeaf.keys = append(newLeaf.keys, child.keys[mid:]...)
		newLeaf.values = append(newLeaf.values, child.values[mid:]...)
		child.keys = child.keys[:mid]
		child.values = child.values[:mid]
		newLeaf.next = child.next
		child.next = newLeaf
		parent.keys = append(parent.keys, newLeaf.keys[0])
		parent.children = append(parent.children, newLeaf)
		sort.Slice(parent.keys, func(i, j int) bool { return parent.keys[i] < parent.keys[j] })
	} else {
		newInternal := newNode[K, V](tree.order, false)
		promoteKey := child.keys[mid]
		newInternal.keys = append(newInternal.keys, child.keys[mid+1:]...)
		newInternal.children = append(newInternal.children, child.children[mid+1:]...)
		child.keys = child.keys[:mid]
		child.children = child.children[:mid+1]
		parent.keys = append(parent.keys, promoteKey)
		parent.children = append(parent.children, newInternal)
		sort.Slice(parent.keys, func(i, j int) bool { return parent.keys[i] < parent.keys[j] })
	}
}

func (tree *BPlusTree[K, V]) Search(key K) (V, bool) {
	tree.mu.RLock()
	defer tree.mu.RUnlock()
	node := tree.findLeaf(tree.root, key)
	idx := sort.Search(len(node.keys), func(i int) bool { return node.keys[i] >= key })
	if idx < len(node.keys) && node.keys[idx] == key {
		return node.values[idx], true
	}
	var zero V
	return zero, false
}

func (tree *BPlusTree[K, V]) findLeaf(node *bTreeNode[K, V], key K) *bTreeNode[K, V] {
	if node.leaf {
		return node
	}
	idx := sort.Search(len(node.keys), func(i int) bool { return key < node.keys[i] })
	return tree.findLeaf(node.children[idx], key)
}

func (tree *BPlusTree[K, V]) InOrderTraversal() []KeyValuePair[K, V] {
	tree.mu.RLock()
	defer tree.mu.RUnlock()
	var result []KeyValuePair[K, V]
	node := tree.root
	for !node.leaf {
		node = node.children[0]
	}
	for node != nil {
		for i, key := range node.keys {
			result = append(result, KeyValuePair[K, V]{key, node.values[i]})
		}
		node = node.next
	}
	return result
}

func (tree *BPlusTree[K, V]) Delete(key K) bool {
	tree.mu.Lock()
	defer tree.mu.Unlock()
	deleted := tree.deleteInternal(tree.root, key)
	if !tree.root.leaf && len(tree.root.children) == 1 {
		tree.root = tree.root.children[0]
	}
	return deleted
}

func (tree *BPlusTree[K, V]) deleteInternal(node *bTreeNode[K, V], key K) bool {
	if node.leaf {
		idx := sort.Search(len(node.keys), func(i int) bool { return node.keys[i] >= key })
		if idx < len(node.keys) && node.keys[idx] == key {
			node.keys = append(node.keys[:idx], node.keys[idx+1:]...)
			node.values = append(node.values[:idx], node.values[idx+1:]...)
			return true
		}
		return false
	}
	idx := sort.Search(len(node.keys), func(i int) bool { return key < node.keys[i] })
	deleted := tree.deleteInternal(node.children[idx], key)
	if !deleted {
		return false
	}
	minKeys := (tree.order - 1) / 2
	if len(node.children[idx].keys) < minKeys {
		if idx > 0 && len(node.children[idx-1].keys) > minKeys {
			child := node.children[idx]
			left := node.children[idx-1]
			child.keys = append([]K{node.keys[idx-1]}, child.keys...)
			child.values = append([]V{left.values[len(left.values)-1]}, child.values...)
			node.keys[idx-1] = left.keys[len(left.keys)-1]
			left.keys = left.keys[:len(left.keys)-1]
			left.values = left.values[:len(left.values)-1]
		} else if idx < len(node.children)-1 && len(node.children[idx+1].keys) > minKeys {
			child := node.children[idx]
			right := node.children[idx+1]
			child.keys = append(child.keys, node.keys[idx])
			child.values = append(child.values, right.values[0])
			node.keys[idx] = right.keys[0]
			right.keys = right.keys[1:]
			right.values = right.values[1:]
		} else {
			if idx > 0 {
				left := node.children[idx-1]
				child := node.children[idx]
				left.keys = append(left.keys, node.keys[idx-1])
				left.keys = append(left.keys, child.keys...)
				left.values = append(left.values, child.values...)
				node.keys = append(node.keys[:idx-1], node.keys[idx:]...)
				node.children = append(node.children[:idx], node.children[idx+1:]...)
			} else {
				child := node.children[idx]
				right := node.children[idx+1]
				child.keys = append(child.keys, node.keys[idx])
				child.keys = append(child.keys, right.keys...)
				child.values = append(child.values, right.values...)
				node.keys = append(node.keys[:idx], node.keys[idx+1:]...)
				node.children = append(node.children[:idx+1], node.children[idx+2:]...)
			}
		}
	}
	return true
}

func (tree *BPlusTree[K, V]) LowerBound(key K) []KeyValuePair[K, V] {
	tree.mu.RLock()
	defer tree.mu.RUnlock()
	var result []KeyValuePair[K, V]
	node := tree.root
	for !node.leaf {
		idx := sort.Search(len(node.keys), func(i int) bool { return key < node.keys[i] })
		node = node.children[idx]
	}
	idx := sort.Search(len(node.keys), func(i int) bool { return node.keys[i] >= key })
	for ; node != nil; node = node.next {
		for i := idx; i < len(node.keys); i++ {
			result = append(result, KeyValuePair[K, V]{node.keys[i], node.values[i]})
		}
		idx = 0
	}
	return result
}

// collectLeaves recursively collects all leaf nodes from the tree in sorted order.
func collectLeaves[K Ordered, V Record](node *bTreeNode[K, V]) []*bTreeNode[K, V] {
	if node.leaf {
		return []*bTreeNode[K, V]{node}
	}
	var leaves []*bTreeNode[K, V]
	for _, child := range node.children {
		leaves = append(leaves, collectLeaves(child)...)
	}
	return leaves
}

func BulkLoad[K Ordered, V Record](order int, pairs []KeyValuePair[K, V]) *BPlusTree[K, V] {
	tree := &BPlusTree[K, V]{order: order}
	if len(pairs) == 0 {
		tree.root = newNode[K, V](order, true)
		return tree
	}
	leafCapacity := order - 1
	numLeaves := (len(pairs) + leafCapacity - 1) / leafCapacity
	leaves := make([]*bTreeNode[K, V], numLeaves)
	var wg sync.WaitGroup
	for i := 0; i < numLeaves; i++ {
		start := i * leafCapacity
		end := start + leafCapacity
		if end > len(pairs) {
			end = len(pairs)
		}
		wg.Add(1)
		go func(i, start, end int) {
			defer wg.Done()
			leaf := newNode[K, V](order, true)
			for j := start; j < end; j++ {
				leaf.keys = append(leaf.keys, pairs[j].Key)
				leaf.values = append(leaf.values, pairs[j].Value)
			}
			leaves[i] = leaf
		}(i, start, end)
	}
	wg.Wait()
	// Link the initially created leaves (this chain may later be broken when grouping into parents)
	for i := 0; i < len(leaves)-1; i++ {
		leaves[i].next = leaves[i+1]
	}
	nodes := leaves
	for len(nodes) > 1 {
		var (
			wg      sync.WaitGroup
			mu      sync.Mutex
			parents []*bTreeNode[K, V]
		)
		chunkSize := order
		for i := 0; i < len(nodes); i += chunkSize {
			end := i + chunkSize
			if end > len(nodes) {
				end = len(nodes)
			}
			chunk := nodes[i:end]
			wg.Add(1)
			go func(chunk []*bTreeNode[K, V]) {
				defer wg.Done()
				parent := newNode[K, V](order, false)
				// For non-leaf nodes, use the first key of each child (except the first)
				if len(chunk) > 1 {
					for _, child := range chunk[1:] {
						if len(child.keys) > 0 {
							parent.keys = append(parent.keys, child.keys[0])
						}
					}
				}
				parent.children = chunk
				mu.Lock()
				parents = append(parents, parent)
				mu.Unlock()
			}(chunk)
		}
		wg.Wait()
		nodes = parents
	}
	tree.root = nodes[0]

	// **New step: Re-link all leaves in the final tree**
	allLeaves := collectLeaves(tree.root)
	for i := 0; i < len(allLeaves)-1; i++ {
		allLeaves[i].next = allLeaves[i+1]
	}
	// Optionally, you can set the next pointer of the final leaf to nil.
	if len(allLeaves) > 0 {
		allLeaves[len(allLeaves)-1].next = nil
	}

	return tree
}

type KeyValuePair[K Ordered, V Record] struct {
	Key   K
	Value V
}

type BloomFilter struct {
	counts []uint8
	m      uint
	k      uint
}

func NewBloomFilter(m, k uint) *BloomFilter {
	return &BloomFilter{
		counts: make([]uint8, m),
		m:      m,
		k:      k,
	}
}

func (bf *BloomFilter) Add(data []byte) {
	h1, h2 := hashDouble(data)
	for i := uint(0); i < bf.k; i++ {
		idx := (h1 + i*h2) % bf.m
		bf.counts[idx]++
	}
}

func (bf *BloomFilter) Remove(data []byte) {
	h1, h2 := hashDouble(data)
	for i := uint(0); i < bf.k; i++ {
		idx := (h1 + i*h2) % bf.m
		if bf.counts[idx] > 0 {
			bf.counts[idx]--
		}
	}
}

func (bf *BloomFilter) Test(data []byte) bool {
	h1, h2 := hashDouble(data)
	for i := uint(0); i < bf.k; i++ {
		idx := (h1 + i*h2) % bf.m
		if bf.counts[idx] == 0 {
			return false
		}
	}
	return true
}

func hashDouble(data []byte) (uint, uint) {
	h1 := fnv.New64a()
	h1.Write(data)
	h2 := fnv.New64()
	h2.Write(data)
	return uint(h1.Sum64()), uint(h2.Sum64())
}

type BKTree struct {
	term     string
	children map[int]*BKTree
}

func NewBKTree(term string) *BKTree {
	return &BKTree{
		term:     term,
		children: make(map[int]*BKTree),
	}
}

func (tree *BKTree) Insert(term string) {
	current := tree
	for {
		d := levenshtein(current.term, term)
		if d == 0 {
			return
		}
		if child, ok := current.children[d]; ok {
			current = child
		} else {
			current.children[d] = NewBKTree(term)
			break
		}
	}
}

func (tree *BKTree) Search(term string, threshold int) []string {
	var results []string
	queue := []*BKTree{tree}
	term = strings.ToLower(term)
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		d := levenshtein(node.term, term)
		if d <= threshold {
			results = append(results, node.term)
		}
		minDist := d - threshold
		maxDist := d + threshold
		for dist, child := range node.children {
			if dist >= minDist && dist <= maxDist {
				queue = append(queue, child)
			}
		}
	}
	return results
}

func (tree *BKTree) Delete(term string) bool {
	term = strings.ToLower(term)
	if tree.term == term {
		if len(tree.children) == 0 {
			return false
		}
		for d, child := range tree.children {
			tree.term = child.term
			for cd, gc := range child.children {
				tree.children[cd] = gc
			}
			delete(tree.children, d)
			return true
		}
	} else {
		d := levenshtein(tree.term, term)
		if child, ok := tree.children[d]; ok {
			deleted := child.Delete(term)
			if deleted && len(child.children) == 0 && child.term == term {
				delete(tree.children, d)
			}
			return deleted
		}
	}
	return false
}

func levenshtein(a, b string) int {
	if a == b {
		return 0
	}
	al, bl := len(a), len(b)
	if al == 0 {
		return bl
	}
	if bl == 0 {
		return al
	}
	v0 := make([]int, bl+1)
	v1 := make([]int, bl+1)
	for i := 0; i <= bl; i++ {
		v0[i] = i
	}
	for i := 0; i < al; i++ {
		v1[0] = i + 1
		for j := 0; j < bl; j++ {
			cost := 0
			if a[i] != b[j] {
				cost = 1
			}
			v1[j+1] = min(v0[j]+cost, min(v0[j+1]+1, v1[j]+1))
		}
		v0, v1 = v1, v0
	}
	return v0[bl]
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

var structFieldsCache sync.Map

func tokenize(text string) []string {
	return strings.FieldsFunc(strings.ToLower(text), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsNumber(r)
	})
}

func extractTokens(record any) []string {
	var tokens []string
	switch rec := record.(type) {
	case string:
		tokens = tokenize(rec)
	case map[string]string:
		tokens = make([]string, 0, len(rec)*2)
		for k, v := range rec {
			tokens = append(tokens, tokenize(k)...)
			tokens = append(tokens, tokenize(v)...)
		}
	case map[string]any:
		tokens = make([]string, 0, len(rec)*2)
		for k, v := range rec {
			tokens = append(tokens, tokenize(k)...)
			tokens = append(tokens, tokenize(fmt.Sprintf("%v", v))...)
		}
	default:
		rv := reflect.ValueOf(record)
		switch rv.Kind() {
		case reflect.Struct:
			typ := rv.Type()
			var indices []int
			if cached, ok := structFieldsCache.Load(typ); ok {
				indices = cached.([]int)
			} else {
				indices = make([]int, rv.NumField())
				for i := 0; i < rv.NumField(); i++ {
					indices[i] = i
				}
				structFieldsCache.Store(typ, indices)
			}
			tokens = make([]string, 0, len(indices))
			for _, i := range indices {
				tokens = append(tokens, tokenize(fmt.Sprintf("%v", rv.Field(i).Interface()))...)
			}
		case reflect.Slice, reflect.Array:
			length := rv.Len()
			tokens = make([]string, 0, length)
			for i := 0; i < length; i++ {
				tokens = append(tokens, tokenize(fmt.Sprintf("%v", rv.Index(i).Interface()))...)
			}
		default:
			tokens = tokenize(fmt.Sprintf("%v", record))
		}
	}
	return tokens
}

type LookupEngine[K Ordered, V Record] struct {
	tree            *BPlusTree[K, V]
	bf              *BloomFilter
	invertedIndex   map[string][]K
	bkTree          *BKTree
	tokens          map[string]struct{}
	mu              sync.RWMutex
	walPath         string
	walMu           sync.Mutex
	walFile         *os.File
	ttl             map[K]time.Time
	metrics         map[string]int
	lastAccess      map[K]time.Time
	persistencePath string
}

func NewLookupEngine[K Ordered, V Record](order int, bfSize, bfHashes uint) *LookupEngine[K, V] {
	return &LookupEngine[K, V]{
		tree:          NewBPlusTree[K, V](order),
		bf:            NewBloomFilter(bfSize, bfHashes),
		invertedIndex: make(map[string][]K),
		tokens:        make(map[string]struct{}),
		lastAccess:    make(map[K]time.Time),
	}
}

func (le *LookupEngine[K, V]) SetWAL(path string) error {
	le.walMu.Lock()
	defer le.walMu.Unlock()
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	le.walPath = path
	le.walFile = file
	return nil
}

type walRecord[K Ordered, V Record] struct {
	Op        string    `json:"op"`
	Timestamp time.Time `json:"ts"`
	Key       K         `json:"key"`
	Value     V         `json:"value,omitempty"`
	TTL       int64     `json:"ttl,omitempty"`
}

func (le *LookupEngine[K, V]) appendWAL(rec walRecord[K, V]) {
	le.walMu.Lock()
	defer le.walMu.Unlock()
	if le.walFile == nil {
		return
	}
	data, err := json.Marshal(rec)
	if err != nil {
		return
	}
	le.walFile.Write(append(data, '\n'))
}

func (le *LookupEngine[K, V]) UpsertWithTTL(key K, value V, ttlSeconds int) {
	le.mu.Lock()
	defer le.mu.Unlock()
	if le.ttl == nil {
		le.ttl = make(map[K]time.Time)
	}
	le.ttl[key] = time.Now().Add(time.Duration(ttlSeconds) * time.Second)
	le.tree.Insert(key, value)
	le.lastAccess[key] = time.Now()
	keyBytes := []byte(fmt.Sprintf("%v", key))
	le.bf.Add(keyBytes)
	tokens := extractTokens(value)
	for _, token := range tokens {
		le.invertedIndex[token] = append(le.invertedIndex[token], key)
		if _, exists := le.tokens[token]; !exists {
			if le.bkTree == nil {
				le.bkTree = NewBKTree(token)
			} else {
				le.bkTree.Insert(token)
			}
			le.tokens[token] = struct{}{}
		}
	}
	if le.metrics == nil {
		le.metrics = make(map[string]int)
	}
	le.metrics["upserts"]++
	rec := walRecord[K, V]{
		Op:        "upsert",
		Timestamp: time.Now(),
		Key:       key,
		Value:     value,
		TTL:       int64(ttlSeconds),
	}
	le.appendWAL(rec)
}

func (le *LookupEngine[K, V]) Insert(key K, value V) {
	le.mu.Lock()
	defer le.mu.Unlock()
	le.tree.Insert(key, value)
	le.lastAccess[key] = time.Now()
	keyBytes := []byte(fmt.Sprintf("%v", key))
	le.bf.Add(keyBytes)
	tokens := extractTokens(value)
	for _, token := range tokens {
		le.invertedIndex[token] = append(le.invertedIndex[token], key)
		if _, exists := le.tokens[token]; !exists {
			if le.bkTree == nil {
				le.bkTree = NewBKTree(token)
			} else {
				le.bkTree.Insert(token)
			}
			le.tokens[token] = struct{}{}
		}
	}
	if le.metrics == nil {
		le.metrics = make(map[string]int)
	}
	le.metrics["inserts"]++
	rec := walRecord[K, V]{
		Op:        "insert",
		Timestamp: time.Now(),
		Key:       key,
		Value:     value,
	}
	le.appendWAL(rec)
}

func (le *LookupEngine[K, V]) Search(key K) (V, bool) {
	le.mu.RLock()
	defer le.mu.RUnlock()
	if exp, ok := le.ttl[key]; ok && time.Now().After(exp) {
		var zero V
		return zero, false
	}
	keyBytes := []byte(fmt.Sprintf("%v", key))
	if !le.bf.Test(keyBytes) {
		var zero V
		return zero, false
	}
	val, ok := le.tree.Search(key)
	if ok {
		le.lastAccess[key] = time.Now()
	}
	return val, ok
}

func (le *LookupEngine[K, V]) InOrderTraversal() []KeyValuePair[K, V] {
	le.mu.RLock()
	defer le.mu.RUnlock()
	return le.tree.InOrderTraversal()
}

func (le *LookupEngine[K, V]) KeywordSearch(query string) []KeyValuePair[K, V] {
	le.mu.RLock()
	defer le.mu.RUnlock()
	word := strings.ToLower(query)
	seen := make(map[K]struct{})
	var results []KeyValuePair[K, V]
	if keys, exists := le.invertedIndex[word]; exists {
		for _, k := range keys {
			if _, found := seen[k]; !found {
				if v, ok := le.tree.Search(k); ok {
					results = append(results, KeyValuePair[K, V]{k, v})
					seen[k] = struct{}{}
				}
			}
		}
	}
	return results
}

func (le *LookupEngine[K, V]) FuzzySearch(query string, threshold int) []KeyValuePair[K, V] {
	le.mu.RLock()
	defer le.mu.RUnlock()
	var results []KeyValuePair[K, V]
	if le.bkTree == nil {
		return results
	}
	matches := le.bkTree.Search(strings.ToLower(query), threshold)
	seen := make(map[K]struct{})
	for _, token := range matches {
		if keys, exists := le.invertedIndex[token]; exists {
			for _, k := range keys {
				if _, found := seen[k]; !found {
					if v, ok := le.tree.Search(k); ok {
						results = append(results, KeyValuePair[K, V]{k, v})
						seen[k] = struct{}{}
					}
				}
			}
		}
	}
	return results
}

type Query struct {
	Keywords []string       `json:"keywords"`
	Fuzzy    map[string]int `json:"fuzzy"`
	Page     int            `json:"page"`
	Size     int            `json:"size"`
	Sort     string         `json:"sort"`
}

func (le *LookupEngine[K, V]) Delete(key K) bool {
	le.mu.Lock()
	defer le.mu.Unlock()
	deleted := le.tree.Delete(key)
	if deleted {
		keyBytes := []byte(fmt.Sprintf("%v", key))
		le.bf.Remove(keyBytes)
		delete(le.ttl, key)
		if le.metrics == nil {
			le.metrics = make(map[string]int)
		}
		le.metrics["deletes"]++
		rec := walRecord[K, V]{
			Op:        "delete",
			Timestamp: time.Now(),
			Key:       key,
		}
		le.appendWAL(rec)
	}
	return deleted
}

func (le *LookupEngine[K, V]) Query(q Query) []KeyValuePair[K, V] {
	le.mu.RLock()
	defer le.mu.RUnlock()
	resultSet := make(map[K]KeyValuePair[K, V])
	now := time.Now()
	for _, kw := range q.Keywords {
		word := strings.ToLower(kw)
		if keys, exists := le.invertedIndex[word]; exists {
			for _, k := range keys {
				if exp, ok := le.ttl[k]; ok && now.After(exp) {
					continue
				}
				if v, ok := le.tree.Search(k); ok {
					resultSet[k] = KeyValuePair[K, V]{k, v}
				}
			}
		}
	}
	for term, thr := range q.Fuzzy {
		fuzzyResults := le.FuzzySearch(strings.ToLower(term), thr)
		for _, pair := range fuzzyResults {
			if exp, ok := le.ttl[pair.Key]; ok && now.After(exp) {
				continue
			}
			resultSet[pair.Key] = pair
		}
	}
	results := make([]KeyValuePair[K, V], 0, len(resultSet))
	for _, pair := range resultSet {
		results = append(results, pair)
	}
	if q.Sort != "" {
		sort.Slice(results, func(i, j int) bool {
			if q.Sort == "asc" {
				return results[i].Key < results[j].Key
			}
			return results[i].Key > results[j].Key
		})
	}
	if q.Page > 0 && q.Size > 0 {
		start := (q.Page - 1) * q.Size
		if start >= len(results) {
			return []KeyValuePair[K, V]{}
		}
		end := start + q.Size
		if end > len(results) {
			end = len(results)
		}
		results = results[start:end]
	}
	if le.metrics == nil {
		le.metrics = make(map[string]int)
	}
	le.metrics["queries"]++
	return results
}

// New query types for multi-query search
type TermQuery struct {
	Term string `json:"term"`
}

type BooleanQuery struct {
	Must    []any `json:"must"`
	MustNot []any `json:"must_not"`
	Should  []any `json:"should"`
}

type KeyValueQuery struct {
	Field string `json:"field"`
	Value string `json:"value"`
	Exact bool   `json:"exact"`
}

func (le *LookupEngine[K, V]) MultiQuery(q any) []KeyValuePair[K, V] {
	resultSet := make(map[K]KeyValuePair[K, V])
	all := le.tree.InOrderTraversal()

	switch query := q.(type) {
	case TermQuery:
		return le.KeywordSearch(query.Term)
	case BooleanQuery:
		var mustResults []map[K]KeyValuePair[K, V]
		for _, sub := range query.Must {
			subRes := le.MultiQuery(sub)
			resMap := make(map[K]KeyValuePair[K, V])
			for _, pair := range subRes {
				resMap[pair.Key] = pair
			}
			mustResults = append(mustResults, resMap)
		}
		if len(mustResults) > 0 {
			for k, pair := range mustResults[0] {
				include := true
				for i := 1; i < len(mustResults); i++ {
					if _, ok := mustResults[i][k]; !ok {
						include = false
						break
					}
				}
				if include {
					resultSet[k] = pair
				}
			}
		} else {
			for _, pair := range all {
				resultSet[pair.Key] = pair
			}
		}
		for _, sub := range query.MustNot {
			subRes := le.MultiQuery(sub)
			for _, pair := range subRes {
				delete(resultSet, pair.Key)
			}
		}
		if len(query.Should) > 0 {
			shouldSet := make(map[K]KeyValuePair[K, V])
			for _, sub := range query.Should {
				subRes := le.MultiQuery(sub)
				for _, pair := range subRes {
					shouldSet[pair.Key] = pair
				}
			}
			if len(resultSet) == 0 {
				resultSet = shouldSet
			} else {
				for k, pair := range shouldSet {
					resultSet[k] = pair
				}
			}
		}
	case KeyValueQuery:
		for _, pair := range all {
			var fieldVal string
			rVal := reflect.ValueOf(pair.Value)
			switch rVal.Kind() {
			case reflect.Map:
				val := rVal.MapIndex(reflect.ValueOf(query.Field))
				if val.IsValid() {
					fieldVal = fmt.Sprintf("%v", val.Interface())
				}
			case reflect.Struct:
				f := rVal.FieldByName(query.Field)
				if f.IsValid() {
					fieldVal = fmt.Sprintf("%v", f.Interface())
				}
			default:
				continue
			}
			if query.Exact {
				if fieldVal == query.Value {
					resultSet[pair.Key] = pair
				}
			} else {
				if strings.Contains(strings.ToLower(fieldVal), strings.ToLower(query.Value)) {
					resultSet[pair.Key] = pair
				}
			}
		}
	}
	var results []KeyValuePair[K, V]
	for _, pair := range resultSet {
		results = append(results, pair)
	}
	return results
}

type SnapshotPayload[K Ordered, V Record] struct {
	Records       []KeyValuePair[K, V] `json:"records"`
	InvertedIndex map[string][]K       `json:"inverted_index"`
	BloomCounts   []uint8              `json:"bloom_counts"`
	BloomM        uint                 `json:"bloom_m"`
	BloomK        uint                 `json:"bloom_k"`
}

func (le *LookupEngine[K, V]) SaveSnapshot(path string) error {
	le.mu.RLock()
	defer le.mu.RUnlock()
	payload := SnapshotPayload[K, V]{
		Records:       le.tree.InOrderTraversal(),
		InvertedIndex: le.invertedIndex,
		BloomCounts:   le.bf.counts,
		BloomM:        le.bf.m,
		BloomK:        le.bf.k,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

func (le *LookupEngine[K, V]) LoadSnapshot(path string) error {
	le.mu.Lock()
	defer le.mu.Unlock()
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var payload SnapshotPayload[K, V]
	if err := json.Unmarshal(data, &payload); err != nil {
		return err
	}
	le.tree = BulkLoad(le.tree.order, payload.Records)
	le.invertedIndex = payload.InvertedIndex
	le.bf = NewBloomFilter(payload.BloomM, payload.BloomK)
	le.bf.counts = payload.BloomCounts
	le.bkTree = nil
	le.tokens = make(map[string]struct{})
	for token := range le.invertedIndex {
		if le.bkTree == nil {
			le.bkTree = NewBKTree(token)
		} else {
			le.bkTree.Insert(token)
		}
		le.tokens[token] = struct{}{}
	}
	return nil
}

func (le *LookupEngine[K, V]) StartBackgroundCleaner(cleanInterval, evictionDuration time.Duration) {
	go func() {
		ticker := time.NewTicker(cleanInterval)
		for range ticker.C {
			le.mu.Lock()
			now := time.Now()
			for key, last := range le.lastAccess {
				if now.Sub(last) > evictionDuration {
					if exp, exists := le.ttl[key]; !exists || now.After(exp) {
						le.tree.Delete(key)
						keyBytes := []byte(fmt.Sprintf("%v", key))
						le.bf.Remove(keyBytes)
						for token, keys := range le.invertedIndex {
							for i, k := range keys {
								if k == key {
									le.invertedIndex[token] = append(keys[:i], keys[i+1:]...)
									break
								}
							}
						}
						delete(le.ttl, key)
						delete(le.lastAccess, key)
					}
				}
			}
			le.PersistInvertedIndex(le.persistencePath)
			le.mu.Unlock()
		}
	}()
}

func (le *LookupEngine[K, V]) PersistInvertedIndex(path string) {
	data, err := json.Marshal(le.invertedIndex)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to marshal inverted index: %v\n", err)
		return
	}
	dir := filepath.Dir(path)
	tmpFile, err := os.CreateTemp(dir, "inverted_index_*.tmp")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create temporary file: %v\n", err)
		return
	}
	tmpName := tmpFile.Name()
	if _, err := tmpFile.Write(data); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to write to temporary file: %v\n", err)
		tmpFile.Close()
		return
	}
	if err := tmpFile.Sync(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to sync temporary file: %v\n", err)
		tmpFile.Close()
		return
	}
	tmpFile.Close()
	if err = os.Rename(tmpName, path); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to rename temporary file: %v\n", err)
		return
	}
}

func (le *LookupEngine[K, V]) LoadInvertedIndex(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var idx map[string][]K
	if err := json.Unmarshal(data, &idx); err != nil {
		return err
	}
	le.invertedIndex = idx
	le.bkTree = nil
	le.tokens = make(map[string]struct{})
	for token := range le.invertedIndex {
		if le.bkTree == nil {
			le.bkTree = NewBKTree(token)
		} else {
			le.bkTree.Insert(token)
		}
		le.tokens[token] = struct{}{}
	}
	return nil
}

func (le *LookupEngine[K, V]) TotalRecords() int {
	le.mu.RLock()
	defer le.mu.RUnlock()
	return len(le.tree.InOrderTraversal())
}
