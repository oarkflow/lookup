package main

import (
	"fmt"
	"hash/fnv"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
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

type KeyValuePair[K Ordered, V Record] struct {
	Key   K
	Value V
}

type BloomFilter struct {
	bitset []uint64
	m      uint
	k      uint
}

func NewBloomFilter(m, k uint) *BloomFilter {
	size := (m + 63) / 64
	return &BloomFilter{
		bitset: make([]uint64, size),
		m:      m,
		k:      k,
	}
}

func (bf *BloomFilter) Add(data []byte) {
	h1, h2 := hashDouble(data)
	for i := uint(0); i < bf.k; i++ {
		combined := (h1 + i*h2) % bf.m
		bf.setBit(combined)
	}
}

func (bf *BloomFilter) Test(data []byte) bool {
	h1, h2 := hashDouble(data)
	for i := uint(0); i < bf.k; i++ {
		combined := (h1 + i*h2) % bf.m
		if !bf.getBit(combined) {
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

func (bf *BloomFilter) setBit(i uint) {
	idx := i / 64
	pos := i % 64
	bf.bitset[idx] |= 1 << pos
}

func (bf *BloomFilter) getBit(i uint) bool {
	idx := i / 64
	pos := i % 64
	return (bf.bitset[idx] & (1 << pos)) != 0
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
	tree          *BPlusTree[K, V]
	bf            *BloomFilter
	invertedIndex map[string][]K
	bkTree        *BKTree
	tokens        map[string]struct{}
	mu            sync.RWMutex
}

func NewLookupEngine[K Ordered, V Record](order int, bfSize, bfHashes uint) *LookupEngine[K, V] {
	return &LookupEngine[K, V]{
		tree:          NewBPlusTree[K, V](order),
		bf:            NewBloomFilter(bfSize, bfHashes),
		invertedIndex: make(map[string][]K),
		tokens:        make(map[string]struct{}),
	}
}

func (le *LookupEngine[K, V]) Insert(key K, value V) {
	keyBytes := []byte(fmt.Sprintf("%v", key))
	le.mu.Lock()
	defer le.mu.Unlock()
	le.bf.Add(keyBytes)
	le.tree.Insert(key, value)
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
}

func (le *LookupEngine[K, V]) Search(key K) (V, bool) {
	keyBytes := []byte(fmt.Sprintf("%v", key))
	le.mu.RLock()
	defer le.mu.RUnlock()
	if !le.bf.Test(keyBytes) {
		var zero V
		return zero, false
	}
	return le.tree.Search(key)
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
	Keywords []string
	Fuzzy    map[string]int
	Page     int
	Size     int
	Sort     string
}

func (le *LookupEngine[K, V]) Query(q Query) []KeyValuePair[K, V] {
	le.mu.RLock()
	defer le.mu.RUnlock()
	resultSet := make(map[K]KeyValuePair[K, V])
	for _, kw := range q.Keywords {
		word := strings.ToLower(kw)
		if keys, exists := le.invertedIndex[word]; exists {
			for _, k := range keys {
				if v, ok := le.tree.Search(k); ok {
					resultSet[k] = KeyValuePair[K, V]{k, v}
				}
			}
		}
	}
	for term, thr := range q.Fuzzy {
		fuzzyResults := le.FuzzySearch(strings.ToLower(term), thr)
		for _, pair := range fuzzyResults {
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
	return results
}

type Person struct {
	Name       string
	Occupation string
	Email      string
}

func main() {
	leStr := NewLookupEngine[int, string](3, 1024, 3)
	for _, k := range []int{10, 20, 5, 6, 12, 30, 7, 17} {
		leStr.Insert(k, "Autocomplete record "+strconv.Itoa(k))
	}
	fmt.Println("String Records:")
	for _, r := range leStr.InOrderTraversal() {
		fmt.Printf("Key: %v, Value: %v\n", r.Key, r.Value)
	}
	fmt.Println("Keyword Search for 'autocomplete':")
	for _, r := range leStr.KeywordSearch("autocomplete") {
		fmt.Printf("Key: %v, Value: %v\n", r.Key, r.Value)
	}
	fmt.Println("Fuzzy Search for 'autocomplet' (threshold 1):")
	for _, r := range leStr.FuzzySearch("autocomplet", 1) {
		fmt.Printf("Key: %v, Value: %v\n", r.Key, r.Value)
	}
	q := Query{Keywords: []string{"autocomplete"}, Fuzzy: map[string]int{"autocomplet": 1}, Page: 1, Size: 10, Sort: "asc"}
	fmt.Println("Combined Query Search:")
	for _, r := range leStr.Query(q) {
		fmt.Printf("Key: %v, Value: %v\n", r.Key, r.Value)
	}

	leMap := NewLookupEngine[int, map[string]string](3, 1024, 3)
	leMap.Insert(101, map[string]string{"title": "Map Record One", "desc": "This is the first map record"})
	leMap.Insert(102, map[string]string{"title": "Map Record Two", "desc": "Second record in a map"})
	leMap.Insert(103, map[string]string{"title": "Another Map", "desc": "Map record three"})
	fmt.Println("\nMap Records:")
	for _, r := range leMap.InOrderTraversal() {
		fmt.Printf("Key: %v, Value: %v\n", r.Key, r.Value)
	}
	fmt.Println("Keyword Search for 'record':")
	for _, r := range leMap.KeywordSearch("record") {
		fmt.Printf("Key: %v, Value: %v\n", r.Key, r.Value)
	}
	fmt.Println("Fuzzy Search for 'map' (threshold 0):")
	for _, r := range leMap.FuzzySearch("map", 0) {
		fmt.Printf("Key: %v, Value: %v\n", r.Key, r.Value)
	}

	leStruct := NewLookupEngine[int, Person](3, 1024, 3)
	leStruct.Insert(201, Person{"Alice", "Engineer", "alice@example.com"})
	leStruct.Insert(202, Person{"Bob", "Artist", "bob@example.com"})
	leStruct.Insert(203, Person{"Charlie", "Doctor", "charlie@example.com"})
	fmt.Println("\nStruct Records:")
	for _, r := range leStruct.InOrderTraversal() {
		fmt.Printf("Key: %v, Value: %+v\n", r.Key, r.Value)
	}
	fmt.Println("Keyword Search for 'doctor':")
	for _, r := range leStruct.KeywordSearch("doctor") {
		fmt.Printf("Key: %v, Value: %+v\n", r.Key, r.Value)
	}
	fmt.Println("Fuzzy Search for 'Alic' (threshold 1):")
	for _, r := range leStruct.FuzzySearch("Alic", 1) {
		fmt.Printf("Key: %v, Value: %+v\n", r.Key, r.Value)
	}
}
