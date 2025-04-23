package main

import (
	"fmt"
	"hash/fnv"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

type Ordered interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~float32 | ~float64 | ~string
}

type Record interface{}

type BPlusTree[K Ordered, V Record] struct {
	order int
	root  *bTreeNode[K, V]
}

type bTreeNode[K Ordered, V Record] struct {
	keys     []K
	children []*bTreeNode[K, V]
	values   []V
	leaf     bool
	next     *bTreeNode[K, V]
}

func newNode[K Ordered, V Record](leaf bool) *bTreeNode[K, V] {
	return &bTreeNode[K, V]{
		keys:     make([]K, 0, 8),
		leaf:     leaf,
		values:   nil,
		children: nil,
		next:     nil,
	}
}

func NewBPlusTree[K Ordered, V Record](order int) *BPlusTree[K, V] {
	if order < 3 {
		panic("B+ tree order must be at least 3")
	}
	root := newNode[K, V](true)
	return &BPlusTree[K, V]{
		order: order,
		root:  root,
	}
}

func (tree *BPlusTree[K, V]) Insert(key K, value V) {
	root := tree.root
	if len(root.keys) == tree.order {
		newRoot := newNode[K, V](false)
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
		node.keys[idx] = key
		copy(node.values[idx+1:], node.values[idx:])
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
		newLeaf := newNode[K, V](true)
		newLeaf.keys = append(newLeaf.keys, child.keys[mid:]...)
		newLeaf.values = append(newLeaf.values, child.values[mid:]...)
		child.keys = child.keys[:mid]
		child.values = child.values[:mid]
		newLeaf.next = child.next
		child.next = newLeaf
		parent.keys = append(parent.keys, newLeaf.keys[0])
		parent.children = append(parent.children, nil)
		copy(parent.keys[idx+1:], parent.keys[idx:])
		parent.keys[idx] = newLeaf.keys[0]
		copy(parent.children[idx+2:], parent.children[idx+1:])
		parent.children[idx+1] = newLeaf
	} else {
		newInternal := newNode[K, V](false)
		promoteKey := child.keys[mid]
		newInternal.keys = append(newInternal.keys, child.keys[mid+1:]...)
		newInternal.children = append(newInternal.children, child.children[mid+1:]...)
		child.keys = child.keys[:mid]
		child.children = child.children[:mid+1]
		parent.keys = append(parent.keys, promoteKey)
		parent.children = append(parent.children, nil)
		copy(parent.keys[idx+1:], parent.keys[idx:])
		parent.keys[idx] = promoteKey
		copy(parent.children[idx+2:], parent.children[idx+1:])
		parent.children[idx+1] = newInternal
	}
}

func (tree *BPlusTree[K, V]) Search(key K) (V, bool) {
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

type KeyValuePair[K Ordered, V Record] struct {
	Key   K
	Value V
}

func (tree *BPlusTree[K, V]) InOrderTraversal() []KeyValuePair[K, V] {
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

type BloomFilter struct {
	bitset []uint64
	m      uint
	k      uint
}

func NewBloomFilter(m uint, k uint) *BloomFilter {
	size := (m + 63) / 64
	return &BloomFilter{
		bitset: make([]uint64, size),
		m:      m,
		k:      k,
	}
}

func (bf *BloomFilter) Add(data []byte) {
	h1 := hash1(data)
	h2 := hash2(data)
	for i := uint(0); i < bf.k; i++ {
		combined := (h1 + i*h2) % bf.m
		bf.setBit(combined)
	}
}

func (bf *BloomFilter) Test(data []byte) bool {
	h1 := hash1(data)
	h2 := hash2(data)
	for i := uint(0); i < bf.k; i++ {
		combined := (h1 + i*h2) % bf.m
		if !bf.getBit(combined) {
			return false
		}
	}
	return true
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

func hash1(data []byte) uint {
	hasher := fnv.New64a()
	hasher.Write(data)
	return uint(hasher.Sum64())
}

func hash2(data []byte) uint {
	hasher := fnv.New64()
	hasher.Write(data)
	return uint(hasher.Sum64()) | 1
}

type BKTree struct {
	term     string
	children map[int]*BKTree
}

func NewBKTree(term string) *BKTree {
	return &BKTree{term: term, children: make(map[int]*BKTree)}
}

func (tree *BKTree) Insert(term string) {
	d := levenshtein(tree.term, term)
	if child, ok := tree.children[d]; ok {
		child.Insert(term)
	} else {
		tree.children[d] = NewBKTree(term)
	}
}

func (tree *BKTree) Search(term string, threshold int) []string {
	var results []string
	d := levenshtein(tree.term, term)
	if d <= threshold {
		results = append(results, tree.term)
	}
	for _, child := range tree.children {
		results = append(results, child.Search(term, threshold)...)
	}
	return results
}

func levenshtein(a, b string) int {
	la, lb := len(a), len(b)
	if la == 0 {
		return lb
	}
	if lb == 0 {
		return la
	}
	prev := make([]int, lb+1)
	for j := 0; j <= lb; j++ {
		prev[j] = j
	}
	for i := 1; i <= la; i++ {
		curr := make([]int, lb+1)
		curr[0] = i
		for j := 1; j <= lb; j++ {
			cost := 0
			if a[i-1] != b[j-1] {
				cost = 1
			}
			curr[j] = min(curr[j-1]+1, min(prev[j]+1, prev[j-1]+cost))
		}
		prev = curr
	}
	return prev[lb]
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func tokenize(text string) []string {
	return strings.Fields(strings.ToLower(text))
}

func extractTokens(record interface{}) []string {
	var tokens []string
	switch rec := record.(type) {
	case string:
		tokens = tokenize(rec)
	case map[string]string:
		for k, v := range rec {
			tokens = append(tokens, tokenize(k)...)
			tokens = append(tokens, tokenize(v)...)
		}
	case map[string]interface{}:
		for k, v := range rec {
			tokens = append(tokens, tokenize(k)...)
			tokens = append(tokens, tokenize(fmt.Sprintf("%v", v))...)
		}
	default:
		rv := reflect.ValueOf(record)
		switch rv.Kind() {
		case reflect.Struct:
			for i := 0; i < rv.NumField(); i++ {
				tokens = append(tokens, tokenize(fmt.Sprintf("%v", rv.Field(i).Interface()))...)
			}
		case reflect.Slice, reflect.Array:
			for i := 0; i < rv.Len(); i++ {
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
	bkTreeTokens  map[string]struct{} // new field
}

func NewLookupEngine[K Ordered, V Record](treeOrder int, bfSize uint, bfHashes uint) *LookupEngine[K, V] {
	return &LookupEngine[K, V]{
		tree:          NewBPlusTree[K, V](treeOrder),
		bf:            NewBloomFilter(bfSize, bfHashes),
		invertedIndex: make(map[string][]K),
		bkTree:        nil,
		bkTreeTokens:  make(map[string]struct{}), // initialize token cache
	}
}

func (le *LookupEngine[K, V]) Insert(key K, value V) {
	keyBytes := []byte(fmt.Sprintf("%v", key))
	le.bf.Add(keyBytes)
	le.tree.Insert(key, value)
	tokens := extractTokens(value)
	for _, token := range tokens {
		le.invertedIndex[token] = append(le.invertedIndex[token], key)
		if _, exists := le.bkTreeTokens[token]; !exists {
			if le.bkTree == nil {
				le.bkTree = NewBKTree(token)
			} else {
				le.bkTree.Insert(token)
			}
			le.bkTreeTokens[token] = struct{}{}
		}
	}
}

func (le *LookupEngine[K, V]) Search(key K) (V, bool) {
	keyBytes := []byte(fmt.Sprintf("%v", key))
	if !le.bf.Test(keyBytes) {
		var zero V
		return zero, false
	}
	return le.tree.Search(key)
}

func (le *LookupEngine[K, V]) InOrderTraversal() []KeyValuePair[K, V] {
	return le.tree.InOrderTraversal()
}

func (le *LookupEngine[K, V]) KeywordSearch(query string) []KeyValuePair[K, V] {
	var result []KeyValuePair[K, V]
	word := strings.ToLower(query)
	seen := make(map[K]struct{})
	if keys, ok := le.invertedIndex[word]; ok {
		for _, k := range keys {
			if _, found := seen[k]; !found {
				if v, ok := le.tree.Search(k); ok {
					result = append(result, KeyValuePair[K, V]{k, v})
					seen[k] = struct{}{}
				}
			}
		}
	}
	return result
}

func (le *LookupEngine[K, V]) FuzzySearch(query string, threshold int) []KeyValuePair[K, V] {
	var result []KeyValuePair[K, V]
	if le.bkTree == nil {
		return result
	}
	matches := le.bkTree.Search(strings.ToLower(query), threshold)
	seen := make(map[K]struct{})
	for _, word := range matches {
		if keys, ok := le.invertedIndex[word]; ok {
			for _, k := range keys {
				if _, found := seen[k]; !found {
					if v, ok := le.tree.Search(k); ok {
						result = append(result, KeyValuePair[K, V]{k, v})
						seen[k] = struct{}{}
					}
				}
			}
		}
	}
	return result
}

type Query struct {
	Keywords []string
	Fuzzy    map[string]int
	Page     int
	Size     int
	Sort     string
}

func (le *LookupEngine[K, V]) Query(q Query) []KeyValuePair[K, V] {
	resultMap := make(map[K]KeyValuePair[K, V])
	for _, kw := range q.Keywords {
		for _, pair := range le.KeywordSearch(strings.ToLower(kw)) {
			resultMap[pair.Key] = pair
		}
	}
	for term, threshold := range q.Fuzzy {
		for _, pair := range le.FuzzySearch(strings.ToLower(term), threshold) {
			resultMap[pair.Key] = pair
		}
	}
	results := make([]KeyValuePair[K, V], 0, len(resultMap))
	for _, pair := range resultMap {
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
	keysStr := []int{10, 20, 5, 6, 12, 30, 7, 17}
	for _, k := range keysStr {
		leStr.Insert(k, "Autocomplete record "+strconv.Itoa(k))
	}
	fmt.Println("String Records:")
	for _, rec := range leStr.InOrderTraversal() {
		fmt.Printf("Key: %v, Value: %v\n", rec.Key, rec.Value)
	}
	fmt.Println("Keyword Search for 'autocomplete':")
	for _, rec := range leStr.KeywordSearch("autocomplete") {
		fmt.Printf("Key: %v, Value: %v\n", rec.Key, rec.Value)
	}
	fmt.Println("Fuzzy Search for 'autocomplet' (threshold 1):")
	for _, rec := range leStr.FuzzySearch("autocomplet", 1) {
		fmt.Printf("Key: %v, Value: %v\n", rec.Key, rec.Value)
	}
	query := Query{
		Keywords: []string{"autocomplete"},
		Fuzzy:    map[string]int{"autocomplet": 1},
		Page:     1,
		Size:     10,
		Sort:     "asc",
	}
	fmt.Println("Combined Query Search:")
	for _, rec := range leStr.Query(query) {
		fmt.Printf("Key: %v, Value: %v\n", rec.Key, rec.Value)
	}

	leMap := NewLookupEngine[int, map[string]string](3, 1024, 3)
	keysMap := []int{101, 102, 103}
	recMap1 := map[string]string{"title": "Map Record One", "desc": "This is the first map record"}
	recMap2 := map[string]string{"title": "Map Record Two", "desc": "Second record in a map"}
	recMap3 := map[string]string{"title": "Another Map", "desc": "Map record three"}
	leMap.Insert(keysMap[0], recMap1)
	leMap.Insert(keysMap[1], recMap2)
	leMap.Insert(keysMap[2], recMap3)
	fmt.Println("\nMap Records:")
	for _, rec := range leMap.InOrderTraversal() {
		fmt.Printf("Key: %v, Value: %v\n", rec.Key, rec.Value)
	}
	fmt.Println("Keyword Search for 'record':")
	for _, rec := range leMap.KeywordSearch("record") {
		fmt.Printf("Key: %v, Value: %v\n", rec.Key, rec.Value)
	}
	fmt.Println("Fuzzy Search for 'map' (threshold 0):")
	for _, rec := range leMap.FuzzySearch("map", 0) {
		fmt.Printf("Key: %v, Value: %v\n", rec.Key, rec.Value)
	}

	leStruct := NewLookupEngine[int, Person](3, 1024, 3)
	keysStruct := []int{201, 202, 203}
	recStruct1 := Person{Name: "Alice", Occupation: "Engineer", Email: "alice@example.com"}
	recStruct2 := Person{Name: "Bob", Occupation: "Artist", Email: "bob@example.com"}
	recStruct3 := Person{Name: "Charlie", Occupation: "Doctor", Email: "charlie@example.com"}
	leStruct.Insert(keysStruct[0], recStruct1)
	leStruct.Insert(keysStruct[1], recStruct2)
	leStruct.Insert(keysStruct[2], recStruct3)
	fmt.Println("\nStruct Records:")
	for _, rec := range leStruct.InOrderTraversal() {
		fmt.Printf("Key: %v, Value: %+v\n", rec.Key, rec.Value)
	}
	fmt.Println("Keyword Search for 'doctor':")
	for _, rec := range leStruct.KeywordSearch("doctor") {
		fmt.Printf("Key: %v, Value: %+v\n", rec.Key, rec.Value)
	}
	fmt.Println("Fuzzy Search for 'Alic' (threshold 1):")
	for _, rec := range leStruct.FuzzySearch("Alic", 1) {
		fmt.Printf("Key: %v, Value: %+v\n", rec.Key, rec.Value)
	}
}
