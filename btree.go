package main

import (
	"fmt"
	"hash/fnv"
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
		keys:     make([]K, 0),
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
	for dist, child := range tree.children {
		if d-dist <= threshold && d+dist >= threshold {
			results = append(results, child.Search(term, threshold)...)
		}
	}
	return results
}

func levenshtein(a, b string) int {
	aLen := len(a)
	bLen := len(b)
	if aLen == 0 {
		return bLen
	}
	if bLen == 0 {
		return aLen
	}
	dp := make([][]int, aLen+1)
	for i := range dp {
		dp[i] = make([]int, bLen+1)
	}
	for i := 0; i <= aLen; i++ {
		dp[i][0] = i
	}
	for j := 0; j <= bLen; j++ {
		dp[0][j] = j
	}
	for i := 1; i <= aLen; i++ {
		for j := 1; j <= bLen; j++ {
			cost := 0
			if a[i-1] != b[j-1] {
				cost = 1
			}
			dp[i][j] = min(dp[i-1][j]+1, min(dp[i][j-1]+1, dp[i-1][j-1]+cost))
		}
	}
	return dp[aLen][bLen]
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

type LookupEngine[K Ordered, V Record] struct {
	tree          *BPlusTree[K, V]
	bf            *BloomFilter
	invertedIndex map[string][]K
	bkTree        *BKTree
}

func NewLookupEngine[K Ordered, V Record](treeOrder int, bfSize uint, bfHashes uint) *LookupEngine[K, V] {
	return &LookupEngine[K, V]{
		tree:          NewBPlusTree[K, V](treeOrder),
		bf:            NewBloomFilter(bfSize, bfHashes),
		invertedIndex: make(map[string][]K),
		bkTree:        nil,
	}
}

func tokenize(text string) []string {
	words := strings.Fields(strings.ToLower(text))
	return words
}

func (le *LookupEngine[K, V]) Insert(key K, value V) {
	keyBytes := []byte(fmt.Sprintf("%v", key))
	le.bf.Add(keyBytes)
	le.tree.Insert(key, value)
	vStr, ok := any(value).(string)
	if ok {
		words := tokenize(vStr)
		for _, word := range words {
			le.invertedIndex[word] = append(le.invertedIndex[word], key)
			if le.bkTree == nil {
				le.bkTree = NewBKTree(word)
			} else {
				exists := false
				for _, candidate := range le.bkTree.Search(word, 0) {
					if candidate == word {
						exists = true
						break
					}
				}
				if !exists {
					le.bkTree.Insert(word)
				}
			}
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
	if keys, ok := le.invertedIndex[word]; ok {
		seen := make(map[K]struct{})
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
	seenKeys := make(map[K]struct{})
	for _, word := range matches {
		if keys, ok := le.invertedIndex[word]; ok {
			for _, k := range keys {
				if _, found := seenKeys[k]; !found {
					if v, ok := le.tree.Search(k); ok {
						result = append(result, KeyValuePair[K, V]{k, v})
						seenKeys[k] = struct{}{}
					}
				}
			}
		}
	}
	return result
}

func main() {
	lookup := NewLookupEngine[int, string](3, 1024, 3)
	keys := []int{10, 20, 5, 6, 12, 30, 7, 17}
	for _, k := range keys {
		lookup.Insert(k, "Value for "+strconv.Itoa(k))
	}
	searchKeys := []int{6, 15, 17}
	for _, sk := range searchKeys {
		if value, found := lookup.Search(sk); found {
			fmt.Printf("Found key %d with value: %s\n", sk, value)
		} else {
			fmt.Printf("Key %d not found.\n", sk)
		}
	}
	fmt.Println("Keyword Search for 'value':")
	kwResults := lookup.KeywordSearch("value")
	for _, record := range kwResults {
		fmt.Printf("Key: %v, Value: %v\n", record.Key, record.Value)
	}
	fmt.Println("Fuzzy Search for 'valeu' with threshold 2:")
	fzResults := lookup.FuzzySearch("valeu", 2)
	for _, record := range fzResults {
		fmt.Printf("Key: %v, Value: %v\n", record.Key, record.Value)
	}
	fmt.Println("In-Order Traversal:")
	records := lookup.InOrderTraversal()
	for _, record := range records {
		fmt.Printf("Key: %v, Value: %v\n", record.Key, record.Value)
	}
}
