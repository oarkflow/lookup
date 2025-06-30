package lookup

import (
	"container/list"
	"fmt"
	"log"
	"os"
	"sort"
	"sync"
	"syscall"

	"golang.org/x/sys/unix"

	"github.com/oarkflow/json"

	"github.com/oarkflow/lookup/utils"
)

type Ordered interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 |
		~float32 | ~float64 | ~string
}

type node[K Ordered, V any] struct {
	id       int
	isLeaf   bool
	keys     []K
	children []*node[K, V]
	values   []V
	next     *node[K, V]
}

type KVPair[K Ordered, V any] struct {
	Key   K
	Value V
}

type LRUCache[K comparable, V any] struct {
	capacity int
	cache    map[K]*list.Element
	ll       *list.List
	mu       sync.Mutex
}

type entry[K comparable, V any] struct {
	key   K
	value V
}

func NewLRUCache[K comparable, V any](capacity int) *LRUCache[K, V] {
	return &LRUCache[K, V]{
		capacity: capacity,
		cache:    make(map[K]*list.Element),
		ll:       list.New(),
	}
}

func (l *LRUCache[K, V]) Get(key K) (V, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if ele, ok := l.cache[key]; ok {
		l.ll.MoveToFront(ele)
		log.Printf("LRUCache: Accessed key %v", key)
		return ele.Value.(entry[K, V]).value, true
	}
	var zero V
	return zero, false
}

func (l *LRUCache[K, V]) Put(key K, value V) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if ele, ok := l.cache[key]; ok {
		l.ll.MoveToFront(ele)
		ele.Value = entry[K, V]{key, value}
		return
	}
	ele := l.ll.PushFront(entry[K, V]{key, value})
	l.cache[key] = ele
	if l.ll.Len() > l.capacity {
		l.removeOldestLocked()
	}
}

func (l *LRUCache[K, V]) Remove(key K) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if ele, ok := l.cache[key]; ok {
		delete(l.cache, key)
		l.ll.Remove(ele)
	}
}

func (l *LRUCache[K, V]) removeOldestLocked() {
	ele := l.ll.Back()
	if ele != nil {
		en := ele.Value.(entry[K, V])
		delete(l.cache, en.key)
		l.ll.Remove(ele)
	}
}

func initStorage(filePath string, size int) ([]byte, error) {
	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = f.Close()
	}()
	if err := f.Truncate(int64(size)); err != nil {
		return nil, err
	}
	data, err := syscall.Mmap(int(f.Fd()), 0, size, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	return data, nil
}

type BPTree[K Ordered, V any] struct {
	root       *node[K, V]
	order      int
	nextNodeID int
	cache      *LRUCache[int, *node[K, V]]
	storage    []byte
	mu         sync.RWMutex
}

func NewBPTree[K Ordered, V any](order int, storageFile string, cacheCapacity int) *BPTree[K, V] {
	t := &BPTree[K, V]{order: order, nextNodeID: 1}
	root := &node[K, V]{
		id:     t.nextNodeID,
		isLeaf: true,
		keys:   make([]K, 0, order),
		values: make([]V, 0, order),
	}
	t.nextNodeID++
	t.root = root
	t.cache = NewLRUCache[int, *node[K, V]](cacheCapacity)
	t.cache.Put(root.id, root)
	if storageFile != "" {
		if data, err := initStorage(storageFile, 5*1024*1024); err == nil {
			t.storage = data
		} else {
			fmt.Println("mmap init error:", err)
		}
	}
	return t
}

func (t *BPTree[K, V]) cacheNode(n *node[K, V]) {
	t.cache.Put(n.id, n)
}

func (t *BPTree[K, V]) Search(key K) (V, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	n := t.root
	for !n.isLeaf {
		i := sort.Search(len(n.keys), func(i int) bool { return key < n.keys[i] })
		n = n.children[i]
	}
	i := sort.Search(len(n.keys), func(i int) bool { return n.keys[i] >= key })
	if i < len(n.keys) && n.keys[i] == key {
		return n.values[i], true
	}
	var zero V
	return zero, false
}

func (t *BPTree[K, V]) Insert(key K, value V) {
	t.mu.Lock()
	defer t.mu.Unlock()
	const maxDepth = 64
	type pathEntry struct {
		n   *node[K, V]
		idx int
	}
	var path [maxDepth]pathEntry
	depth := 0

	cur := t.root
	for !cur.isLeaf {
		i := sort.Search(len(cur.keys), func(i int) bool { return key < cur.keys[i] })
		path[depth] = pathEntry{n: cur, idx: i}
		depth++
		cur = cur.children[i]
	}
	i := sort.Search(len(cur.keys), func(i int) bool { return cur.keys[i] >= key })
	if i < len(cur.keys) && cur.keys[i] == key {
		cur.values[i] = value
		t.cacheNode(cur)
		return
	}

	cur.keys = cur.keys[:len(cur.keys)+1]
	cur.values = cur.values[:len(cur.values)+1]
	copy(cur.keys[i+1:], cur.keys[i:len(cur.keys)-1])
	cur.keys[i] = key
	copy(cur.values[i+1:], cur.values[i:len(cur.values)-1])
	cur.values[i] = value

	if len(cur.keys) < t.order {
		t.cacheNode(cur)
		return
	}
	newChild, splitKey := t.splitLeaf(cur)
	t.cacheNode(cur)
	t.cacheNode(newChild)
	for depth > 0 {
		depth--
		parent := path[depth].n
		childIndex := path[depth].idx
		parent.keys = parent.keys[:len(parent.keys)+1]
		copy(parent.keys[childIndex+1:], parent.keys[childIndex:len(parent.keys)-1])
		parent.keys[childIndex] = splitKey
		parent.children = parent.children[:len(parent.children)+1]
		copy(parent.children[childIndex+2:], parent.children[childIndex+1:len(parent.children)-1])
		parent.children[childIndex+1] = newChild
		if len(parent.children) <= t.order {
			t.cacheNode(parent)
			return
		}
		newChild, splitKey = t.splitInternal(parent)
		t.cacheNode(newChild)
	}

	newRoot := &node[K, V]{
		id:       t.nextNodeID,
		isLeaf:   false,
		keys:     make([]K, 0, t.order),
		children: make([]*node[K, V], 0, t.order+1),
	}
	t.nextNodeID++
	newRoot.keys = append(newRoot.keys, splitKey)
	newRoot.children = append(newRoot.children, t.root, newChild)
	t.root = newRoot
	t.cacheNode(newRoot)
}

func (t *BPTree[K, V]) splitLeaf(n *node[K, V]) (newNode *node[K, V], splitKey K) {
	mid := (t.order + 1) / 2
	newNode = &node[K, V]{
		id:     t.nextNodeID,
		isLeaf: true,
		keys:   make([]K, 0, t.order),
		values: make([]V, 0, t.order),
		next:   n.next,
	}
	t.nextNodeID++
	newNode.keys = append(newNode.keys, n.keys[mid:]...)
	newNode.values = append(newNode.values, n.values[mid:]...)
	n.keys = n.keys[:mid]
	n.values = n.values[:mid]
	splitKey = newNode.keys[0]
	n.next = newNode
	return newNode, splitKey
}

func (t *BPTree[K, V]) splitInternal(n *node[K, V]) (newNode *node[K, V], promotedKey K) {
	mid := t.order / 2
	promotedKey = n.keys[mid]
	newNode = &node[K, V]{
		id:       t.nextNodeID,
		isLeaf:   false,
		keys:     make([]K, 0, t.order),
		children: make([]*node[K, V], 0, t.order+1),
	}
	t.nextNodeID++
	newNode.keys = append(newNode.keys, n.keys[mid+1:]...)
	newNode.children = append(newNode.children, n.children[mid+1:]...)
	n.keys = n.keys[:mid]
	n.children = n.children[:mid+1]
	return newNode, promotedKey
}

func (t *BPTree[K, V]) Delete(key K) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	deleted := t.delete(nil, t.root, key, 0)
	if !t.root.isLeaf && len(t.root.keys) == 0 {
		t.root = t.root.children[0]
	}
	return deleted
}

func (t *BPTree[K, V]) delete(parent *node[K, V], n *node[K, V], key K, idx int) bool {
	if n.isLeaf {
		i := sort.Search(len(n.keys), func(i int) bool { return n.keys[i] >= key })
		if i >= len(n.keys) || n.keys[i] != key {
			return false
		}
		n.keys = append(n.keys[:i], n.keys[i+1:]...)
		n.values = append(n.values[:i], n.values[i+1:]...)
		if parent != nil && len(n.keys) < (t.order+1)/2 {
			t.rebalance(parent, n, idx)
		}
		return true
	}
	i := sort.Search(len(n.keys), func(i int) bool { return key < n.keys[i] })
	if t.delete(n, n.children[i], key, i) {
		if i < len(n.children) && len(n.children[i].keys) < (t.order+1)/2 {
			t.rebalance(n, n.children[i], i)
		}
		return true
	}
	return false
}

func (t *BPTree[K, V]) rebalance(parent, child *node[K, V], idx int) {
	var left, right *node[K, V]
	if idx > 0 {
		left = parent.children[idx-1]
	}
	if idx < len(parent.children)-1 {
		right = parent.children[idx+1]
	}
	minKeys := (t.order + 1) / 2
	if left != nil && len(left.keys) > minKeys {
		if child.isLeaf {
			child.keys = child.keys[:len(child.keys)+1]
			copy(child.keys[1:], child.keys)
			child.keys[0] = left.keys[len(left.keys)-1]
			child.values = child.values[:len(child.values)+1]
			copy(child.values[1:], child.values)
			child.values[0] = left.values[len(left.values)-1]
			left.keys = left.keys[:len(left.keys)-1]
			left.values = left.values[:len(left.values)-1]
			parent.keys[idx-1] = child.keys[0]
		} else {
			child.keys = child.keys[:len(child.keys)+1]
			copy(child.keys[1:], child.keys)
			child.keys[0] = parent.keys[idx-1]
			child.children = child.children[:len(child.children)+1]
			copy(child.children[1:], child.children)
			child.children[0] = left.children[len(left.children)-1]
			left.keys = left.keys[:len(left.keys)-1]
			left.children = left.children[:len(left.children)-1]
			parent.keys[idx-1] = child.keys[0]
		}
		return
	}
	if right != nil && len(right.keys) > minKeys {
		if child.isLeaf {
			child.keys = append(child.keys, right.keys[0])
			child.values = append(child.values, right.values[0])
			right.keys = right.keys[1:]
			right.values = right.values[1:]
			parent.keys[idx] = right.keys[0]
		} else {
			child.keys = append(child.keys, parent.keys[idx])
			child.children = append(child.children, right.children[0])
			parent.keys[idx] = right.keys[0]
			right.keys = right.keys[1:]
			right.children = right.children[1:]
		}
		return
	}
	if left != nil {
		if child.isLeaf {
			left.keys = append(left.keys, child.keys...)
			left.values = append(left.values, child.values...)
			left.next = child.next
		} else {
			left.keys = append(left.keys, parent.keys[idx-1])
			left.keys = append(left.keys, child.keys...)
			left.children = append(left.children, child.children...)
		}
		parent.keys = append(parent.keys[:idx-1], parent.keys[idx:]...)
		parent.children = append(parent.children[:idx], parent.children[idx+1:]...)
	} else if right != nil {
		if child.isLeaf {
			child.keys = append(child.keys, right.keys...)
			child.values = append(child.values, right.values...)
			child.next = right.next
		} else {
			child.keys = append(child.keys, parent.keys[idx])
			child.keys = append(child.keys, right.keys...)
			child.children = append(child.children, right.children...)
		}
		parent.keys = append(parent.keys[:idx], parent.keys[idx+1:]...)
		parent.children = append(parent.children[:idx+1], parent.children[idx+2:]...)
	}
}

func (t *BPTree[K, V]) ForEach(fn func(key K, value V) bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	n := t.root
	for !n.isLeaf {
		n = n.children[0]
	}
	for n != nil {
		for i, key := range n.keys {
			if !fn(key, n.values[i]) {
				return
			}
		}
		n = n.next
	}
}

func (t *BPTree[K, V]) RangeSearch(start, end K) []KVPair[K, V] {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var results []KVPair[K, V]
	n := t.root

	for !n.isLeaf {
		i := sort.Search(len(n.keys), func(i int) bool { return start < n.keys[i] })
		n = n.children[i]
	}

	for n != nil {
		for i, k := range n.keys {
			if k >= start && k <= end {
				results = append(results, KVPair[K, V]{Key: k, Value: n.values[i]})
			}
			if k > end {
				return results
			}
		}
		n = n.next
	}
	return results
}

func (t *BPTree[K, V]) BulkInsert(pairs []KVPair[K, V]) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, pair := range pairs {
		t.Insert(pair.Key, pair.Value)
	}
}

func (t *BPTree[K, V]) Flush() error {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.storage != nil {
		if err := unix.Msync(t.storage, unix.MS_SYNC); err != nil {
			return err
		}
	}
	return nil
}

func (t *BPTree[K, V]) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.storage != nil {
		if err := unix.Munmap(t.storage); err != nil {
			return err
		}
		t.storage = nil
	}
	return nil
}

func StoreFromJSON(tree *BPTree[string, map[string]any], file string, keyField string) error {
	data, err := os.ReadFile(file)
	if err != nil {
		return err
	}
	var records []map[string]any
	if err := json.Unmarshal(data, &records); err != nil {
		return err
	}
	for _, rec := range records {
		v, ok := rec[keyField]
		if !ok {
			v = utils.NewID().String()
		}
		key := utils.ToString(v)
		tree.Insert(key, rec)
	}
	return nil
}
