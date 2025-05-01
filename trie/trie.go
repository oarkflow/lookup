package trie

import (
	"bytes"
	"sync"
)

// Trie represents the root of the trie
type Trie struct {
	root *node
	pool sync.Pool
}

// node represents a node in the trie
type node struct {
	edge   byte       // the byte (char) that led to this node
	label  []byte     // compressed path (can be more than one byte)
	value  any        // optional value stored at the node
	child  [256]*node // direct access to child nodes (O(1) access)
	isLeaf bool
}

// NewTrie creates a new Trie
func NewTrie() *Trie {
	t := &Trie{}
	t.pool.New = func() any {
		return &node{}
	}
	t.root = t.newNode()
	return t
}

// Insert adds a key-value pair to the trie
func (t *Trie) Insert(key string, value any) {
	k := []byte(key)
	t.root = t.insert(t.root, k, value)
}

func (t *Trie) insert(n *node, key []byte, value any) *node {
	if len(key) == 0 {
		n.value = value
		n.isLeaf = true
		return n
	}

	c := key[0]
	child := n.child[c]

	if child == nil {
		newNode := t.newNode()
		newNode.edge = c
		newNode.label = key
		newNode.value = value
		newNode.isLeaf = true
		n.child[c] = newNode
		return n
	}

	common := commonPrefix(key, child.label)
	if len(common) == len(child.label) {
		child = t.insert(child, key[len(common):], value)
		n.child[c] = child
		return n
	}

	// If key is a prefix of child's label, assign value here.
	if len(common) == len(key) {
		split := t.newNode()
		split.edge = c
		split.label = common
		split.value = value
		split.isLeaf = true
		// Adjust existing child.
		child.edge = child.label[len(common)]
		child.label = child.label[len(common):]
		split.child[child.edge] = child
		n.child[c] = split
		return n
	}

	// Otherwise, key extends beyond the common prefix.
	split := t.newNode()
	split.edge = c
	split.label = common

	// Existing child becomes a branch.
	child.edge = child.label[len(common)]
	child.label = child.label[len(common):]
	split.child[child.edge] = child

	// New leaf becomes a branch
	newLeaf := t.newNode()
	suffix := key[len(common):]
	newLeaf.edge = suffix[0]
	newLeaf.label = suffix
	newLeaf.value = value
	newLeaf.isLeaf = true
	split.child[newLeaf.edge] = newLeaf

	n.child[c] = split
	return n
}

// Get returns the value for a given key
func (t *Trie) Get(key string) (any, bool) {
	k := []byte(key)
	n := t.root

	for len(k) > 0 {
		c := k[0]
		child := n.child[c]
		if child == nil {
			return nil, false
		}

		if !bytes.HasPrefix(k, child.label) {
			return nil, false
		}

		k = k[len(child.label):]
		n = child
	}

	if n.isLeaf {
		return n.value, true
	}
	return nil, false
}

// Delete removes a key from the trie
func (t *Trie) Delete(key string) {
	k := []byte(key)
	t.delete(t.root, k)
}

func (t *Trie) delete(n *node, key []byte) bool {
	if len(key) == 0 {
		n.value = nil
		n.isLeaf = false
		return len(n.label) == 0
	}

	c := key[0]
	child := n.child[c]
	if child == nil || !bytes.HasPrefix(key, child.label) {
		return false
	}

	if t.delete(child, key[len(child.label):]) {
		n.child[c] = nil
	}

	return false
}

// Traverse iterates over all key-value pairs in the trie
func (t *Trie) Traverse(fn func(key string, value any)) {
	var traverseNode func(n *node, prefix []byte)
	traverseNode = func(n *node, prefix []byte) {
		// If node holds a value, report the full key.
		if n.isLeaf && n.value != nil {
			fn(string(append(prefix, n.label...)), n.value)
		}
		// Traverse all children.
		for i := 0; i < len(n.child); i++ {
			child := n.child[i]
			if child != nil {
				// Append current node's label then traverse child.
				traverseNode(child, append(prefix, n.label...))
			}
		}
	}
	traverseNode(t.root, []byte{})
}

// Helper: allocate a new node from pool
func (t *Trie) newNode() *node {
	n := t.pool.Get().(*node)
	*n = node{} // clear contents
	return n
}

// Helper: find common prefix
func commonPrefix(a, b []byte) []byte {
	min := len(a)
	if len(b) < min {
		min = len(b)
	}
	i := 0
	for i < min && a[i] == b[i] {
		i++
	}
	return a[:i]
}
