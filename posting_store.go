package lookup

import "sync"

// PostingStore abstracts how posting lists are stored and retrieved.
type PostingStore interface {
	Append(term string, postings []Posting)
	Replace(term string, postings []Posting)
	Get(term string) []Posting
	Delete(term string)
	Range(fn func(term string, postings []Posting) bool)
	Snapshot() map[string][]Posting
	Len() int
}

func newPostingStore() PostingStore {
	return &mapPostingStore{data: make(map[string][]Posting)}
}

// mapPostingStore is the default in-memory posting storage.
type mapPostingStore struct {
	mu   sync.RWMutex
	data map[string][]Posting
}

func (m *mapPostingStore) Append(term string, postings []Posting) {
	if len(postings) == 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[term] = append(m.data[term], postings...)
}

func (m *mapPostingStore) Replace(term string, postings []Posting) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(postings) == 0 {
		delete(m.data, term)
		return
	}
	buf := make([]Posting, len(postings))
	copy(buf, postings)
	m.data[term] = buf
}

func (m *mapPostingStore) Get(term string) []Posting {
	m.mu.RLock()
	defer m.mu.RUnlock()
	postings, ok := m.data[term]
	if !ok || len(postings) == 0 {
		return nil
	}
	buf := make([]Posting, len(postings))
	copy(buf, postings)
	return buf
}

func (m *mapPostingStore) Delete(term string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, term)
}

func (m *mapPostingStore) Range(fn func(term string, postings []Posting) bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for term, postings := range m.data {
		buf := make([]Posting, len(postings))
		copy(buf, postings)
		if !fn(term, buf) {
			return
		}
	}
}

func (m *mapPostingStore) Snapshot() map[string][]Posting {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make(map[string][]Posting, len(m.data))
	for term, postings := range m.data {
		buf := make([]Posting, len(postings))
		copy(buf, postings)
		result[term] = buf
	}
	return result
}

func (m *mapPostingStore) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.data)
}

// compressedPostingStore wraps CompressedIndex to provide PostingStore behavior.
type compressedPostingStore struct {
	ci *CompressedIndex
}

func newCompressedPostingStore() *compressedPostingStore {
	return &compressedPostingStore{ci: NewCompressedIndex()}
}

func (c *compressedPostingStore) Append(term string, postings []Posting) {
	if len(postings) == 0 {
		return
	}
	existing := c.ci.DecompressPostings(term)
	combined := append(existing, postings...)
	c.ci.CompressPostings(term, combined)
}

func (c *compressedPostingStore) Replace(term string, postings []Posting) {
	c.ci.CompressPostings(term, postings)
}

func (c *compressedPostingStore) Get(term string) []Posting {
	return c.ci.DecompressPostings(term)
}

func (c *compressedPostingStore) Delete(term string) {
	c.ci.Delete(term)
}

func (c *compressedPostingStore) Range(fn func(term string, postings []Posting) bool) {
	for _, term := range c.ci.Terms() {
		postings := c.ci.DecompressPostings(term)
		if !fn(term, postings) {
			return
		}
	}
}

func (c *compressedPostingStore) Snapshot() map[string][]Posting {
	result := make(map[string][]Posting, c.ci.Len())
	for _, term := range c.ci.Terms() {
		postings := c.ci.DecompressPostings(term)
		buf := make([]Posting, len(postings))
		copy(buf, postings)
		result[term] = buf
	}
	return result
}

func (c *compressedPostingStore) Len() int {
	return c.ci.Len()
}
