package lookup

import (
	"regexp"
	"sort"
	"strings"

	"github.com/oarkflow/filters"

	"github.com/oarkflow/lookup/utils"
)

type Query interface {
	Evaluate(index *Index) []int64
	Tokens() []string
}

type TermQuery struct {
	Term           string
	Fuzzy          bool
	FuzzyThreshold int
}

func NewTermQuery(term string, fuzzy bool, threshold int) TermQuery {
	return TermQuery{
		Term:           term,
		Fuzzy:          fuzzy,
		FuzzyThreshold: threshold,
	}
}

func (tq TermQuery) Evaluate(index *Index) []int64 {
	term := strings.ToLower(tq.Term)
	if !tq.Fuzzy {
		if postings, ok := index.index[term]; ok {
			out := make([]int64, len(postings))
			for i, p := range postings {
				out[i] = p.DocID
			}
			return out
		}
		return nil
	}
	candidates := index.FuzzySearch(term, tq.FuzzyThreshold)
	candidates = append(candidates, term)
	seen := map[int64]struct{}{}
	var out []int64
	for _, tok := range candidates {
		if postings, ok := index.index[tok]; ok {
			for _, p := range postings {
				if _, exists := seen[p.DocID]; !exists {
					seen[p.DocID] = struct{}{}
					out = append(out, p.DocID)
				}
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func (tq TermQuery) Tokens() []string {
	return []string{strings.ToLower(tq.Term)}
}

// intersectSorted returns the intersection of two sorted slices (O(n+m)).
func intersectSorted(a, b []int64) []int64 {
	var res []int64
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] == b[j]:
			res = append(res, a[i])
			i++
			j++
		case a[i] < b[j]:
			i++
		default:
			j++
		}
	}
	return res
}

type PhraseQuery struct {
	Phrase         string
	Fuzzy          bool
	FuzzyThreshold int
}

func NewPhraseQuery(phrase string, fuzzy bool, threshold int) PhraseQuery {
	return PhraseQuery{
		Phrase:         phrase,
		Fuzzy:          fuzzy,
		FuzzyThreshold: threshold,
	}
}

func (pq PhraseQuery) Evaluate(index *Index) []int64 {
	tokens := utils.Tokenize(strings.ToLower(pq.Phrase))
	if len(tokens) == 0 {
		return nil
	}
	var lists [][]int64
	if pq.Fuzzy {
		for _, token := range tokens {
			fuzzyTokens := index.FuzzySearch(token, pq.FuzzyThreshold)
			fuzzyTokens = append(fuzzyTokens, token)
			set := make(map[int64]struct{})
			for _, ft := range fuzzyTokens {
				if postings, ok := index.index[ft]; ok {
					for _, p := range postings {
						set[p.DocID] = struct{}{}
					}
				}
			}
			if len(set) == 0 {
				return nil
			}
			var ids []int64
			for id := range set {
				ids = append(ids, id)
			}
			lists = append(lists, ids)
		}
	} else {
		for _, token := range tokens {
			postings, ok := index.index[token]
			if !ok {
				return nil
			}
			var ids []int64
			for _, p := range postings {
				ids = append(ids, p.DocID)
			}
			lists = append(lists, ids)
		}
	}
	// Intersect posting lists.
	result := lists[0]
	for i := 1; i < len(lists); i++ {
		result = intersectSorted(result, lists[i])
		if len(result) == 0 {
			return nil
		}
	}
	// Exact mode: filter results to ensure the document contains the exact phrase.
	if !pq.Fuzzy {
		queryLower := strings.ToLower(pq.Phrase)
		var filtered []int64
		for _, docID := range result {
			// Retrieve the document from the BPTree. We assume GetDocument returns GenericRecord.
			recRaw, ok := index.GetDocument(docID)
			if !ok {
				continue
			}
			rec, ok := recRaw.(GenericRecord)
			if !ok {
				continue
			}
			// Use the document's string representation.
			if strings.Contains(strings.ToLower(rec.String(index.fieldsToIndex)), queryLower) {
				filtered = append(filtered, docID)
			}
		}
		return filtered
	}
	return result
}

func (pq PhraseQuery) Tokens() []string {
	return utils.Tokenize(strings.ToLower(pq.Phrase))
}

type WildcardQuery struct {
	Field   string
	Pattern string
}

func (wq WildcardQuery) Evaluate(index *Index) []int64 {
	var result []int64
	regexPattern := "^" + regexp.QuoteMeta(wq.Pattern) + "$"
	regexPattern = strings.ReplaceAll(regexPattern, "\\*", ".*")
	re, err := regexp.Compile(regexPattern)
	if err != nil {
		return result
	}
	index.documents.ForEach(func(docID int64, rec GenericRecord) bool {
		if val, ok := rec[wq.Field]; ok {
			strVal := utils.ToString(val)
			if re.MatchString(strVal) {
				result = append(result, docID)
			}
		}
		return true
	})
	return result
}

func (wq WildcardQuery) Tokens() []string {
	// Return the pattern as token (wildcard removed for simplicity).
	return []string{strings.ToLower(wq.Pattern)}
}

type SQLQuery struct {
	SQL  string
	Term Query
}

func NewSQLQuery(sql string, term ...Query) *SQLQuery {
	query := &SQLQuery{SQL: sql}
	if len(term) > 0 {
		query.Term = term[0]
	}
	return query
}

func (sq SQLQuery) Evaluate(index *Index) []int64 {
	var base, result []int64
	if sq.Term != nil {
		base = sq.Term.Evaluate(index)
	}
	rule, err := filters.ParseSQL(sq.SQL)
	if err != nil || rule == nil {
		return base
	}
	index.documents.ForEach(func(docID int64, rec GenericRecord) bool {
		if rule.Match(rec) {
			result = append(result, docID)
		}
		return true
	})
	if len(result) == 0 || sq.Term == nil {
		return result
	}
	return utils.Intersect(base, result)
}

func (sq SQLQuery) Tokens() []string {
	// SQL query is parsed internally; no tokens extracted.
	return []string{}
}

type FilterQuery struct {
	Filters *filters.Rule
	Term    Query
}

func NewFilterQuery(term Query, operator filters.Boolean, reverse bool, conditions ...filters.Condition) FilterQuery {
	if len(conditions) > 0 {
		rule := filters.NewRule()
		rule.AddCondition(operator, reverse, conditions...)
		return FilterQuery{Filters: rule, Term: term}
	}
	return FilterQuery{Term: term}
}

func (fq FilterQuery) Evaluate(index *Index) []int64 {
	var base, result []int64
	if fq.Term != nil {
		base = fq.Term.Evaluate(index)
	}
	if fq.Filters == nil {
		return base
	}
	index.documents.ForEach(func(docID int64, rec GenericRecord) bool {
		if fq.Filters.Match(rec) {
			result = append(result, docID)
		}
		return true
	})
	if len(result) == 0 || fq.Term == nil {
		return result
	}
	return utils.Intersect(base, result)
}

func (fq FilterQuery) Tokens() []string {
	if fq.Term != nil {
		return fq.Term.Tokens()
	}
	return []string{}
}
