package lookup

import (
	"regexp"
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
	var tokens []string
	if tq.Fuzzy {
		tokens = index.FuzzySearch(strings.ToLower(tq.Term), tq.FuzzyThreshold)
	} else {
		tokens = []string{strings.ToLower(tq.Term)}
	}
	return index.Evaluate(tokens)
}

func (tq TermQuery) Tokens() []string {
	return []string{strings.ToLower(tq.Term)}
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
	// Tokenize the phrase to get each word.
	tokens := utils.Tokenize(strings.ToLower(pq.Phrase))
	// For each token, get the matching document IDs.
	var docsByToken [][]int64
	if pq.Fuzzy {
		// For fuzzy search, each token's posting list is the union of fuzzy matches and the token itself.
		for _, token := range tokens {
			fuzzyTokens := index.FuzzySearch(token, pq.FuzzyThreshold)
			fuzzyTokens = append(fuzzyTokens, token) // include the original token
			set := make(map[int64]struct{})
			for _, ft := range fuzzyTokens {
				if postings, ok := index.index[ft]; ok {
					for _, p := range postings {
						set[p.DocID] = struct{}{}
					}
				}
			}
			if len(set) == 0 {
				return []int64{}
			}
			var ids []int64
			for id := range set {
				ids = append(ids, id)
			}
			docsByToken = append(docsByToken, ids)
		}
	} else {
		// Exact search: for each token, get the posting list directly.
		for _, token := range tokens {
			postings, ok := index.index[token]
			if !ok {
				return []int64{}
			}
			var ids []int64
			for _, p := range postings {
				ids = append(ids, p.DocID)
			}
			docsByToken = append(docsByToken, ids)
		}
	}
	// Intersect the document ID slices.
	result := docsByToken[0]
	for i := 1; i < len(docsByToken); i++ {
		result = utils.Intersect(result, docsByToken[i])
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
