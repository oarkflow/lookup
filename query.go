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
	docSet := make(map[int64]struct{})
	for _, token := range tokens {
		if postings, ok := index.Index[token]; ok {
			for _, p := range postings {
				docSet[p.DocID] = struct{}{}
			}
		}
	}
	var result []int64
	for docID := range docSet {
		result = append(result, docID)
	}
	return result
}

func (tq TermQuery) Tokens() []string {
	return []string{strings.ToLower(tq.Term)}
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
	index.Documents.ForEach(func(docID int64, rec GenericRecord) bool {
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
	index.Documents.ForEach(func(docID int64, rec GenericRecord) bool {
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
	index.Documents.ForEach(func(docID int64, rec GenericRecord) bool {
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
