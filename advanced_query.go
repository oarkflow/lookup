package lookup

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/oarkflow/lookup/utils"
)

// QueryProcessor provides advanced query parsing and processing
type QueryProcessor struct {
	enableStemming bool
	synonyms       map[string][]string
	stopWords      map[string]bool
}

// NewQueryProcessor creates a new query processor
func NewQueryProcessor() *QueryProcessor {
	qp := &QueryProcessor{
		synonyms:  make(map[string][]string),
		stopWords: make(map[string]bool),
	}

	// Initialize common English stop words
	qp.initializeStopWords()

	return qp
}

// initializeStopWords initializes common English stop words
func (qp *QueryProcessor) initializeStopWords() {
	stopWordsList := []string{
		"a", "an", "and", "are", "as", "at", "be", "by", "for", "from",
		"has", "he", "in", "is", "it", "its", "of", "on", "that", "the",
		"to", "was", "will", "with", "the", "this", "but", "they", "have",
		"had", "what", "said", "each", "which", "she", "do", "how", "their",
	}

	for _, word := range stopWordsList {
		qp.stopWords[word] = true
	}
}

// AddSynonym adds a synonym mapping
func (qp *QueryProcessor) AddSynonym(word string, synonyms []string) {
	qp.synonyms[utils.ToLower(word)] = synonyms
}

// EnableStemming enables or disables stemming
func (qp *QueryProcessor) EnableStemming(enable bool) {
	qp.enableStemming = enable
}

// ParseAdvancedQuery parses advanced query syntax
func (qp *QueryProcessor) ParseAdvancedQuery(queryString string) (Query, error) {
	queryString = strings.TrimSpace(queryString)
	if queryString == "" {
		return nil, fmt.Errorf("empty query")
	}

	// Handle quoted phrases
	if strings.Contains(queryString, "\"") {
		return qp.parseQuotedQuery(queryString)
	}

	// Handle boolean operators
	if qp.containsBooleanOperators(queryString) {
		return qp.parseBooleanQuery(queryString)
	}

	// Handle field-specific queries
	if strings.Contains(queryString, ":") {
		return qp.parseFieldQuery(queryString)
	}

	// Handle wildcard queries
	if strings.ContainsAny(queryString, "*?") {
		return qp.parseWildcardQuery(queryString)
	}

	// Default to term or phrase query
	tokens := qp.tokenize(queryString)
	if len(tokens) == 1 {
		return NewTermQuery(tokens[0], true, 1), nil
	}

	return NewPhraseQuery(queryString, true, 1), nil
}

// parseQuotedQuery handles quoted phrase queries
func (qp *QueryProcessor) parseQuotedQuery(queryString string) (Query, error) {
	re := regexp.MustCompile(`"([^"]+)"`)
	matches := re.FindAllStringSubmatch(queryString, -1)

	if len(matches) == 0 {
		return qp.ParseAdvancedQuery(strings.ReplaceAll(queryString, "\"", ""))
	}

	var queries []Query
	remainder := queryString

	for _, match := range matches {
		phrase := match[1]
		queries = append(queries, NewPhraseQuery(phrase, false, 0)) // Exact phrase
		remainder = strings.Replace(remainder, match[0], "", 1)
	}

	// Handle remaining terms
	remainder = strings.TrimSpace(remainder)
	if remainder != "" {
		remainingTokens := qp.tokenize(remainder)
		for _, token := range remainingTokens {
			if !qp.isStopWord(token) {
				queries = append(queries, NewTermQuery(token, true, 1))
			}
		}
	}

	if len(queries) == 1 {
		return queries[0], nil
	}

	return NewBooleanQuery(queries, "AND"), nil
}

// parseBooleanQuery handles boolean operators (AND, OR, NOT)
func (qp *QueryProcessor) parseBooleanQuery(queryString string) (Query, error) {
	// Simple boolean query parsing
	tokens := strings.Fields(queryString)
	var queries []Query
	var operator string = "AND"

	for i, token := range tokens {
		upperToken := strings.ToUpper(token)
		switch upperToken {
		case "AND", "OR":
			operator = upperToken
		case "NOT":
			if i+1 < len(tokens) {
				nextToken := tokens[i+1]
				queries = append(queries, NewNotQuery(NewTermQuery(nextToken, true, 1)))
				i++ // Skip next token
			}
		default:
			if !qp.isStopWord(utils.ToLower(token)) {
				queries = append(queries, NewTermQuery(token, true, 1))
			}
		}
	}

	if len(queries) == 0 {
		return nil, fmt.Errorf("no valid terms in boolean query")
	}

	if len(queries) == 1 {
		return queries[0], nil
	}

	return NewBooleanQuery(queries, operator), nil
}

// parseFieldQuery handles field-specific queries (field:value)
func (qp *QueryProcessor) parseFieldQuery(queryString string) (Query, error) {
	parts := strings.SplitN(queryString, ":", 2)
	if len(parts) != 2 {
		return qp.ParseAdvancedQuery(strings.ReplaceAll(queryString, ":", " "))
	}

	field := strings.TrimSpace(parts[0])
	value := strings.TrimSpace(parts[1])

	if field == "" || value == "" {
		return nil, fmt.Errorf("invalid field query format")
	}

	return FieldQuery{Field: field, Value: value}, nil
}

// parseWildcardQuery handles wildcard queries
func (qp *QueryProcessor) parseWildcardQuery(queryString string) (Query, error) {
	// Extract field and pattern if field-specific
	if strings.Contains(queryString, ":") {
		parts := strings.SplitN(queryString, ":", 2)
		if len(parts) == 2 {
			field := strings.TrimSpace(parts[0])
			pattern := strings.TrimSpace(parts[1])
			return WildcardQuery{Field: field, Pattern: pattern}, nil
		}
	}

	// Default wildcard query across all fields
	return WildcardQuery{Field: "", Pattern: queryString}, nil
}

// containsBooleanOperators checks if query contains boolean operators
func (qp *QueryProcessor) containsBooleanOperators(queryString string) bool {
	upperQuery := strings.ToUpper(queryString)
	return strings.Contains(upperQuery, " AND ") ||
		strings.Contains(upperQuery, " OR ") ||
		strings.Contains(upperQuery, " NOT ")
}

// tokenize tokenizes a query string
func (qp *QueryProcessor) tokenize(text string) []string {
	var tokens []string

	if qp.enableStemming {
		tokens = utils.TokenizeWithStemming(text)
	} else {
		tokens = utils.Tokenize(text)
	}

	// Apply synonym expansion
	var expandedTokens []string
	for _, token := range tokens {
		if !qp.isStopWord(token) {
			expandedTokens = append(expandedTokens, token)
			if synonyms, ok := qp.synonyms[token]; ok {
				expandedTokens = append(expandedTokens, synonyms...)
			}
		}
	}

	return expandedTokens
}

// isStopWord checks if a word is a stop word
func (qp *QueryProcessor) isStopWord(word string) bool {
	return qp.stopWords[utils.ToLower(word)]
}

// BooleanQuery represents a boolean combination of queries
type BooleanQuery struct {
	Queries  []Query
	Operator string // "AND" or "OR"
}

// NewBooleanQuery creates a new boolean query
func NewBooleanQuery(queries []Query, operator string) BooleanQuery {
	return BooleanQuery{
		Queries:  queries,
		Operator: strings.ToUpper(operator),
	}
}

// Evaluate evaluates the boolean query
func (bq BooleanQuery) Evaluate(index *Index) []int64 {
	if len(bq.Queries) == 0 {
		return nil
	}

	if len(bq.Queries) == 1 {
		return bq.Queries[0].Evaluate(index)
	}

	result := bq.Queries[0].Evaluate(index)

	for i := 1; i < len(bq.Queries); i++ {
		queryResult := bq.Queries[i].Evaluate(index)

		switch bq.Operator {
		case "AND":
			result = utils.FastIntersect(result, queryResult)
		case "OR":
			result = utils.Union(result, queryResult)
		}

		if bq.Operator == "AND" && len(result) == 0 {
			break // Early termination for AND operations
		}
	}

	return utils.SortAndDedupe(result)
}

// Tokens returns all tokens from the boolean query
func (bq BooleanQuery) Tokens() []string {
	var allTokens []string
	for _, query := range bq.Queries {
		allTokens = append(allTokens, query.Tokens()...)
	}
	return allTokens
}

// NotQuery represents a NOT query
type NotQuery struct {
	Query Query
}

// NewNotQuery creates a new NOT query
func NewNotQuery(query Query) NotQuery {
	return NotQuery{Query: query}
}

// Evaluate evaluates the NOT query
func (nq NotQuery) Evaluate(index *Index) []int64 {
	// Get all document IDs
	var allDocs []int64
	index.documents.ForEach(func(docID int64, rec GenericRecord) bool {
		allDocs = append(allDocs, docID)
		return true
	})

	// Get documents that match the inner query
	matchingDocs := nq.Query.Evaluate(index)

	// Return documents that don't match
	return utils.Subtract(allDocs, matchingDocs)
}

// Tokens returns tokens from the inner query
func (nq NotQuery) Tokens() []string {
	return nq.Query.Tokens()
}

// RangeQuery represents a range query for numeric or date fields
type RangeQuery struct {
	Field string
	Min   interface{}
	Max   interface{}
}

// NewRangeQuery creates a new range query
func NewRangeQuery(field string, min, max interface{}) RangeQuery {
	return RangeQuery{
		Field: field,
		Min:   min,
		Max:   max,
	}
}

// Evaluate evaluates the range query
func (rq RangeQuery) Evaluate(index *Index) []int64 {
	var result []int64

	index.documents.ForEach(func(docID int64, rec GenericRecord) bool {
		if val, ok := rec[rq.Field]; ok {
			if rq.isInRange(val) {
				result = append(result, docID)
			}
		}
		return true
	})

	return result
}

// isInRange checks if a value is within the specified range
func (rq RangeQuery) isInRange(value interface{}) bool {
	// Convert values to float64 for comparison
	val, ok := rq.toFloat64(value)
	if !ok {
		return false
	}

	min, minOk := rq.toFloat64(rq.Min)
	max, maxOk := rq.toFloat64(rq.Max)

	if !minOk || !maxOk {
		return false
	}

	return val >= min && val <= max
}

// toFloat64 converts various types to float64
func (rq RangeQuery) toFloat64(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case string:
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f, true
		}
	}
	return 0, false
}

// Tokens returns an empty slice as range queries don't have text tokens
func (rq RangeQuery) Tokens() []string {
	return []string{}
}

// FuzzyQuery represents a fuzzy search query
type FuzzyQuery struct {
	Term      string
	Threshold int
	Field     string // Optional: search in specific field
}

// NewFuzzyQuery creates a new fuzzy query
func NewFuzzyQuery(term string, threshold int, field ...string) FuzzyQuery {
	fq := FuzzyQuery{
		Term:      term,
		Threshold: threshold,
	}
	if len(field) > 0 {
		fq.Field = field[0]
	}
	return fq
}

// Evaluate evaluates the fuzzy query
func (fq FuzzyQuery) Evaluate(index *Index) []int64 {
	if fq.Field != "" {
		return fq.evaluateFieldSpecific(index)
	}

	term := utils.ToLower(fq.Term)
	candidates := index.FuzzySearch(term, fq.Threshold)
	candidates = append(candidates, term)

	seen := make(map[int64]struct{})
	var result []int64

	for _, candidate := range candidates {
		postings := index.getPostings(candidate)
		for _, p := range postings {
			if _, exists := seen[p.DocID]; !exists {
				seen[p.DocID] = struct{}{}
				result = append(result, p.DocID)
			}
		}
	}

	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}

// evaluateFieldSpecific evaluates fuzzy search on a specific field
func (fq FuzzyQuery) evaluateFieldSpecific(index *Index) []int64 {
	var result []int64
	term := utils.ToLower(fq.Term)

	index.documents.ForEach(func(docID int64, rec GenericRecord) bool {
		if val, ok := rec[fq.Field]; ok {
			strVal := utils.ToLower(utils.ToString(val))
			tokens := utils.Tokenize(strVal)

			for _, token := range tokens {
				if utils.BoundedLevenshtein(term, token, fq.Threshold) <= fq.Threshold {
					result = append(result, docID)
					return true // Found match in this document
				}
			}
		}
		return true
	})

	return result
}

// Tokens returns the fuzzy term as a token
func (fq FuzzyQuery) Tokens() []string {
	return []string{utils.ToLower(fq.Term)}
}

// QueryBuilder provides a fluent interface for building complex queries
type QueryBuilder struct {
	queries   []Query
	operator  string
	processor *QueryProcessor
}

// NewQueryBuilder creates a new query builder
func NewQueryBuilder() *QueryBuilder {
	return &QueryBuilder{
		queries:   make([]Query, 0),
		operator:  "AND",
		processor: NewQueryProcessor(),
	}
}

// Term adds a term query
func (qb *QueryBuilder) Term(term string) *QueryBuilder {
	qb.queries = append(qb.queries, NewTermQuery(term, true, 1))
	return qb
}

// Phrase adds a phrase query
func (qb *QueryBuilder) Phrase(phrase string) *QueryBuilder {
	qb.queries = append(qb.queries, NewPhraseQuery(phrase, false, 0))
	return qb
}

// Field adds a field-specific query
func (qb *QueryBuilder) Field(field, value string) *QueryBuilder {
	qb.queries = append(qb.queries, FieldQuery{Field: field, Value: value})
	return qb
}

// Range adds a range query
func (qb *QueryBuilder) Range(field string, min, max interface{}) *QueryBuilder {
	qb.queries = append(qb.queries, NewRangeQuery(field, min, max))
	return qb
}

// Fuzzy adds a fuzzy query
func (qb *QueryBuilder) Fuzzy(term string, threshold int) *QueryBuilder {
	qb.queries = append(qb.queries, NewFuzzyQuery(term, threshold))
	return qb
}

// Wildcard adds a wildcard query
func (qb *QueryBuilder) Wildcard(field, pattern string) *QueryBuilder {
	qb.queries = append(qb.queries, WildcardQuery{Field: field, Pattern: pattern})
	return qb
}

// And sets the operator to AND
func (qb *QueryBuilder) And() *QueryBuilder {
	qb.operator = "AND"
	return qb
}

// Or sets the operator to OR
func (qb *QueryBuilder) Or() *QueryBuilder {
	qb.operator = "OR"
	return qb
}

// Build builds the final query
func (qb *QueryBuilder) Build() Query {
	if len(qb.queries) == 0 {
		return nil
	}

	if len(qb.queries) == 1 {
		return qb.queries[0]
	}

	return NewBooleanQuery(qb.queries, qb.operator)
}

// AutoCorrectQuery attempts to auto-correct common query mistakes
type AutoCorrectQuery struct {
	originalQuery  string
	correctedQuery Query
	suggestions    []string
}

// NewAutoCorrectQuery creates a new auto-correct query
func NewAutoCorrectQuery(query string, index *Index) *AutoCorrectQuery {
	acq := &AutoCorrectQuery{
		originalQuery: query,
		suggestions:   make([]string, 0),
	}

	// Simple auto-correction logic
	tokens := utils.Tokenize(query)
	correctedTokens := make([]string, len(tokens))

	for i, token := range tokens {
		correctedTokens[i] = acq.correctToken(token, index)
		if correctedTokens[i] != token {
			acq.suggestions = append(acq.suggestions, correctedTokens[i])
		}
	}

	correctedQueryString := strings.Join(correctedTokens, " ")
	if len(correctedTokens) == 1 {
		acq.correctedQuery = NewTermQuery(correctedTokens[0], true, 1)
	} else {
		acq.correctedQuery = NewPhraseQuery(correctedQueryString, true, 1)
	}

	return acq
}

// correctToken attempts to correct a single token
func (acq *AutoCorrectQuery) correctToken(token string, index *Index) string {
	index.RLock()
	defer index.RUnlock()

	// If token exists in index, return as-is
	if len(index.getPostings(token)) > 0 {
		return token
	}

	// Find closest match using Levenshtein distance
	bestMatch := token
	bestDistance := 3 // Maximum distance to consider

	index.rangePostings(func(indexedToken string, _ []Posting) bool {
		distance := utils.BoundedLevenshtein(token, indexedToken, bestDistance)
		if distance < bestDistance {
			bestDistance = distance
			bestMatch = indexedToken
		}
		return true
	})

	return bestMatch
}

// Evaluate evaluates the auto-corrected query
func (acq *AutoCorrectQuery) Evaluate(index *Index) []int64 {
	return acq.correctedQuery.Evaluate(index)
}

// Tokens returns tokens from the corrected query
func (acq *AutoCorrectQuery) Tokens() []string {
	return acq.correctedQuery.Tokens()
}

// GetSuggestions returns correction suggestions
func (acq *AutoCorrectQuery) GetSuggestions() []string {
	return acq.suggestions
}

// WasCorrected returns true if the query was corrected
func (acq *AutoCorrectQuery) WasCorrected() bool {
	return len(acq.suggestions) > 0
}
