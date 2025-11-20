package lookup

import (
	"strings"

	"github.com/oarkflow/lookup/utils"
)

// AnalyzerFunc allows plain functions to satisfy the Analyzer interface.
type AnalyzerFunc func(field string, value any) []Token

// Analyze implements Analyzer by invoking the wrapped function.
func (fn AnalyzerFunc) Analyze(field string, value any) []Token {
	return fn(field, value)
}

// SimpleAnalyzer provides a lightweight text pipeline with optional stemming and stop-word filtering.
type SimpleAnalyzer struct {
	enableStemming  bool
	stopWords       map[string]struct{}
	customTokenizer func(string) []string
}

// SimpleAnalyzerOption configures a SimpleAnalyzer.
type SimpleAnalyzerOption func(*SimpleAnalyzer)

// SimpleAnalyzerWithStemming toggles stemming support.
func SimpleAnalyzerWithStemming(enable bool) SimpleAnalyzerOption {
	return func(sa *SimpleAnalyzer) {
		sa.enableStemming = enable
	}
}

// SimpleAnalyzerWithStopWords overrides the default stop-word set.
func SimpleAnalyzerWithStopWords(words ...string) SimpleAnalyzerOption {
	return func(sa *SimpleAnalyzer) {
		sa.stopWords = make(map[string]struct{}, len(words))
		for _, w := range words {
			if w == "" {
				continue
			}
			sa.stopWords[strings.ToLower(w)] = struct{}{}
		}
	}
}

// SimpleAnalyzerWithTokenizer installs a custom tokenizer function.
func SimpleAnalyzerWithTokenizer(tokenizer func(string) []string) SimpleAnalyzerOption {
	return func(sa *SimpleAnalyzer) {
		sa.customTokenizer = tokenizer
	}
}

// NewSimpleAnalyzer returns a configured SimpleAnalyzer instance.
func NewSimpleAnalyzer(opts ...SimpleAnalyzerOption) *SimpleAnalyzer {
	sa := &SimpleAnalyzer{
		stopWords: make(map[string]struct{}, len(defaultStopWordList)),
	}
	for _, word := range defaultStopWordList {
		sa.stopWords[word] = struct{}{}
	}
	for _, opt := range opts {
		opt(sa)
	}
	return sa
}

// Analyze converts the provided value into normalized tokens.
func (sa *SimpleAnalyzer) Analyze(_ string, value any) []Token {
	text := strings.TrimSpace(utils.ToString(value))
	if text == "" {
		return nil
	}
	tokens := sa.tokenize(text)
	if len(tokens) == 0 {
		return nil
	}
	freq := make(map[string]int, len(tokens))
	for _, tok := range tokens {
		tok = strings.TrimSpace(tok)
		if tok == "" {
			continue
		}
		if _, skip := sa.stopWords[tok]; skip {
			continue
		}
		freq[tok]++
	}
	out := make([]Token, 0, len(freq))
	for term, count := range freq {
		out = append(out, Token{Term: term, Frequency: count})
	}
	return out
}

func (sa *SimpleAnalyzer) tokenize(text string) []string {
	lower := utils.ToLower(text)
	if sa.customTokenizer != nil {
		return sa.customTokenizer(lower)
	}
	if sa.enableStemming {
		return utils.TokenizeWithStemming(lower)
	}
	return utils.Tokenize(lower)
}

var defaultStopWordList = []string{
	"a", "an", "and", "are", "as", "at", "be", "by", "for", "from",
	"has", "he", "in", "is", "it", "its", "of", "on", "that", "the",
	"to", "was", "will", "with", "this", "but", "they", "have", "had",
	"what", "said", "each", "which", "she", "do", "how", "their",
}

var defaultAnalyzer Analyzer = NewSimpleAnalyzer()
