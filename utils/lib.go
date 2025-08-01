package utils

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"unicode"
	"unsafe"

	"github.com/oarkflow/json"
	"github.com/oarkflow/xid/wuid"
)

const (
	toLowerTable = "\x00\x01\x02\x03\x04\x05\x06\a\b\t\n\v\f\r\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f !\"#$%&'()*+,-./0123456789:;<=>?@abcdefghijklmnopqrstuvwxyz[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~\u007f\x80\x81\x82\x83\x84\x85\x86\x87\x88\x89\x8a\x8b\x8c\x8d\x8e\x8f\x90\x91\x92\x93\x94\x95\x96\x97\x98\x99\x9a\x9b\x9c\x9d\x9e\x9f\xa0\xa1\xa2\xa3\xa4\xa5\xa6\xa7\xa8\xa9\xaa\xab\xac\xad\xae\xaf\xb0\xb1\xb2\xb3\xb4\xb5\xb6\xb7\xb8\xb9\xba\xbb\xbc\xbd\xbe\xbf\xc0\xc1\xc2\xc3\xc4\xc5\xc6\xc7\xc8\xc9\xca\xcb\xcc\xcd\xce\xcf\xd0\xd1\xd2\xd3\xd4\xd5\xd6\xd7\xd8\xd9\xda\xdb\xdc\xdd\xde\xdf\xe0\xe1\xe2\xe3\xe4\xe5\xe6\xe7\xe8\xe9\xea\xeb\xec\xed\xee\xef\xf0\xf1\xf2\xf3\xf4\xf5\xf6\xf7\xf8\xf9\xfa\xfb\xfc\xfd\xfe\xff"
	toUpperTable = "\x00\x01\x02\x03\x04\x05\x06\a\b\t\n\v\f\r\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`ABCDEFGHIJKLMNOPQRSTUVWXYZ{|}~\u007f\x80\x81\x82\x83\x84\x85\x86\x87\x88\x89\x8a\x8b\x8c\x8d\x8e\x8f\x90\x91\x92\x93\x94\x95\x96\x97\x98\x99\x9a\x9b\x9c\x9d\x9e\x9f\xa0\xa1\xa2\xa3\xa4\xa5\xa6\xa7\xa8\xa9\xaa\xab\xac\xad\xae\xaf\xb0\xb1\xb2\xb3\xb4\xb5\xb6\xb7\xb8\xb9\xba\xbb\xbc\xbd\xbe\xbf\xc0\xc1\xc2\xc3\xc4\xc5\xc6\xc7\xc8\xc9\xca\xcb\xcc\xcd\xce\xcf\xd0\xd1\xd2\xd3\xd4\xd5\xd6\xd7\xd8\xd9\xda\xdb\xdc\xdd\xde\xdf\xe0\xe1\xe2\xe3\xe4\xe5\xe6\xe7\xe8\xe9\xea\xeb\xec\xed\xee\xef\xf0\xf1\xf2\xf3\xf4\xf5\xf6\xf7\xf8\xf9\xfa\xfb\xfc\xfd\xfe\xff"
)

func NewID() wuid.ID {
	return wuid.New()
}

// ToLower converts ascii string to lower-case
func ToLower(b string) string {
	res := make([]byte, len(b))
	copy(res, b)
	for i := 0; i < len(res); i++ {
		res[i] = toLowerTable[res[i]]
	}

	return UnsafeString(res)
}

// ToUpper converts ascii string to upper-case
func ToUpper(b string) string {
	res := make([]byte, len(b))
	copy(res, b)
	for i := 0; i < len(res); i++ {
		res[i] = toUpperTable[res[i]]
	}

	return UnsafeString(res)
}

// IfToLower returns an lowercase version of the input ASCII string.
//
// It first checks if the string contains any uppercase characters before converting it.
//
// For strings that are already lowercase,this function will be faster than `ToLower`.
//
// In the case of mixed-case or uppercase strings, this function will be slightly slower than `ToLower`.
func IfToLower(s string) string {
	hasUpper := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		if toLowerTable[c] != c {
			hasUpper = true
			break
		}
	}

	if !hasUpper {
		return s
	}
	return ToLower(s)
}

// IfToUpper returns an uppercase version of the input ASCII string.
//
// It first checks if the string contains any lowercase characters before converting it.
//
// For strings that are already uppercase,this function will be faster than `ToUpper`.
//
// In the case of mixed-case or lowercase strings, this function will be slightly slower than `ToUpper`.
func IfToUpper(s string) string {
	hasLower := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		if toUpperTable[c] != c {
			hasLower = true
			break
		}
	}

	if !hasLower {
		return s
	}
	return ToUpper(s)
}

// UnsafeString returns a string pointer without allocation
func UnsafeString(b []byte) string {
	// the new way is slower `return unsafe.String(unsafe.SliceData(b), len(b))`
	// unsafe.Pointer variant: 0.3538 ns/op vs unsafe.String variant: 0.5410 ns/op
	// #nosec G103
	return *(*string)(unsafe.Pointer(&b))
}

// UnsafeBytes returns a byte pointer without allocation.
func UnsafeBytes(s string) []byte {
	// #nosec G103
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

func Intersect(a, b []int64) []int64 {
	m := make(map[int64]bool)
	for _, id := range a {
		m[id] = true
	}
	var result []int64
	for _, id := range b {
		if m[id] {
			result = append(result, id)
		}
	}
	return result
}

func Union(a, b []int64) []int64 {
	m := make(map[int64]bool)
	for _, id := range a {
		m[id] = true
	}
	for _, id := range b {
		m[id] = true
	}
	var result []int64
	for id := range m {
		result = append(result, id)
	}
	return result
}

func Subtract(a, b []int64) []int64 {
	m := make(map[int64]bool)
	for _, id := range b {
		m[id] = true
	}
	var result []int64
	for _, id := range a {
		if !m[id] {
			result = append(result, id)
		}
	}
	return result
}

func Abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

func RowCount(filePath string) (int, error) {
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("Failed to open file: %v", err)
	}
	defer func() {
		_ = file.Close()
	}()
	reader := bufio.NewReader(file)
	dec := json.NewDecoder(reader)
	tok, err := dec.Token()
	if err != nil || tok != json.Delim('[') {
		return 0, fmt.Errorf("expected JSON array start")
	}
	count := 0
	for dec.More() {
		var v json.RawMessage
		if err := dec.Decode(&v); err != nil {
			return count, fmt.Errorf("decode error: %w", err)
		}
		count++
	}
	return count, nil
}

// Tokenize tokenizes a string and returns a slice of tokens
// It converts ASCII letters to lowercase without modifying the original string
func Tokenize(text string) []string {
	// Make a safe copy to avoid modifying the original string
	buf := make([]byte, len(text))
	copy(buf, UnsafeBytes(text))

	var tokens []string
	i := 0
	for i < len(buf) {
		// Skip delimiters. Here, valid characters are [A-Za-z0-9].
		// Any character outside this set is treated as a separator.
		for i < len(buf) {
			if isAlphaNum(buf[i]) {
				break
			}
			i++
		}
		// Mark the beginning of a token.
		start := i
		// Process token characters: convert to lowercase if needed.
		for i < len(buf) {
			b := buf[i]
			if !isAlphaNum(b) {
				break
			}
			// Fast in-place conversion for ASCII: if an uppercase letter, lower it.
			if b >= 'A' && b <= 'Z' {
				buf[i] = b + ('a' - 'A')
			}
			i++
		}
		// If we collected a token, append it.
		if start < i {
			// Create a new string from the processed bytes
			tokens = append(tokens, string(buf[start:i]))
		}
	}
	return tokens
}

// TokenizeUnicode tokenizes a string using Unicode-aware rules (letters, digits, underscores).
func TokenizeUnicode(text string) []string {
	var tokens []string
	var sb strings.Builder
	for _, r := range text {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' {
			sb.WriteRune(unicode.ToLower(r))
		} else {
			if sb.Len() > 0 {
				tokens = append(tokens, sb.String())
				sb.Reset()
			}
		}
	}
	if sb.Len() > 0 {
		tokens = append(tokens, sb.String())
	}
	return tokens
}

// TokenizeWithStemming tokenizes text and applies simple English stemming
func TokenizeWithStemming(text string) []string {
	tokens := Tokenize(text)
	for i, token := range tokens {
		tokens[i] = SimpleStem(token)
	}
	return tokens
}

// SimpleStem applies basic English stemming rules
func SimpleStem(word string) string {
	if len(word) <= 3 {
		return word
	}

	// Remove common suffixes
	suffixes := []struct {
		suffix, replacement string
		minLen              int
	}{
		{"ying", "", 5},   // running -> runn -> run (will be handled by next rule)
		{"ing", "", 4},    // running -> run
		{"ly", "", 5},     // quickly -> quick
		{"ed", "", 4},     // worked -> work
		{"ies", "y", 4},   // flies -> fly
		{"ied", "y", 4},   // tried -> try
		{"ried", "ry", 5}, // carried -> carry
		{"s", "", 3},      // cats -> cat (but not "as" -> "a")
	}

	for _, rule := range suffixes {
		if len(word) >= rule.minLen && strings.HasSuffix(word, rule.suffix) {
			stemmed := word[:len(word)-len(rule.suffix)] + rule.replacement
			// Ensure we don't create words that are too short
			if len(stemmed) >= 2 {
				// Special case for -ing: if it creates a double consonant, remove one
				if rule.suffix == "ing" && len(stemmed) > 2 {
					last := stemmed[len(stemmed)-1]
					secondLast := stemmed[len(stemmed)-2]
					if last == secondLast && last != 'l' && last != 's' && last != 'z' {
						return stemmed[:len(stemmed)-1]
					}
				}
				return stemmed
			}
		}
	}
	return word
}

// isAlphaNum returns true if b is an ASCII letter or digit.
func isAlphaNum(b byte) bool {
	return (b >= 'a' && b <= 'z') ||
		(b >= 'A' && b <= 'Z') ||
		(b >= '0' && b <= '9') || b == '_'
}

func TokenizeOld(text string) []string {
	text = ToLower(text)
	var sb strings.Builder
	for _, r := range text {

		if unicode.IsLetter(r) || unicode.IsDigit(r) || unicode.IsSpace(r) {
			sb.WriteRune(r)
		} else {
			sb.WriteRune(' ')
		}
	}
	return strings.Fields(sb.String())
}

func BoundedLevenshtein(a, b string, threshold int) int {
	la, lb := len(a), len(b)
	if Abs(la-lb) > threshold {
		return threshold + 1
	}
	prev := make([]int, lb+1)
	for j := 0; j <= lb; j++ {
		prev[j] = j
	}
	for i := 1; i <= la; i++ {
		current := make([]int, lb+1)
		current[0] = i
		minVal := current[0]
		for j := 1; j <= lb; j++ {
			cost := 0
			if a[i-1] != b[j-1] {
				cost = 1
			}
			// Fix: use separate min calls instead of three arguments
			current[j] = min(
				min(current[j-1]+1, prev[j]+1),
				prev[j-1]+cost,
			)
			if current[j] < minVal {
				minVal = current[j]
			}
		}
		if minVal > threshold {
			return threshold + 1
		}
		prev = current
	}
	if prev[lb] > threshold {
		return threshold + 1
	}
	return prev[lb]
}

// Compare returns the comparison result and an error if types are unsupported.
func Compare(a, b any) (int, error) {
	switch aVal := a.(type) {
	case int:
		switch bVal := b.(type) {
		case int:
			return aVal - bVal, nil
		case int32:
			return aVal - int(bVal), nil
		case int64:
			return int(int64(aVal) - bVal), nil
		case float32:
			return int(float64(aVal) - float64(bVal)), nil
		case float64:
			return int(float64(aVal) - bVal), nil
		}
	case int32:
		switch bVal := b.(type) {
		case int:
			return int(aVal) - bVal, nil
		case int32:
			return int(aVal - bVal), nil
		case int64:
			return int(int64(aVal) - bVal), nil
		case float32:
			return int(float64(aVal) - float64(bVal)), nil
		case float64:
			return int(float64(aVal) - bVal), nil
		}
	case int64:
		switch bVal := b.(type) {
		case int:
			return int(aVal - int64(bVal)), nil
		case int32:
			return int(aVal - int64(bVal)), nil
		case int64:
			return int(aVal - bVal), nil
		case float32:
			return int(float64(aVal) - float64(bVal)), nil
		case float64:
			return int(float64(aVal) - bVal), nil
		}
	case float32:
		switch bVal := b.(type) {
		case int:
			return int(float64(aVal) - float64(bVal)), nil
		case int32:
			return int(float64(aVal) - float64(bVal)), nil
		case int64:
			return int(float64(aVal) - float64(bVal)), nil
		case float32:
			diff := aVal - bVal
			if diff < 0 {
				return -1, nil
			} else if diff > 0 {
				return 1, nil
			}
			return 0, nil
		case float64:
			diff := float64(aVal) - bVal
			if diff < 0 {
				return -1, nil
			} else if diff > 0 {
				return 1, nil
			}
			return 0, nil
		}
	case float64:
		switch bVal := b.(type) {
		case int:
			diff := aVal - float64(bVal)
			if diff < 0 {
				return -1, nil
			} else if diff > 0 {
				return 1, nil
			}
			return 0, nil
		case int32:
			diff := aVal - float64(bVal)
			if diff < 0 {
				return -1, nil
			} else if diff > 0 {
				return 1, nil
			}
			return 0, nil
		case int64:
			diff := aVal - float64(bVal)
			if diff < 0 {
				return -1, nil
			} else if diff > 0 {
				return 1, nil
			}
			return 0, nil
		case float32:
			diff := aVal - float64(bVal)
			if diff < 0 {
				return -1, nil
			} else if diff > 0 {
				return 1, nil
			}
			return 0, nil
		case float64:
			diff := aVal - bVal
			if diff < 0 {
				return -1, nil
			} else if diff > 0 {
				return 1, nil
			}
			return 0, nil
		}
	case string:
		if bVal, ok := b.(string); ok {
			if aVal < bVal {
				return -1, nil
			} else if aVal > bVal {
				return 1, nil
			}
			return 0, nil
		}
	}
	return 0, fmt.Errorf("unsupported compare types: %T and %T", a, b)
}

// MustCompare panics on unsupported types (legacy behavior).
func MustCompare(a, b any) int {
	cmp, err := Compare(a, b)
	if err != nil {
		panic(err)
	}
	return cmp
}

func ToFloat(val any) (float64, bool) {
	switch v := val.(type) {
	case float64:
		return v, true
	case int:
		return float64(v), true
	case string:
		if parsed, err := strconv.ParseFloat(v, 64); err == nil {
			return parsed, true
		}
	case json.Number:
		parsed, err := v.Float64()
		if err == nil {
			return parsed, true
		}
	default:
		fmt.Println(reflect.TypeOf(v), v, "not supported")
	}
	return 0, false
}

func ToString(val any) string {
	switch val := val.(type) {
	case string:
		return val
	case []byte:
		return string(val)
	case int, int32, int64, int8, int16, uint, uint32, uint64, uint8, uint16:
		return fmt.Sprintf("%d", val)
	case float32:
		buf := make([]byte, 0, 32)
		buf = strconv.AppendFloat(buf, float64(val), 'f', -1, 64)
		return string(buf)
	case float64:
		buf := make([]byte, 0, 32)
		buf = strconv.AppendFloat(buf, val, 'f', -1, 64)
		return string(buf)
	case bool:
		if val {
			return "true"
		}
		return "false"
	default:
		return fmt.Sprintf("%v", val)
	}
}

// FastIntersect performs intersection on sorted slices using two-pointer technique
func FastIntersect(a, b []int64) []int64 {
	if len(a) == 0 || len(b) == 0 {
		return nil
	}

	result := make([]int64, 0, min(len(a), len(b)))
	i, j := 0, 0

	for i < len(a) && j < len(b) {
		if a[i] == b[j] {
			result = append(result, a[i])
			i++
			j++
		} else if a[i] < b[j] {
			i++
		} else {
			j++
		}
	}
	return result
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// max returns the maximum of two integers
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// RemoveDuplicatesInt64 removes duplicates from a sorted slice
func RemoveDuplicatesInt64(slice []int64) []int64 {
	if len(slice) <= 1 {
		return slice
	}

	result := make([]int64, 1, len(slice))
	result[0] = slice[0]

	for i := 1; i < len(slice); i++ {
		if slice[i] != slice[i-1] {
			result = append(result, slice[i])
		}
	}
	return result
}

// SortAndDedupe sorts and removes duplicates from a slice
func SortAndDedupe(slice []int64) []int64 {
	if len(slice) <= 1 {
		return slice
	}

	sort.Slice(slice, func(i, j int) bool { return slice[i] < slice[j] })
	return RemoveDuplicatesInt64(slice)
}
