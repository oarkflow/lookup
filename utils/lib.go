package utils

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"reflect"
	"strconv"
	"strings"
	"unicode"

	"github.com/oarkflow/json"
)

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

func Tokenize(text string) []string {
	text = strings.ToLower(text)
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
			current[j] = min(
				current[j-1]+1,
				prev[j]+1,
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

func Compare(a, b any) int {
	switch aVal := a.(type) {
	case int:
		switch bVal := b.(type) {
		case int:
			return aVal - bVal
		case int32:
			return aVal - int(bVal)
		case int64:
			return int(int64(aVal) - bVal)
		case float32:
			return int(float64(aVal) - float64(bVal))
		case float64:
			return int(float64(aVal) - bVal)
		}
	case int32:
		switch bVal := b.(type) {
		case int:
			return int(aVal) - bVal
		case int32:
			return int(aVal - bVal)
		case int64:
			return int(int64(aVal) - bVal)
		case float32:
			return int(float64(aVal) - float64(bVal))
		case float64:
			return int(float64(aVal) - bVal)
		}
	case int64:
		switch bVal := b.(type) {
		case int:
			return int(aVal - int64(bVal))
		case int32:
			return int(aVal - int64(bVal))
		case int64:
			return int(aVal - bVal)
		case float32:
			return int(float64(aVal) - float64(bVal))
		case float64:
			return int(float64(aVal) - bVal)
		}
	case float32:
		switch bVal := b.(type) {
		case int:
			return int(float64(aVal) - float64(bVal))
		case int32:
			return int(float64(aVal) - float64(bVal))
		case int64:
			return int(float64(aVal) - float64(bVal))
		case float32:
			diff := aVal - bVal
			if diff < 0 {
				return -1
			} else if diff > 0 {
				return 1
			}
			return 0
		case float64:
			diff := float64(aVal) - bVal
			if diff < 0 {
				return -1
			} else if diff > 0 {
				return 1
			}
			return 0
		}
	case float64:
		switch bVal := b.(type) {
		case int:
			diff := aVal - float64(bVal)
			if diff < 0 {
				return -1
			} else if diff > 0 {
				return 1
			}
			return 0
		case int32:
			diff := aVal - float64(bVal)
			if diff < 0 {
				return -1
			} else if diff > 0 {
				return 1
			}
			return 0
		case int64:
			diff := aVal - float64(bVal)
			if diff < 0 {
				return -1
			} else if diff > 0 {
				return 1
			}
			return 0
		case float32:
			diff := aVal - float64(bVal)
			if diff < 0 {
				return -1
			} else if diff > 0 {
				return 1
			}
			return 0
		case float64:
			diff := aVal - bVal
			if diff < 0 {
				return -1
			} else if diff > 0 {
				return 1
			}
			return 0
		}
	case string:
		if bVal, ok := b.(string); ok {
			if aVal < bVal {
				return -1
			} else if aVal > bVal {
				return 1
			}
			return 0
		}
	}

	panic(fmt.Sprintf("unsupported compare types: %T and %T", a, b))
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
