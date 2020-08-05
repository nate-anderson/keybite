package store

import (
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

// MaxKeyLength is the max length of a string that can be used as a key
const MaxKeyLength = 150

// MaxMapKey returns the max integer key of a map
func MaxMapKey(m map[uint64]string) uint64 {
	var maxNumber uint64
	for maxNumber = range m {
		break
	}
	for n := range m {
		if n > maxNumber {
			maxNumber = n
		}
	}
	return maxNumber
}

// chars not allowed in map keys
var forbiddenKeyChar = regexp.MustCompile(`(:|\s+)`)

// HashStringToKey to unique unsigned integer. Strings that can be parses as integers are not hashed.
// https://stackoverflow.com/a/16524816
func HashStringToKey(s string) (uint64, error) {
	if num, err := strconv.ParseUint(s, 10, 64); err == nil {
		return num, nil
	}

	if forbiddenKeyChar.MatchString(s) {
		return 0, fmt.Errorf("cannot hash string containing whitespace or colon into a map key")
	}

	const pow = 27
	if len(s) > MaxKeyLength {
		return 0, fmt.Errorf("cannot hash string longer than %d characters", MaxKeyLength)
	}
	var result uint64 = 0
	for i, char := range s {
		result += ((uint64(MaxKeyLength) - uint64(i) - 1) ^ uint64(pow)) * (1 + uint64(char) - uint64('a'))
	}

	return result, nil
}

// PathToIndexPage splits a path into an index name and a file name
func PathToIndexPage(path string) (fileName string, indexName string, err error) {
	// strip leading slash if present
	cleanPath := path
	if strings.HasPrefix(path, "/") {
		cleanPath = path[1:]
	}

	tokens := strings.Split(cleanPath, "/")
	if len(tokens) > 2 {
		err = errors.New("PathToIndexPage error: path must be relative and look like 'index_name/file_name.kb'")
		return
	}
	indexName = tokens[0]
	fileName = tokens[1]
	return
}

// StripExtension drops file extension from a file name
func StripExtension(filename string) string {
	return strings.TrimSuffix(filename, filepath.Ext(filename))
}

var doubleQuotesRegex = regexp.MustCompile("\"")

// EscapeDoubleQuotes escapes all double quotes in a string
func EscapeDoubleQuotes(str string) string {
	return doubleQuotesRegex.ReplaceAllString(str, `\"`)
}

// Max returns the larger of x or y.
func Max(x, y uint64) uint64 {
	if x < y {
		return y
	}
	return x
}

// Min returns the smaller of x or y.
func Min(x, y uint64) uint64 {
	if x > y {
		return y
	}
	return x
}
