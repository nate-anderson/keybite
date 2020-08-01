package util

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// MaxKeyLength is the max length of a string that can be used as a key
const MaxKeyLength = 150

// StringToKeyValue converts a line of text to a key-value pair used to read a page file
func StringToKeyValue(str string) (uint64, string, error) {
	parts, err := SplitOnFirst(str, ':')
	if err != nil || len(parts) != 2 {
		return 0, "", fmt.Errorf("cannot parse archive entry %s into key-value pair: separator ':' count != 0", str)
	}

	key, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return 0, "", fmt.Errorf("cannot parse archive entry %s into key-value pair: key %v is not a valid int64", str, parts[0])
	}

	return key, parts[1], nil
}

// StringToMapKeyValue converts a line of text to a key-value pair used to read a map page file
func StringToMapKeyValue(str string) (string, string, error) {
	parts, err := SplitOnFirst(str, ':')
	if err != nil || len(parts) != 2 {
		return "", "", fmt.Errorf("cannot parse archive entry %s into key-value pair: separator ':' count != 0", str)
	}

	return parts[0], parts[1], nil
}

// SplitOnFirst splits a string into two substrings after the first appearance of rune 'split'
func SplitOnFirst(str string, split rune) ([]string, error) {
	for i, char := range str {
		if char == split {
			return []string{str[:i], str[(i + 1):]}, nil
		}
	}
	return []string{}, fmt.Errorf("provided string '%s' does not contain split character '%v'", str, split)

}

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
