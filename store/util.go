package store

import (
	"errors"
	"fmt"
	"strconv"
)

// MaxKeyLength is the max length of a string that can be used as a key
const MaxKeyLength = 150

func stringToKeyValue(str string) (int64, string, error) {
	parts, err := splitOnFirst(str, ':')
	if err != nil || len(parts) != 2 {
		return 0, "", fmt.Errorf("cannot parse archive entry %s into key-value pair: separator ':' count != 0", str)
	}

	key, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, "", fmt.Errorf("cannot parse archive entry %s into key-value pair: key %v is not a valid int64", str, parts[0])
	}

	return key, parts[1], nil
}

func stringToMapKeyValue(str string) (uint64, string, error) {
	parts, err := splitOnFirst(str, ':')
	if err != nil || len(parts) != 2 {
		return 0, "", fmt.Errorf("cannot parse archive entry %s into key-value pair: separator ':' count != 0", str)
	}

	key, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return 0, "", fmt.Errorf("cannot parse archive entry %s into key-value pair: key %v is not a valid int64", str, parts[0])
	}

	return key, parts[1], nil
}

// splitOnFirst splits a string into two substrings after the first appearance of rune 'split'
func splitOnFirst(str string, split rune) ([]string, error) {
	for i, char := range str {
		if char == split {
			return []string{str[:i], str[(i + 1):]}, nil
		}
	}
	return []string{}, fmt.Errorf("provided string '%s' does not contain split character '%v'", str, split)

}

func maxMapKey(m map[int64]string) int64 {
	var maxNumber int64
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

// hashString to unique unsigned integer
// https://stackoverflow.com/a/16524816
func hashString(s string) (uint64, error) {
	pow := 27
	if len(s) > MaxKeyLength {
		return 0, errors.New("cannot hash string longer than 255 characters")
	}
	var result uint64 = 0
	for i, char := range s {
		result += ((uint64(MaxKeyLength) - uint64(i) - 1) ^ uint64(pow)) * (1 + uint64(char) - uint64('a'))
	}

	return result, nil
}
