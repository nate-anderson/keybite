package driver

import (
	"fmt"
	"strconv"
)

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

	if len(parts[0]) == 0 {
		return "", "", fmt.Errorf("data invalid: contains empty key")
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
