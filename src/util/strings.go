package util

import (
	"path/filepath"
	"regexp"
	"strings"
)

// AddSuffixIfNotExist adds a suffix to a string unless it is already present
func AddSuffixIfNotExist(root, suff string) string {
	if !strings.HasSuffix(root, suff) {
		return root + suff
	}

	return root
}

// StripStringPrefixes removes n characters from each string in the given slice
func StripStringPrefixes(ss []string, n int) []string {
	results := make([]string, len(ss))
	for i, s := range ss {
		results[i] = s[n:]
	}
	return results
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
