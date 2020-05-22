package util

import "strings"

// AddSuffix adds a suffix to a string unless it is already present
func AddSuffix(root, suff string) string {
	if !strings.HasSuffix(root, suff) {
		return root + suff
	}

	return root
}
