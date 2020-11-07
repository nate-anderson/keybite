package driver

import (
	"strings"
)

// addSuffixIfNotExist adds a suffix to a string unless it is already present
func addSuffixIfNotExist(root, suff string) string {
	if !strings.HasSuffix(root, suff) {
		return root + suff
	}

	return root
}
