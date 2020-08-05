package driver

import (
	"strings"
)

// AddSuffixIfNotExist adds a suffix to a string unless it is already present
func AddSuffixIfNotExist(root, suff string) string {
	if !strings.HasSuffix(root, suff) {
		return root + suff
	}

	return root
}
