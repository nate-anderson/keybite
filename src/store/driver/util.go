package driver

import (
	"keybite/util/log"
	"sort"
	"strconv"
	"strings"
)

// sort files based on the numeric value of the root of their filename
// used for sorting page files based on their name
func sortFileNames(fileNames []string, fileExtension string) []string {
	sort.Slice(fileNames, func(i, j int) bool {
		iClean := strings.TrimSuffix(fileNames[i], fileExtension)
		jClean := strings.TrimSuffix(fileNames[j], fileExtension)
		iNum, err := strconv.ParseUint(iClean, 10, 64)
		if err != nil {
			log.Warnf("data corruption warning: unexpected filename %s in index directory")
		}
		jNum, err := strconv.ParseUint(jClean, 10, 64)
		if err != nil {
			log.Warnf("data corruption warning: unexpected filename %s in index directory")
		}
		return iNum < jNum
	})
	return fileNames
}

// AddSuffixIfNotExist adds a suffix to a string unless it is already present
func AddSuffixIfNotExist(root, suff string) string {
	if !strings.HasSuffix(root, suff) {
		return root + suff
	}

	return root
}
