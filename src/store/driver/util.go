package driver

import (
	"keybite/util/log"
	"sort"
	"strconv"
	"strings"
)

// sort files based on the numeric value of the root of their filename
// used for sorting page files based on their name
func sortFileNames(fileNames []string, fileExtension string, desc bool) []string {
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
		if desc {
			return iNum > jNum
		}
		return iNum < jNum
	})
	return fileNames
}

func escapeNewlines(in string) string {
	out := strings.ReplaceAll(in, "\r", `\\r`)
	out = strings.ReplaceAll(out, "\n", `\\n`)
	return out
}

func unescapeNewlines(in string) string {
	out := strings.ReplaceAll(in, `\\r`, `\r`)
	out = strings.ReplaceAll(out, `\\n`, `\n`)
	return out
}
