package driver

import (
	"keybite/util"
	"strings"
	"testing"
)

func TestAddSuffixIfNotExist(t *testing.T) {
	suffix := "suf"
	ins := []string{"isuf", "i", "", "suf"}

	for _, in := range ins {
		out := addSuffixIfNotExist(in, suffix)
		util.Assert(
			t,
			strings.HasSuffix(out, suffix),
			"output %s should have suffix %s",
			out,
			suffix,
		)
	}
}
