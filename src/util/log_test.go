package util_test

import (
	"bytes"
	"keybite/util"
	"testing"
)

func TestPrependString(t *testing.T) {
	pre := "[info]"
	str := "log message"
	exp := "[info] log message"
	act := util.PrependString(pre, str)
	util.Equals(t, exp, act)
}

func TestLogInfo(t *testing.T) {
	b := bytes.NewBufferString("")

	logger := util.NewLogger(util.LogLevelInfo, b)

	port := ":8000"
	logger.Infof("Serving HTTP on port %s", port)
	t.Log(b.String())
	t.Fail()
}
