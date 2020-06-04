package util_test

import (
	"keybite/util"
	"testing"
	"time"
)

func TestMakeTimestamp(t *testing.T) {
	now := time.Now()
	ts := util.MakeTimestamp()

	parsed := time.Unix(0, ts*int64(time.Millisecond))

	util.Equals(t, parsed.Hour(), now.Hour())
	util.Equals(t, parsed.Minute(), now.Minute())
	util.Equals(t, parsed.Weekday(), now.Weekday())
	util.Equals(t, parsed.Month(), now.Month())
	util.Equals(t, parsed.Day(), now.Day())
}
