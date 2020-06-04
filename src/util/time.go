package util

import "time"

// MakeTimestamp returns an int64 of current milliseconds
func MakeTimestamp() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}
