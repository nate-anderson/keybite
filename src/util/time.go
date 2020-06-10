package util

import (
	"strconv"
	"time"
)

// MakeTimestamp returns an int64 of current milliseconds
func MakeTimestamp() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

// ParseMillisString parses a string containing an integer milliseconds since epoch into time.Time
func ParseMillisString(millis string) (time.Time, error) {
	msInt, err := strconv.ParseInt(millis, 10, 64)
	if err != nil {
		return time.Time{}, err
	}

	return time.Unix(0, msInt*int64(time.Millisecond)), nil
}
