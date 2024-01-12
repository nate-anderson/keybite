package util

import (
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
)

// borrowed from
// https://github.com/benbjohnson/testing

// Assert fails the test if the condition is false.
func Assert(tb testing.TB, condition bool, msg string, v ...any) {
	if !condition {
		_, file, line, _ := runtime.Caller(1)
		tb.Errorf("%s:%d: "+msg+"\n\n", append([]any{filepath.Base(file), line}, v...)...)
	}
}

// Ok fails the test if an err is not nil.
func Ok(tb testing.TB, err error) {
	if err != nil {
		_, file, line, _ := runtime.Caller(1)
		tb.Errorf("%s:%d: unexpected error: %s\n\n", filepath.Base(file), line, err.Error())
	}
}

// Equals fails the test if exp is not equal to act.
func Equals[T comparable](tb testing.TB, exp, act T) {
	if !reflect.DeepEqual(exp, act) {
		_, file, line, _ := runtime.Caller(1)
		tb.Errorf("%s:%d:\n\n\texp: %#v (%T)\n\n\tgot: %#v (%T)\n\n", filepath.Base(file), line, exp, exp, act, act)
	}
}

// SliceContains asserts a slice contains a value
func SliceContains[T comparable](member T, sl []T) bool {
	for _, el := range sl {
		if el == member {
			return true
		}
	}
	return false
}

func IsNil(tb testing.TB, value any) {
	if value != nil {
		tb.Errorf("value %+v should be nil", value)
	}
}

// RepeatString repeats a string n times in a slice
func RepeatString(str string, n int) (result []string) {
	for i := 0; i < n; i++ {
		result = append(result, str)
	}
	return
}
