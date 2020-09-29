package util

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
	"time"
)

// testing functions borrowed from
// https://github.com/benbjohnson/testing

// Assert fails the test if the condition is false.
func Assert(tb testing.TB, condition bool, msg string, v ...interface{}) {
	if !condition {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("%s:%d: "+msg+"\n\n", append([]interface{}{filepath.Base(file), line}, v...)...)
		tb.Fail()
	}
}

// Ok fails the test if an err is not nil.
func Ok(tb testing.TB, err error) {
	if err != nil {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("%s:%d: unexpected error: %s\n\n", filepath.Base(file), line, err.Error())
		tb.Fail()
	}
}

// Equals fails the test if exp is not equal to act.
func Equals(tb testing.TB, exp, act interface{}) {
	if !reflect.DeepEqual(exp, act) {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("%s:%d:\n\n\texp: %#v (%T)\n\n\tgot: %#v (%T)\n\n", filepath.Base(file), line, exp, exp, act, act)
		tb.Fail()
	}
}

// StrSliceContains string slice contains
func StrSliceContains(str string, sl []string) bool {
	for _, el := range sl {
		if el == str {
			return true
		}
	}
	return false
}

// Uint64SliceContains uint64 slice contains
func Uint64SliceContains(i uint64, sl []uint64) bool {
	for _, el := range sl {
		if el == i {
			return true
		}
	}
	return false
}

// RepeatString repeats a string n times in a slice
func RepeatString(str string, n int) (result []string) {
	for i := 0; i < n; i++ {
		result = append(result, str)
	}
	return
}

var seededRand *rand.Rand = rand.New(
	rand.NewSource(time.Now().UnixNano()))

// RandStringFrom generates a string of length n from charset
// borrowed from https://www.calhoun.io/creating-random-strings-in-go/
func RandStringFrom(n int, charset string) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}

// CharsetAlphaNum is a character set for generating alphanumeric random strings
const CharsetAlphaNum = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890 "
