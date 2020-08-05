package store

import (
	"encoding/json"
	"fmt"
	"keybite/util/log"
)

// Result contains a single result or collection of results
type Result interface {
	MarshalJSON() ([]byte, error)
	String() string
	Valid() bool
}

// SingleResult contains a scalar query result
type SingleResult string

// MarshalJSON returns a JSON byte array representation of the result
func (r SingleResult) MarshalJSON() ([]byte, error) {
	if r == "" {
		return []byte("null"), nil
	}

	if json.Valid([]byte(r)) {
		return []byte(r), nil
	}

	log.Debugf("marshaling JSON from '%s'", r)
	escaped := EscapeDoubleQuotes(string(r))

	return []byte(`"` + escaped + `"`), nil
}

// String returns a string encoding of the result
func (r SingleResult) String() string {
	return string(r)
}

// Valid indicates whether the result was resolved successfully
func (r SingleResult) Valid() bool {
	if len(r) > 0 {
		return true
	}
	return false
}

// CollectionResult contains an array of results
type CollectionResult []string

// MarshalJSON returns a JSON byte array representation of the result
func (r CollectionResult) MarshalJSON() ([]byte, error) {
	strs := []string(r)
	return json.Marshal(strs)
}

// String returns a string encoding of the result
func (r CollectionResult) String() string {
	return fmt.Sprint([]string(r))
}

// Valid indicates whether the result was resolved successfully
func (r CollectionResult) Valid() bool {
	if len(r) > 0 {
		return true
	}
	return false
}

// EmptyResult returns a null result
func EmptyResult() Result {
	return SingleResult("")
}
