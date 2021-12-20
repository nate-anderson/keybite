package store

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// Result contains a single result or collection of results
type Result interface {
	String() string
	Valid() bool
}

// SingleResult contains a scalar query result
type SingleResult string

// NewIDSingleResult converts a uint64 ID into a SingleResult
func NewIDSingleResult(id uint64) SingleResult {
	idStr := strconv.FormatUint(id, 10)
	return SingleResult(idStr)
}

// MarshalJSON returns a JSON byte array representation of the result
func (r SingleResult) MarshalJSON() ([]byte, error) {
	if r == "" {
		return []byte("null"), nil
	}

	if json.Valid([]byte(r)) {
		return []byte(r), nil
	}

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
type CollectionResult []SingleResult

// NewCollectionResult creates a collection result from a string slice
func NewCollectionResult(strs []string) CollectionResult {
	collection := make(CollectionResult, len(strs))
	for i, str := range strs {
		collection[i] = SingleResult(str)
	}
	return collection
}

// String returns a string encoding of the result
func (r CollectionResult) String() string {
	out := make([]string, len(r))
	for i, res := range r {
		out[i] = string(res)
	}
	joined := strings.Join(out, ",")
	return fmt.Sprintf("[%s]", joined)
}

// Valid indicates whether the result was resolved successfully
func (r CollectionResult) Valid() bool {
	if len(r) > 0 {
		return true
	}
	return false
}

// EmptyResult returns a null result
func EmptyResult() SingleResult {
	return SingleResult("")
}

// ListItem interface is used to allow type enforcement for ListResult
type ListItem interface {
	keyValue() (string, string)
}

// AutoListItem is a named value in a list result with a uint64 key
type AutoListItem struct {
	Key   uint64 `json:"key"`
	Value string `json:"value"`
}

func (i AutoListItem) keyValue() (key string, value string) {
	key = strconv.FormatUint(i.Key, 10)
	value = i.Value
	return
}

// MapListItem is a named value in a list result with a string key
type MapListItem struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func (i MapListItem) keyValue() (key string, value string) {
	key = i.Key
	value = i.Value
	return
}

// ListResult contains the result of a list query
type ListResult []ListItem

// MarshalJSON returns a JSON byte array representation of the result
func (r ListResult) MarshalJSON() ([]byte, error) {
	return json.Marshal([]ListItem(r))
}

// String returns a string encoding of the result
func (r ListResult) String() string {
	var str string
	for _, item := range r {
		key, value := item.keyValue()
		str += fmt.Sprintf("%s\t%s\n", key, value)
	}
	return str
}

// Valid indicates whether the result was resolved successfully
func (r ListResult) Valid() bool {
	return len(r) > 0
}

// IDResult creates a single result of a uint64 ID
func IDResult(id uint64) SingleResult {
	idStr := strconv.FormatUint(id, 10)
	return SingleResult(idStr)
}
