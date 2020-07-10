package store

import (
	"encoding/json"
	"fmt"
	"strconv"
)

// Result contains a single or multiple result values
type Result struct {
	value  string
	values []string
	isArr  bool
	isNull bool
}

// MarshalJSON marshals the response into a JSON byte array
func (r Result) MarshalJSON() ([]byte, error) {
	if r.isArr {
		bytes, err := json.Marshal(r.values)
		if err != nil {
			return bytes, fmt.Errorf("error marshaling JSON from Result: %w", err)
		}
		return bytes, nil
	} else if r.isNull {
		return []byte(`null`), nil
	}
	return []byte(`"` + r.value + `"`), nil
}

// Valid indicates whether the result contains any values
func (r Result) Valid() bool {
	return r.value != "" || len(r.values) > 0
}

// NewNumberResult creates a new Result from a Uint64
func NewNumberResult(num uint64) Result {
	numStr := strconv.FormatUint(num, 10)
	return Result{value: numStr}
}

// NewNumberSliceResult creates a new Result from a slice of Uint64s
func NewNumberSliceResult(nums []uint64) Result {
	numStrs := make([]string, len(nums))
	for i, num := range nums {
		numStrs[i] = strconv.FormatUint(num, 10)
	}
	return Result{values: numStrs}
}

// NewStringResult creates a new Result from a string
func NewStringResult(str string) Result {
	return Result{value: str}
}

// NewStringSliceResult creates a new Result from a slice of strings
func NewStringSliceResult(strs []string) Result {
	return Result{values: strs}
}

// NewEmptyResult returns a result that will marshal to JSON null
func NewEmptyResult() Result {
	return Result{}
}
