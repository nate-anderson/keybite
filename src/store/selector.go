package store

import (
	"fmt"
	"keybite/util"
	"strconv"
	"strings"
)

// Selector provides methods for making a selection from an index
type Selector interface {
	Next() bool
	Select() uint64
	Length() int
}

// ArraySelector allows selecting an arbitrary collection of IDs
type ArraySelector struct {
	ids     []uint64
	current uint64
	i       int
}

// NewArraySelector instantiates a new ArraySelector
func NewArraySelector(ids []uint64) ArraySelector {
	return ArraySelector{ids: ids}
}

// Next returns a bool indicating whether there is a next value
func (s *ArraySelector) Next() bool {
	if s.i < len(s.ids) {
		s.current = s.ids[s.i]
		s.i++
		return true
	}
	return false
}

// Select returns the selector's current value
func (s ArraySelector) Select() uint64 {
	return s.current
}

// Length denotes if this selection is a collection of multiple values, or a single value
func (s ArraySelector) Length() int {
	return len(s.ids)
}

// RangeSelector allows selecting a range of values from start to end
type RangeSelector struct {
	to   uint64
	from uint64
}

// NewRangeSelector instantiates a new RangeSelector
func NewRangeSelector(min, max uint64) RangeSelector {
	return RangeSelector{from: min, to: max}
}

// Next indicates whether the selector has more values
func (s *RangeSelector) Next() bool {
	if s.from <= s.to {
		s.from++
		return true
	}
	return false
}

// Select the selector's value
func (s RangeSelector) Select() uint64 {
	return s.from - 1
}

// Length denotes if this selection is a collection of multiple values, or a single value
func (s RangeSelector) Length() int {
	return int(s.to - s.from)
}

// SingleSelector selects a single ID
type SingleSelector struct {
	id   uint64
	used bool
}

// NewSingleSelector instantiates a new SingleSelector
func NewSingleSelector(id uint64) SingleSelector {
	return SingleSelector{id: id}
}

// NewMapSingleSelector instantiates a new single selector from a key string
func NewMapSingleSelector(key string) (SingleSelector, error) {
	id, err := util.HashString(key)
	if err != nil {
		return SingleSelector{}, err
	}
	return NewSingleSelector(id), nil
}

// Next returns true once, because there's only one value
func (s *SingleSelector) Next() bool {
	if s.used {
		return false
	}
	s.used = true
	return true
}

// Select the selector's value
func (s SingleSelector) Select() uint64 {
	return s.id
}

// Length denotes if this selection is a collection of multiple values, or a single value
func (s SingleSelector) Length() int {
	return 1
}

// ParseSelector parses a string into a selector. Acceptable formats are 6, [6:10], [6, 7, 8]
func ParseSelector(token string) (Selector, error) {
	if token[0] == '[' {
		body := StripBrackets(token)
		if strings.Contains(body, ",") {
			collection, err := parseCollection(body)
			return &ArraySelector{ids: collection}, err
		}
		if strings.Contains(body, ":") {
			min, max, err := parseRange(body)
			return &RangeSelector{to: max, from: min}, err
		}
	}

	selected, err := strconv.ParseUint(token, 10, 64)
	return &SingleSelector{id: selected}, err
}

// StripBrackets removes surrounding square brackets
func StripBrackets(token string) string {
	return strings.TrimPrefix(
		strings.TrimSuffix(token, "]"),
		"[",
	)
}

// [6,7,8]
func parseCollection(token string) ([]uint64, error) {
	strs := strings.Split(token, ",")
	vals := make([]uint64, len(strs))
	for i, str := range strs {
		id, err := strconv.ParseUint(str, 10, 64)
		if err != nil {
			return vals, err
		}
		vals[i] = id
	}

	return vals, nil
}

// [1:3]
func parseRange(token string) (min uint64, max uint64, err error) {
	parts := strings.Split(token, ":")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid range selection: must specify min:max")
	}
	min, err = strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid range selection: min and max must be positive integers")
	}
	max, err = strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid range selection: min and max must be positive integers")
	}
	if max < min {
		return 0, 0, fmt.Errorf("invalid range: max must be >= min")
	}
	return
}

// NewMapArraySelector turns a slice of string keys into an ArraySelector
func NewMapArraySelector(keys []string) (ArraySelector, error) {
	ids := make([]uint64, len(keys))
	for i, key := range keys {
		id, err := util.HashString(key)
		if err != nil {
			return ArraySelector{}, err
		}
		ids[i] = id
	}
	return NewArraySelector(ids), nil
}
