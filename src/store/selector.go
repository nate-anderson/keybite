package store

import "strings"

// AutoSelector provides methods for making a selection from an auto index
type AutoSelector interface {
	Next() bool
	Select() uint64
	Length() int
}

// MapSelector provides methods for making a selection from a map index
type MapSelector interface {
	Next() bool
	Select() string
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
	return ArraySelector{ids: ids, current: ids[0]}
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
	if s.from < s.to {
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

// EmptySelector returns an empty selector for error returns
func EmptySelector() AutoSelector {
	return &SingleSelector{id: 0}
}

// MapSingleSelector makes a single selection from a map index
type MapSingleSelector struct {
	key  string
	used bool
}

// NewMapSingleSelector instantiates a new single selector from a key string
func NewMapSingleSelector(key string) MapSingleSelector {
	return MapSingleSelector{key: key}
}

// Next returns true once, because there's only one value
func (s *MapSingleSelector) Next() bool {
	if s.used {
		return false
	}
	s.used = true
	return true
}

// Select the selector's value
func (s MapSingleSelector) Select() string {
	return s.key
}

// Length denotes if this selection is a collection of multiple values, or a single value
func (s MapSingleSelector) Length() int {
	return 1
}

// MapArraySelector selects multiple keys from a map index
type MapArraySelector struct {
	keys    []string
	current string
	i       int
}

// NewMapArraySelector instantiates a new ArraySelector
func NewMapArraySelector(keys []string) MapArraySelector {
	cleanKeys := make([]string, len(keys))
	for i, key := range keys {
		cleanKeys[i] = strings.TrimSpace(key)
	}
	return MapArraySelector{keys: cleanKeys, current: keys[0]}
}

// Next returns a bool indicating whether there is a next value
func (s *MapArraySelector) Next() bool {
	if s.i < len(s.keys) {
		s.current = s.keys[s.i]
		s.i++
		return true
	}
	return false
}

// Select returns the selector's current value
func (s MapArraySelector) Select() string {
	return s.current
}

// Length denotes if this selection is a collection of multiple values, or a single value
func (s MapArraySelector) Length() int {
	return len(s.keys)
}
