package store

import (
	"keybite/util"
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

// EmptySelector returns an empty selector for error returns
func EmptySelector() Selector {
	return &SingleSelector{id: 0}
}
