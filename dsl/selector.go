package dsl

import (
	"fmt"
	"keybite/store"
	"strconv"
	"strings"
)

/*
Tools for parsing selectors
*/

// ParseAutoSelector parses a string into a selector. Acceptable formats are 6, [6:10], [6, 7, 8]
func ParseAutoSelector(token string) (store.AutoSelector, error) {
	if token[0] == '[' {
		body, err := StripBrackets(token)
		if err != nil {
			return &store.ArraySelector{}, fmt.Errorf("invalid auto selector: %s", err.Error())
		}
		// array
		if strings.Contains(body, ",") {
			collection, err := parseCollection(body)
			if err != nil {
				return store.EmptySelector(), err
			}
			selector := store.NewArraySelector(collection)
			return &selector, err
		}
		// range
		if strings.Contains(body, ":") {
			min, max, err := parseRange(body)
			if err != nil {
				return store.EmptySelector(), err
			}
			selector := store.NewRangeSelector(min, max)
			return &selector, nil
		}
	}

	selected, err := strconv.ParseUint(token, 10, 64)
	selector := store.NewSingleSelector(selected)
	return &selector, err
}

// ParseMapSelector parses a selection of map keys
func ParseMapSelector(token string) (store.MapSelector, error) {
	// if selection resembles an array, try to create an array selector
	if token[0] == '[' {
		body, err := StripBrackets(token)
		if err != nil {
			return &store.MapArraySelector{}, fmt.Errorf("invalid map selector: %s", err.Error())
		}
		collection := strings.Split(body, ",")
		selector := store.NewMapArraySelector(collection)
		return &selector, nil
	}

	// else treat it as a single selection
	selector := store.NewMapSingleSelector(token)
	return &selector, nil
}

// StripBrackets removes surrounding square brackets
func StripBrackets(token string) (string, error) {
	if !strings.HasSuffix(token, "]") {
		return token, fmt.Errorf("closing bracket expected")
	}
	return strings.TrimPrefix(
		strings.TrimSuffix(token, "]"),
		"[",
	), nil
}

// [6,7,8]
func parseCollection(token string) ([]uint64, error) {
	strs := strings.Split(token, ",")
	vals := make([]uint64, len(strs))
	for i, str := range strs {
		id, err := strconv.ParseUint(str, 10, 64)
		if err != nil {
			return vals, fmt.Errorf("error parsing query selector '%s': %w", token, err)
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
