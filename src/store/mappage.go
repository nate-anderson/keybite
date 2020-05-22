package store

import (
	"fmt"
)

// MapPage is an easily transported relevant portion of a MapIndex
type MapPage struct {
	vals map[uint64]string
	name string
}

// EmptyMapPage returns an initialized empty map page. Does not create a file for the page.
func EmptyMapPage(name string) MapPage {
	vals := map[uint64]string{}
	return MapPage{
		name: name,
		vals: vals,
	}
}

// Query for value
func (m MapPage) Query(id uint64) (string, error) {
	val, ok := m.vals[id]
	if !ok {
		return "", fmt.Errorf("ID %d not found in this page", id)
	}

	return val, nil
}

// Set a value to a key
func (m MapPage) Set(id uint64, val string) uint64 {
	m.vals[id] = val
	return id
}
