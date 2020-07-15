package store

import (
	"errors"
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

// Add a value with a key
func (m MapPage) Add(id uint64, val string) (uint64, error) {
	_, exists := m.vals[id]
	if exists {
		return 0, errors.New("cannot add key to map page: key exists")
	}
	m.vals[id] = val
	return id, nil
}

// Overwrite an existing value
func (m MapPage) Overwrite(id uint64, val string) error {
	_, exists := m.vals[id]
	if !exists {
		return errors.New("cannot update key in map page: key doesn't exist")
	}
	m.vals[id] = val
	return nil
}

// Upsert == idempotent insert
func (m MapPage) Upsert(id uint64, val string) uint64 {
	m.vals[id] = val
	return id
}

// Delete an existing value
func (m MapPage) Delete(id uint64) error {
	_, exists := m.vals[id]
	if !exists {
		return fmt.Errorf("cannot delete key from map page: key doesn't exist")
	}
	delete(m.vals, id)
	return nil
}
