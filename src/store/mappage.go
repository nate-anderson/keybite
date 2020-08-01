package store

import (
	"errors"
	"fmt"
)

// MapPage is an easily transported relevant portion of a MapIndex
type MapPage struct {
	vals map[string]string
	name string
}

// EmptyMapPage returns an initialized empty map page. Does not create a file for the page.
func EmptyMapPage(name string) MapPage {
	vals := map[string]string{}
	return MapPage{
		name: name,
		vals: vals,
	}
}

// Query for value
func (m MapPage) Query(key string) (string, error) {
	val, ok := m.vals[key]
	if !ok {
		return "", fmt.Errorf("ID %d not found in this page", key)
	}

	return val, nil
}

// Add a value with a key
func (m MapPage) Add(key string, val string) (string, error) {
	_, exists := m.vals[key]
	if exists {
		return "", errors.New("cannot add key to map page: key exists")
	}
	m.vals[key] = val
	return key, nil
}

// Overwrite an existing value
func (m MapPage) Overwrite(key string, val string) error {
	_, exists := m.vals[key]
	if !exists {
		return errors.New("cannot update key in map page: key doesn't exist")
	}
	m.vals[key] = val
	return nil
}

// Upsert == idempotent insert
func (m MapPage) Upsert(key string, val string) string {
	m.vals[key] = val
	return key
}

// Delete an existing value
func (m MapPage) Delete(key string) error {
	_, exists := m.vals[key]
	if !exists {
		return fmt.Errorf("cannot delete key from map page: key doesn't exist")
	}
	delete(m.vals, key)
	return nil
}
