package store

import (
	"fmt"
)

// Page is an easily transported relevant portion of an index
type Page struct {
	vals        map[uint64]string
	name        string
	minKey      uint64
	orderedKeys []uint64
}

// EmptyPage returns an initialized empty page. Does not create a file for the page
func EmptyPage(name string) Page {
	return Page{
		name:        name,
		vals:        map[uint64]string{},
		orderedKeys: []uint64{},
	}
}

// SetMinimumKey sets the minimum possible key for this page, useful when incrementing into a new page
func (p *Page) SetMinimumKey(minKey uint64) {
	p.minKey = minKey
}

// MaxKey returns the maximum key held by this map
func (p Page) MaxKey() uint64 {
	return MaxMapKey(p.vals)
}

// Query the page for ID
func (p Page) Query(id uint64) (string, error) {
	val, ok := p.vals[id]
	if !ok {
		return "", fmt.Errorf("ID %d not found in this page", id)
	}

	return val, nil
}

// Append a single value to this page and return the ID
func (p *Page) Append(val string) uint64 {
	// the insert ID should either be the greater of the current max key +1, and the minimum key set for this page
	id := Max((MaxMapKey(p.vals) + 1), p.minKey)
	p.vals[id] = val
	p.orderedKeys = append(p.orderedKeys, id)
	return id
}

// Overwrite value at id
func (p *Page) Overwrite(id uint64, newVal string) error {
	_, ok := p.vals[id]
	if !ok {
		return fmt.Errorf("cannot update non-existent record at id %d", id)
	}

	p.vals[id] = newVal
	return nil
}

// Delete an existing value
func (p Page) Delete(id uint64) error {
	_, exists := p.vals[id]
	if !exists {
		return fmt.Errorf("cannot delete id %d from page: key doesn't exist", id)
	}
	delete(p.vals, id)
	p.orderedKeys = removeUint64FromSlice(p.orderedKeys, id)
	return nil
}

// Length of the underlying map
func (p Page) Length() int {
	return len(p.vals)
}

func removeUint64FromSlice(slice []uint64, item uint64) []uint64 {
	for i, el := range slice {
		if el == item {
			return append(slice[:i], slice[i+1:]...)
		}
	}
	return slice
}
