package store

import (
	"fmt"
	"keybite/util"
)

// Page is an easily transported relevant portion of an index
type Page struct {
	vals   map[uint64]string
	name   string
	minKey uint64
}

// EmptyPage returns an initialized empty page. Does not create a file for the page
func EmptyPage(name string) Page {
	vals := map[uint64]string{}
	return Page{
		name: name,
		vals: vals,
	}
}

// SetMinimumKey sets the minimum possible key for this page, useful when incrementing into a new page
func (p *Page) SetMinimumKey(minKey uint64) {
	p.minKey = minKey
}

// MaxKey returns the maximum key held by this map
func (p Page) MaxKey() uint64 {
	return util.MaxMapKey(p.vals)
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
	id := util.Max((util.MaxMapKey(p.vals) + 1), p.minKey)
	p.vals[id] = val
	return id
}

// Overwrite value at id
func (p *Page) Overwrite(id uint64, newVal string) error {
	_, ok := p.vals[id]
	if !ok {
		return fmt.Errorf("cannot update non-existant record at id %d", id)
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
	return nil
}
