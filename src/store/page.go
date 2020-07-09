package store

import (
	"fmt"
	"keybite/util"
)

// Page is an easily transported relevant portion of an index
type Page struct {
	vals map[uint64]string
	name string
}

// EmptyPage returns an initialized empty page. Does not create a file for the page
func EmptyPage(name string) Page {
	vals := map[uint64]string{}
	return Page{
		name: name,
		vals: vals,
	}
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
	id := util.MaxMapKey(p.vals) + 1
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
