package store

import (
	"bufio"
	"fmt"
	"os"
)

// Page is an easily transported relevant portion of an index
type Page struct {
	vals map[int64]string
	path string
}

// EmptyPage returns an initialized empty page. Does not create a file for the page
func EmptyPage(path string) Page {
	vals := map[int64]string{}
	return Page{
		path: path,
		vals: vals,
	}
}

// FileToPage returns a page populated with data from a file
func FileToPage(path string, pageSize int) (Page, error) {
	vals := make(map[int64]string, pageSize)
	pageFile, err := os.Open(path)
	defer pageFile.Close()
	if err != nil {
		return Page{}, err
	}

	page := Page{
		vals: vals,
		path: path,
	}
	scanner := bufio.NewScanner(pageFile)
	for scanner.Scan() {
		key, value, err := stringToKeyValue(scanner.Text())
		if err != nil {
			return Page{}, err
		}
		page.vals[key] = value
	}

	return page, nil
}

// Write the page to the specified file
func (p Page) Write() error {
	file, err := os.OpenFile(p.path, os.O_RDWR, 0755)
	defer file.Close()
	if err != nil {
		return err
	}

	for key, value := range p.vals {
		line := fmt.Sprintf("%d:%s\n", key, value)
		_, err = file.Write([]byte(line))
		if err != nil {
			return err
		}
	}

	return nil
}

// Query the page for ID
func (p Page) Query(id int64) (string, error) {
	val, ok := p.vals[id]
	if !ok {
		return "", fmt.Errorf("ID %d not found in this page", id)
	}

	return val, nil
}

// Append a single value to this page
func (p *Page) Append(val string) int64 {
	id := maxMapKey(p.vals) + 1
	p.vals[id] = val
	return id
}

// Overwrite value at id
func (p *Page) Overwrite(id int64, newVal string) error {
	_, ok := p.vals[id]
	if !ok {
		return fmt.Errorf("cannot update non-existant record at id %d", id)
	}

	p.vals[id] = newVal
	return nil
}
