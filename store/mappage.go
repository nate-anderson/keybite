package store

import (
	"bufio"
	"fmt"
	"os"
)

// MapPage is an easily transported relevant portion of a MapIndex
type MapPage struct {
	vals map[uint64]string
	path string
}

// EmptyMapPage returns an initialized empty map page. Does not create a file for the page.
func EmptyMapPage(path string) MapPage {
	vals := map[uint64]string{}
	return MapPage{
		path: path,
		vals: vals,
	}
}

// FileToMapPage reads a map file into a MapPage
func FileToMapPage(path string, pageSize int) (MapPage, error) {
	vals := map[uint64]string{}
	pageFile, err := os.Open(path)
	defer pageFile.Close()

	if err != nil {
		return MapPage{}, err
	}

	page := MapPage{
		vals: vals,
		path: path,
	}

	scanner := bufio.NewScanner(pageFile)
	for scanner.Scan() {
		key, value, err := stringToMapKeyValue(scanner.Text())
		if err != nil {
			return MapPage{}, err
		}
		page.vals[key] = value
	}

	return page, nil
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

// Write the page to the specified file
func (m MapPage) Write() error {
	file, err := os.OpenFile(m.path, os.O_RDWR, 0755)
	defer file.Close()

	if err != nil {
		return err
	}

	for key, value := range m.vals {
		line := fmt.Sprintf("%d:%s\n", key, value)
		_, err = file.Write([]byte(line))
		if err != nil {
			return err
		}
	}

	return nil
}
