package store

import (
	"keybite-http/store/driver"
	"keybite-http/util"
	"os"
	"strconv"
)

// MapIndex is an index that acts like a map, mapping unique keys to values
type MapIndex struct {
	Name     string
	pageSize int
	driver   driver.StorageDriver
}

// NewMapIndex returns an index object, validating that index data exists in the data directory
func NewMapIndex(name string, driver driver.StorageDriver, pageSize int) (MapIndex, error) {
	return MapIndex{
		Name:     name,
		pageSize: pageSize,
		driver:   driver,
	}, nil
}

// readPage returns page with provided ID belonging to this index
func (m MapIndex) readPage(pageID uint64) (MapPage, error) {
	pageIDStr := strconv.FormatUint(pageID, 10)
	fileName := pageIDStr
	vals, err := m.driver.ReadMapPage(fileName, m.Name, m.pageSize)
	if err != nil {
		return MapPage{}, err
	}

	return MapPage{
		vals: vals,
		name: pageIDStr,
	}, nil
}

// readOrCreatePage reads or creates the map page for this page ID
func (m MapIndex) readOrCreatePage(pageID uint64) (MapPage, error) {
	p, err := m.readPage(pageID)
	if err == nil {
		return p, err
	}

	// if there is no page file with this name, create one
	if os.IsNotExist(err) {
		fileName := strconv.FormatUint(pageID, 10)
		return m.WriteEmptyPage(fileName)
	}

	// if there is another error type, return it
	return MapPage{}, err
}

// Query the MapIndex for the specified key
func (m MapIndex) Query(key string) (string, error) {
	id, err := util.HashString(key)
	if err != nil {
		return "", err
	}

	pageID := id / uint64(m.pageSize)
	page, err := m.readPage(pageID)
	if err != nil {
		return "", err
	}

	return page.Query(id)
}

// Insert value at key
func (m MapIndex) Insert(key string, value string) (string, error) {
	id, err := util.HashString(key)
	if err != nil {
		return "", err
	}

	pageID := id / uint64(m.pageSize)
	page, err := m.readOrCreatePage(pageID)
	if err != nil {
		return "", err
	}

	page.Set(id, value)
	err = m.driver.WriteMapPage(page.vals, page.name, m.Name)
	return key, err
}

// Update existing data
func (m MapIndex) Update(key string, newValue string) error {
	id, err := util.HashString(key)
	if err != nil {
		return err
	}

	pageID := id / uint64(m.pageSize)
	page, err := m.readPage(pageID)
	if err != nil {
		return err
	}

	page.Set(id, newValue)
	err = m.driver.WriteMapPage(page.vals, page.name, m.Name)
	return err
}

// WriteEmptyPage creates an empty page file for the specified page ID
func (m MapIndex) WriteEmptyPage(pageIDStr string) (MapPage, error) {
	fileName := pageIDStr
	mapPage := EmptyMapPage(fileName)
	err := m.driver.WriteMapPage(mapPage.vals, mapPage.name, m.Name)
	return mapPage, err

}
