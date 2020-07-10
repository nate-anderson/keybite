package store

import (
	"keybite/store/driver"
	"keybite/util"
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
	vals, err := m.driver.ReadMapPage(pageIDStr, m.Name, m.pageSize)
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
	if driver.IsNotExistError(err) {
		fileName := strconv.FormatUint(pageID, 10)
		return m.WriteEmptyPage(fileName)
	}

	// if there is another error type, return it
	return MapPage{}, err
}

// Query the MapIndex for the specified key
func (m MapIndex) Query(s Selector) (result Result, err error) {
	pageID := s.Select() / uint64(m.pageSize)

	page, err := m.readPage(pageID)
	if err != nil {
		return
	}
	resultStr, err := page.Query(s.Select())
	result = SingleResult(resultStr)
	return
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

	_, err = page.Add(id, value)
	if err != nil {
		return key, err
	}

	return wrapInMapWriteLock(m.driver, m.Name, func() (string, error) {
		writeErr := m.driver.WriteMapPage(page.vals, page.name, m.Name)
		return key, writeErr
	})
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

	_, err = page.Update(id, newValue)
	if err != nil {
		return err
	}

	_, err = wrapInMapWriteLock(m.driver, m.Name, func() (string, error) {
		writeErr := m.driver.WriteMapPage(page.vals, page.name, m.Name)
		return key, writeErr
	})

	return err
}

// Upsert inserts or modifies a value at the given key
func (m MapIndex) Upsert(key string, newValue string) error {
	id, err := util.HashString(key)
	if err != nil {
		return err
	}

	pageID := id / uint64(m.pageSize)
	page, err := m.readOrCreatePage(pageID)
	if err != nil {
		return err
	}

	page.Upsert(id, newValue)

	_, err = wrapInMapWriteLock(m.driver, m.Name, func() (string, error) {
		writeErr := m.driver.WriteMapPage(page.vals, page.name, m.Name)
		return key, writeErr
	})

	return err
}

// WriteEmptyPage creates an empty page file for the specified page ID
func (m MapIndex) WriteEmptyPage(pageIDStr string) (MapPage, error) {
	fileName := pageIDStr
	mapPage := EmptyMapPage(fileName)
	err := m.driver.WriteMapPage(mapPage.vals, mapPage.name, m.Name)
	return mapPage, err

}
