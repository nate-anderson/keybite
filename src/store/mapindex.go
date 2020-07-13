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
func (m MapIndex) Query(s MapSelector) (result Result, err error) {
	// if there are multiple query selections, return a collection result
	if s.Length() > 1 {
		resultStrs := make([]string, s.Length())
		for i := 0; s.Next(); i++ {
			id, err := util.HashString(s.Select())
			if err != nil {
				return EmptyResult(), err
			}

			pageID := id / uint64(m.pageSize)
			page, err := m.readPage(pageID)
			if err != nil {
				return EmptyResult(), err
			}
			resultStrs[i], err = page.Query(id)
			if err != nil {
				return EmptyResult(), err
			}
		}
		return CollectionResult(resultStrs), nil
	}

	// else return a single result
	id, err := util.HashString(s.Select())
	if err != nil {
		return EmptyResult(), err
	}

	pageID := id / uint64(m.pageSize)
	page, err := m.readPage(pageID)
	if err != nil {
		return
	}
	resultStr, err := page.Query(id)
	result = SingleResult(resultStr)
	return
}

// Insert value at key
func (m MapIndex) Insert(s MapSelector, value string) (string, error) {
	id, err := util.HashString(s.Select())
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
		return s.Select(), err
	}

	return wrapInMapWriteLock(m.driver, m.Name, func() (string, error) {
		writeErr := m.driver.WriteMapPage(page.vals, page.name, m.Name)
		return s.Select(), writeErr
	})
}

// Update existing data
func (m MapIndex) Update(s MapSelector, newValue string) (Result, error) {
	// if there are multiple query selections, update all
	if s.Length() > 1 {
		updatedIds := make([]string, s.Length())
		for i := 0; s.Next(); i++ {
			key := s.Select()
			id, err := util.HashString(key)
			if err != nil {
				return EmptyResult(), err
			}

			pageID := id / uint64(m.pageSize)
			page, err := m.readPage(pageID)
			if err != nil {
				return EmptyResult(), err
			}

			err = page.Overwrite(id, newValue)
			if err != nil {
				return EmptyResult(), err
			}
			updatedIds[i] = key

			_, err = wrapInMapWriteLock(m.driver, m.Name, func() (string, error) {
				writeErr := m.driver.WriteMapPage(page.vals, page.name, m.Name)
				return s.Select(), writeErr
			})
		}

		return CollectionResult(updatedIds), nil
	}

	key := s.Select()
	id, err := util.HashString(key)
	if err != nil {
		return EmptyResult(), err
	}

	pageID := id / uint64(m.pageSize)
	page, err := m.readPage(pageID)
	if err != nil {
		return EmptyResult(), err
	}

	err = page.Overwrite(id, newValue)
	if err != nil {
		return EmptyResult(), err
	}

	_, err = wrapInMapWriteLock(m.driver, m.Name, func() (string, error) {
		writeErr := m.driver.WriteMapPage(page.vals, page.name, m.Name)
		return s.Select(), writeErr
	})

	return SingleResult(key), err
}

// Upsert inserts or modifies a value at the given key
func (m MapIndex) Upsert(s MapSelector, newValue string) (Result, error) {
	key := s.Select()
	id, err := util.HashString(key)
	pageID := id / uint64(m.pageSize)
	page, err := m.readOrCreatePage(pageID)
	if err != nil {
		return EmptyResult(), err
	}

	page.Upsert(id, newValue)

	_, err = wrapInMapWriteLock(m.driver, m.Name, func() (string, error) {
		writeErr := m.driver.WriteMapPage(page.vals, page.name, m.Name)
		return s.Select(), writeErr
	})

	return SingleResult(key), err
}

// WriteEmptyPage creates an empty page file for the specified page ID
func (m MapIndex) WriteEmptyPage(pageIDStr string) (MapPage, error) {
	fileName := pageIDStr
	mapPage := EmptyMapPage(fileName)
	err := m.driver.WriteMapPage(mapPage.vals, mapPage.name, m.Name)
	return mapPage, err

}
