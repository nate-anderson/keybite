package store

import (
	"keybite/store/driver"
	"keybite/util"
	"keybite/util/log"
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

// write a map page to storage using a mutex for concurrency safety
func (m MapIndex) writePage(p MapPage) error {
	return wrapInWriteLock(m.driver, m.Name, func() error {
		return m.driver.WriteMapPage(p.vals, p.name, m.Name)
	})
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
		var lastPageID uint64
		var page MapPage
		var loaded bool
		for i := 0; s.Next(); i++ {
			key := s.Select()
			id, err := util.HashString(s.Select())
			if err != nil {
				log.Infof("error hashing string key %s :: %s", key, err.Error())
				continue
			}

			pageID := id / uint64(m.pageSize)
			// if the page housing the queried ID is different than the loaded page, or no page has been loaded
			// load the needed page
			if pageID != lastPageID || !loaded {
				page, err = m.readPage(pageID)
				if err != nil {
					log.Infof("error loading page %d :: %s", pageID, err.Error())
					continue
				}
				loaded = true
				lastPageID = pageID
			}

			resultStrs[i], err = page.Query(id)
			if err != nil {
				log.Infof("error querying page %d for ID %d (key '%s') :: %s", pageID, id, key, err.Error())
				continue
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
	key := s.Select()
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

	err = m.writePage(page)
	return key, err
}

// Update existing data
func (m MapIndex) Update(s MapSelector, newValue string) (Result, error) {
	// if there are multiple query selections, update all
	if s.Length() > 1 {
		updatedIds := make([]string, s.Length())
		var lastPageID uint64
		var page MapPage
		var loaded bool
		for i := 0; s.Next(); i++ {
			key := s.Select()
			id, err := util.HashString(key)
			if err != nil {
				log.Infof("error hashing key '%s' :: %s", key, err.Error())
				continue
			}

			pageID := id / uint64(m.pageSize)
			// if the page housing the update ID is different than the loaded page, or no page has been loaded yet,
			// load the needed page
			if !loaded {
				page, err = m.readPage(pageID)
				if err != nil {
					log.Infof("error loading page %d :: %s", pageID, err.Error())
					continue
				}
				loaded = true
				lastPageID = pageID
				// if a page has been loaded and a new page is needed, write the changes to the previous page first
			} else if pageID != lastPageID {
				err := m.writePage(page)
				if err != nil {
					log.Infof("error in locked page write: %s", err.Error())
					continue
				}

				// then load the next page
				page, err = m.readPage(pageID)
				if err != nil {
					log.Infof("error loading page %d :: %s", pageID, err.Error())
					continue
				}
				lastPageID = pageID
			}

			// update the value in the loaded page
			err = page.Overwrite(id, newValue)
			if err != nil {
				log.Infof("error overwriting ID %d in page %d :: %s", id, pageID, err.Error())
				continue
			}

			updatedIds[i] = key
		}

		// write the updated map to file, conscious of other requests
		err := m.writePage(page)
		if err != nil {
			log.Infof("error in locked page write: %s", err.Error())
		}
		return CollectionResult(updatedIds), err
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

	err = m.writePage(page)

	return SingleResult(key), err
}

// Upsert inserts or modifies a value at the given key
func (m MapIndex) Upsert(s MapSelector, newValue string) (Result, error) {
	if s.Length() > 1 {
		upsertedKeys := make([]string, s.Length())
		var lastPageID uint64
		var page MapPage
		var loaded bool
		for i := 0; s.Next(); i++ {
			key := s.Select()
			id, err := util.HashString(key)
			if err != nil {
				log.Infof("error hashing string key '%s' :: %s", key, err.Error())
				continue
			}
			pageID := id / uint64(m.pageSize)
			// if the page housing the update ID is different than the loaded page, or no page has been loaded yet,
			// load the needed page
			if !loaded {
				page, err = m.readOrCreatePage(pageID)
				if err != nil {
					log.Infof("error loading page %d :: %s", pageID, err.Error())
					continue
				}
				loaded = true
				lastPageID = pageID
			} else if pageID != lastPageID {
				err := m.writePage(page)
				if err != nil {
					log.Infof("error loading page %d :: %s", pageID, err.Error())
					continue
				}
				lastPageID = pageID
			}

			// update or insert value in loaded page
			page.Upsert(id, newValue)
			upsertedKeys[i] = key
		}

		// write the updated map to file, conscious of other requests
		err := m.writePage(page)
		if err != nil {
			log.Infof("error in locked page write: %s", err.Error())
		}

		return CollectionResult(upsertedKeys), err
	}

	key := s.Select()
	id, err := util.HashString(key)
	pageID := id / uint64(m.pageSize)
	page, err := m.readOrCreatePage(pageID)
	if err != nil {
		return EmptyResult(), err
	}

	page.Upsert(id, newValue)

	err = m.writePage(page)
	if err != nil {
		log.Infof("error in locked page write: %s", err.Error())
	}

	return SingleResult(key), err
}

// WriteEmptyPage creates an empty page file for the specified page ID
func (m MapIndex) WriteEmptyPage(pageIDStr string) (MapPage, error) {
	fileName := pageIDStr
	mapPage := EmptyMapPage(fileName)
	err := m.driver.WriteMapPage(mapPage.vals, mapPage.name, m.Name)
	return mapPage, err

}
