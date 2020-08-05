package store

import (
	"keybite/store/driver"
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
		resultStrs := make([]string, 0, s.Length())
		var lastPageID uint64
		var page MapPage
		var loaded bool
		for i := 0; s.Next(); i++ {
			key := s.Select()
			hashAddr, err := HashStringToKey(key)
			if err != nil {
				log.Infof("error hashing string key %s :: %s", key, err.Error())
				continue
			}

			pageID := hashAddr / uint64(m.pageSize)
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

			resultStr, err := page.Query(key)
			if err != nil {
				log.Infof("error querying page %d for key %s :: %s", pageID, key, err.Error())
				continue
			}
			resultStrs = append(resultStrs, resultStr)
		}
		return CollectionResult(resultStrs), nil
	}

	// else return a single result
	key := s.Select()
	hashAddr, err := HashStringToKey(key)
	if err != nil {
		return EmptyResult(), err
	}

	pageID := hashAddr / uint64(m.pageSize)
	page, err := m.readPage(pageID)
	if err != nil {
		return
	}
	resultStr, err := page.Query(key)
	result = SingleResult(resultStr)
	return
}

// Insert value at key
func (m MapIndex) Insert(s MapSelector, value string) (Result, error) {
	// if there are multiple query selections, update all
	if s.Length() > 1 {
		updatedKeys := make([]string, 0, s.Length())
		var lastPageID uint64
		var page MapPage
		var loaded bool
		for i := 0; s.Next(); i++ {
			key := s.Select()
			hashAddr, err := HashStringToKey(key)
			if err != nil {
				log.Infof("error hashing key '%s' :: %s", key, err.Error())
				continue
			}

			pageID := hashAddr / uint64(m.pageSize)
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

			// insert the value into the loaded page
			_, err = page.Add(key, value)
			if err != nil {
				log.Infof("error inserting key %s into page %d :: %s", key, pageID, err.Error())
				continue
			}

			updatedKeys = append(updatedKeys, key)
		}

		// write the updated map to file, conscious of other requests
		err := m.writePage(page)
		if err != nil {
			log.Infof("error in locked page write: %s", err.Error())
		}
		return CollectionResult(updatedKeys), err
	}

	key := s.Select()
	hashAddr, err := HashStringToKey(key)
	if err != nil {
		return EmptyResult(), err
	}

	pageID := hashAddr / uint64(m.pageSize)
	page, err := m.readOrCreatePage(pageID)
	if err != nil {
		return EmptyResult(), err
	}

	_, err = page.Add(key, value)
	if err != nil {
		return EmptyResult(), err
	}

	err = m.writePage(page)

	return SingleResult(key), err
}

// Update existing data
func (m MapIndex) Update(s MapSelector, newValue string) (Result, error) {
	// if there are multiple query selections, update all
	if s.Length() > 1 {
		updatedKeys := make([]string, 0, s.Length())
		var lastPageID uint64
		var page MapPage
		var loaded bool
		for i := 0; s.Next(); i++ {
			key := s.Select()
			hashAddr, err := HashStringToKey(key)
			if err != nil {
				log.Infof("error hashing key '%s' :: %s", key, err.Error())
				continue
			}

			pageID := hashAddr / uint64(m.pageSize)
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
			err = page.Overwrite(key, newValue)
			if err != nil {
				log.Infof("error overwriting key %s in page %d :: %s", key, pageID, err.Error())
				continue
			}

			updatedKeys = append(updatedKeys, key)
		}

		// write the updated map to file, conscious of other requests
		err := m.writePage(page)
		if err != nil {
			log.Infof("error in locked page write: %s", err.Error())
		}
		return CollectionResult(updatedKeys), err
	}

	key := s.Select()
	id, err := HashStringToKey(key)
	if err != nil {
		return EmptyResult(), err
	}

	pageID := id / uint64(m.pageSize)
	page, err := m.readPage(pageID)
	if err != nil {
		return EmptyResult(), err
	}

	err = page.Overwrite(key, newValue)
	if err != nil {
		return EmptyResult(), err
	}

	err = m.writePage(page)

	return SingleResult(key), err
}

// Upsert inserts or modifies a value at the given key
func (m MapIndex) Upsert(s MapSelector, newValue string) (Result, error) {
	if s.Length() > 1 {
		upsertedKeys := make([]string, 0, s.Length())
		var lastPageID uint64
		var page MapPage
		var loaded bool
		for i := 0; s.Next(); i++ {
			key := s.Select()
			hashAddr, err := HashStringToKey(key)
			if err != nil {
				log.Infof("error hashing string key '%s' :: %s", key, err.Error())
				continue
			}
			pageID := hashAddr / uint64(m.pageSize)
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
			page.Upsert(key, newValue)
			upsertedKeys = append(upsertedKeys, key)
		}

		// write the updated map to file, conscious of other requests
		err := m.writePage(page)
		if err != nil {
			log.Infof("error in locked page write: %s", err.Error())
		}

		return CollectionResult(upsertedKeys), err
	}

	key := s.Select()
	hashAddr, err := HashStringToKey(key)
	if err != nil {
		log.Infof("error hashing string key '%s' :: %s", key, err.Error())
		return EmptyResult(), err
	}
	pageID := hashAddr / uint64(m.pageSize)
	page, err := m.readOrCreatePage(pageID)
	if err != nil {
		return EmptyResult(), err
	}

	page.Upsert(key, newValue)

	err = m.writePage(page)
	if err != nil {
		log.Infof("error in locked page write: %s", err.Error())
		return EmptyResult(), err
	}

	return SingleResult(key), err
}

// Delete an item from the map index
func (m MapIndex) Delete(s MapSelector) (Result, error) {
	if s.Length() > 1 {
		deletedKeys := make([]string, 0, s.Length())
		var lastPageID uint64
		var page MapPage
		var loaded bool
		var err error
		for i := 0; s.Next(); i++ {
			key := s.Select()
			hashAddr, err := HashStringToKey(key)
			if err != nil {
				log.Infof("error hashing string key '%s' :: %s", key, err.Error())
				continue
			}
			pageID := hashAddr / uint64(m.pageSize)
			if !loaded {
				page, err = m.readPage(pageID)
				if err != nil {
					log.Infof("error loading page %d :: %s", pageID, err.Error())
					continue
				}
				loaded = true
				lastPageID = pageID
				// if the loaded page does not contain the needed ID, write the changes to the current page
			} else if pageID != lastPageID {
				err := m.writePage(page)
				if err != nil {
					log.Info("error in locked page write: %s", err.Error())
					continue
				}
				// then load the correct page
				page, err = m.readPage(pageID)
				if err != nil {
					log.Infof("error loading page %d :: %s", pageID, err.Error())
					continue
				}
				lastPageID = pageID
			}

			// delete the value in the loaded page
			err = page.Delete(key)
			if err != nil {
				log.Infof("error deleting key %s in page %d :: %s", key, pageID, err.Error())
				continue
			}

			deletedKeys = append(deletedKeys, key)
		}

		// write the updated map to file
		err = m.writePage(page)
		if err != nil {
			log.Infof("error in locked page write: %s", err.Error())
			return EmptyResult(), err
		}
	}

	key := s.Select()
	hashAddr, err := HashStringToKey(key)
	if err != nil {
		log.Infof("error hashing string key '%s' :: %s", key, err.Error())
		return EmptyResult(), err
	}

	pageID := hashAddr / uint64(m.pageSize)
	page, err := m.readPage(pageID)
	if err != nil {
		return EmptyResult(), err
	}

	err = page.Delete(key)
	if err != nil {
		return EmptyResult(), err
	}

	err = m.writePage(page)
	if err != nil {
		return EmptyResult(), err
	}

	return SingleResult(key), nil
}

// WriteEmptyPage creates an empty page file for the specified page ID
func (m MapIndex) WriteEmptyPage(pageIDStr string) (MapPage, error) {
	fileName := pageIDStr
	mapPage := EmptyMapPage(fileName)
	err := m.driver.WriteMapPage(mapPage.vals, mapPage.name, m.Name)
	return mapPage, err
}
