package store

import (
	"fmt"
	"keybite/store/driver"
	"keybite/util/log"
	"strconv"
)

// AutoIndex is an auto-incrementing index
type AutoIndex struct {
	Name     string
	pageSize int
	driver   driver.StorageDriver
}

// NewAutoIndex returns an index object, validating that index data exists in the data directory
func NewAutoIndex(name string, driver driver.StorageDriver, pageSize int) (AutoIndex, error) {
	return AutoIndex{
		Name:     name,
		pageSize: pageSize,
		driver:   driver,
	}, nil
}

// readPage returns page with provided ID belonging to this index
func (i AutoIndex) readPage(pageID uint64) (Page, error) {
	pageIDStr := strconv.FormatUint(pageID, 10)
	vals, orderedKeys, err := i.driver.ReadPage(pageIDStr, i.Name, i.pageSize)
	if err != nil {
		return Page{}, err
	}

	return Page{
		vals:        vals,
		name:        pageIDStr,
		orderedKeys: orderedKeys,
	}, nil
}

// write a page to storage using a mutex for concurrency safety
func (i AutoIndex) writePage(p Page) error {
	return wrapInWriteLock(i.driver, i.Name, func() error {
		return i.driver.WritePage(p.vals, p.orderedKeys, p.name, i.Name)
	})
}

// Query queries the index for the provided ID
func (i AutoIndex) Query(s AutoSelector) (Result, error) {
	// if there are multiple query selections, return a collection result
	if s.Length() > 1 {
		results := make(CollectionResult, 0, s.Length())
		var lastPageID uint64
		var page Page
		var loaded bool
		var err error
		for j := 0; s.Next(); j++ {
			id := s.Select()
			pageID := autoPageID(id, i.pageSize)
			// if the page housing the queried ID is different than the loaded page, or no page has been loaded
			// load the needed page
			if pageID != lastPageID || !loaded {
				page, err = i.readPage(pageID)
				if err != nil {
					err = maybeMissingKeyError(i.Name, s.Select(), err)
					log.Infof("error loading page %d :: %s", pageID, err.Error())
					results = append(results, EmptyResult())
					continue
				}
				loaded = true
				lastPageID = pageID
			}

			resultStr, err := page.Query(id)
			if err != nil {
				err = errKeyNotExist(i.Name, s.Select(), err)
				log.Infof("error querying page %d for id %d :: %s", pageID, id, err.Error())
				results = append(results, EmptyResult())
				continue
			}
			results = append(results, SingleResult(resultStr))
		}
		return results, nil
	}

	// else return a single result
	id := s.Select()
	pageID := autoPageID(id, i.pageSize)
	page, err := i.readPage(pageID)
	if err != nil {
		err = maybeMissingKeyError(i.Name, s.Select(), err)
		return EmptyResult(), err
	}
	resultStr, err := page.Query(id)
	if err != nil {
		err = errKeyNotExist(i.Name, s.Select(), err)
		return EmptyResult(), err
	}
	result := SingleResult(resultStr)
	return result, nil
}

// getLatestPage returns the highest ID page in the index (useful for inserts)
func (i AutoIndex) getLatestPage() (Page, uint64, error) {
	pageFiles, err := i.driver.ListPages(i.Name, true)
	if err != nil {
		return Page{}, 0, err
	}

	// if the index already contains pages, get the latest page
	if len(pageFiles) > 0 {
		fileName := pageFiles[0]

		vals, orderedKeys, err := i.driver.ReadPage(fileName, i.Name, i.pageSize)
		if err != nil {
			return Page{}, 0, err
		}

		pageIDStr := StripExtension(fileName)
		pageID, err := strconv.ParseUint(pageIDStr, 10, 64)
		if err != nil {
			return Page{}, 0, fmt.Errorf("error determining page ID from filename '%s' :: %w", fileName, err)
		}

		return Page{
			vals:        vals,
			name:        fileName,
			orderedKeys: orderedKeys,
		}, pageID, nil
	}

	// else create the initial page
	firstPage, err := i.createInitialPage()
	return firstPage, 0, err
}

// create the first page in an index
func (i AutoIndex) createInitialPage() (Page, error) {
	return i.createEmptyPage(0)
}

func (i AutoIndex) createEmptyPage(id uint64) (Page, error) {
	fileName := strconv.FormatUint(id, 10)
	emptyVals := map[uint64]string{}
	err := i.driver.WritePage(emptyVals, []uint64{}, fileName, i.Name)
	if err != nil {
		return Page{}, err
	}
	page := EmptyPage(fileName)
	return page, nil
}

// Insert a value into this index's latest page, returning its ID
func (i AutoIndex) Insert(val string) (result Result, err error) {
	latestPage, latestPageID, err := i.getLatestPage()
	if err != nil {
		return
	}

	nextID := (latestPage.MaxKey() + 1)
	insertPageID := autoPageID(nextID, i.pageSize)

	// if this insert would result in an ID belonging in the next page, create the next page
	if insertPageID > latestPageID {
		// increment page ID, create next page
		latestPage, err = i.createEmptyPage(insertPageID)
		if err != nil {
			return EmptyResult(), err
		}

		// set minimum key of new page to maximum key of previous page + 1
		latestPage.SetMinimumKey(nextID)

	}

	id := latestPage.Append(val)
	result = IDResult(id)

	err = i.writePage(latestPage)

	return
}

// Update a value stored in the index. Attempting to update a value not yet stored returns an error
func (i AutoIndex) Update(s AutoSelector, newVal string) (Result, error) {
	// if there are multiple query selections, update all
	if s.Length() > 1 {
		insertedIDs := make(CollectionResult, 0, s.Length())
		var lastPageID uint64
		var page Page
		var loaded bool
		var err error
		for j := 0; s.Next(); j++ {
			id := s.Select()
			pageID := autoPageID(id, i.pageSize)
			// if the page housing the update ID is different than the loaded page a no page has been loaded yet,
			// load the needed page
			if !loaded {
				page, err = i.readPage(pageID)
				if err != nil {
					err = maybeMissingKeyError(i.Name, s.Select(), err)
					log.Info(err.Error())
					insertedIDs = append(insertedIDs, EmptyResult())
					continue
				}
				loaded = true
				lastPageID = pageID
				// if a page has been loaded and a new page is needed, write the changes to the previous page first
			} else if pageID != lastPageID {
				err := i.writePage(page)
				if err != nil {
					log.Info(err.Error())
					insertedIDs = append(insertedIDs, EmptyResult())
					continue
				}
				// then load the next page
				page, err = i.readPage(pageID)
				if err != nil {
					err = maybeMissingKeyError(i.Name, s.Select(), err)
					log.Info(err.Error())
					insertedIDs = append(insertedIDs, EmptyResult())
					continue
				}
				lastPageID = pageID
			}

			// update the value in the loaded page
			err = page.Overwrite(id, newVal)
			if err != nil {
				log.Infof(err.Error())
				insertedIDs = append(insertedIDs, EmptyResult())
				continue
			}

			insertedIDs = append(insertedIDs, NewIDSingleResult(id))
		}
		// write the updated map to file, conscious of other requests
		err = i.writePage(page)
		if err != nil {
			log.Info(err.Error())
		}
		return insertedIDs, err
	}

	id := s.Select()
	pageID := autoPageID(id, i.pageSize)
	page, err := i.readPage(pageID)
	if err != nil {
		err = maybeMissingKeyError(i.Name, s.Select(), err)
		return EmptyResult(), err
	}

	// update the value in the map
	err = page.Overwrite(id, newVal)
	if err != nil {
		log.Infof(err.Error())
		return EmptyResult(), err
	}

	// write the updated map to file, conscious of other requests
	err = i.writePage(page)
	if err != nil {
		return EmptyResult(), err
	}

	return SingleResult(strconv.FormatUint(id, 10)), nil
}

// Delete a value stored in the autoindex
func (i AutoIndex) Delete(s AutoSelector) (Result, error) {
	if s.Length() > 1 {
		deletedIDs := make(CollectionResult, 0, s.Length())
		var lastPageID uint64
		var page Page
		var loaded bool
		var err error
		for j := 0; s.Next(); j++ {
			id := s.Select()
			pageID := autoPageID(id, i.pageSize)
			// if no page has been loaded, load the first page
			if !loaded {
				page, err = i.readPage(pageID)
				if err != nil {
					err = maybeMissingKeyError(i.Name, s.Select(), err)
					log.Info(err.Error())
					deletedIDs = append(deletedIDs, EmptyResult())
					continue
				}
				loaded = true
				lastPageID = pageID
				// if the page loaded does not contain the needed ID, write changes to the current page
			} else if pageID != lastPageID {
				err := i.writePage(page)
				if err != nil {
					log.Info(err.Error())
					deletedIDs = append(deletedIDs, EmptyResult())
					continue
				}
				// then load the correct page
				page, err = i.readPage(pageID)
				if err != nil {
					err = maybeMissingKeyError(i.Name, s.Select(), err)
					log.Info(err.Error())
					deletedIDs = append(deletedIDs, EmptyResult())
					continue
				}
				lastPageID = pageID
			}

			// delete the value in the loaded page
			err := page.Delete(id)
			if err != nil {
				log.Info(err.Error())
				deletedIDs = append(deletedIDs, EmptyResult())
				continue
			}

			deletedIDs = append(deletedIDs, NewIDSingleResult(id))
		}

		// write the updated map to file
		err = i.writePage(page)
		if err != nil {
			log.Infof(err.Error())
			return EmptyResult(), err
		}
		return deletedIDs, err
	}

	id := s.Select()
	pageID := autoPageID(id, i.pageSize)
	page, err := i.readPage(pageID)
	if err != nil {
		err = maybeMissingKeyError(i.Name, s.Select(), err)
		return EmptyResult(), err
	}

	err = page.Delete(id)
	if err != nil {
		err = errKeyNotExist(i.Name, s.Select(), err)
		return EmptyResult(), err
	}

	err = i.writePage(page)
	if err != nil {
		return EmptyResult(), err
	}

	return SingleResult(strconv.FormatUint(id, 10)), nil
}

// List a subset of results from the index
func (i AutoIndex) List(limit, offset int, desc bool) (ListResult, error) {
	pageNames, err := i.driver.ListPages(i.Name, desc)
	if err != nil {
		return ListResult{}, err
	}

	// keep track of the number of records read for limit
	recordsRead := 0

	// keep track of the number of records skipped for offset
	recordsSkipped := 0

	results := make(ListResult, 0, limit)

PageLoop:
	for _, fileName := range pageNames {
		pageIDStr := StripExtension(fileName)
		pageID, err := strconv.ParseUint(pageIDStr, 10, 64)
		if err != nil {
			// filename could not be parsed
			err = errBadData(i.Name, fileName, err)
			log.Error(err.Error())
			return ListResult{}, err
		}

		page, err := i.readPage(pageID)
		if err != nil {
			return ListResult{}, err
		}

		// if this page is excluded by the offset, move along
		if (recordsRead + page.Length()) <= offset {
			recordsRead += page.Length()
			continue PageLoop
		}

		orderedKeys := page.orderedKeys
		if desc {
			orderedKeys = copyAndReverseUint64Slice(orderedKeys)
		}

		// read any relevant records from the page
	RecordLoop:
		for _, key := range orderedKeys {
			// skip offset values
			if recordsSkipped < offset {
				recordsSkipped++
				continue RecordLoop
			}

			if recordsRead >= limit && limit != 0 {
				break PageLoop
			}

			results = append(results, AutoListItem{Key: key, Value: page.vals[key]})
			recordsRead++
		}
	}

	return results, nil
}

// Count the number of records present in the index
func (i AutoIndex) Count() (Result, error) {
	var count uint64
	pageNames, err := i.driver.ListPages(i.Name, false)
	if err != nil {
		return EmptyResult(), err
	}

	for _, fileName := range pageNames {
		pageIDStr := StripExtension(fileName)
		pageID, err := strconv.ParseUint(pageIDStr, 10, 64)
		if err != nil {
			// filename could not be parsed
			err = errBadData(i.Name, fileName, err)
			log.Error(err)
			return EmptyResult(), err
		}

		page, err := i.readPage(pageID)
		if err != nil {
			return EmptyResult(), err
		}

		count += uint64(page.Length())
	}

	countStr := strconv.FormatUint(count, 10)

	return SingleResult(countStr), nil
}

// copyAndReverseUint64Slice reverses a slice (copy) for descending sort
func copyAndReverseUint64Slice(orderedKeys []uint64) []uint64 {
	length := len(orderedKeys)
	copied := make([]uint64, length)
	copy(copied, orderedKeys)
	for i, el := range orderedKeys {
		copied[length-i-1] = el
	}
	return copied
}
