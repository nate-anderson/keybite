package store

import (
	"keybite/store/driver"
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
	fileName := pageIDStr
	vals, err := i.driver.ReadPage(fileName, i.Name, i.pageSize)
	if err != nil {
		return Page{}, err
	}

	return Page{
		vals: vals,
		name: pageIDStr,
	}, nil
}

// Query queries the index for the provided ID
func (i AutoIndex) Query(s Selector) (result string, err error) {
	var lastPageID uint64
	var page Page
	for s.Next() {
		pageID := s.Select() / uint64(i.pageSize)
		if pageID != lastPageID {
			page, err = i.readPage(pageID)
			if err != nil {
				return
			}
		}

	}
	// identify the expected page ID of the record ID passed
	return page.Query(s.Select())
}

// getLatestPage returns the highest ID page in the index (useful for inserts)
func (i AutoIndex) getLatestPage() (Page, error) {
	pageFiles, err := i.driver.ListPages(i.Name)
	if err != nil {
		return Page{}, err
	}

	// if the index already contains pages, get the latest page
	if len(pageFiles) > 0 {
		var fileName string
		for _, fileName = range pageFiles {
			continue
		}

		vals, err := i.driver.ReadPage(fileName, i.Name, i.pageSize)
		if err != nil {
			return Page{}, err
		}

		return Page{
			vals: vals,
			name: fileName,
		}, nil
	}

	// else create the initial page
	return i.createInitialPage()
}

// create the first page in an index
func (i AutoIndex) createInitialPage() (Page, error) {
	fileName := "0"
	emptyVals := map[uint64]string{}
	err := i.driver.WritePage(emptyVals, fileName, i.Name)
	if err != nil {
		return Page{}, err
	}
	page := EmptyPage(fileName)
	return page, nil
}

// Insert a value into this index's latest page, returning its ID
func (i AutoIndex) Insert(val string) (id uint64, err error) {
	latestPage, err := i.getLatestPage()
	if err != nil {
		return
	}

	id = latestPage.Append(val)

	id, err = wrapInAutoWriteLock(i.driver, i.Name, func() (uint64, error) {
		err := i.driver.WritePage(latestPage.vals, latestPage.name, i.Name)
		return id, err
	})

	return
}

// Update a value stored in the index. Attempting to update a value not yet stored returns an error
func (i AutoIndex) Update(id uint64, newVal string) error {
	pageID := id / uint64(i.pageSize)
	page, err := i.readPage(pageID)
	if err != nil {
		return err
	}

	err = page.Overwrite(id, newVal)
	if err != nil {
		return err
	}

	_, err = wrapInAutoWriteLock(i.driver, i.Name, func() (uint64, error) {
		writeErr := i.driver.WritePage(page.vals, page.name, i.Name)
		return id, writeErr
	})

	return err
}
