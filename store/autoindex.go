package store

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strconv"
)

// AutoIndex is an auto-incrementing index
type AutoIndex struct {
	Name     string
	pageSize int
	Dir      string
}

// NewAutoIndex returns an index object, validating that index data exists in the data directory
func NewAutoIndex(name string, dataDir string, pageSize int) (AutoIndex, error) {
	dir := path.Join(dataDir, name)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return AutoIndex{}, fmt.Errorf("no index named %s could be found", name)
	}

	return AutoIndex{
		Name:     name,
		pageSize: pageSize,
		Dir:      dir,
	}, nil
}

// readPage returns page with provided ID belonging to this index
func (i AutoIndex) readPage(pageID int64) (Page, error) {
	pageIDStr := strconv.FormatInt(pageID, 10)
	filePath := path.Join(i.Dir, pageIDStr+".kb")
	return FileToPage(filePath, i.pageSize)
}

// Query queries the index for the provided ID
func (i AutoIndex) Query(id int64) (string, error) {
	// identify the expected page ID of the record ID passed
	pageID := id / int64(i.pageSize)
	page, err := i.readPage(pageID)
	if err != nil {
		log.Println("error reading index page: ", err.Error())
		return "", err
	}

	return page.Query(id)

}

// getLatestPage returns the highest ID page in the index (useful for inserts)
func (i AutoIndex) getLatestPage() (Page, error) {
	pageFiles, err := ioutil.ReadDir(i.Dir)
	if err != nil {
		return Page{}, err
	}

	var file os.FileInfo
	// if the index already contains pages, get the latest page
	if len(pageFiles) > 0 {
		for _, file = range pageFiles {
			continue
		}

		pageFilePath := path.Join(i.Dir, file.Name())
		return FileToPage(pageFilePath, i.pageSize)
	}

	// else create the initial page
	return i.createInitialPage()
}

// create the first page in an index
func (i AutoIndex) createInitialPage() (Page, error) {
	filePath := i.Dir + "/0.kb"
	_, err := os.Create(filePath)
	if err != nil {
		log.Printf("error creating initial page for index %s: %v", i.Name, err)
		return Page{}, err
	}
	page := EmptyPage(filePath)
	return page, nil
}

// Insert a value into this index's latest page, returning its ID
func (i AutoIndex) Insert(val string) (id int64, err error) {
	latestPage, err := i.getLatestPage()
	if err != nil {
		return
	}

	id = latestPage.Append(val)
	err = latestPage.Write()
	return
}

// Update a value stored in the index. Attempting to update a value not yet stored returns an error
func (i AutoIndex) Update(id int64, newVal string) error {
	pageID := id / int64(i.pageSize)
	page, err := i.readPage(pageID)
	if err != nil {
		log.Printf("error reading page %d to update value at ID %d: %v", pageID, id, err)
		return err
	}

	err = page.Overwrite(id, newVal)
	if err != nil {
		log.Printf("error overwriting ID %d in page %d", id, pageID)
		return err
	}

	return page.Write()

}
