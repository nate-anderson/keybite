package store

import (
	"fmt"
	"os"
	"path"
	"strconv"
)

// MapIndex is an index that acts like a map, mapping unique keys to values
type MapIndex struct {
	Name     string
	pageSize int
	Dir      string
}

// NewMapIndex returns an index object, validating that index data exists in the data directory
func NewMapIndex(name string, dataDir string, pageSize int) (MapIndex, error) {
	dir := path.Join(dataDir, name)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return MapIndex{}, fmt.Errorf("no index named %s could be found", name)
	}

	return MapIndex{
		Name:     name,
		pageSize: pageSize,
		Dir:      dir,
	}, nil
}

// readPage returns page with provided ID belonging to this index
func (m MapIndex) readPage(pageID uint64) (MapPage, error) {
	pageIDStr := strconv.FormatUint(pageID, 10)
	filePath := path.Join(m.Dir, pageIDStr+".kb")
	return FileToMapPage(filePath, m.pageSize)
}

// readOrCreatePage reads or creates the map page for this page ID
func (m MapIndex) readOrCreatePage(pageID uint64) (MapPage, error) {
	pageIDStr := strconv.FormatUint(pageID, 10)
	filePath := path.Join(m.Dir, pageIDStr+".kb")
	p, err := FileToMapPage(filePath, m.pageSize)
	if err == nil {
		return p, err
	}

	if err == os.ErrNotExist {
		p = EmptyMapPage(filePath)
		err = p.Write()
	}

	return MapPage{}, err

}

// Query the MapIndex for the specified key
func (m MapIndex) Query(key string) (string, error) {
	id, err := hashString(key)
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
	id, err := hashString(key)
	if err != nil {
		return "", err
	}

	pageID := id / uint64(m.pageSize)
	page, err := m.readOrCreatePage(pageID)
	if err != nil {
		return "", err
	}

	page.Set(id, value)
	err = page.Write()
	return key, err
}

// Update existing data
func (m MapIndex) Update(key string, newValue string) error {
	id, err := hashString(key)
	if err != nil {
		return err
	}

	pageID := id / uint64(m.pageSize)
	page, err := m.readPage(pageID)
	if err != nil {
		return err
	}

	page.Set(id, newValue)
	err = page.Write()
	return err
}
