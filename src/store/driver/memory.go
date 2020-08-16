package driver

import (
	"fmt"
	"time"
)

// MemoryDriver is an in-memory ephemeral storage driver for testing
type MemoryDriver struct {
	autoIndexes map[string]memoryAutoIndex
	mapIndexes  map[string]memoryMapIndex
}

// NewMemoryDriver instantiates a memory storage driver
func NewMemoryDriver() MemoryDriver {
	return MemoryDriver{
		autoIndexes: make(map[string]memoryAutoIndex, 10),
		mapIndexes:  make(map[string]memoryMapIndex, 10),
	}
}

// memoryAutoPage an in-memory auto pagefile
type memoryAutoPage struct {
	vals        map[uint64]string
	orderedKeys []uint64
}

// memoryAutoIndex is an in-memory datadir, mapping "filenames" to map pages
type memoryAutoIndex map[string]memoryAutoPage

// memoryMapPage is an in-memory map pagefile
type memoryMapPage struct {
	vals        map[string]string
	orderedKeys []string
}

// memoryMapIndex is an in-memory map index
type memoryMapIndex map[string]memoryMapPage

// ReadPage reads a page
func (d MemoryDriver) ReadPage(filename string, indexName string, pageSize int) (map[uint64]string, []uint64, error) {
	index, ok := d.autoIndexes[indexName]
	if !ok {
		return map[uint64]string{}, []uint64{}, ErrNotExist(filename, indexName, fmt.Errorf("auto index '%s' does not exist in memory", indexName))
	}

	page, ok := index[filename]
	if !ok {
		return map[uint64]string{}, []uint64{}, ErrNotExist(filename, indexName, fmt.Errorf("page '%s' does not exist in memory auto index '%s'", filename, indexName))
	}

	return page.vals, page.orderedKeys, nil
}

// ReadMapPage reads a map page
func (d MemoryDriver) ReadMapPage(filename string, indexName string, pageSize int) (map[string]string, []string, error) {
	index, ok := d.mapIndexes[indexName]
	if !ok {
		return map[string]string{}, []string{}, ErrNotExist(filename, indexName, fmt.Errorf("map index '%s' does not exist in memory", indexName))
	}

	page, ok := index[filename]
	if !ok {
		return map[string]string{}, []string{}, ErrNotExist(filename, indexName, fmt.Errorf("page '%s' does not exist in memory auto index '%s'", filename, indexName))
	}

	return page.vals, page.orderedKeys, nil
}

// WritePage commits an auto page to the memory store
func (d MemoryDriver) WritePage(vals map[uint64]string, orderedKeys []uint64, filename string, indexName string) error {
	index, ok := d.autoIndexes[indexName]
	if !ok {
		return ErrNotExist(filename, indexName, fmt.Errorf("auto index '%s' does not exist in memory", indexName))
	}

	index[filename] = memoryAutoPage{
		vals,
		orderedKeys,
	}

	return nil
}

// WriteMapPage commits a map page to the memory store
func (d MemoryDriver) WriteMapPage(vals map[string]string, orderedKeys []string, fileName string, indexName string) error {
	index, ok := d.mapIndexes[indexName]
	if !ok {
		return ErrNotExist(fileName, indexName, fmt.Errorf("map index '%s' does not exist in memory", indexName))
	}

	index[fileName] = memoryMapPage{
		vals,
		orderedKeys,
	}

	return nil
}

// ListPages returns a list of pages in a given index
// Note that this driver holds map indexes and auto indexes separately,
// so having an auto index and map index with the same name may cause undesired behavior
func (d MemoryDriver) ListPages(indexName string) ([]string, error) {
	for name, index := range d.autoIndexes {
		if name == indexName {
			fileNames := make([]string, 0, len(index))
			for key := range index {
				fileNames = append(fileNames, key)
			}
			return fileNames, nil
		}
	}

	for name, index := range d.mapIndexes {
		if name == indexName {
			fileNames := make([]string, 0, len(index))
			for key := range index {
				fileNames = append(fileNames, key)
			}
			return fileNames, nil
		}
	}

	return []string{}, fmt.Errorf("index with name '%s' does not exist in memory store", indexName)
}

// CreateAutoIndex creates an empty auto index in the memory store
func (d MemoryDriver) CreateAutoIndex(indexName string) error {
	d.autoIndexes[indexName] = make(memoryAutoIndex, 100)
	return nil
}

// CreateMapIndex creates an empty map index in the memory store
func (d MemoryDriver) CreateMapIndex(indexName string) error {
	d.mapIndexes[indexName] = make(memoryMapIndex, 100)
	return nil
}

// indexes are updated instantaneously with this driver so locking is unnecessary
// there are no file writes or network ops

// IndexIsLocked indicates if the index is locked for writes. always false for this driver
func (d MemoryDriver) IndexIsLocked(indexName string) (bool, time.Time, error) {
	return false, time.Now(), nil
}

// LockIndex does nothing, since there are no locks
func (d MemoryDriver) LockIndex(indexName string) error {
	return nil
}

// UnlockIndex does nothing, since there are no locks
func (d MemoryDriver) UnlockIndex(indexName string) error {
	return nil
}
