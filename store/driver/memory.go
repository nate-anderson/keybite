package driver

import (
	"fmt"
	"keybite/util"
	"sort"
	"time"
)

// MemoryDriver is an in-memory ephemeral storage driver for testing
type MemoryDriver struct {
	autoIndexes map[string]*memoryAutoIndex
	mapIndexes  map[string]*memoryMapIndex
}

// NewMemoryDriver instantiates a memory storage driver
func NewMemoryDriver() MemoryDriver {
	return MemoryDriver{
		autoIndexes: make(map[string]*memoryAutoIndex, 10),
		mapIndexes:  make(map[string]*memoryMapIndex, 10),
	}
}

// memoryAutoPage an in-memory auto pagefile
type memoryAutoPage struct {
	vals        map[uint64]string
	orderedKeys []uint64
}

// memoryAutoIndex is an in-memory datadir, mapping "filenames" to map pages
type memoryAutoIndex struct {
	pages            map[string]*memoryAutoPage
	orderedPageNames []string
}

func (i *memoryAutoIndex) addPage(page *memoryAutoPage, name string) {
	i.pages[name] = page
	if !util.SliceContains(name, i.orderedPageNames) {
		i.orderedPageNames = append(i.orderedPageNames, name)
		sort.Strings(i.orderedPageNames)
	}
}

// memoryMapPage is an in-memory map pagefile
type memoryMapPage struct {
	vals        map[string]string
	orderedKeys []string
}

// memoryMapIndex is an in-memory map index
type memoryMapIndex struct {
	pages            map[string]*memoryMapPage
	orderedPageNames []string
}

func (i *memoryMapIndex) addPage(page *memoryMapPage, name string) {
	i.pages[name] = page
	if !util.SliceContains(name, i.orderedPageNames) {
		i.orderedPageNames = append(i.orderedPageNames, name)
		sort.Strings(i.orderedPageNames)
	}
}

// ReadPage reads a page
func (d MemoryDriver) ReadPage(fileName string, indexName string, pageSize int) (map[uint64]string, []uint64, error) {
	_, ok := d.autoIndexes[indexName]
	if !ok {
		return map[uint64]string{}, []uint64{}, errIndexNotExist(indexName, fmt.Errorf("memory driver does not contain index %s", indexName))
	}

	page, ok := d.autoIndexes[indexName].pages[fileName]
	if !ok {
		return map[uint64]string{}, []uint64{}, errPageNotExist(indexName, fileName, fmt.Errorf("index has no page '%s'", fileName))
	}

	return page.vals, page.orderedKeys, nil
}

// ReadMapPage reads a map page
func (d MemoryDriver) ReadMapPage(fileName string, indexName string, pageSize int) (map[string]string, []string, error) {
	_, ok := d.mapIndexes[indexName]
	if !ok {
		return map[string]string{}, []string{}, errIndexNotExist(indexName, fmt.Errorf("memory driver does not contain index %s", indexName))
	}

	page, ok := d.mapIndexes[indexName].pages[fileName]
	if !ok {
		return map[string]string{}, []string{}, errPageNotExist(indexName, fileName, fmt.Errorf("index has no page '%s'", fileName))
	}

	return page.vals, page.orderedKeys, nil
}

// WritePage commits an auto page to the memory store
func (d *MemoryDriver) WritePage(vals map[uint64]string, orderedKeys []uint64, fileName string, indexName string) error {
	_, ok := d.autoIndexes[indexName]
	if !ok {
		return errIndexNotExist(indexName, fmt.Errorf("memory driver does not contain index %s", indexName))
	}

	d.autoIndexes[indexName].addPage(&memoryAutoPage{
		vals,
		orderedKeys,
	}, fileName)

	return nil
}

// WriteMapPage commits a map page to the memory store
func (d *MemoryDriver) WriteMapPage(vals map[string]string, orderedKeys []string, fileName string, indexName string) error {
	_, ok := d.mapIndexes[indexName]
	if !ok {
		return errIndexNotExist(indexName, fmt.Errorf("memory driver does not contain index %s", indexName))
	}

	d.mapIndexes[indexName].addPage(&memoryMapPage{
		vals,
		orderedKeys,
	}, fileName)

	return nil
}

// ListPages returns a list of pages in a given index
// Note that this driver holds map indexes and auto indexes separately,
// so having an auto index and map index with the same name may cause undesired behavior
func (d MemoryDriver) ListPages(indexName string, desc bool) ([]string, error) {
	for name, index := range d.autoIndexes {
		if name == indexName {
			return sortFileNames(index.orderedPageNames, "", desc), nil
		}
	}

	for name, index := range d.mapIndexes {
		if name == indexName {
			return sortFileNames(index.orderedPageNames, "", desc), nil
		}
	}

	return []string{}, errIndexNotExist(indexName, fmt.Errorf("memory driver does not contain index %s", indexName))
}

// CreateAutoIndex creates an empty auto index in the memory store
func (d MemoryDriver) CreateAutoIndex(indexName string) error {
	d.autoIndexes[indexName] = &memoryAutoIndex{
		pages:            make(map[string]*memoryAutoPage, 10),
		orderedPageNames: []string{},
	}
	return nil
}

// CreateMapIndex creates an empty map index in the memory store
func (d MemoryDriver) CreateMapIndex(indexName string) error {
	d.mapIndexes[indexName] = &memoryMapIndex{
		pages:            make(map[string]*memoryMapPage, 10),
		orderedPageNames: []string{},
	}
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

// DeepInspect creates a formatted inspection of the driver
func (d MemoryDriver) DeepInspect() string {
	result := "Auto indexes:\n"
	for indexName, index := range d.autoIndexes {
		result += fmt.Sprintf("\tindex %s\n", indexName)
		for fileName, page := range index.pages {
			result += fmt.Sprintf("\t\tpage %s\n", fileName)
			for _, key := range page.orderedKeys {
				result += fmt.Sprintf("\t\t\t%d : %s\n", key, page.vals[key])
			}
		}
	}

	result += "\nMap indexes:\n"
	for indexName, index := range d.mapIndexes {
		result += fmt.Sprintf("\tindex %s\n", indexName)
		for fileName, page := range index.pages {
			result += fmt.Sprintf("\t\tpage %s\n", fileName)
			for _, key := range page.orderedKeys {
				result += fmt.Sprintf("\t\t\t%s : %s\n", key, page.vals[key])
			}
		}
	}
	return result
}

// DropAutoIndex deletes an index from the memory driver
func (d MemoryDriver) DropAutoIndex(indexName string) error {
	_, exists := d.autoIndexes[indexName]
	if !exists {
		return fmt.Errorf("failed deleting memory index '%s': does not exist", indexName)
	}
	delete(d.autoIndexes, indexName)
	return nil
}

// DropMapIndex deletes an index from the memory driver
func (d MemoryDriver) DropMapIndex(indexName string) error {
	_, exists := d.mapIndexes[indexName]
	if !exists {
		return fmt.Errorf("failed deleting memory index '%s': does not exist", indexName)
	}
	delete(d.mapIndexes, indexName)
	return nil
}
