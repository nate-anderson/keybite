package driver

import (
	"fmt"
	"keybite/util"
	"strconv"
	"testing"
)

const pageSize = 100

func TestMemoryDriverWriteAutoIndexPage(t *testing.T) {
	d := NewMemoryDriver()

	indexName := "test_index"

	err := d.CreateAutoIndex(indexName)
	util.Ok(t, err)

	vals := map[uint64]string{
		1: "test value",
	}

	keys := []uint64{1}

	err = d.WritePage(vals, keys, "1", indexName)
	util.Ok(t, err)

	readPage, readKeys, err := d.ReadPage("1", indexName, pageSize)
	util.Ok(t, err)

	retrieved, ok := readPage[1]
	util.Assert(t, ok, "auto index values did not contain inserted test key")
	util.Equals(t, "test value", retrieved)
	util.Equals(t, 1, len(readKeys))
	util.Equals(t, uint64(1), readKeys[0])
}

func TestMemoryDriverWriteMapIndexPage(t *testing.T) {
	d := NewMemoryDriver()

	indexName := "test_map_index"
	err := d.CreateMapIndex(indexName)
	util.Ok(t, err)

	vals := map[string]string{
		"testKey": "testValue",
	}
	keys := []string{"testKey"}

	err = d.WriteMapPage(vals, keys, "1", indexName)
	util.Ok(t, err)

	readPage, readKeys, err := d.ReadMapPage("1", indexName, pageSize)
	util.Ok(t, err)

	retrieved, ok := readPage["testKey"]
	util.Assert(t, ok, "map index values did not contain inserted test key")
	util.Equals(t, "testValue", retrieved)
	util.Equals(t, 1, len(readKeys))
	util.Equals(t, "testKey", readKeys[0])

}

func TestMemoryDriverListPages(t *testing.T) {
	d := NewMemoryDriver()

	indexName := "test_index"
	err := d.CreateAutoIndex(indexName)
	util.Ok(t, err)

	expected := []string{}

	for i := 1; i <= 10; i++ {
		iStr := strconv.Itoa(i)
		expected = append(expected, iStr)

		err = d.WritePage(map[uint64]string{}, []uint64{}, iStr, indexName)
		util.Ok(t, err)
	}

	pages, err := d.ListPages(indexName, false)
	util.Ok(t, err)

	t.Log(pages)

	util.Equals(t, 10, len(pages))
	for _, el := range expected {
		util.Assert(t, util.StrSliceContains(el, pages), fmt.Sprintf("page %s not included in results", el))
	}
}

func makeFakeAutoPage(size, min int) (map[uint64]string, []uint64) {
	prefix := "test_value"
	vals := make(map[uint64]string, size)
	keys := make([]uint64, 0, size)
	minKey := uint64(min)
	for i := uint64(minKey + 1); i <= uint64(minKey)+uint64(size); i++ {
		val := prefix + strconv.FormatUint(i, 10)
		vals[i] = val
		keys = append(keys, i)
	}
	return vals, keys
}

func TestMemoryDriverWriteManyAutoIndexPages(t *testing.T) {
	d := NewMemoryDriver()
	indexName := "test_index"

	err := d.CreateAutoIndex(indexName)
	util.Ok(t, err)

	filenames := []string{"1", "2", "3", "4", "5"}

	minKey := 1
	for _, name := range filenames {
		pageVals, pageKeys := makeFakeAutoPage(pageSize, minKey)
		err := d.WritePage(pageVals, pageKeys, name, indexName)
		util.Ok(t, err)
		minKey += pageSize
	}

	for _, name := range filenames {
		vals, keys, err := d.ReadPage(name, indexName, pageSize)
		util.Ok(t, err)
		util.Equals(t, pageSize, len(vals))
		util.Equals(t, pageSize, len(keys))
		for _, key := range keys {
			_, ok := vals[key]
			util.Assert(t, ok, "page contains value at key")
		}
	}
}

func TestMemoryDriverDropAutoIndex(t *testing.T) {
	d := NewMemoryDriver()

	indexName := "test_auto_index_drop"
	err := d.CreateAutoIndex(indexName)
	util.Ok(t, err)

	vals := map[uint64]string{
		1: "test value",
	}

	keys := []uint64{1}

	err = d.WritePage(vals, keys, "1", indexName)
	util.Ok(t, err)

	// delete index
	err = d.DropAutoIndex(indexName)
	util.Ok(t, err)

	_, _, err = d.ReadPage("1", indexName, pageSize)
	util.Assert(t, err != nil, "error should be non-nill reading page from deleted index")
}

func TestMemoryDriverDropMapIndex(t *testing.T) {
	d := NewMemoryDriver()

	indexName := "test_map_index_drop"
	err := d.CreateMapIndex(indexName)
	util.Ok(t, err)

	vals := map[string]string{
		"1": "test value",
	}

	keys := []string{"1"}

	err = d.WriteMapPage(vals, keys, "1", indexName)
	util.Ok(t, err)

	// delete index
	err = d.DropMapIndex(indexName)
	util.Ok(t, err)

	_, _, err = d.ReadMapPage("1", indexName, pageSize)
	util.Assert(t, err != nil, "error should be non-nill reading page from deleted index")
}
