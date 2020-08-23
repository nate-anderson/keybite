package store_test

import (
	"keybite/store"
	"keybite/store/driver"
	"keybite/util"
	"strconv"
	"testing"
	"time"
)

const testPageSize = 10

var testLockDuration = time.Millisecond * 50

func newTestingIndex(t *testing.T) store.AutoIndex {
	indexName := "test_index"
	driver := driver.NewMemoryDriver()
	driver.CreateAutoIndex(indexName)
	index, err := store.NewAutoIndex(indexName, &driver, testPageSize)
	util.Ok(t, err)
	return index
}

func TestAutoIndexInsertQuery(t *testing.T) {
	index := newTestingIndex(t)
	count, err := index.Count()
	util.Ok(t, err)
	util.Equals(t, "0", count.String())

	// test insert & retrieve value
	testVal := "testVal"
	insertRes, err := index.Insert(testVal)
	util.Ok(t, err)
	util.Equals(t, "1", insertRes.String())

	count2, err := index.Count()
	util.Ok(t, err)
	util.Equals(t, "1", count2.String())

	sel := store.NewSingleSelector(1)
	queryRes, err := index.Query(&sel)
	util.Ok(t, err)
	util.Equals(t, testVal, queryRes.String())

	// retrieve missing value should return err
	missingSel := store.NewSingleSelector(10)
	missingRes, err := index.Query(&missingSel)
	util.Assert(t, err != nil, "querying for missing value should return an error")
	util.Equals(t, "", missingRes.String())
}

func TestAutoInsertMany(t *testing.T) {
	dri := driver.NewMemoryDriver()
	indexName := "test_index"
	err := dri.CreateAutoIndex(indexName)
	index, err := store.NewAutoIndex(indexName, &dri, testPageSize)
	util.Ok(t, err)
	numRecords := (testPageSize * 2) + 1
	var currentResult store.Result

	for i := 1; i <= numRecords; i++ {
		testVal := "test_value_" + strconv.Itoa(i)
		currentResult, err = index.Insert(testVal)
		util.Ok(t, err)
		util.Equals(t, strconv.Itoa(i), currentResult.String())
	}

	countResult, err := index.Count()
	t.Log("### COUNT", countResult.String())
	t.Log("DI", dri.DeepInspect())
	util.Equals(t, strconv.Itoa(numRecords), countResult.String())

	idStr := strconv.Itoa(numRecords)
	util.Equals(t, idStr, currentResult.String())
}

func TestAutoIndexDeleteOne(t *testing.T) {
	dri := driver.NewMemoryDriver()
	indexName := "test_index"
	err := dri.CreateAutoIndex(indexName)
	index, err := store.NewAutoIndex(indexName, &dri, testPageSize)
	util.Ok(t, err)

	testVal := "test_value_0001"

	insertResult, err := index.Insert(testVal)
	util.Ok(t, err)

	// confirm index size 1
	countResult, err := index.Count()
	util.Ok(t, err)

	util.Equals(t, "1", countResult.String())

	id, err := strconv.ParseUint(insertResult.String(), 10, 64)
	util.Ok(t, err)

	selector := store.NewSingleSelector(id)
	_, err = index.Delete(&selector)
	util.Ok(t, err)

	// confirm index size zero
	countResult, err = index.Count()
	util.Ok(t, err)

	util.Equals(t, "0", countResult.String())
}

func TestAutoIndexDeleteMany(t *testing.T) {
	dri, err := driver.NewFilesystemDriver("test_data", ".kb", testLockDuration)
	util.Ok(t, err)
	indexName := "test_index_deletemany"
	err = dri.CreateAutoIndex(indexName)
	index, err := store.NewAutoIndex(indexName, &dri, testPageSize)
	util.Ok(t, err)

	testVal := "test_value_000"
	numInserts := 10

	insertIds := make([]uint64, 0, numInserts)

	for i := 0; i < numInserts; i++ {
		insertResult, err := index.Insert(testVal)
		util.Ok(t, err)
		id, err := strconv.ParseUint(insertResult.String(), 10, 64)
		util.Ok(t, err)
		insertIds = append(insertIds, id)
	}

	// confirm index size
	countResult, err := index.Count()
	util.Ok(t, err)
	util.Equals(t, "10", countResult.String())

	// t.Log("DeIn", dri.DeepInspect())

	selector := store.NewArraySelector(insertIds)
	_, err = index.Delete(&selector)
	util.Ok(t, err)

	// confirm index size zero
	countResult, err = index.Count()
	util.Ok(t, err)

	util.Equals(t, "0", countResult.String())
}
