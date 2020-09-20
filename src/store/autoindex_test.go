package store_test

import (
	"encoding/json"
	"fmt"
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

func TestAutoIndexInsertQueryOne(t *testing.T) {
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

func TestAutoInsertQueryMany(t *testing.T) {
	dri := driver.NewMemoryDriver()
	indexName := "test_index"
	err := dri.CreateAutoIndex(indexName)
	index, err := store.NewAutoIndex(indexName, &dri, testPageSize)
	util.Ok(t, err)
	numRecords := (testPageSize * 2) + 1
	var currentResult store.Result
	insertIDs := make([]uint64, 0, numRecords)

	for i := 1; i <= numRecords; i++ {
		testVal := "test_value_" + strconv.Itoa(i)
		currentResult, err = index.Insert(testVal)
		util.Ok(t, err)
		util.Equals(t, strconv.Itoa(i), currentResult.String())
		insertID, err := strconv.ParseUint(currentResult.String(), 10, 64)
		util.Ok(t, err)
		insertIDs = append(insertIDs, insertID)
	}

	countResult, err := index.Count()
	util.Equals(t, strconv.Itoa(numRecords), countResult.String())

	idStr := strconv.Itoa(numRecords)
	util.Equals(t, idStr, currentResult.String())

	querySelector := store.NewArraySelector(insertIDs)
	queryRes, err := index.Query(&querySelector)
	util.Ok(t, err)

	queryResults := make([]string, 0, numRecords)
	queryResJSON, err := json.Marshal(queryRes)
	util.Ok(t, err)
	err = json.Unmarshal(queryResJSON, &queryResults)
	util.Ok(t, err)

	for i, item := range queryResults {
		expectedVal := fmt.Sprintf("test_value_%d", i+1)
		util.Equals(t, expectedVal, item)
	}
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
	index := newTestingIndex(t)

	testVal := "test_value_000"
	numInserts := (testPageSize * 2) + 1

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
	util.Equals(t, strconv.Itoa(numInserts), countResult.String())

	selector := store.NewArraySelector(insertIds)
	_, err = index.Delete(&selector)
	util.Ok(t, err)

	// confirm index size zero
	countResult, err = index.Count()
	util.Ok(t, err)

	util.Equals(t, "0", countResult.String())
}

func TestAutoIndexUpdateOne(t *testing.T) {
	index := newTestingIndex(t)

	testVal := "test_value_000"
	insertResult, err := index.Insert(testVal)
	util.Ok(t, err)

	id, err := strconv.ParseUint(insertResult.String(), 10, 64)
	util.Ok(t, err)

	newVal := "test_value_001"
	updateSelector := store.NewSingleSelector(id)
	updateResult, err := index.Update(&updateSelector, newVal)
	util.Ok(t, err)

	util.Equals(t, updateResult.String(), insertResult.String())

	querySelector := store.NewSingleSelector(id)
	queryResult, err := index.Query(&querySelector)
	util.Ok(t, err)

	util.Equals(t, queryResult.String(), newVal)
}

func TestAutoIndexUpdateMany(t *testing.T) {
	index := newTestingIndex(t)

	testVal := "test_value_000"
	numInserts := (testPageSize * 2) + 1

	insertIds := make([]uint64, 0, numInserts)

	for i := 0; i < numInserts; i++ {
		insertResult, err := index.Insert(testVal)
		util.Ok(t, err)
		id, err := strconv.ParseUint(insertResult.String(), 10, 64)
		util.Ok(t, err)
		insertIds = append(insertIds, id)
	}

	selector := store.NewArraySelector(insertIds)
	firstResult, err := index.Query(&selector)
	util.Ok(t, err)

	expected := util.RepeatString(testVal, numInserts)
	expectedResult := store.NewCollectionResult(expected)

	util.Equals(t, firstResult.String(), expectedResult.String())

	newVal := "test_value_001"
	selector = store.NewArraySelector(insertIds)
	updateResult, err := index.Update(&selector, newVal)
	util.Ok(t, err)
	util.Assert(t, updateResult.Valid(), "update result is valid")

	selector = store.NewArraySelector(insertIds)
	updatedQueried, err := index.Query(&selector)
	util.Ok(t, err)

	expected = util.RepeatString(newVal, numInserts)
	expectedResult = store.NewCollectionResult(expected)

	util.Equals(t, updatedQueried.String(), expectedResult.String())

}

func TestAutoIndexList(t *testing.T) {
	indexName := "test_index"
	driver := driver.NewMemoryDriver()
	driver.CreateAutoIndex(indexName)
	index, err := store.NewAutoIndex(indexName, &driver, testPageSize)
	util.Ok(t, err)

	numInserts := testPageSize * 10
	insertKeys := make([]string, 0, numInserts)

	for i := 0; i < numInserts; i++ {
		testValue := fmt.Sprintf("test_value_%d", i)
		id, err := index.Insert(testValue)
		util.Ok(t, err)
		insertKeys = append(insertKeys, id.String())
		util.Ok(t, err)
	}

	results, err := index.List(0, 0)
	util.Ok(t, err)

	resultJSON, err := results.MarshalJSON()
	util.Ok(t, err)

	type keyValue struct {
		key   string
		value string
	}

	resultArr := make([]keyValue, numInserts)
	err = json.Unmarshal(resultJSON, &resultArr)
	util.Ok(t, err)

	util.Equals(t, numInserts, len(resultArr))
}

func TestAutoIndexCount(t *testing.T) {
	indexName := "test_index"
	driver := driver.NewMemoryDriver()
	driver.CreateAutoIndex(indexName)
	index, err := store.NewAutoIndex(indexName, &driver, testPageSize)
	util.Ok(t, err)

	numInserts := testPageSize * 10
	insertKeys := make([]string, 0, numInserts)

	for i := 0; i < numInserts; i++ {
		testValue := fmt.Sprintf("test_value_%d", i)

		id, err := index.Insert(testValue)
		util.Ok(t, err)
		insertKeys = append(insertKeys, id.String())
		util.Ok(t, err)
	}

	result, err := index.Count()
	util.Ok(t, err)

	util.Equals(t, strconv.Itoa(numInserts), result.String())
}
