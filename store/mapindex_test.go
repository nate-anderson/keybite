package store

import (
	"encoding/json"
	"fmt"
	"keybite/store/driver"
	"keybite/util"
	"strconv"
	"testing"
)

func newTestingMapIndex(t *testing.T) MapIndex {
	indexName := "test_map_index"
	driver := driver.NewMemoryDriver()
	driver.CreateMapIndex(indexName)
	index, err := NewMapIndex(indexName, &driver, testPageSize)
	util.Ok(t, err)
	return index
}

func TestMapIndexInsertQueryOne(t *testing.T) {
	index := newTestingMapIndex(t)
	count, err := index.Count()
	util.Ok(t, err)

	util.Equals(t, "0", count.String())

	// test insert & retrieve value
	testVal := "testVal"
	testKey := "testKey"
	selector := NewMapSingleSelector(testKey)
	insertRes, err := index.Insert(&selector, testVal)
	util.Ok(t, err)
	util.Equals(t, testKey, insertRes.String())

	queryRes, err := index.Query(&selector)
	util.Ok(t, err)
	util.Equals(t, testVal, queryRes.String())
}

func TestMapIndexInsertQueryMany(t *testing.T) {
	indexName := "test_map_index"
	driver := driver.NewMemoryDriver()
	driver.CreateMapIndex(indexName)
	index, err := NewMapIndex(indexName, &driver, testPageSize)
	util.Ok(t, err)

	numInserts := testPageSize * 10
	insertKeys := make([]string, 0, numInserts)

	for i := 0; i < numInserts; i++ {
		testValue := fmt.Sprintf("test_value_%d", i)
		testKey := fmt.Sprintf("test_key_%d", i)
		selector := NewMapSingleSelector(testKey)
		id, err := index.Insert(&selector, testValue)
		util.Ok(t, err)
		insertKeys = append(insertKeys, id.String())
		util.Ok(t, err)
	}

	selector := NewMapArraySelector(insertKeys)
	queryRes, err := index.Query(&selector)
	util.Ok(t, err)

	results := make([]string, numInserts)
	resultJSON, err := json.Marshal(queryRes)
	util.Ok(t, err)

	err = json.Unmarshal(resultJSON, &results)
	util.Ok(t, err)

}

func TestMapIndexInsertManyQueryMany(t *testing.T) {
	indexName := "test_map_index"
	driver := driver.NewMemoryDriver()
	driver.CreateMapIndex(indexName)
	index, err := NewMapIndex(indexName, &driver, testPageSize)
	util.Ok(t, err)

	numInserts := testPageSize * 10
	insertKeys := make([]string, 0, numInserts)

	testValue := "test_value"

	for i := 0; i < numInserts; i++ {
		testKey := fmt.Sprintf("test_key_%d", i)
		insertKeys = append(insertKeys, testKey)
	}

	insertSelector := NewMapArraySelector(insertKeys)

	insertRes, err := index.Insert(&insertSelector, testValue)
	util.Ok(t, err)

	insertedKeys := make([]string, 0, numInserts)
	insertedKeysJSON, err := json.Marshal(insertRes)
	util.Ok(t, err)
	err = json.Unmarshal(insertedKeysJSON, &insertedKeys)
	util.Ok(t, err)

	util.Equals(t, numInserts, len(insertedKeys))

	for _, key := range insertKeys {
		util.Assert(t, util.SliceContains(key, insertKeys), "inserted keys include test insert keys")
	}

	selector := NewMapArraySelector(insertKeys)
	queryRes, err := index.Query(&selector)
	util.Ok(t, err)

	results := make([]string, numInserts)
	resultJSON, err := json.Marshal(queryRes)
	util.Ok(t, err)

	err = json.Unmarshal(resultJSON, &results)
	util.Ok(t, err)

}

func TestMapIndexUpdateOne(t *testing.T) {
	index := newTestingMapIndex(t)
	count, err := index.Count()
	util.Ok(t, err)

	util.Equals(t, "0", count.String())

	// test insert & retrieve value
	testVal := "testVal"
	testKey := "testKey"
	selector := NewMapSingleSelector(testKey)
	insertRes, err := index.Insert(&selector, testVal)
	util.Ok(t, err)
	util.Equals(t, testKey, insertRes.String())

	queryRes, err := index.Query(&selector)
	util.Ok(t, err)
	util.Equals(t, testVal, queryRes.String())

	// update the value
	selector = NewMapSingleSelector(testKey)
	testVal2 := "testVal2"
	_, err = index.Update(&selector, testVal2)
	util.Ok(t, err)

	queryRes, err = index.Query(&selector)
	util.Ok(t, err)
	util.Equals(t, testVal2, queryRes.String())
}

func TestMapIndexUpdateMany(t *testing.T) {
	index := newTestingMapIndex(t)

	numInserts := testPageSize * 10
	insertKeys := make([]string, 0, numInserts)

	testValue := "test_value"

	for i := 0; i < numInserts; i++ {
		testKey := fmt.Sprintf("test_key_%d", i)
		insertKeys = append(insertKeys, testKey)
	}

	insertSelector := NewMapArraySelector(insertKeys)
	_, err := index.Insert(&insertSelector, testValue)
	util.Ok(t, err)

	testValue2 := "test_value_2"
	updateSelector := NewMapArraySelector(insertKeys)
	updateRes, err := index.Update(&updateSelector, testValue2)
	util.Ok(t, err)

	updatedKeys := make([]string, 0, numInserts)
	updatedKeyJSON, err := json.Marshal(updateRes)
	util.Ok(t, err)

	err = json.Unmarshal(updatedKeyJSON, &updatedKeys)
	util.Ok(t, err)

	util.Equals(t, numInserts, len(updatedKeys))

	// query & verify updated values
	querySelector := NewMapArraySelector(insertKeys)
	queryRes, err := index.Query(&querySelector)
	util.Ok(t, err)

	queryValues := make([]string, 0, numInserts)
	queryResJSON, err := json.Marshal(queryRes)
	util.Ok(t, err)
	err = json.Unmarshal(queryResJSON, &queryValues)
	util.Ok(t, err)

	util.Equals(t, numInserts, len(queryValues))

	for _, value := range queryValues {
		util.Equals(t, testValue2, value)
	}
}

func TestMapIndexUpsertOne(t *testing.T) {
	index := newTestingMapIndex(t)
	count, err := index.Count()
	util.Ok(t, err)

	util.Equals(t, "0", count.String())

	// test insert & retrieve value
	testVal := "testVal"
	testKey := "testKey"
	selector := NewMapSingleSelector(testKey)
	insertRes, err := index.Upsert(&selector, testVal)
	util.Ok(t, err)
	util.Equals(t, testKey, insertRes.String())

	queryRes, err := index.Query(&selector)
	util.Ok(t, err)
	util.Equals(t, testVal, queryRes.String())

	// update the value
	selector = NewMapSingleSelector(testKey)
	testVal2 := "testVal2"
	_, err = index.Upsert(&selector, testVal2)
	util.Ok(t, err)

	queryRes, err = index.Query(&selector)
	util.Ok(t, err)
	util.Equals(t, testVal2, queryRes.String())
}

func TestMapIndexUpsertMany(t *testing.T) {
	index := newTestingMapIndex(t)

	numInserts := testPageSize * 10
	insertKeys := make([]string, 0, numInserts)

	testValue := "test_value"

	for i := 0; i < numInserts; i++ {
		testKey := fmt.Sprintf("test_key_%d", i)
		insertKeys = append(insertKeys, testKey)
	}

	insertSelector := NewMapArraySelector(insertKeys)
	_, err := index.Upsert(&insertSelector, testValue)
	util.Ok(t, err)

	testValue2 := "test_value_2"
	updateSelector := NewMapArraySelector(insertKeys)
	updateRes, err := index.Upsert(&updateSelector, testValue2)
	util.Ok(t, err)

	updatedKeys := make([]string, 0, numInserts)
	updatedKeyJSON, err := json.Marshal(updateRes)
	util.Ok(t, err)

	err = json.Unmarshal(updatedKeyJSON, &updatedKeys)
	util.Ok(t, err)

	util.Equals(t, numInserts, len(updatedKeys))

	// query & verify updated values
	querySelector := NewMapArraySelector(insertKeys)
	queryRes, err := index.Query(&querySelector)
	util.Ok(t, err)

	queryValues := make([]string, 0, numInserts)
	queryResJSON, err := json.Marshal(queryRes)
	util.Ok(t, err)
	err = json.Unmarshal(queryResJSON, &queryValues)
	util.Ok(t, err)

	util.Equals(t, numInserts, len(queryValues))

	for _, value := range queryValues {
		util.Equals(t, testValue2, value)
	}
}

func TestMapIndexDeleteOne(t *testing.T) {
	index := newTestingMapIndex(t)
	count, err := index.Count()
	util.Ok(t, err)

	util.Equals(t, "0", count.String())

	// test insert & retrieve value
	testVal := "testVal"
	testKey := "testKey"
	selector := NewMapSingleSelector(testKey)
	insertRes, err := index.Insert(&selector, testVal)
	util.Ok(t, err)
	util.Equals(t, testKey, insertRes.String())

	queryRes, err := index.Query(&selector)
	util.Ok(t, err)
	util.Equals(t, testVal, queryRes.String())

	// update the value
	selector = NewMapSingleSelector(testKey)
	_, err = index.Delete(&selector)
	util.Ok(t, err)
}

func TestMapIndexDeleteMany(t *testing.T) {
	indexName := "test_map_index"
	driver := driver.NewMemoryDriver()
	driver.CreateMapIndex(indexName)
	index, err := NewMapIndex(indexName, &driver, testPageSize)
	util.Ok(t, err)

	numInserts := testPageSize * 10
	insertKeys := make([]string, 0, numInserts)

	testValue := "test_value"

	for i := 0; i < numInserts; i++ {
		testKey := fmt.Sprintf("test_key_%d", i)
		insertKeys = append(insertKeys, testKey)
	}

	insertSelector := NewMapArraySelector(insertKeys)
	_, err = index.Insert(&insertSelector, testValue)
	util.Ok(t, err)

	deleteSelector := NewMapArraySelector(insertKeys)
	deleteRes, err := index.Delete(&deleteSelector)
	util.Ok(t, err)

	deletedKeys := make([]string, 0, numInserts)
	deletedKeysJSON, err := json.Marshal(deleteRes)
	util.Ok(t, err)

	err = json.Unmarshal(deletedKeysJSON, &deletedKeys)
	util.Ok(t, err)

	util.Equals(t, numInserts, len(deletedKeys))

	// query & verify updated values
	querySelector := NewMapArraySelector(insertKeys)
	queryRes, err := index.Query(&querySelector)
	util.Ok(t, err)

	queryValues := make([]string, 0, numInserts)
	queryResJSON, err := json.Marshal(queryRes)
	util.Ok(t, err)
	err = json.Unmarshal(queryResJSON, &queryValues)
	util.Ok(t, err)

	util.Equals(t, numInserts, len(queryValues))

}

func TestMapIndexList(t *testing.T) {
	indexName := "test_map_index"
	driver := driver.NewMemoryDriver()
	driver.CreateMapIndex(indexName)
	index, err := NewMapIndex(indexName, &driver, testPageSize)
	util.Ok(t, err)

	numInserts := testPageSize * 10
	insertKeys := make([]string, 0, numInserts)

	for i := 0; i < numInserts; i++ {
		testValue := fmt.Sprintf("test_value_%d", i)
		testKey := fmt.Sprintf("test_key_%d", i)
		selector := NewMapSingleSelector(testKey)
		id, err := index.Insert(&selector, testValue)
		util.Ok(t, err)
		insertKeys = append(insertKeys, id.String())
		util.Ok(t, err)
	}

	results, err := index.List(0, 0, false)
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

func TestMapIndexCount(t *testing.T) {
	indexName := "test_map_index"
	driver := driver.NewMemoryDriver()
	driver.CreateMapIndex(indexName)
	index, err := NewMapIndex(indexName, &driver, testPageSize)
	util.Ok(t, err)

	numInserts := testPageSize * 10
	insertKeys := make([]string, 0, numInserts)

	for i := 0; i < numInserts; i++ {
		testValue := fmt.Sprintf("test_value_%d", i)
		testKey := fmt.Sprintf("test_key_%d", i)
		selector := NewMapSingleSelector(testKey)
		id, err := index.Insert(&selector, testValue)
		util.Ok(t, err)
		insertKeys = append(insertKeys, id.String())
		util.Ok(t, err)
	}

	result, err := index.Count()
	util.Ok(t, err)

	util.Equals(t, strconv.Itoa(numInserts), result.String())
}
