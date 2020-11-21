package dsl

import (
	"encoding/json"
	"fmt"
	"keybite/config"
	"keybite/store"
	"keybite/util"
	"os"
	"strconv"
	"strings"
	"testing"
)

// number of records to insert for batch operations
const nBatch = 200

// config for test environment
var testConf = config.Config(map[string]string{
	"AUTO_PAGE_SIZE":   "100",
	"MAP_PAGE_SIZE":    "1000",
	"DRIVER":           "filesystem",
	"DATA_DIR":         "/tmp/keybite_testing",
	"ENVIRONMENT":      "linux",
	"LOG_LEVEL":        "info",
	"PAGE_EXTENSION":   ".kb",
	"LOCK_DURATION_FS": "50",
})

func createTestIndexes(t *testing.T, conf config.Config) (autoIndexName, mapIndexName string) {
	// create data dir
	dataPath, err := conf.GetString("DATA_DIR")
	util.Ok(t, err)

	if _, err := os.Stat(dataPath); os.IsNotExist(err) {
		os.Mkdir(dataPath, 0777)
	}

	autoIndexName, mapIndexName = "test_auto_index", "test_map_index"

	_, err = Execute(fmt.Sprintf("create_auto_index %s", autoIndexName), conf)
	util.Ok(t, err)
	_, err = Execute(fmt.Sprintf("create_map_index %s", mapIndexName), conf)
	util.Ok(t, err)
	return
}

func dropTestIndexes(t *testing.T, conf config.Config) {
	dataPath, err := conf.GetString("DATA_DIR")
	util.Ok(t, err)

	autoIndexName, mapIndexName := "test_auto_index", "test_map_index"
	_, err = Execute(fmt.Sprintf("drop_auto_index %s", autoIndexName), conf)
	util.Ok(t, err)
	_, err = Execute(fmt.Sprintf("drop_map_index %s", mapIndexName), conf)
	util.Ok(t, err)
	err = os.RemoveAll(dataPath)
	util.Ok(t, err)
}

// insert and query one record in an auto index
func TestExecuteAutoInsertQueryOne(t *testing.T) {
	autoIndex, _ := createTestIndexes(t, testConf)
	defer dropTestIndexes(t, testConf)

	testValue := "test_value"

	insertStr := fmt.Sprintf("insert %s %s", autoIndex, testValue)
	// the insert result contains the ID of the inserted record
	insertResult, err := Execute(insertStr, testConf)
	util.Ok(t, err)

	queryStr := fmt.Sprintf("query %s %s", autoIndex, insertResult.String())
	queryRes, err := Execute(queryStr, testConf)
	util.Ok(t, err)

	util.Equals(t, testValue, queryRes.String())
}

// insert and query one record in an auto index
func TestExecuteAutoInsertQueryMany(t *testing.T) {
	autoIndex, _ := createTestIndexes(t, testConf)
	defer dropTestIndexes(t, testConf)

	testValueFmt := "test_value_%d"

	for i := 0; i < nBatch; i++ {
		value := fmt.Sprintf(testValueFmt, i)
		insertStmt := fmt.Sprintf("insert %s %s", autoIndex, value)
		res, err := Execute(insertStmt, testConf)
		util.Ok(t, err)
		_, err = strconv.ParseUint(res.String(), 10, 64)
		util.Ok(t, err)
	}

	querySelector := fmt.Sprintf("[1:%d]", nBatch)

	queryStr := fmt.Sprintf("query %s %s", autoIndex, querySelector)
	queryRes, err := Execute(queryStr, testConf)
	util.Ok(t, err)

	resultArr := parseArrayResult(queryRes)

	util.Equals(t, nBatch, len(resultArr))
	for i, item := range resultArr {
		expected := fmt.Sprintf(testValueFmt, i)
		util.Equals(t, expected, item)
	}
}

// insert and query one record in a map index
func TestExecuteMapInsertQueryOne(t *testing.T) {
	_, mapIndex := createTestIndexes(t, testConf)
	defer dropTestIndexes(t, testConf)

	testValue := "test_value"
	testKey := "test_key"

	insertStr := fmt.Sprintf("insert_key %s %s %s", mapIndex, testKey, testValue)
	// the insert result contains the ID of the inserted record
	insertResult, err := Execute(insertStr, testConf)
	util.Ok(t, err)

	queryStr := fmt.Sprintf("query_key %s %s", mapIndex, insertResult.String())
	queryRes, err := Execute(queryStr, testConf)
	util.Ok(t, err)

	util.Equals(t, testValue, queryRes.String())
}

// insert and query one record in a map index
func TestExecuteMapInsertQueryMany(t *testing.T) {
	_, mapIndex := createTestIndexes(t, testConf)
	defer dropTestIndexes(t, testConf)

	testValueFmt := "test_value_%d"
	testKeyFmt := "test_key_%d"

	insertKeys := make([]string, nBatch)

	for i := 0; i < nBatch; i++ {
		key := fmt.Sprintf(testKeyFmt, i)
		value := fmt.Sprintf(testValueFmt, i)
		insertStmt := fmt.Sprintf("insert_key %s %s %s", mapIndex, key, value)
		_, err := Execute(insertStmt, testConf)
		util.Ok(t, err)
		insertKeys[i] = key
	}

	querySelector := "[" + strings.Join(insertKeys, ",") + "]"

	queryStr := fmt.Sprintf("query_key %s %s", mapIndex, querySelector)
	queryRes, err := Execute(queryStr, testConf)
	util.Ok(t, err)

	resultArr := parseArrayResult(queryRes)

	util.Equals(t, nBatch, len(resultArr))
	for i, item := range resultArr {
		expected := fmt.Sprintf(testValueFmt, i)
		util.Equals(t, expected, item)
	}
}

// insert and update one record in an auto index
func TestExecuteAutoUpdateOne(t *testing.T) {
	autoIndex, _ := createTestIndexes(t, testConf)
	defer dropTestIndexes(t, testConf)

	testValue := "test_value"

	insertStr := fmt.Sprintf("insert %s %s", autoIndex, testValue)
	// the insert result contains the ID of the inserted record
	insertRes, err := Execute(insertStr, testConf)
	util.Ok(t, err)

	updatedValue := "test_value_updated"

	updateStr := fmt.Sprintf("update %s %s %s", autoIndex, insertRes.String(), updatedValue)
	updateRes, err := Execute(updateStr, testConf)
	util.Ok(t, err)

	util.Equals(t, insertRes.String(), updateRes.String())

	queryStr := fmt.Sprintf("query %s %s", autoIndex, updateRes.String())
	queryRes, err := Execute(queryStr, testConf)
	util.Ok(t, err)

	util.Equals(t, queryRes.String(), updatedValue)
}

// insert and update many records in an auto index
func TestExecuteAutoUpdateMany(t *testing.T) {
	autoIndex, _ := createTestIndexes(t, testConf)
	defer dropTestIndexes(t, testConf)

	testValueFmt := "test_value_%d"

	for i := 0; i < nBatch; i++ {
		value := fmt.Sprintf(testValueFmt, i)
		insertStmt := fmt.Sprintf("insert %s %s", autoIndex, value)
		res, err := Execute(insertStmt, testConf)
		util.Ok(t, err)
		_, err = strconv.ParseUint(res.String(), 10, 64)
		util.Ok(t, err)
	}

	updatedValue := "test_value_updated"
	selector := fmt.Sprintf("[1:%d]", nBatch)

	updateStr := fmt.Sprintf("update %s %s %s", autoIndex, selector, updatedValue)
	updateRes, err := Execute(updateStr, testConf)
	util.Ok(t, err)

	updateResultArr := parseIDArrayResult(updateRes)

	util.Equals(t, nBatch, len(updateResultArr))

	queryStr := fmt.Sprintf("query %s %s", autoIndex, selector)
	queryRes, err := Execute(queryStr, testConf)
	util.Ok(t, err)

	queryResultArr := parseArrayResult(queryRes)
	util.Equals(t, nBatch, len(queryResultArr))

	for _, item := range queryResultArr {
		util.Equals(t, updatedValue, item)
	}

}

// insert and update one record in a map index
func TestExecuteMapUpdateOne(t *testing.T) {
	_, mapIndex := createTestIndexes(t, testConf)
	defer dropTestIndexes(t, testConf)

	testValue := "test_value"
	testKey := "test_key"

	insertStr := fmt.Sprintf("insert_key %s %s %s", mapIndex, testKey, testValue)
	// the insert result contains the ID of the inserted record
	insertRes, err := Execute(insertStr, testConf)
	util.Ok(t, err)

	updatedValue := "test_value_updated"

	updateStr := fmt.Sprintf("update_key %s %s %s", mapIndex, insertRes.String(), updatedValue)
	updateRes, err := Execute(updateStr, testConf)
	util.Ok(t, err)

	util.Equals(t, insertRes.String(), updateRes.String())

	queryStr := fmt.Sprintf("query_key %s %s", mapIndex, updateRes.String())
	queryRes, err := Execute(queryStr, testConf)
	util.Ok(t, err)

	util.Equals(t, queryRes.String(), updatedValue)
}

// insert and update many records in a map index
func TestExecuteMapUpdateMany(t *testing.T) {
	_, mapIndex := createTestIndexes(t, testConf)
	defer dropTestIndexes(t, testConf)

	testValueFmt := "test_value_%d"
	testKeyFmt := "test_key_%d"

	insertKeys := make([]string, nBatch)

	for i := 0; i < nBatch; i++ {
		key := fmt.Sprintf(testKeyFmt, i)
		value := fmt.Sprintf(testValueFmt, i)
		insertStmt := fmt.Sprintf("insert_key %s %s %s", mapIndex, key, value)
		_, err := Execute(insertStmt, testConf)
		util.Ok(t, err)
		insertKeys[i] = key
	}

	updatedValue := "test_value_updated"
	selector := "[" + strings.Join(insertKeys, ",") + "]"

	updateStr := fmt.Sprintf("update_key %s %s %s", mapIndex, selector, updatedValue)
	updateRes, err := Execute(updateStr, testConf)
	util.Ok(t, err)

	updateResultArr := parseArrayResult(updateRes)

	util.Equals(t, nBatch, len(updateResultArr))

	queryStr := fmt.Sprintf("query_key %s %s", mapIndex, selector)
	queryRes, err := Execute(queryStr, testConf)
	util.Ok(t, err)

	queryResultArr := parseArrayResult(queryRes)
	util.Equals(t, nBatch, len(queryResultArr))

	for _, item := range queryResultArr {
		util.Equals(t, updatedValue, item)
	}
}

// upsert one record in a map index
func TestExecuteMapUpsertOne(t *testing.T) {
	_, mapIndex := createTestIndexes(t, testConf)
	defer dropTestIndexes(t, testConf)

	testValue := "test_value"
	testKey := "test_key"

	insertStr := fmt.Sprintf("upsert_key %s %s %s", mapIndex, testKey, testValue)
	// the insert result contains the ID of the inserted record
	insertRes, err := Execute(insertStr, testConf)
	util.Ok(t, err)

	updatedValue := "test_value_updated"

	updateStr := fmt.Sprintf("upsert_key %s %s %s", mapIndex, insertRes.String(), updatedValue)
	updateRes, err := Execute(updateStr, testConf)
	util.Ok(t, err)

	util.Equals(t, insertRes.String(), updateRes.String())

	queryStr := fmt.Sprintf("query_key %s %s", mapIndex, updateRes.String())
	queryRes, err := Execute(queryStr, testConf)
	util.Ok(t, err)

	util.Equals(t, queryRes.String(), updatedValue)
}

// upsert many records in a map index
func TestExecuteMapUpsertMany(t *testing.T) {
	_, mapIndex := createTestIndexes(t, testConf)
	defer dropTestIndexes(t, testConf)

	testValueFmt := "test_value_%d"
	testKeyFmt := "test_key_%d"

	insertKeys := make([]string, nBatch)

	for i := 0; i < nBatch; i++ {
		key := fmt.Sprintf(testKeyFmt, i)
		value := fmt.Sprintf(testValueFmt, i)
		insertStmt := fmt.Sprintf("upsert_key %s %s %s", mapIndex, key, value)
		_, err := Execute(insertStmt, testConf)
		util.Ok(t, err)
		insertKeys[i] = key
	}

	updatedValue := "test_value_updated"
	selector := "[" + strings.Join(insertKeys, ",") + "]"

	updateStr := fmt.Sprintf("upsert_key %s %s %s", mapIndex, selector, updatedValue)
	updateRes, err := Execute(updateStr, testConf)
	util.Ok(t, err)

	updateResultArr := parseArrayResult(updateRes)

	util.Equals(t, nBatch, len(updateResultArr))

	queryStr := fmt.Sprintf("query_key %s %s", mapIndex, selector)
	queryRes, err := Execute(queryStr, testConf)
	util.Ok(t, err)

	queryResultArr := parseArrayResult(queryRes)
	util.Equals(t, nBatch, len(queryResultArr))

	for _, item := range queryResultArr {
		util.Equals(t, updatedValue, item)
	}
}

// insert and delete one record in an auto index
func TestExecuteAutoDeleteOne(t *testing.T) {
	autoIndex, _ := createTestIndexes(t, testConf)
	defer dropTestIndexes(t, testConf)

	testValue := "test_value"

	insertStr := fmt.Sprintf("insert %s %s", autoIndex, testValue)
	// the insert result contains the ID of the inserted record
	insertRes, err := Execute(insertStr, testConf)
	util.Ok(t, err)

	deleteStr := fmt.Sprintf("delete %s %s", autoIndex, insertRes.String())
	deleteRes, err := Execute(deleteStr, testConf)
	util.Ok(t, err)

	util.Equals(t, insertRes.String(), deleteRes.String())

	queryStr := fmt.Sprintf("query %s %s", autoIndex, deleteRes.String())
	queryRes, err := Execute(queryStr, testConf)
	t.Log(queryRes)
	util.Assert(t, err != nil, "querying deleted key returns error")
}

// insert and delete many records in an auto index
func TestExecuteAutoDeleteMany(t *testing.T) {
	autoIndex, _ := createTestIndexes(t, testConf)
	defer dropTestIndexes(t, testConf)

	testValueFmt := "test_value_%d"

	for i := 0; i < nBatch; i++ {
		value := fmt.Sprintf(testValueFmt, i)
		insertStmt := fmt.Sprintf("insert %s %s", autoIndex, value)
		res, err := Execute(insertStmt, testConf)
		util.Ok(t, err)
		_, err = strconv.ParseUint(res.String(), 10, 64)
		util.Ok(t, err)
	}

	selector := fmt.Sprintf("[1:%d]", nBatch)

	deleteStr := fmt.Sprintf("delete %s %s", autoIndex, selector)
	deleteRes, err := Execute(deleteStr, testConf)
	util.Ok(t, err)

	deleteResultArr := parseIDArrayResult(deleteRes)

	util.Equals(t, nBatch, len(deleteResultArr))

	queryStr := fmt.Sprintf("query %s %s", autoIndex, selector)
	queryRes, err := Execute(queryStr, testConf)

	queryResArr := parseArrayResult(queryRes)

	for _, res := range queryResArr {
		util.Equals(t, "", res)
	}

	// @TODO #39 delete multiple-selector should return an error on partial or whole failure
	// util.Assert(t, err != nil, "querying deleted ids should return error")
}

// insert and delete one record in a map index
func TestExecuteMapDeleteOne(t *testing.T) {
	_, mapIndex := createTestIndexes(t, testConf)
	defer dropTestIndexes(t, testConf)

	testValue := "test_value"
	testKey := "test_key"

	insertStr := fmt.Sprintf("insert_key %s %s %s", mapIndex, testKey, testValue)
	// the insert result contains the ID of the inserted record
	insertRes, err := Execute(insertStr, testConf)
	util.Ok(t, err)

	deleteStr := fmt.Sprintf("delete_key %s %s", mapIndex, insertRes.String())
	deleteRes, err := Execute(deleteStr, testConf)
	util.Ok(t, err)

	util.Equals(t, insertRes.String(), deleteRes.String())

	queryStr := fmt.Sprintf("query_key %s %s", mapIndex, deleteRes.String())
	queryRes, err := Execute(queryStr, testConf)

	queryResArr := parseArrayResult(queryRes)
	util.Equals(t, 0, len(queryResArr))

}

// insert and update many records in a map index
func TestExecuteMapDeleteMany(t *testing.T) {
	_, mapIndex := createTestIndexes(t, testConf)
	defer dropTestIndexes(t, testConf)

	testValueFmt := "test_value_%d"
	testKeyFmt := "test_key_%d"

	insertKeys := make([]string, nBatch)

	for i := 0; i < nBatch; i++ {
		key := fmt.Sprintf(testKeyFmt, i)
		value := fmt.Sprintf(testValueFmt, i)
		insertStmt := fmt.Sprintf("insert_key %s %s %s", mapIndex, key, value)
		_, err := Execute(insertStmt, testConf)
		util.Ok(t, err)
		insertKeys[i] = key
	}

	selector := "[" + strings.Join(insertKeys, ",") + "]"

	deleteStr := fmt.Sprintf("delete_key %s %s", mapIndex, selector)
	deleteRes, err := Execute(deleteStr, testConf)
	util.Ok(t, err)

	deleteResultArr := parseArrayResult(deleteRes)

	util.Equals(t, nBatch, len(deleteResultArr))

	queryStr := fmt.Sprintf("query_key %s %s", mapIndex, selector)
	queryRes, err := Execute(queryStr, testConf)

	queryResultArr := parseArrayResult(queryRes)
	util.Equals(t, nBatch, len(queryResultArr))
	for _, res := range queryResultArr {
		util.Equals(t, "", res)
	}

	// @TODO #39 delete multiple-selector should return an error on partial or whole failure
	// util.Assert(t, err != nil, "querying deleted ids should return error")
}

func TestExecuteAutoList(t *testing.T) {
	autoIndex, _ := createTestIndexes(t, testConf)
	defer dropTestIndexes(t, testConf)

	testValueFmt := "test_value_%d"

	for i := 0; i < nBatch; i++ {
		value := fmt.Sprintf(testValueFmt, i)
		insertStmt := fmt.Sprintf("insert %s %s", autoIndex, value)
		res, err := Execute(insertStmt, testConf)
		util.Ok(t, err)
		_, err = strconv.ParseUint(res.String(), 10, 64)
		util.Ok(t, err)
	}

	listStr := fmt.Sprintf("list %s", autoIndex)
	listRes, err := Execute(listStr, testConf)
	util.Ok(t, err)

	listResArr := parseListResult(listRes)
	util.Equals(t, nBatch, len(listResArr))

	// test limit
	limit := 10
	listLimitStr := fmt.Sprintf("list %s %d", autoIndex, limit)
	listLimitRes, err := Execute(listLimitStr, testConf)
	util.Ok(t, err)

	listLimitResArr := parseListResult(listLimitRes)
	util.Equals(t, limit, len(listLimitResArr))

	// test offset
	offset := 10
	listOffsetStr := fmt.Sprintf("list %s 0 %d", autoIndex, offset)
	listOffsetRes, err := Execute(listOffsetStr, testConf)
	util.Ok(t, err)

	listOffsetResArr := parseListResult(listOffsetRes)

	for _, item := range listOffsetResArr {
		util.Assert(t, item.Key > uint64(offset), fmt.Sprintf("fetched IDs should all be greater than offset: id %d > %d", item.Key, offset))
	}

	// test limit + offset
	listLimitOffsetStr := fmt.Sprintf("list %s %d %d", autoIndex, limit, offset)
	listLimitOffsetRes, err := Execute(listLimitOffsetStr, testConf)
	util.Ok(t, err)

	listLimitOffsetResArr := parseListResult(listLimitOffsetRes)

	util.Equals(t, limit, len(listLimitOffsetResArr))

	for _, item := range listLimitOffsetResArr {
		util.Assert(t, item.Key > uint64(offset), fmt.Sprintf("fetched IDs should all be greater than offset: id %d > %d", item.Key, offset))
	}
}

func TestExecuteMapList(t *testing.T) {
	_, mapIndex := createTestIndexes(t, testConf)
	defer dropTestIndexes(t, testConf)

	testValueFmt := "test_value_%d"
	testKeyFmt := "tk%d"

	for i := 0; i < nBatch; i++ {
		key := fmt.Sprintf(testKeyFmt, i)
		value := fmt.Sprintf(testValueFmt, i)
		insertStmt := fmt.Sprintf("insert_key %s %s %s", mapIndex, key, value)
		_, err := Execute(insertStmt, testConf)
		util.Ok(t, err)
	}

	listStr := fmt.Sprintf("list_key %s", mapIndex)
	listRes, err := Execute(listStr, testConf)
	util.Ok(t, err)

	listResArr := parseMapListResult(listRes)
	util.Equals(t, nBatch, len(listResArr))

	// test limit
	limit := 10

	listLimitStr := fmt.Sprintf("list_key %s %d", mapIndex, limit)
	listLimitRes, err := Execute(listLimitStr, testConf)
	util.Ok(t, err)

	listLimitResArr := parseMapListResult(listLimitRes)
	util.Equals(t, limit, len(listLimitResArr))

	// test offset
	offset := 10
	listOffsetStr := fmt.Sprintf("list_key %s %d %d", mapIndex, limit, offset)
	listOffsetRes, err := Execute(listOffsetStr, testConf)
	util.Ok(t, err)

	listOffsetResArr := parseMapListResult(listOffsetRes)

	for _, item := range listOffsetResArr {
		minKey := fmt.Sprintf("test_key_%d", offset)
		util.Assert(t, item.Key > minKey, fmt.Sprintf("fetched IDs should all be greater than offset: id %s > %d", item.Key, offset))
	}

	// test limit + offset
	listLimitOffsetStr := fmt.Sprintf("list_key %s %d %d", mapIndex, limit, offset)
	listLimitOffsetRes, err := Execute(listLimitOffsetStr, testConf)
	util.Ok(t, err)

	listLimitOffsetResArr := parseMapListResult(listLimitOffsetRes)
	util.Equals(t, limit, len(listLimitOffsetResArr))

	minKey := fmt.Sprintf(testKeyFmt, offset)

	for _, item := range listLimitOffsetResArr {
		util.Assert(t, item.Key > minKey, fmt.Sprintf("fetched IDs should all be greater than offset: id %s > %d", item.Key, offset))
	}
}

func TestExecuteAutoCount(t *testing.T) {
	autoIndex, _ := createTestIndexes(t, testConf)
	defer dropTestIndexes(t, testConf)

	testValueFmt := "test_value_%d"

	for i := 0; i < nBatch; i++ {
		value := fmt.Sprintf(testValueFmt, i)
		insertStmt := fmt.Sprintf("insert %s %s", autoIndex, value)
		res, err := Execute(insertStmt, testConf)
		util.Ok(t, err)
		_, err = strconv.ParseUint(res.String(), 10, 64)
		util.Ok(t, err)
	}

	countStr := fmt.Sprintf("count %s", autoIndex)
	countRes, err := Execute(countStr, testConf)
	util.Ok(t, err)

	count, err := strconv.Atoi(countRes.String())
	util.Equals(t, nBatch, count)
}

func TestExecuteMapCount(t *testing.T) {
	_, mapIndex := createTestIndexes(t, testConf)
	defer dropTestIndexes(t, testConf)

	testValueFmt := "test_value_%d"
	testKeyFmt := "test_key_%d"

	for i := 0; i < nBatch; i++ {
		key := fmt.Sprintf(testKeyFmt, i)
		value := fmt.Sprintf(testValueFmt, i)
		insertStmt := fmt.Sprintf("insert_key %s %s %s", mapIndex, key, value)
		_, err := Execute(insertStmt, testConf)
		util.Ok(t, err)
	}

	countStr := fmt.Sprintf("count_key %s", mapIndex)
	countRes, err := Execute(countStr, testConf)
	util.Ok(t, err)

	count, err := strconv.Atoi(countRes.String())
	util.Equals(t, nBatch, count)
}

func TestExecuteQueryAutoArraySelector(t *testing.T) {
	_, mapIndex := createTestIndexes(t, testConf)
	defer dropTestIndexes(t, testConf)

	for i := 0; i < 3; i++ {
		key := strconv.Itoa(i)
		value := "testVal"
		insertStmt := fmt.Sprintf("insert_key %s %s %s", mapIndex, key, value)
		_, err := Execute(insertStmt, testConf)
		util.Ok(t, err)
	}

	queryStr := fmt.Sprintf("query %s [1,2,3]", mapIndex)
	queryRes, err := Execute(queryStr, testConf)
	util.Ok(t, err)
	util.Assert(t, queryRes.Valid(), "query result should be valid")

}

func TestUnexpectedEndOfInputError(t *testing.T) {
	_, mapIndex := createTestIndexes(t, testConf)
	defer dropTestIndexes(t, testConf)

	missingIndexQueries := []string{
		"query",
		"query_key",
		"insert",
		"insert_key",
		"update",
		"update_key",
		"upsert_key",
		"delete",
		"delete_key",
		"list",
		"list_key",
		"count",
		"count_key",
	}

	for _, query := range missingIndexQueries {
		_, err := Execute(query, testConf)
		util.Assert(t, err != nil, fmt.Sprintf("error for query with missing index '%s' should be non-nil", query))
	}

	missingSelectorFormats := []string{
		"query %s",
		"query_key %s",
		"insert_key %s",
		"update %s",
		"update_key %s",
		"upsert_key %s",
		"delete %s",
		"delete_key %s",
		"upsert_key %s",
	}

	for _, format := range missingSelectorFormats {
		query := fmt.Sprintf(format, mapIndex)
		_, err := Execute(query, testConf)
		util.Assert(t, err != nil, fmt.Sprintf("error for query with missing selector '%s' should be non-nil", query))
	}

	invalidSelectorFormats := []string{
		"query %s [",
		"query_key %s [:",
		"insert_key %s [,",
		"update %s ]",
		"update_key %s ]",
		"upsert_key %s []",
		"delete %s [:,",
		"delete_key %s ]",
		"upsert_key %s :",
	}

	for _, format := range invalidSelectorFormats {
		query := fmt.Sprintf(format, mapIndex)
		_, err := Execute(query, testConf)
		util.Assert(t, err != nil, fmt.Sprintf("error for query with invalid selector '%s' should be non-nil", query))
	}
}

func TestInvalidSortDirectionError(t *testing.T) {
	autoIndex, mapIndex := createTestIndexes(t, testConf)
	defer dropTestIndexes(t, testConf)

	_, err := Execute(fmt.Sprintf("list %s up", autoIndex), testConf)
	util.Assert(t, err != nil, "error for invalid sort direction query (auto index) should be non-nil")

	_, err = Execute(fmt.Sprintf("list_key %s down", mapIndex), testConf)
	util.Assert(t, err != nil, "error for invalid sort direction query (map index) should be non-nill")
}

func TestLimit(t *testing.T) {
	in := []string{"this", "is", "a", "test", "string"}
	strLimit := 4
	limited := limit(in, strLimit)
	util.Equals(t, (strLimit - 1), len(limited))

	longStrLimit := 10
	longLimited := limit(in, longStrLimit)
	util.Equals(t, len(in), len(longLimited))
}

func TestExecuteUnknownKeyword(t *testing.T) {
	_, err := Execute("unknown", testConf)
	util.Assert(t, err != nil, "unknown query keyword should return error")
}

func TestError(t *testing.T) {
	e := Error{
		InternalError:   fmt.Errorf("test error"),
		Message:         "fake message",
		RemainingTokens: []string{"test", "tokens", "for", "testing", "purposes"},
	}

	str := e.Error()
	util.Assert(t, len(str) > 0, "error builds string")

	snippet := e.makeSnippet()
	split := strings.Fields(snippet)
	util.Equals(t, maxSnippetTokens, len(split))
}

// this function uses panic because err should never be non-nil
func parseArrayResult(result store.Result) []string {
	arr := []string{}
	// json as intermediate structure
	jsonBytes, err := json.Marshal(result)
	if err != nil {
		panic(err)
	}
	err = json.Unmarshal(jsonBytes, &arr)
	if err != nil {
		panic(err)
	}
	return arr
}

// this function uses panic because err should never be non-nil
func parseIDArrayResult(result store.Result) []uint64 {
	arr := []uint64{}
	// json as intermediate structure
	jsonBytes, err := json.Marshal(result)
	if err != nil {
		panic(err)
	}
	err = json.Unmarshal(jsonBytes, &arr)
	if err != nil {
		panic(err)
	}
	return arr
}

type keyValue struct {
	Key   uint64 `json:"key"`
	Value string `json:"value"`
}

type mapKeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func parseListResult(result store.Result) []keyValue {
	arr := []keyValue{}
	jsonBytes, err := json.Marshal(result)
	if err != nil {
		panic(err)
	}
	err = json.Unmarshal(jsonBytes, &arr)
	if err != nil {
		panic(err)
	}
	return arr
}

func parseMapListResult(result store.Result) []mapKeyValue {
	arr := []mapKeyValue{}
	jsonBytes, err := json.Marshal(result)
	if err != nil {
		panic(err)
	}
	err = json.Unmarshal(jsonBytes, &arr)
	if err != nil {
		panic(err)
	}
	return arr
}
