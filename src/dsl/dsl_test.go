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

	updateResultArr := parseArrayResult(updateRes)

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

	deleteResultArr := parseArrayResult(deleteRes)

	util.Equals(t, nBatch, len(deleteResultArr))

	queryStr := fmt.Sprintf("query %s %s", autoIndex, selector)
	queryRes, err := Execute(queryStr, testConf)

	queryResArr := parseArrayResult(queryRes)

	util.Equals(t, 0, len(queryResArr))
	util.Assert(t, err != nil, "querying deleted ids should return error")

}

// insert and update one record in a map index
func TestExecuteMapDeleteOne(t *testing.T) {
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

// this function uses panic because err should never be non-nil
func parseArrayResult(result store.Result) []string {
	arr := []string{}
	jsonBytes, err := result.MarshalJSON()
	if err != nil {
		panic(err)
	}
	err = json.Unmarshal(jsonBytes, &arr)
	if err != nil {
		panic(err)
	}
	return arr
}
