package dsl

import (
	"errors"
	"fmt"
	"keybite/config"
	"keybite/store"
	"keybite/store/driver"
	"strconv"
)

type command struct {
	keyword     string // the keyword that starts the command
	numTokens   int    // the number of tokens before the command payload
	description string // a brief description of the command's use
	example     string // an example command
	// the function to call to get the result. assumes correct input.
	execute func(tokens []string, payload string, conf config.Config) (store.Result, error)
}

// Query existing data
var Query = command{
	keyword:     "query",
	numTokens:   2, // query index "..."
	description: "Query an auto index for the given key",
	example:     "query index_name 1",
	execute: func(tokens []string, payload string, conf config.Config) (store.Result, error) {
		storageDriver, err := driver.GetConfiguredDriver(conf)
		if err != nil {
			return store.EmptyResult(), err
		}

		pageSize, err := conf.GetInt("AUTO_PAGE_SIZE")
		if err != nil {
			return store.EmptyResult(), errors.New("Invalid auto index page size from environment")
		}

		index, err := store.NewAutoIndex(tokens[1], storageDriver, pageSize)
		if err != nil {
			return store.EmptyResult(), fmt.Errorf("reading index %s failed: %w", tokens[1], err)
		}

		selector, err := ParseAutoSelector(tokens[2])
		if err != nil {
			return store.EmptyResult(), fmt.Errorf("could not parse query: %w", err)
		}

		result, err := index.Query(selector)
		if err != nil {
			return result, fmt.Errorf("query on index %s failed: %w", tokens[1], err)
		}
		return result, nil
	},
}

// Insert new data
var Insert = command{
	keyword:     "insert",
	numTokens:   2, // insert index "..."
	description: "Insert the given value into an auto index. Everything after the index name is treated as a single string value.",
	example:     "insert index_name the string to insert",
	execute: func(tokens []string, payload string, conf config.Config) (store.Result, error) {
		storageDriver, err := driver.GetConfiguredDriver(conf)
		if err != nil {
			return store.EmptyResult(), err
		}

		pageSize, err := conf.GetInt("AUTO_PAGE_SIZE")
		if err != nil {
			return store.EmptyResult(), errors.New("Invalid auto index page size from environment")
		}

		index, err := store.NewAutoIndex(tokens[1], storageDriver, pageSize)
		if err != nil {
			return store.EmptyResult(), fmt.Errorf("reading index %s failed: %w", tokens[1], err)
		}

		result, err := index.Insert(payload)
		if err != nil {
			return store.EmptyResult(), fmt.Errorf("inserting into index %s failed: %w", index.Name, err)
		}

		resultStr, err := strconv.FormatUint(result, 10), nil
		if err != nil {
			return store.EmptyResult(), fmt.Errorf("error formatting insert result %d: %w", result, err)
		}

		return store.SingleResult(resultStr), nil
	},
}

// Update existing data
var Update = command{
	keyword:     "update",
	numTokens:   3, // update index 3 "..."
	description: "Update the existing record at the given key. Fails if the record does not exist.",
	example:     "update index_name 2 the new value of the key",
	execute: func(tokens []string, payload string, conf config.Config) (store.Result, error) {
		storageDriver, err := driver.GetConfiguredDriver(conf)
		if err != nil {
			return store.EmptyResult(), err
		}

		pageSize, err := conf.GetInt("AUTO_PAGE_SIZE")
		if err != nil {
			return store.EmptyResult(), errors.New("Invalid auto index page size from environment")
		}

		index, err := store.NewAutoIndex(tokens[1], storageDriver, pageSize)
		if err != nil {
			return store.EmptyResult(), fmt.Errorf("reading index %s failed: %w", tokens[1], err)
		}

		selector, err := ParseAutoSelector(tokens[2])
		if err != nil {
			return store.EmptyResult(), fmt.Errorf("cannot query non-integer ID %s", tokens[2])
		}

		result, err := index.Update(selector, payload)
		if err != nil {
			return store.EmptyResult(), fmt.Errorf("updating index %s failed: %w", tokens[1], err)
		}
		return result, nil
	},
}

// Delete existing data
var Delete = command{
	keyword:     "delete",
	numTokens:   2, // "delete index ..."
	description: "Delete the existing record at the given key. Fails if the key does not exist",
	example:     "delete index_name 2",
	execute: func(tokens []string, payload string, conf config.Config) (store.Result, error) {
		storageDriver, err := driver.GetConfiguredDriver(conf)
		if err != nil {
			return store.EmptyResult(), err
		}

		pageSize, err := conf.GetInt("AUTO_PAGE_SIZE")
		if err != nil {
			return store.EmptyResult(), errors.New("Invalid auto index page size from environment")
		}

		index, err := store.NewAutoIndex(tokens[1], storageDriver, pageSize)
		if err != nil {
			return store.EmptyResult(), fmt.Errorf("reading index %s failed: %w", tokens[1], err)
		}

		selector, err := ParseAutoSelector(tokens[2])
		if err != nil {
			return store.EmptyResult(), fmt.Errorf("cannot delete non-integer ID %s", tokens[2])
		}

		result, err := index.Delete(selector)
		if err != nil {
			return result, fmt.Errorf("delete from index %s failed: %w", tokens[1], err)
		}
		return result, nil

	},
}

// CreateAutoIndex in data dir
var CreateAutoIndex = command{
	keyword:     "create_auto_index",
	numTokens:   1, // create_index spam
	description: "Create a new auto index with the given name",
	example:     "create_auto_index index_name",
	execute: func(tokens []string, payload string, conf config.Config) (store.Result, error) {
		storageDriver, err := driver.GetConfiguredDriver(conf)
		if err != nil {
			return store.EmptyResult(), err
		}
		indexName := tokens[1]

		return store.SingleResult(indexName), storageDriver.CreateAutoIndex(indexName)
	},
}

// CreateMapIndex in data dir
var CreateMapIndex = command{
	keyword:     "create_map_index",
	numTokens:   1, // create_map_index spam
	description: "Create a new map index with the given name",
	example:     "create_map_index map_index_name",
	execute: func(tokens []string, payload string, conf config.Config) (store.Result, error) {
		storageDriver, err := driver.GetConfiguredDriver(conf)
		if err != nil {
			return store.EmptyResult(), err
		}
		indexName := tokens[1]

		return store.SingleResult(indexName), storageDriver.CreateMapIndex(indexName)
	},
}

// QueryKey queries a map index.
// @TODO remove this, query should work on either index type
var QueryKey = command{
	keyword:     "query_key",
	numTokens:   2,
	description: "Query a map index for the given key",
	example:     "query_key map_index_name user1_email",
	execute: func(tokens []string, payload string, conf config.Config) (store.Result, error) {
		indexName := tokens[1]
		key := tokens[2]
		selector := ParseMapSelector(key)

		storageDriver, err := driver.GetConfiguredDriver(conf)
		if err != nil {
			return store.EmptyResult(), err
		}

		pageSize, err := conf.GetInt("MAP_PAGE_SIZE")
		if err != nil {
			return store.EmptyResult(), errors.New("Invalid auto index page size from environment")
		}

		mapIndex, err := store.NewMapIndex(indexName, storageDriver, pageSize)
		if err != nil {
			return store.EmptyResult(), fmt.Errorf("reading index %s failed: %w", indexName, err)
		}

		result, err := mapIndex.Query(selector)
		if err != nil {
			return result, fmt.Errorf("query on index %s failed: %w", indexName, err)
		}
		return result, nil
	},
}

// InsertKey inserts into a map index
var InsertKey = command{
	keyword:     "insert_key",
	numTokens:   3,
	description: "Insert a key and value into a map index",
	example:     "insert_key map_index_name user1_email johndoe@example.com",
	execute: func(tokens []string, payload string, conf config.Config) (store.Result, error) {
		indexName := tokens[1]

		storageDriver, err := driver.GetConfiguredDriver(conf)
		if err != nil {
			return store.EmptyResult(), err
		}

		pageSize, err := conf.GetInt("MAP_PAGE_SIZE")
		if err != nil {
			return store.EmptyResult(), errors.New("Invalid auto index page size from environment")
		}

		selector := ParseMapSelector(tokens[2])
		mapIndex, err := store.NewMapIndex(indexName, storageDriver, pageSize)
		if err != nil {
			return store.EmptyResult(), fmt.Errorf("reading index %s failed: %w", indexName, err)
		}

		resultStr, err := mapIndex.Insert(selector, payload)
		if err != nil {
			return store.EmptyResult(), fmt.Errorf("inserting into index %s failed: %w", indexName, err)
		}

		return store.SingleResult(resultStr), nil
	},
}

// UpdateKey updates an existing key in a map index
var UpdateKey = command{
	keyword:     "update_key",
	numTokens:   3,
	description: "Update an existing record at the provided key. Fails if the record does not exist.",
	example:     "update_key map_index_name user1_email janedoe@example.com",
	execute: func(tokens []string, payload string, conf config.Config) (store.Result, error) {
		indexName := tokens[1]
		selector := ParseMapSelector(tokens[2])
		storageDriver, err := driver.GetConfiguredDriver(conf)
		if err != nil {
			return store.EmptyResult(), err
		}

		pageSize, err := conf.GetInt("MAP_PAGE_SIZE")
		if err != nil {
			return store.EmptyResult(), errors.New("Invalid or missing map index page size from environment")
		}

		mapIndex, err := store.NewMapIndex(indexName, storageDriver, pageSize)
		if err != nil {
			return store.EmptyResult(), fmt.Errorf("reading index %s failed: %w", tokens[1], err)
		}
		return mapIndex.Update(selector, payload)
	},
}

// UpsertKey idempotent inserts into a map index, overwriting any existing value at provided key
var UpsertKey = command{
	keyword:     "upsert_key",
	numTokens:   3,
	description: "Update or insert a record with the specified key",
	example:     "upsert_key map_index_name user1_email janedoe@example.com",
	execute: func(tokens []string, payload string, conf config.Config) (store.Result, error) {
		indexName := tokens[1]
		selector := ParseMapSelector(tokens[2])
		storageDriver, err := driver.GetConfiguredDriver(conf)
		if err != nil {
			return store.EmptyResult(), err
		}

		pageSize, err := conf.GetInt("MAP_PAGE_SIZE")
		if err != nil {
			return store.EmptyResult(), errors.New("Invalid or missing map index page size from environment")
		}

		mapIndex, err := store.NewMapIndex(indexName, storageDriver, pageSize)
		if err != nil {
			return store.EmptyResult(), fmt.Errorf("reading index %s failed: %w", tokens[1], err)
		}

		result, err := mapIndex.Upsert(selector, payload)
		if err != nil {
			return result, fmt.Errorf("upserting into index %s failed: %w", tokens[1], err)
		}
		return result, nil
	},
}

// DeleteKey deletes a record from a map index
var DeleteKey = command{
	keyword:     "delete_key",
	numTokens:   2, // "delete_key index ..."
	description: "Delete a record with the specified key",
	example:     "delete_key map_index_name user1_email",
	execute: func(tokens []string, payload string, conf config.Config) (store.Result, error) {
		indexName := tokens[1]
		selector := ParseMapSelector(tokens[2])
		storageDriver, err := driver.GetConfiguredDriver(conf)
		if err != nil {
			return store.EmptyResult(), err
		}

		pageSize, err := conf.GetInt("MAP_PAGE_SIZE")
		if err != nil {
			return store.EmptyResult(), errors.New("Invalid or missing map index page size from environment")
		}

		mapIndex, err := store.NewMapIndex(indexName, storageDriver, pageSize)
		if err != nil {
			return store.EmptyResult(), fmt.Errorf("reading index %s failed: %w", indexName, err)
		}

		result, err := mapIndex.Delete(selector)
		if err != nil {
			return result, fmt.Errorf("delete from index %s failed: %w", tokens[1], err)
		}
		return result, nil
	},
}

// Commands available to the DSL
var Commands = []command{
	Query,
	Insert,
	Update,
	Delete,
	CreateAutoIndex,
	CreateMapIndex,
	QueryKey,
	InsertKey,
	UpdateKey,
	UpsertKey,
	DeleteKey,
}
