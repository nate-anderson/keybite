package dsl

import (
	"errors"
	"fmt"
	"keybite-http/config"
	"keybite-http/store"
	"path"
	"strconv"
)

type command struct {
	keyword   string // the keyword that starts the command
	numTokens int    // the number of tokens before the command payload
	// the function to call to get the result. assumes correct input.
	execute func(tokens []string, payload string, conf config.Config) (string, error)
}

// Query existing data
var Query = command{
	keyword:   "query",
	numTokens: 2, // query index "..."
	execute: func(tokens []string, payload string, conf config.Config) (string, error) {
		dataDir, err := conf.GetString("DATA_DIR")
		if err != nil {
			return "", errors.New("could not get data directory path from environment")
		}

		pageSize, err := conf.GetInt("AUTO_PAGE_SIZE")
		if err != nil {
			return "", errors.New("Invalid auto index page size from environment")
		}

		index, err := store.NewAutoIndex(tokens[1], dataDir, pageSize)
		if err != nil {
			return "", err
		}

		queryID, err := strconv.ParseInt(tokens[2], 10, 64)
		if err != nil {
			return "", fmt.Errorf("cannot query non-integer ID %s", tokens[2])
		}

		return index.Query(queryID)
	},
}

// Insert new data
var Insert = command{
	keyword:   "insert",
	numTokens: 2, // insert index "..."
	execute: func(tokens []string, payload string, conf config.Config) (string, error) {
		dataDir, err := conf.GetString("DATA_DIR")
		if err != nil {
			return "", errors.New("could not get data directory path from environment")
		}

		pageSize, err := conf.GetInt("AUTO_PAGE_SIZE")
		if err != nil {
			return "", errors.New("Invalid auto index page size from environment")
		}

		index, err := store.NewAutoIndex(tokens[1], dataDir, pageSize)
		if err != nil {
			return "", err
		}

		result, err := index.Insert(payload)
		if err != nil {
			return "", err
		}

		return strconv.FormatInt(result, 10), nil
	},
}

// Update existing data
var Update = command{
	keyword:   "update",
	numTokens: 3, // update index 3 "..."
	execute: func(tokens []string, payload string, conf config.Config) (string, error) {
		dataDir, err := conf.GetString("DATA_DIR")
		if err != nil {
			return "", errors.New("could not get data directory path from environment")
		}

		pageSize, err := conf.GetInt("AUTO_PAGE_SIZE")
		if err != nil {
			return "", errors.New("Invalid auto index page size from environment")
		}

		index, err := store.NewAutoIndex(tokens[1], dataDir, pageSize)
		if err != nil {
			return "", err
		}

		queryID, err := strconv.ParseInt(tokens[2], 10, 64)
		if err != nil {
			return "", fmt.Errorf("cannot query non-integer ID %s", tokens[2])
		}

		err = index.Update(queryID, payload)
		if err != nil {
			return "", err
		}

		return strconv.FormatInt(queryID, 10), nil
	},
}

// CreateAutoIndex in data dir
var CreateAutoIndex = command{
	keyword:   "create_auto_index",
	numTokens: 1, // create_index spam
	execute: func(tokens []string, payload string, conf config.Config) (string, error) {
		indexName := tokens[1]
		dataDir, err := conf.GetString("DATA_DIR")
		if err != nil {
			return "", errors.New("could not get data directory path from environment")
		}
		indexPath := path.Join(dataDir, indexName)
		return indexName, store.CreateIndexDirectory(indexPath)
	},
}

// CreateMapIndex in data dir
var CreateMapIndex = command{
	keyword:   "create_map_index",
	numTokens: 1, // create_map_index spam
	execute: func(tokens []string, payload string, conf config.Config) (string, error) {
		indexName := tokens[1]
		dataDir, err := conf.GetString("DATA_DIR")
		if err != nil {
			return "", errors.New("could not get data directory path from environment")
		}

		indexPath := path.Join(dataDir, indexName)
		return indexName, store.CreateIndexDirectory(indexPath)
	},
}

// QueryKey queries a map index.
// @TODO remove this, query should work on either index type
var QueryKey = command{
	keyword:   "query_key",
	numTokens: 2,
	execute: func(tokens []string, payload string, conf config.Config) (string, error) {
		indexName := tokens[1]
		key := tokens[2]
		dataDir, err := conf.GetString("DATA_DIR")
		if err != nil {
			return "", errors.New("could not get data directory path from environment")
		}

		pageSize, err := conf.GetInt("MAP_PAGE_SIZE")
		if err != nil {
			return "", errors.New("Invalid auto index page size from environment")
		}

		mapIndex, err := store.NewMapIndex(indexName, dataDir, pageSize)
		if err != nil {
			return "", err
		}
		return mapIndex.Query(key)
	},
}

// InsertKey inserts into a map index
var InsertKey = command{
	keyword:   "insert_key",
	numTokens: 3, // insert_key index key [...]
	execute: func(tokens []string, payload string, conf config.Config) (string, error) {
		indexName := tokens[1]
		dataDir, err := conf.GetString("DATA_DIR")
		if err != nil {
			return "", errors.New("could not get data directory path from environment")
		}

		pageSize, err := conf.GetInt("MAP_PAGE_SIZE")
		if err != nil {
			return "", errors.New("Invalid auto index page size from environment")
		}

		key := tokens[2]
		mapIndex, err := store.NewMapIndex(indexName, dataDir, pageSize)
		if err != nil {
			return "", err
		}
		return mapIndex.Insert(key, payload)
	},
}

// UpdateKey updates an existing key in a map index
var UpdateKey = command{
	keyword:   "update_key",
	numTokens: 3, // update_key index key [...]
	execute: func(tokens []string, payload string, conf config.Config) (string, error) {
		indexName := tokens[1]
		key := tokens[2]
		dataDir, err := conf.GetString("DATA_DIR")
		if err != nil {
			return "", errors.New("could not get data directory path from environment")
		}

		pageSize, err := conf.GetInt("MAP_PAGE_SIZE")
		if err != nil {
			return "", errors.New("Invalid or missing auto index page size from environment")
		}

		mapIndex, err := store.NewMapIndex(indexName, dataDir, pageSize)
		if err != nil {
			return "", err
		}
		return key, mapIndex.Update(key, payload)
	},
}

// Commands available to the DSL
var Commands = []command{
	Query,
	Insert,
	Update,
	CreateAutoIndex,
	CreateMapIndex,
	QueryKey,
	InsertKey,
	UpdateKey,
}
