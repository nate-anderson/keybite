package dsl

import (
	"fmt"
	"keybite-http/store"
	"path"
	"strconv"
)

type command struct {
	keyword   string // the keyword that starts the command
	numTokens int    // the number of tokens before the command payload
	// the function to call to get the result. assumes correct input.
	execute func(tokens []string, payload string, dataDir string, pageSize int) (string, error)
}

// Query existing data
var Query = command{
	keyword:   "query",
	numTokens: 2, // query index "..."
	execute: func(tokens []string, payload string, dataDir string, pageSize int) (string, error) {
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
	execute: func(tokens []string, payload string, dataDir string, pageSize int) (string, error) {
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
	execute: func(tokens []string, payload string, dataDir string, pageSize int) (string, error) {
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
	execute: func(tokens []string, payload string, dataDir string, pageSize int) (string, error) {
		indexName := tokens[1]
		indexPath := path.Join(dataDir, indexName)
		return indexName, store.CreateIndexDirectory(indexPath)
	},
}

// CreateMapIndex in data dir
var CreateMapIndex = command{
	keyword:   "create_map_index",
	numTokens: 1, // create_map_index spam
	execute: func(tokens []string, payload string, dataDir string, pageSize int) (string, error) {
		indexName := tokens[1]
		indexPath := path.Join(dataDir, indexName)
		return indexName, store.CreateIndexDirectory(indexPath)
	},
}

// Commands available to the DSL
var Commands = []command{
	Query,
	Insert,
	Update,
	CreateAutoIndex,
	CreateMapIndex,
}
