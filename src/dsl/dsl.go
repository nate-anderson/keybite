package dsl

import (
	"errors"
	"fmt"
	"keybite/config"
	"keybite/store"
	"keybite/store/driver"
)

// Execute a statement on the data in the provided datadir
func Execute(input string, conf config.Config) (store.Result, error) {
	parser := newParser(input)

	query, err := parser.Parse()
	if err != nil {
		return store.EmptyResult(), err
	}

	autoPageSize, err := conf.GetInt("AUTO_PAGE_SIZE")
	if err != nil {
		return store.EmptyResult(), errors.New("Invalid auto index page size from environment")
	}

	mapPageSize, err := conf.GetInt("MAP_PAGE_SIZE")
	if err != nil {
		return store.EmptyResult(), errors.New("Invalid map index page size from environment")
	}

	storageDriver, err := driver.GetConfiguredDriver(conf)
	if err != nil {
		return store.EmptyResult(), err
	}

	// handle query based on type
	switch query.oType {
	case typeQuery:
		autoIndex, err := store.NewAutoIndex(query.indexName, storageDriver, autoPageSize)
		if err != nil {
			return store.EmptyResult(), fmt.Errorf("reading index %s failed: %w", query.indexName, err)
		}
		return autoIndex.Query(query.autoSel)
	case typeQueryKey:
		mapIndex, err := store.NewMapIndex(query.indexName, storageDriver, mapPageSize)
		if err != nil {
			return store.EmptyResult(), fmt.Errorf("reading index %s failed: %w", query.indexName, err)
		}
		return mapIndex.Query(query.mapSel)
	case typeInsert:
		autoIndex, err := store.NewAutoIndex(query.indexName, storageDriver, autoPageSize)
		if err != nil {
			return store.EmptyResult(), fmt.Errorf("reading index %s failed: %w", query.indexName, err)
		}
		return autoIndex.Insert(query.payload)
	case typeInsertKey:
		mapIndex, err := store.NewMapIndex(query.indexName, storageDriver, mapPageSize)
		if err != nil {
			return store.EmptyResult(), fmt.Errorf("reading index %s failed: %w", query.indexName, err)
		}
	case typeUpdate:
		autoIndex, err := store.NewAutoIndex(query.indexName, storageDriver, autoPageSize)
		if err != nil {
			return store.EmptyResult(), fmt.Errorf("reading index %s failed: %w", query.indexName, err)
		}
	case typeUpdateKey:
		mapIndex, err := store.NewMapIndex(query.indexName, storageDriver, mapPageSize)
		if err != nil {
			return store.EmptyResult(), fmt.Errorf("reading index %s failed: %w", query.indexName, err)
		}
	case typeUpsertKey:
		mapIndex, err := store.NewMapIndex(query.indexName, storageDriver, mapPageSize)
		if err != nil {
			return store.EmptyResult(), fmt.Errorf("reading index %s failed: %w", query.indexName, err)
		}
	case typeDelete:
		autoIndex, err := store.NewAutoIndex(query.indexName, storageDriver, autoPageSize)
		if err != nil {
			return store.EmptyResult(), fmt.Errorf("reading index %s failed: %w", query.indexName, err)
		}
	case typeDeleteKey:
		mapIndex, err := store.NewMapIndex(query.indexName, storageDriver, mapPageSize)
		if err != nil {
			return store.EmptyResult(), fmt.Errorf("reading index %s failed: %w", query.indexName, err)
		}
	case typeList:
		autoIndex, err := store.NewAutoIndex(query.indexName, storageDriver, autoPageSize)
		if err != nil {
			return store.EmptyResult(), fmt.Errorf("reading index %s failed: %w", query.indexName, err)
		}
	case typeListKey:
		mapIndex, err := store.NewMapIndex(query.indexName, storageDriver, mapPageSize)
		if err != nil {
			return store.EmptyResult(), fmt.Errorf("reading index %s failed: %w", query.indexName, err)
		}
	case typeCount:
		autoIndex, err := store.NewAutoIndex(query.indexName, storageDriver, autoPageSize)
		if err != nil {
			return store.EmptyResult(), fmt.Errorf("reading index %s failed: %w", query.indexName, err)
		}
	case typeCountKey:
		mapIndex, err := store.NewMapIndex(query.indexName, storageDriver, mapPageSize)
		if err != nil {
			return store.EmptyResult(), fmt.Errorf("reading index %s failed: %w", query.indexName, err)
		}
	case typeCreateAutoIndex:
		return store.SingleResult(query.indexName), storageDriver.CreateAutoIndex(query.indexName)
	case typeCreateMapIndex:
		return store.SingleResult(query.indexName), storageDriver.CreateMapIndex(query.indexName)
	}
}

func displayCommandList() {
	fmt.Println("Available query commands: ")
	for _, command := range Commands {
		fmt.Println(command.keyword)
		fmt.Println("  ", command.description)
		fmt.Println("   Example:", command.example)
		fmt.Println()
	}
}
