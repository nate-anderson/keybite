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
		return mapIndex.Insert(query.mapSel, query.payload)

	case typeUpdate:
		autoIndex, err := store.NewAutoIndex(query.indexName, storageDriver, autoPageSize)
		if err != nil {
			return store.EmptyResult(), fmt.Errorf("reading index %s failed: %w", query.indexName, err)
		}
		return autoIndex.Update(query.autoSel, query.payload)

	case typeUpdateKey:
		mapIndex, err := store.NewMapIndex(query.indexName, storageDriver, mapPageSize)
		if err != nil {
			return store.EmptyResult(), fmt.Errorf("reading index %s failed: %w", query.indexName, err)
		}
		return mapIndex.Update(query.mapSel, query.payload)

	case typeUpsertKey:
		mapIndex, err := store.NewMapIndex(query.indexName, storageDriver, mapPageSize)
		if err != nil {
			return store.EmptyResult(), fmt.Errorf("reading index %s failed: %w", query.indexName, err)
		}
		return mapIndex.Upsert(query.mapSel, query.payload)

	case typeDelete:
		autoIndex, err := store.NewAutoIndex(query.indexName, storageDriver, autoPageSize)
		if err != nil {
			return store.EmptyResult(), fmt.Errorf("reading index %s failed: %w", query.indexName, err)
		}
		return autoIndex.Delete(query.autoSel)

	case typeDeleteKey:
		mapIndex, err := store.NewMapIndex(query.indexName, storageDriver, mapPageSize)
		if err != nil {
			return store.EmptyResult(), fmt.Errorf("reading index %s failed: %w", query.indexName, err)
		}
		return mapIndex.Delete(query.mapSel)

	case typeList:
		autoIndex, err := store.NewAutoIndex(query.indexName, storageDriver, autoPageSize)
		if err != nil {
			return store.EmptyResult(), fmt.Errorf("reading index %s failed: %w", query.indexName, err)
		}
		return autoIndex.List(query.limit, query.offset, query.listDesc)

	case typeListKey:
		mapIndex, err := store.NewMapIndex(query.indexName, storageDriver, mapPageSize)
		if err != nil {
			return store.EmptyResult(), fmt.Errorf("reading index %s failed: %w", query.indexName, err)
		}
		return mapIndex.List(query.limit, query.offset, query.listDesc)

	case typeCount:
		autoIndex, err := store.NewAutoIndex(query.indexName, storageDriver, autoPageSize)
		if err != nil {
			return store.EmptyResult(), fmt.Errorf("reading index %s failed: %w", query.indexName, err)
		}
		return autoIndex.Count()

	case typeCountKey:
		mapIndex, err := store.NewMapIndex(query.indexName, storageDriver, mapPageSize)
		if err != nil {
			return store.EmptyResult(), fmt.Errorf("reading index %s failed: %w", query.indexName, err)
		}
		return mapIndex.Count()

	case typeCreateAutoIndex:
		return store.SingleResult(query.indexName), storageDriver.CreateAutoIndex(query.indexName)

	case typeCreateMapIndex:
		return store.SingleResult(query.indexName), storageDriver.CreateMapIndex(query.indexName)

	case typeDropAutoIndex:
		return store.SingleResult(query.indexName), storageDriver.DropAutoIndex(query.indexName)

	case typeDropMapIndex:
		return store.SingleResult(query.indexName), storageDriver.DropMapIndex(query.indexName)
	}

	return store.EmptyResult(), errors.New("query keyword did not match any commands")
}
