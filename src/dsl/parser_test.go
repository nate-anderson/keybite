package dsl

import (
	"keybite/util"
	"testing"
)

func TestParseQuery(t *testing.T) {
	queryText := "query default 1"
	queryParser := newParser(queryText)
	queryObj, err := queryParser.Parse()
	util.Ok(t, err)

	util.Assert(t, queryObj.autoSel.Next(), "query selector != DSL selected")
	util.Equals(t, typeQuery, queryObj.oType)
	util.Equals(t, nil, queryObj.mapSel)
	util.Equals(t, "default", queryObj.indexName)
	util.Equals(t, uint64(1), queryObj.autoSel.Select())
	util.Equals(t, uint64(0), queryObj.limit)
	util.Equals(t, uint64(0), queryObj.offset)
}

func TestParseQueryKey(t *testing.T) {
	queryKeyText := "query_key map_default testKey"
	queryKeyParser := newParser(queryKeyText)
	queryObj, err := queryKeyParser.Parse()
	util.Ok(t, err)

	util.Assert(t, queryObj.mapSel.Next(), "query_key selector != DSL selected")
	util.Equals(t, typeQueryKey, queryObj.oType)
	util.Equals(t, nil, queryObj.autoSel)
	util.Equals(t, "map_default", queryObj.indexName)
	util.Equals(t, "testKey", queryObj.mapSel.Select())
	util.Equals(t, uint64(0), queryObj.limit)
	util.Equals(t, uint64(0), queryObj.offset)
}

func TestParseInsert(t *testing.T) {
	insertText := "insert default test payload"
	insertParser := newParser(insertText)
	queryObj, err := insertParser.Parse()
	util.Ok(t, err)

	util.Equals(t, typeInsert, queryObj.oType)
	util.Equals(t, nil, queryObj.autoSel)
	util.Equals(t, "default", queryObj.indexName)
	util.Equals(t, "test payload", queryObj.payload)
	util.Equals(t, uint64(0), queryObj.limit)
	util.Equals(t, uint64(0), queryObj.offset)
}

func TestParseInsertKey(t *testing.T) {
	insertKeyText := "insert_key map_default testKey payload strings"
	insertKeyParser := newParser(insertKeyText)
	queryObj, err := insertKeyParser.Parse()
	util.Ok(t, err)

	util.Assert(t, queryObj.mapSel.Next(), "insert_key selector != DSL selected")
	util.Equals(t, typeInsertKey, queryObj.oType)
	util.Equals(t, nil, queryObj.autoSel)
	util.Equals(t, "map_default", queryObj.indexName)
	util.Equals(t, "payload strings", queryObj.payload)
	util.Equals(t, "testKey", queryObj.mapSel.Select())
	util.Equals(t, uint64(0), queryObj.limit)
	util.Equals(t, uint64(0), queryObj.offset)
}

func TestParseUpdate(t *testing.T) {
	updateText := "update default 2 new value"
	updateParser := newParser(updateText)
	queryObj, err := updateParser.Parse()
	util.Ok(t, err)

	util.Assert(t, queryObj.autoSel.Next(), "update selector != DSL selected")
	util.Equals(t, typeUpdate, queryObj.oType)
	util.Equals(t, nil, queryObj.mapSel)
	util.Equals(t, "default", queryObj.indexName)
	util.Equals(t, "new value", queryObj.payload)
	util.Equals(t, uint64(2), queryObj.autoSel.Select())
	util.Equals(t, uint64(0), queryObj.limit)
	util.Equals(t, uint64(0), queryObj.offset)
}

func TestParseUpdateKey(t *testing.T) {
	updateText := "update_key map_default theKey new value"
	updateParser := newParser(updateText)
	queryObj, err := updateParser.Parse()
	util.Ok(t, err)

	util.Assert(t, queryObj.mapSel.Next(), "update_key selector != DSL selected")
	util.Equals(t, typeUpdateKey, queryObj.oType)
	util.Equals(t, nil, queryObj.autoSel)
	util.Equals(t, "map_default", queryObj.indexName)
	util.Equals(t, "new value", queryObj.payload)
	util.Equals(t, "theKey", queryObj.mapSel.Select())
	util.Equals(t, uint64(0), queryObj.limit)
	util.Equals(t, uint64(0), queryObj.offset)
}

func TestParseUpsertKey(t *testing.T) {
	upsertKeyText := "upsert_key map_default2 theKey2 new or updated value"
	upsertKeyParser := newParser(upsertKeyText)
	queryObj, err := upsertKeyParser.Parse()
	util.Ok(t, err)

	util.Assert(t, queryObj.mapSel.Next(), "upsert_key selector != DSL selected")
	util.Equals(t, typeUpsertKey, queryObj.oType)
	util.Equals(t, nil, queryObj.autoSel)
	util.Equals(t, "map_default2", queryObj.indexName)
	util.Equals(t, "new or updated value", queryObj.payload)
	util.Equals(t, "theKey2", queryObj.mapSel.Select())
	util.Equals(t, uint64(0), queryObj.limit)
	util.Equals(t, uint64(0), queryObj.offset)
}

func TestParseDelete(t *testing.T) {
	deleteText := "delete default_index 26"
	deleteParser := newParser(deleteText)
	queryObj, err := deleteParser.Parse()
	util.Ok(t, err)

	util.Assert(t, queryObj.autoSel.Next(), "delete selector != DSL selected")
	util.Equals(t, typeDelete, queryObj.oType)
	util.Equals(t, nil, queryObj.mapSel)
	util.Equals(t, "default_index", queryObj.indexName)
	util.Equals(t, uint64(26), queryObj.autoSel.Select())
	util.Equals(t, uint64(0), queryObj.limit)
	util.Equals(t, uint64(0), queryObj.offset)
}

func TestParseDeleteKey(t *testing.T) {
	deleteKeyText := "delete_key map_default theDeleteKey"
	deleteKeyParser := newParser(deleteKeyText)
	queryObj, err := deleteKeyParser.Parse()
	util.Ok(t, err)

	util.Assert(t, queryObj.mapSel.Next(), "delete selector != DSL selected")
	util.Equals(t, typeDeleteKey, queryObj.oType)
	util.Equals(t, nil, queryObj.autoSel)
	util.Equals(t, "map_default", queryObj.indexName)
	util.Equals(t, "theDeleteKey", queryObj.mapSel.Select())
	util.Equals(t, uint64(0), queryObj.limit)
	util.Equals(t, uint64(0), queryObj.offset)
}

func TestParseList(t *testing.T) {
	listText := "list default 10 50"
	listParser := newParser(listText)
	queryObj, err := listParser.Parse()
	util.Ok(t, err)

	util.Equals(t, typeList, queryObj.oType)
	util.Equals(t, nil, queryObj.mapSel)
	util.Equals(t, nil, queryObj.autoSel)
	util.Equals(t, "default", queryObj.indexName)
	util.Equals(t, uint64(10), queryObj.limit)
	util.Equals(t, uint64(50), queryObj.offset)
}

func TestParseListKey(t *testing.T) {
	listKeyText := "list_key map_default 25 500"
	listKeyParser := newParser(listKeyText)
	queryObj, err := listKeyParser.Parse()
	util.Ok(t, err)

	util.Equals(t, typeListKey, queryObj.oType)
	util.Equals(t, nil, queryObj.mapSel)
	util.Equals(t, nil, queryObj.autoSel)
	util.Equals(t, "map_default", queryObj.indexName)
	util.Equals(t, uint64(25), queryObj.limit)
	util.Equals(t, uint64(500), queryObj.offset)
}

func TestParseCount(t *testing.T) {
	countText := "count my_index"
	countParser := newParser(countText)
	queryObj, err := countParser.Parse()
	util.Ok(t, err)

	util.Equals(t, typeCount, queryObj.oType)
	util.Equals(t, nil, queryObj.mapSel)
	util.Equals(t, nil, queryObj.autoSel)
	util.Equals(t, "my_index", queryObj.indexName)
	util.Equals(t, uint64(0), queryObj.limit)
	util.Equals(t, uint64(0), queryObj.offset)
}

func TestParseCountKey(t *testing.T) {
	countText := "count_key my_map_index"
	countParser := newParser(countText)
	queryObj, err := countParser.Parse()
	util.Ok(t, err)

	util.Equals(t, typeCountKey, queryObj.oType)
	util.Equals(t, nil, queryObj.mapSel)
	util.Equals(t, nil, queryObj.autoSel)
	util.Equals(t, "my_map_index", queryObj.indexName)
	util.Equals(t, uint64(0), queryObj.limit)
	util.Equals(t, uint64(0), queryObj.offset)
}

func TestParseCreateAutoIndex(t *testing.T) {
	countText := "create_auto_index my_map_index"
	countParser := newParser(countText)
	queryObj, err := countParser.Parse()
	util.Ok(t, err)

	util.Equals(t, typeCreateAutoIndex, queryObj.oType)
	util.Equals(t, nil, queryObj.mapSel)
	util.Equals(t, nil, queryObj.autoSel)
	util.Equals(t, "my_map_index", queryObj.indexName)
	util.Equals(t, uint64(0), queryObj.limit)
	util.Equals(t, uint64(0), queryObj.offset)
}

func TestParseCreateMapIndex(t *testing.T) {
	countText := "create_map_index my_map_index"
	countParser := newParser(countText)
	queryObj, err := countParser.Parse()
	util.Ok(t, err)

	util.Equals(t, typeCreateMapIndex, queryObj.oType)
	util.Equals(t, nil, queryObj.mapSel)
	util.Equals(t, nil, queryObj.autoSel)
	util.Equals(t, "my_map_index", queryObj.indexName)
	util.Equals(t, uint64(0), queryObj.limit)
	util.Equals(t, uint64(0), queryObj.offset)
}
