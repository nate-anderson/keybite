package store_test

import (
	"keybite/store"
	"keybite/util"
	"testing"
)

func TestMapPageInsertQuery(t *testing.T) {
	p := store.EmptyMapPage("test_map_page")

	testKey := "testKey"
	testVal := "testVal"

	// test insert and query
	key, err := p.Add(testKey, testVal)
	util.Ok(t, err)
	util.Equals(t, testKey, key)

	util.Equals(t, 1, p.Length())

	val, err := p.Query(testKey)
	util.Ok(t, err)
	util.Equals(t, testVal, val)

	// inserting key that already exists should fail
	_, err = p.Add(testKey, testVal)
	util.Assert(t, err != nil, "inserting duplicate key should fail")

}

func TestMapPageInsertUpdate(t *testing.T) {
	p := store.EmptyMapPage("test_map_page")

	testKey := "testKey"
	initial := "initial"

	// test insert, update, retrieved
	key, err := p.Add(testKey, initial)
	util.Ok(t, err)
	util.Equals(t, testKey, key)

	updated := "updated"
	err = p.Overwrite(testKey, updated)
	util.Ok(t, err)

	key2, err := p.Query(testKey)
	util.Ok(t, err)
	util.Equals(t, updated, key2)
}

func TestMapPageDelete(t *testing.T) {
	p := store.EmptyMapPage("test_map_page")

	testKey := "testKey"
	testVal := "testVal"

	// test insert then delete key
	_, err := p.Add(testKey, testVal)
	util.Ok(t, err)

	err = p.Delete(testKey)
	util.Ok(t, err)

	val, err := p.Query(testKey)
	util.Assert(t, err != nil, "deleting missing key should return err")
	util.Equals(t, "", val)

	// deleting missing key should fail
	err = p.Delete(testKey)
	util.Assert(t, err != nil, "deleting missing key should fail")
}

func TestMapPageUpsert(t *testing.T) {
	p := store.EmptyMapPage("test_map_page")

	testKey := "testKey"
	initial := "testVal"

	// test upsert with new value
	key := p.Upsert(testKey, initial)
	util.Equals(t, 1, p.Length())

	val, err := p.Query(key)
	util.Ok(t, err)
	util.Equals(t, val, initial)

	// test upsert with overwrite
	updated := "updated"
	key2 := p.Upsert(testKey, updated)
	util.Equals(t, testKey, key2)
	util.Equals(t, 1, p.Length())

	val2, err := p.Query(key)
	util.Ok(t, err)
	util.Equals(t, updated, val2)

}

func TestMapPageOverwrite(t *testing.T) {
	p := store.EmptyMapPage("test_map_page")

	err := p.Overwrite("unknownKey", "value")
	util.Assert(t, err != nil, "updating missing key should return error")
}
