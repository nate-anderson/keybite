package store_test

import (
	"keybite/store"
	"keybite/util"
	"strings"
	"testing"
)

func TestMaxMapKey(t *testing.T) {
	testMap := map[uint64]string{
		1:   "low",
		2:   "high",
		3:   "higher",
		150: "highest",
	}

	maxKey := store.MaxMapKey(testMap)
	util.Equals(t, maxKey, uint64(150))
}

func TestHashString(t *testing.T) {
	// test that strings with same letters hash to different values
	testCases := []string{
		"mush",
		"shum",
		"ushm",
		"husm",
		"smuh",
		"smhu",
	}

	outcomes := make([]uint64, len(testCases))

	for i, testCase := range testCases {
		var err error
		outcomes[i], err = store.HashStringToKey(testCase)
		if err != nil {
			t.FailNow()
		}
	}

	// test for uniqueness
	for x, outcome := range outcomes {
		for y, outcome1 := range outcomes {
			if x == y {
				continue
			}
			util.Assert(t, outcome != outcome1, "non-unique hash!")
		}
	}

}

func TestHashStringNotAcceptOverMaxLength(t *testing.T) {
	testString := strings.Repeat("s", 200)
	_, err := store.HashStringToKey(testString)
	if err == nil {
		t.Fail()
	}
}

func TestPathToIndexPage(t *testing.T) {
	exps := map[string][]string{
		"ind/p.kb":  {"ind", "p.kb"},
		"/ind/p.kb": {"ind", "p.kb"},
	}

	for in, exp := range exps {
		pf, ind, err := store.PathToIndexPage(in)
		util.Ok(t, err)
		t.Logf("ind should be %s, pf should be %s", exp[0], exp[1])
		t.Logf("got ind %s, pf %s", ind, pf)
		util.Equals(t, ind, exp[0])
		util.Equals(t, pf, exp[1])
	}

	// test that unusable path throws err
	badPath := "/too/many/levels.kb"
	_, _, err := store.PathToIndexPage(badPath)
	if err == nil {
		t.Log("no err thrown on bad path", badPath)
		t.Fail()
	}
}

func TestPageAppendQuery(t *testing.T) {
	p := store.EmptyPage("test_page")
	testVal := "the test value"

	id := p.Append(testVal)

	val, err := p.Query(id)
	util.Ok(t, err)
	util.Equals(t, testVal, val)
	util.Equals(t, 1, p.Length())
}

func TestPageAppendDelete(t *testing.T) {
	p := store.EmptyPage("test_page")
	testVal := "the test value to delete"

	id := p.Append(testVal)

	val, err := p.Query(id)
	util.Ok(t, err)
	util.Equals(t, testVal, val)

	err = p.Delete(id)
	util.Ok(t, err)

	util.Equals(t, 0, p.Length())

	val, err = p.Query(id)
	util.Assert(t, err != nil, "retrieving missing key should return error")
	util.Equals(t, "", val)

	err = p.Delete(id)
	util.Assert(t, err != nil, "deleting missing key should return error")
}

func TestPageOverwrite(t *testing.T) {
	p := store.EmptyPage("test_page")
	initial := "initial"

	id := p.Append(initial)

	val, err := p.Query(id)
	util.Ok(t, err)
	util.Equals(t, initial, val)

	// test updated value is stored
	updated := "updated"
	err = p.Overwrite(id, updated)
	util.Ok(t, err)

	val2, err := p.Query(id)
	util.Ok(t, err)
	util.Equals(t, updated, val2)

	// test updating non-existent value fails
	err = p.Overwrite(10, "uh-oh!")
	util.Assert(t, err != nil, "updating unregistered key should throw error")
}

func TestSetMinimumKey(t *testing.T) {
	p := store.EmptyPage("test_page")
	testVal := "initial"
	var minKey uint64 = 100

	p.SetMinimumKey(minKey)
	id := p.Append(testVal)

	util.Assert(t, id == minKey, "first insert should be equal to miminum map key")

	val, err := p.Query(id)
	util.Ok(t, err)

	util.Equals(t, testVal, val)

	// for page of size 1, max key should equal min key
	maxKey := p.MaxKey()
	util.Equals(t, maxKey, minKey)
}
