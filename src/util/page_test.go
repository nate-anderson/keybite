package util_test

import (
	"keybite/util"
	"strings"
	"testing"
)

type KeyValueAssertion struct {
	Key           int64
	Value         string
	Line          string
	ShouldSucceed bool
}

func TestStringToKeyValue(t *testing.T) {
	cases := []KeyValueAssertion{
		{1, "hi", "1:hi", true},
		{2, "hi:hi", "2:hi:hi", true},
		{3, "hi", "3", false},
		{3, "hi", ":", false},
	}

	for _, testCase := range cases {
		key, val, err := util.StringToKeyValue(testCase.Line)
		util.Equals(t, err == nil, testCase.ShouldSucceed)
		if testCase.ShouldSucceed {
			util.Equals(t, key, testCase.Key)
			util.Equals(t, val, testCase.Value)
		}
	}

}

type MapKeyValueAssertion struct {
	Key           uint64
	Value         string
	Line          string
	ShouldSucceed bool
}

func TestStringToMapKeyValue(t *testing.T) {
	cases := []MapKeyValueAssertion{
		{1, "hi", "1:hi", true},
		{2, "hi:hi", "2:hi:hi", true},
		{3, "hi", "3", false},
		{3, "hi", ":", false},
	}

	for _, testCase := range cases {
		key, val, err := util.StringToMapKeyValue(testCase.Line)
		util.Equals(t, err == nil, testCase.ShouldSucceed)
		if testCase.ShouldSucceed {
			util.Equals(t, key, testCase.Key)
			util.Equals(t, val, testCase.Value)
		}
	}

}

func TestSplitOnFirst(t *testing.T) {
	ins := []string{
		"hi:there",
		"hi:there:again",
		"hi::there",
		":",
	}
	outs := [][]string{
		{"hi", "there"},
		{"hi", "there:again"},
		{"hi", ":there"},
		{"", ""},
	}

	for i, in := range ins {
		actual, err := util.SplitOnFirst(in, ':')
		util.Ok(t, err)
		util.Equals(t, outs[i][0], actual[0])
		util.Equals(t, outs[i][1], actual[1])
	}
}

func TestMaxMapKey(t *testing.T) {
	testMap := map[uint64]string{
		1:   "low",
		2:   "high",
		3:   "higher",
		150: "highest",
	}

	maxKey := util.MaxMapKey(testMap)
	util.Equals(t, maxKey, int64(150))
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
		outcomes[i], err = util.HashString(testCase)
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
	_, err := util.HashString(testString)
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
		pf, ind, err := util.PathToIndexPage(in)
		util.Ok(t, err)
		t.Logf("ind should be %s, pf should be %s", exp[0], exp[1])
		t.Logf("got ind %s, pf %s", ind, pf)
		util.Equals(t, ind, exp[0])
		util.Equals(t, pf, exp[1])
	}

	// test that unusable path throws err
	badPath := "/too/many/levels.kb"
	_, _, err := util.PathToIndexPage(badPath)
	if err == nil {
		t.Log("no err thrown on bad path", badPath)
		t.Fail()
	}
}
