package driver

import (
	"keybite/util"
	"testing"
)

type KeyValueAssertion struct {
	Key           uint64
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
		key, val, err := stringToKeyValue(testCase.Line)
		util.Equals(t, err == nil, testCase.ShouldSucceed)
		if testCase.ShouldSucceed {
			util.Equals(t, key, testCase.Key)
			util.Equals(t, val, testCase.Value)
		}
	}

}

type MapKeyValueAssertion struct {
	Key           string
	Value         string
	Line          string
	ShouldSucceed bool
}

func TestStringToMapKeyValue(t *testing.T) {
	cases := []MapKeyValueAssertion{
		{"1", "hi", "1:hi", true},
		{"2", "hi:hi", "2:hi:hi", true},
		{"3", "hi", "3", false},
		{"3", "hi", ":", false},
	}

	for _, testCase := range cases {
		key, val, err := stringToMapKeyValue(testCase.Line)
		t.Logf("key: '%s' :: value '%s' :: line '%s' :: err '%s'", key, val, testCase.Line, err)
		util.Equals(t, testCase.ShouldSucceed, err == nil)
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
		actual, err := splitOnFirst(in, ':')
		util.Ok(t, err)
		util.Equals(t, outs[i][0], actual[0])
		util.Equals(t, outs[i][1], actual[1])
	}
}
