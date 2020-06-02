package config_test

import (
	"keybite/config"
	"keybite/util"
	"os"
	"testing"
)

func TestReadString(t *testing.T) {
	testKey := "TEST_KEY"
	testVal := "TEST_VAL"

	os.Setenv(testKey, testVal)

	conf, err := config.MakeConfig()
	util.Ok(t, err)

	fetched, err := conf.GetString(testKey)
	util.Ok(t, err)

	util.Equals(t, fetched, testVal)
}

func TestReadInt64(t *testing.T) {
	testKey := "TEST_KEY"
	testValStr := "2"
	testVal := int64(2)

	os.Setenv(testKey, testValStr)

	conf, err := config.MakeConfig()
	util.Ok(t, err)

	fetched, err := conf.GetInt64(testKey)
	util.Ok(t, err)

	util.Equals(t, fetched, testVal)
}

func TestReadInt(t *testing.T) {
	testKey := "TEST_KEY"
	testValStr := "2"
	testVal := 2

	os.Setenv(testKey, testValStr)

	conf, err := config.MakeConfig()
	util.Ok(t, err)

	fetched, err := conf.GetInt(testKey)
	util.Ok(t, err)

	util.Equals(t, fetched, testVal)
}
