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

	// test that config values are cached after first read (they should not change mid-run)
	os.Setenv(testKey, testVal+"2")
	fetched2, err := conf.GetString(testKey)
	util.Ok(t, err)

	util.Equals(t, fetched2, testVal)
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

	// test that config values are cached after first read (they should not change mid-run)
	os.Setenv(testKey, "3")
	fetched2, err := conf.GetInt64(testKey)
	util.Ok(t, err)

	util.Equals(t, fetched2, testVal)
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

	// test that config values are cached after first read (they should not change mid-run)
	os.Setenv(testKey, "3")
	fetched2, err := conf.GetInt(testKey)
	util.Ok(t, err)

	util.Equals(t, fetched2, testVal)
}

func TestGetStringOrEmpty(t *testing.T) {
	testKey := "BS_KEY"
	conf, err := config.MakeConfig()
	util.Ok(t, err)

	fetched := conf.GetStringOrEmpty(testKey)
	util.Ok(t, err)

	util.Equals(t, fetched, "")

	// test that config values are cached after first read (they should not change mid-run)
	testKey2 := "BS_KEY1"
	testVal := "3"
	os.Setenv(testKey2, testVal)
	fetched2 := conf.GetStringOrEmpty(testKey2)

	util.Equals(t, testVal, fetched2)

	os.Setenv(testKey2, testVal+"a")
	fetched3 := conf.GetStringOrEmpty(testKey2)
	util.Equals(t, testVal, fetched3)
}

func TestMakeConfig(t *testing.T) {
	conf, err := config.MakeConfig("test.env")
	util.Ok(t, err)

	testVal := "aabbcc"
	testKey := "TEST_FILE_KEY"
	fetched, err := conf.GetString(testKey)
	util.Ok(t, err)

	util.Equals(t, testVal, fetched)
}

func TestMissingEnvReturnsErr(t *testing.T) {
	unconfiguredKey := "BS_KEY2"

	conf, err := config.MakeConfig()
	util.Ok(t, err)

	_, err = conf.GetString(unconfiguredKey)
	if err == nil {
		t.Log("conf.GetString did not return err on unconfigured var")
		t.Fail()
	}

	_, err = conf.GetInt(unconfiguredKey)
	if err == nil {
		t.Log("conf.GetInt did not return err on unconfigured var")
		t.Fail()
	}

	_, err = conf.GetInt64(unconfiguredKey)
	if err == nil {
		t.Log("conf.GetInt64 did not return err on unconfigured var")
		t.Fail()
	}

}

// MakeConfig should not load variables from .env files when in lambda mode
func TestMakeConfigSkipsFileForLambda(t *testing.T) {
	os.Setenv("ENVIRONMENT", "lambda")
	testKey := "TEST_FILE_KEY"

	conf, err := config.MakeConfig("test.env")
	util.Ok(t, err)

	_, err = conf.GetString(testKey)
	if err == nil {
		t.Log("MakeConfig read value from a file when environment was 'lambda'")
	}
}
