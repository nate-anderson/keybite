package driver_test

import (
	"keybite/config"
	"keybite/store/driver"
	"keybite/util"
	"os"
	"path"
	"testing"
	"time"
)

var testConf config.Config
var testLockDuration = driver.ToMillisDuration(50)

// test that CreateAutoIndex creates a folder
func TestFSCreateAutoIndex(t *testing.T) {
	dirName := "test_data"
	err := os.Mkdir(dirName, 0755)
	util.Ok(t, err)

	defer os.RemoveAll(dirName)

	fsd, err := driver.NewFilesystemDriver(dirName, ".kb", testLockDuration)
	util.Ok(t, err)

	indexName := "test_index"
	err = fsd.CreateAutoIndex(indexName)
	util.Ok(t, err)

	if _, err := os.Stat(path.Join(dirName, indexName)); os.IsNotExist(err) {
		t.Fail()
	}
}

// test that CreateMapIndex creates a folder
func TestFSCreateMapIndex(t *testing.T) {
	dirName := "test_data"
	err := os.Mkdir(dirName, 0755)
	util.Ok(t, err)

	defer os.RemoveAll(dirName)

	fsd, err := driver.NewFilesystemDriver(dirName, ".kb", testLockDuration)
	util.Ok(t, err)

	indexName := "test_index"
	err = fsd.CreateMapIndex(indexName)
	util.Ok(t, err)

	if _, err := os.Stat(path.Join(dirName, indexName)); os.IsNotExist(err) {
		t.Fail()
	}
}

// test that NewFilesystemDriver only works on existing folder
func TestFSNewFilesystemDriver(t *testing.T) {
	dirName := "test_data"

	_, err := driver.NewFilesystemDriver(dirName, ".kb", testLockDuration)
	if err == nil {
		t.Logf("attempting to instantiate filesystem driver on missing directory %s should fail", dirName)
		t.FailNow()
	}

	err = os.Mkdir(dirName, 0755)
	util.Ok(t, err)

	defer os.RemoveAll(dirName)

	_, err = driver.NewFilesystemDriver(dirName, ".kb", testLockDuration)
	util.Ok(t, err)
}

// test reading and writing data maps for auto index
func TestFSWritePageReadPage(t *testing.T) {
	dirName := "test_data"
	err := os.Mkdir(dirName, 0755)
	util.Ok(t, err)

	defer os.RemoveAll(dirName)

	fsd, err := driver.NewFilesystemDriver(dirName, ".kb", testLockDuration)
	util.Ok(t, err)

	indexName := "test_index"
	err = fsd.CreateMapIndex(indexName)
	util.Ok(t, err)

	testMap := map[uint64]string{
		1: "hello",
		2: "world",
	}

	testKeys := []uint64{1, 2}

	testFileName := "1"
	testDataPath := path.Join(dirName, indexName, (testFileName + ".kb"))
	_, err = os.Create(testDataPath)
	util.Ok(t, err)

	err = fsd.WritePage(testMap, testKeys, testFileName, indexName)
	util.Ok(t, err)

	vals, _, err := fsd.ReadPage(testFileName, indexName, 10)
	util.Ok(t, err)

	util.Equals(t, "hello", vals[1])
	util.Equals(t, "world", vals[2])
}

// test reading and writing data maps for map index
func TestFSWriteMapPageReadMapPage(t *testing.T) {
	dirName := "test_data"
	err := os.Mkdir(dirName, 0755)
	util.Ok(t, err)

	defer os.RemoveAll(dirName)

	fsd, err := driver.NewFilesystemDriver(dirName, ".kb", testLockDuration)
	util.Ok(t, err)

	indexName := "test_index"
	err = fsd.CreateMapIndex(indexName)
	util.Ok(t, err)

	testMap := map[string]string{
		"1": "hello",
		"2": "world",
	}

	testKeys := []string{"1", "2"}

	testFileName := "1"
	testDataPath := path.Join(dirName, indexName, (testFileName + ".kb"))
	_, err = os.Create(testDataPath)
	util.Ok(t, err)

	err = fsd.WriteMapPage(testMap, testKeys, testFileName, indexName)
	util.Ok(t, err)

	vals, _, err := fsd.ReadMapPage(testFileName, indexName, 10)
	util.Ok(t, err)

	util.Equals(t, "hello", vals["1"])
	util.Equals(t, "world", vals["2"])
}

// test that ListPages returns the pages in an index dir
func TestFSListPages(t *testing.T) {
	dirName := "test_data"
	err := os.Mkdir(dirName, 0755)
	util.Ok(t, err)

	defer os.RemoveAll(dirName)

	fsd, err := driver.NewFilesystemDriver(dirName, ".kb", testLockDuration)
	util.Ok(t, err)

	indexName := "test_index"
	err = fsd.CreateMapIndex(indexName)
	util.Ok(t, err)

	testFileNames := []string{"1", "2", "3"}
	for _, fileName := range testFileNames {
		testDataPath := path.Join(dirName, indexName, (fileName + ".kb"))
		_, err = os.Create(testDataPath)
		util.Ok(t, err)
	}

	pages, err := fsd.ListPages(indexName)
	util.Ok(t, err)
	util.Equals(t, 3, len(pages))

	for i, pageName := range pages {
		util.Equals(t, pageName, pages[i])
	}
}

func TestFSLockUnlockIndex(t *testing.T) {
	dirName := "test_data"
	err := os.Mkdir(dirName, 0755)
	util.Ok(t, err)

	defer os.RemoveAll(dirName)

	fsd, err := driver.NewFilesystemDriver(dirName, ".kb", testLockDuration)
	util.Ok(t, err)

	indexName := "test_index"
	err = fsd.CreateMapIndex(indexName)
	util.Ok(t, err)

	err = fsd.LockIndex(indexName)
	util.Ok(t, err)

	beforeLock := time.Now()

	isLocked, until, err := fsd.IndexIsLocked(indexName)
	util.Ok(t, err)

	util.Assert(t, isLocked, "index is locked")
	util.Assert(t, until.After(beforeLock), "index is locked until after initial operation")

	err = fsd.UnlockIndex(indexName)
	util.Ok(t, err)

	isLocked, _, err = fsd.IndexIsLocked(indexName)
	util.Ok(t, err)

	util.Assert(t, !isLocked, "index unlocked after unlock")
}

func TestFSDropAutoIndex(t *testing.T) {
	// create auto index
	dirName := "test_data"
	err := os.Mkdir(dirName, 0755)
	util.Ok(t, err)

	defer os.RemoveAll(dirName)

	driver, err := driver.NewFilesystemDriver(dirName, ".kb", testLockDuration)
	util.Ok(t, err)

	indexName := "test_index_drop"
	err = driver.CreateAutoIndex(indexName)
	util.Ok(t, err)

	// test drop existing index
	err = driver.DropAutoIndex(indexName)
	util.Ok(t, err)

	if _, err := os.Stat(path.Join(dirName, indexName)); err == nil {
		t.Logf("deleting existing index should not return error. error returned: %s", err.Error())
		t.Fail()
	}

	// test that dropping non-existent index returns error
	err = driver.DropAutoIndex(indexName)
	if err == nil {
		t.Log("deleting missing index should return error.")
		t.Fail()
	}

}

func TestFSDropMapIndex(t *testing.T) {
	// create auto index
	dirName := "test_data"
	err := os.Mkdir(dirName, 0755)
	util.Ok(t, err)

	defer os.RemoveAll(dirName)

	driver, err := driver.NewFilesystemDriver(dirName, ".kb", testLockDuration)
	util.Ok(t, err)

	indexName := "test_index_drop"
	err = driver.CreateMapIndex(indexName)
	util.Ok(t, err)

	// test drop existing index
	err = driver.DropMapIndex(indexName)
	if _, err := os.Stat(path.Join(dirName, indexName)); err == nil {
		t.Logf("deleting existing index should not return error. error returned: %s", err.Error())
		t.Fail()
	}

	// test that dropping non-existent index returns error
	err = driver.DropMapIndex(indexName)
	if err == nil {
		t.Log("deleting missing index should return error.")
		t.Fail()
	}

}
