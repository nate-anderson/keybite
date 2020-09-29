package driver

import (
	"fmt"
	"keybite/util"
	"testing"
)

const testDataDir = "./test_data"
const testExtension = ".kb"

var testLockDuration = ToMillisDuration(50)

func randomAutoVals(size, strLength int) (map[uint64]string, []uint64) {
	vals := make(map[uint64]string, size)
	keys := make([]uint64, size)
	for i := uint64(0); i < uint64(size); i++ {
		vals[i+1] = util.RandStringFrom(strLength, util.CharsetAlphaNum)
		keys[i] = i + 1
	}
	return vals, keys
}

func randomMapVals(size, strLength int) (map[string]string, []string) {
	vals := make(map[string]string, size)
	keys := make([]string, size)
	for i := uint64(0); i < uint64(size); i++ {
		vals[fmt.Sprintf("key%d", i+1)] = util.RandStringFrom(strLength, util.CharsetAlphaNum)
		keys[i] = fmt.Sprintf("key%d", i+1)
	}
	return vals, keys
}

func BenchmarkFilesystemDriverAutoIndex(b *testing.B) {
	n := 5000
	valueLength := 200
	indexName := "benchmark_filesystem_driver_auto"
	pageName := "1"
	dri, err := NewFilesystemDriver(testDataDir, testExtension, testLockDuration)
	util.Ok(b, err)
	inVals, inKeys := randomAutoVals(n, valueLength)
	err = dri.CreateAutoIndex(indexName)
	util.Ok(b, err)
	defer dri.DropAutoIndex(indexName)

	err = dri.WritePage(inVals, inKeys, pageName, indexName)
	util.Ok(b, err)

	_, _, err = dri.ReadPage(pageName, indexName, n)
	util.Ok(b, err)
}

func BenchmarkFilesystemDriverMapIndex(b *testing.B) {
	n := 5000
	valueLength := 200
	indexName := "benchmark_filesystem_driver_map"
	pageName := "1"
	dri, err := NewFilesystemDriver(testDataDir, testExtension, testLockDuration)
	util.Ok(b, err)
	inVals, inKeys := randomMapVals(n, valueLength)
	err = dri.CreateMapIndex(indexName)
	util.Ok(b, err)
	defer dri.DropMapIndex(indexName)

	err = dri.WriteMapPage(inVals, inKeys, pageName, indexName)
	util.Ok(b, err)

	_, _, err = dri.ReadMapPage(pageName, indexName, n)
	util.Ok(b, err)
}

func BenchmarkJsonDriverAutoIndex(b *testing.B) {
	n := 5000
	valueLength := 200
	indexName := "benchmark_json_driver_auto"
	pageName := "1"
	dri, err := NewJSONDriver(testDataDir, testExtension, testLockDuration)
	util.Ok(b, err)
	inVals, inKeys := randomAutoVals(n, valueLength)
	err = dri.CreateAutoIndex(indexName)
	util.Ok(b, err)
	defer dri.DropAutoIndex(indexName)

	err = dri.WritePage(inVals, inKeys, pageName, indexName)
	util.Ok(b, err)

	_, _, err = dri.ReadPage(pageName, indexName, n)
	util.Ok(b, err)
}

func BenchmarkJsonDriverMapIndex(b *testing.B) {
	n := 5000
	valueLength := 200
	indexName := "benchmark_json_driver_map"
	pageName := "1"
	dri, err := NewJSONDriver(testDataDir, testExtension, testLockDuration)
	util.Ok(b, err)
	inVals, inKeys := randomMapVals(n, valueLength)
	err = dri.CreateMapIndex(indexName)
	util.Ok(b, err)
	defer dri.DropMapIndex(indexName)

	err = dri.WriteMapPage(inVals, inKeys, pageName, indexName)
	util.Ok(b, err)

	_, _, err = dri.ReadMapPage(pageName, indexName, n)
	util.Ok(b, err)
}

func BenchmarkGobDriverAutoIndex(b *testing.B) {
	n := 5000
	valueLength := 200
	indexName := "benchmark_gob_driver_auto"
	pageName := "1"
	dri, err := NewGobDriver(testDataDir, testExtension, testLockDuration)
	util.Ok(b, err)
	inVals, inKeys := randomAutoVals(n, valueLength)
	err = dri.CreateAutoIndex(indexName)
	util.Ok(b, err)
	defer dri.DropAutoIndex(indexName)

	err = dri.WritePage(inVals, inKeys, pageName, indexName)
	util.Ok(b, err)

	_, _, err = dri.ReadPage(pageName, indexName, n)
	util.Ok(b, err)
}

func BenchmarkGobDriverMapIndex(b *testing.B) {
	n := 5000
	valueLength := 200
	indexName := "benchmark_gob_driver_map"
	pageName := "1"
	dri, err := NewGobDriver(testDataDir, testExtension, testLockDuration)
	util.Ok(b, err)
	inVals, inKeys := randomMapVals(n, valueLength)
	err = dri.CreateMapIndex(indexName)
	util.Ok(b, err)
	defer dri.DropMapIndex(indexName)

	err = dri.WriteMapPage(inVals, inKeys, pageName, indexName)
	util.Ok(b, err)

	_, _, err = dri.ReadMapPage(pageName, indexName, n)
	util.Ok(b, err)
}
