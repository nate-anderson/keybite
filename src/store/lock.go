package store

import (
	"keybite/store/driver"
	"time"
)

const pauseBeforeRetry int64 = 5000

func wrapInMapWriteLock(driver driver.StorageDriver, indexName string, action func() (string, error)) (string, error) {
	now := time.Now()
	locked, exp, err := driver.IndexIsLocked(indexName)

	for locked && exp.After(now) {
		if err != nil {
			return "", err
		}
		time.Sleep(time.Duration(pauseBeforeRetry))
		locked, exp, err = driver.IndexIsLocked(indexName)
	}

	err = driver.LockIndex(indexName)
	if err != nil {
		return "", err
	}

	res, resErr := action()

	err = driver.UnlockIndex(indexName)
	if err != nil {
		return "", err
	}

	return res, resErr
}

func wrapInAutoWriteLock(driver driver.StorageDriver, indexName string, action func() (uint64, error)) (uint64, error) {
	now := time.Now()
	locked, exp, err := driver.IndexIsLocked(indexName)

	for locked && exp.After(now) {
		if err != nil {
			return 0, err
		}
		time.Sleep(time.Duration(pauseBeforeRetry))
		locked, exp, err = driver.IndexIsLocked(indexName)
	}

	err = driver.LockIndex(indexName)
	if err != nil {
		return 0, err
	}

	res, resErr := action()

	err = driver.UnlockIndex(indexName)
	if err != nil {
		return 0, err
	}

	return res, resErr
}
