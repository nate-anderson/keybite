package store

import (
	"keybite/store/driver"
	"time"
)

const pauseBeforeRetry int64 = 5000

func wrapInWriteLock(driver driver.StorageDriver, indexName string, action func() error) error {
	now := time.Now()
	locked, exp, err := driver.IndexIsLocked(indexName)

	for locked && exp.After(now) {
		if err != nil {
			return err
		}
		time.Sleep(time.Duration(pauseBeforeRetry))
		locked, exp, err = driver.IndexIsLocked(indexName)
	}

	err = driver.LockIndex(indexName)
	if err != nil {
		return err
	}

	resErr := action()

	err = driver.UnlockIndex(indexName)
	if err != nil {
		return err
	}

	return resErr
}
