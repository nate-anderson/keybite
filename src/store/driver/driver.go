package driver

import (
	"fmt"
	"keybite/config"
	"keybite/util"
	"path/filepath"
	"strings"
	"time"
)

var lockDuration time.Duration = 50 * time.Millisecond

// StorageDriver is the interface needed to read and persist files that make up the DB
// A storage driver should handle these IO operations and should handle all paths and
// file extensions (i.e. the filename passed should not end in an extension, as this may
// vary by driver and environment)
type StorageDriver interface {
	ReadPage(filename string, indexName string, pageSize int) (map[int64]string, error)
	ReadMapPage(filename string, indexName string, pageSize int) (map[uint64]string, error)
	WritePage(vals map[int64]string, filename string, indexName string) error
	WriteMapPage(vals map[uint64]string, filename string, indexName string) error
	CreateAutoIndex(indexName string) error
	CreateMapIndex(indexName string) error
	// return an ascending-sorted list of pagefiles in the index datadir
	ListPages(indexName string) ([]string, error)
	// check if an index is locked by another request process, returning the time at which it was locked if true
	IndexIsLocked(indexName string) (bool, time.Time, error)
	// lock an index
	LockIndex(indexName string) error
	// unlock an index
	UnlockIndex(indexName string) error
}

const lockfileExtension = ".lock"

// GetConfiguredDriver returns the correct driver based on config
func GetConfiguredDriver(conf config.Config, log util.Logger) (StorageDriver, error) {
	driverType, err := conf.GetString("DRIVER")
	if err != nil {
		return nil, err
	}

	pageExtension, err := conf.GetString("PAGE_EXTENSION")
	if err != nil {
		return nil, err
	}

	switch strings.ToLower(driverType) {
	case "filesystem":
		dataDir, err := conf.GetString("DATA_DIR")
		if err != nil {
			return nil, err
		}

		return NewFilesystemDriver(dataDir, pageExtension, log)

	case "s3":
		bucketName, err := conf.GetString("BUCKET_NAME")
		if err != nil {
			return nil, err
		}

		accessKeyID, err := conf.GetString("AWS_ACCESS_KEY_ID")
		if err != nil {
			return nil, err
		}

		accessKeySecret, err := conf.GetString("AWS_SECRET_ACCESS_KEY")
		if err != nil {
			return nil, err
		}

		accessKeyToken := conf.GetStringOrEmpty("AWS_SESSION_TOKEN")

		return NewBucketDriver(pageExtension, bucketName, accessKeyID, accessKeySecret, accessKeyToken, log)

	default:
		err := fmt.Errorf("there is no driver available with name %s", driverType)
		return nil, err
	}
}

func isLockfile(path string) bool {
	return filepath.Ext(path) == lockfileExtension
}

func filenameToLockTimestamp(fileName string) (time.Time, error) {
	// if path is given, only look at filename
	cleanName := filepath.Base(fileName)
	// split on dots to get filename before extensions
	nameTokens := strings.Split(cleanName, ".")
	timeString := nameTokens[0]
	return util.ParseMillisString(timeString)
}
