package driver

import (
	"fmt"
	"keybite/config"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// StorageDriver is the interface needed to read and persist files that make up the DB
// A storage driver should handle these IO operations and should handle all paths and
// file extensions (i.e. the filename passed should not end in an extension, as this may
// vary by driver and environment)
type StorageDriver interface {
	ReadPage(filename string, indexName string, pageSize int) (map[uint64]string, []uint64, error)
	ReadMapPage(filename string, indexName string, pageSize int) (map[string]string, []string, error)
	WritePage(vals map[uint64]string, orderedKeys []uint64, filename string, indexName string) error
	WriteMapPage(vals map[string]string, orderedKeys []string, filename string, indexName string) error
	CreateAutoIndex(indexName string) error
	CreateMapIndex(indexName string) error
	DropAutoIndex(indexName string) error
	DropMapIndex(indexName string) error
	// return an ascending-sorted list of pagefiles in the index datadir
	ListPages(indexName string, desc bool) ([]string, error)
	// check if an index is locked by another request process, returning the time at which it was locked if true
	IndexIsLocked(indexName string) (bool, time.Time, error)
	// lock an index
	LockIndex(indexName string) error
	// unlock an index
	UnlockIndex(indexName string) error
}

const lockfileExtension = ".lock"

// GetConfiguredDriver returns the correct driver based on config
func GetConfiguredDriver(conf config.Config) (StorageDriver, error) {
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

		lockMs, err := conf.GetInt64("LOCK_DURATION_FS")
		if err != nil {
			return nil, err
		}

		lockDuration := ToMillisDuration(lockMs)

		return NewFilesystemDriver(dataDir, pageExtension, lockDuration)

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

		lockMs, err := conf.GetInt64("LOCK_DURATION_S3")
		if err != nil {
			return nil, err
		}

		lockDuration := ToMillisDuration(lockMs)

		return NewBucketDriver(pageExtension, bucketName, accessKeyID, accessKeySecret, accessKeyToken, lockDuration)

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
	return ParseMillisString(timeString)
}

// ToMillisDuration turn an int64 millisecond duration into time.Duration
func ToMillisDuration(millis int64) time.Duration {
	return (time.Duration(millis) * time.Millisecond)
}

// ParseMillisString parses a string containing an integer milliseconds since epoch into time.Time
func ParseMillisString(millis string) (time.Time, error) {
	msInt, err := strconv.ParseInt(millis, 10, 64)
	if err != nil {
		return time.Time{}, err
	}

	return time.Unix(0, msInt*int64(time.Millisecond)), nil
}

// timeToMillis returns an int64 of current milliseconds
func timeToMillis(t time.Time) int64 {
	return t.UnixNano() / int64(time.Millisecond)
}
