package driver

import (
	"fmt"
	"keybite-http/config"
	"strings"
)

// StorageDriver is the interface needed to read and persist files that make up the DB
// A storage driver should handle these IO operations and should handle all paths and
// file extensions (i.e. the filename passed should not end in an extension, as this may
// vary by driver and environment)
type StorageDriver interface {
	ReadPage(filename string, indexName string, pageSize int) (map[int64]string, error)
	ReadMapPage(filename string, indexName string, pageSize int) (map[uint64]string, error)
	WritePage(vals map[int64]string, filename string, indexName string) error
	WriteMapPage(vals map[uint64]string, filename string, indexName string) error
	// return an ascending-sorted list of pagefiles in the index datadir
	ListPages(indexName string) ([]string, error)
}

// GetConfiguredDriver returns the correct driver based on config, or panics on missing env var
func GetConfiguredDriver(conf config.Config) StorageDriver {
	driverType, err := conf.GetString("DRIVER")
	if err != nil {
		panic(err)
	}

	dataDir, err := conf.GetString("DATA_DIR")
	if err != nil {
		panic(err)
	}

	pageExtension, err := conf.GetString("PAGE_EXTENSION")
	if err != nil {
		panic(err)
	}

	switch strings.ToLower(driverType) {
	case "filesystem":
		if fsd, err := NewFilesystemDriver(dataDir, pageExtension); err == nil {
			return fsd
		} else {
			panic(err)
		}
	case "s3":
		bucketName, err := conf.GetString("BUCKET_NAME")
		if err != nil {
			panic(err)
		}

		accessKeyID, err := conf.GetString("AWS_ACCESS_KEY_ID")
		if err != nil {
			panic(err)
		}

		accessKeySecret, err := conf.GetString("AWS_ACCESS_KEY_SECRET")
		if err != nil {
			panic(err)
		}

		if bd, err := NewBucketDriver(pageExtension, bucketName, accessKeyID, accessKeySecret); err == nil {
			return bd
		} else {
			panic(err)
		}
	default:
		err := fmt.Errorf("there is no driver available with name %s", driverType)
		panic(err)
	}
}
