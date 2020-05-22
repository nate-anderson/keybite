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
		return FilesystemDriver{
			pageExtension: pageExtension,
			dataDir:       dataDir,
		}
	default:
		err := fmt.Errorf("there is no driver available with name %s", driverType)
		panic(err)
	}
}
