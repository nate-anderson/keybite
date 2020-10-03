package driver

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"keybite/util/log"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"time"
)

// FilesystemDriver enables writing and reading indexes from local filesystem
type FilesystemDriver struct {
	dataDir       string
	pageExtension string
	lockDuration  time.Duration
}

// NewFilesystemDriver instantiates a new filesystem storage driver
func NewFilesystemDriver(dataDir string, pageExtension string, lockDuration time.Duration) (FilesystemDriver, error) {
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		return FilesystemDriver{}, ErrDataDirNotExist(dataDir, err)
	}

	return FilesystemDriver{
		dataDir:       dataDir,
		pageExtension: pageExtension,
		lockDuration:  lockDuration,
	}, nil
}

// ReadPage reads a file into a map
func (d FilesystemDriver) ReadPage(fileName string, indexName string, pageSize int) (map[uint64]string, []uint64, error) {
	vals := make(map[uint64]string, pageSize)
	orderedKeys := make([]uint64, 0, pageSize)
	path := path.Join(d.dataDir, indexName, AddSuffixIfNotExist(fileName, d.pageExtension))

	pageFile, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return vals, orderedKeys, ErrNotExist(path, indexName, err)
		}
		return vals, orderedKeys, ErrReadFile(fileName, indexName, err)
	}
	defer pageFile.Close()

	jsonPage := jsonAutoPage{
		&vals,
		&orderedKeys,
	}
	decoder := json.NewDecoder(pageFile)
	err = decoder.Decode(&jsonPage)
	if err != nil {
		return vals, orderedKeys, ErrBadData(fileName, indexName, err)
	}

	return vals, orderedKeys, nil
}

// ReadMapPage reads a file into a map page
func (d FilesystemDriver) ReadMapPage(fileName string, indexName string, pageSize int) (map[string]string, []string, error) {
	vals := map[string]string{}
	orderedKeys := make([]string, 0, pageSize)
	filePath := path.Join(d.dataDir, indexName, AddSuffixIfNotExist(fileName, d.pageExtension))

	pageFile, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return vals, orderedKeys, ErrNotExist(filePath, indexName, err)
		}
		return vals, orderedKeys, ErrReadFile(fileName, indexName, err)
	}
	defer pageFile.Close()

	jsonPage := jsonMapPage{
		&vals,
		&orderedKeys,
	}
	decoder := json.NewDecoder(pageFile)
	err = decoder.Decode(&jsonPage)
	if err != nil {
		return vals, orderedKeys, ErrBadData(fileName, indexName, err)
	}

	return vals, orderedKeys, nil
}

// WritePage persists a new or updated page as a file in the datadir
func (d FilesystemDriver) WritePage(vals map[uint64]string, orderedKeys []uint64, filename string, indexName string) error {
	filePath := path.Join(d.dataDir, indexName, AddSuffixIfNotExist(filename, d.pageExtension))
	file, err := os.OpenFile(filePath, os.O_RDWR, 0755)
	if err != nil {
		if os.IsNotExist(err) {
			file, err = os.Create(filePath)
			if err != nil {
				return ErrWriteFile(filename, indexName, err)
			}
		} else {
			return ErrReadFile(filename, indexName, err)
		}
	}
	defer file.Close()

	// truncate file before writing
	err = file.Truncate(0)
	if err != nil {
		return ErrWriteFile(filename, indexName, err)
	}

	jsonPage := jsonAutoPage{
		&vals,
		&orderedKeys,
	}
	encoder := json.NewEncoder(file)
	err = encoder.Encode(jsonPage)
	if err != nil {
		return ErrWriteFile(filename, indexName, err)
	}

	return nil
}

// WriteMapPage persists a new or updated map page as a file in the dataDir
func (d FilesystemDriver) WriteMapPage(vals map[string]string, orderedKeys []string, fileName string, indexName string) error {
	filePath := path.Join(d.dataDir, indexName, AddSuffixIfNotExist(fileName, d.pageExtension))
	file, err := os.OpenFile(filePath, os.O_RDWR, 0755)
	if err != nil {
		if os.IsNotExist(err) {
			file, err = os.Create(filePath)
			if err != nil {
				return err
			}
		} else {
			return ErrReadFile(fileName, indexName, err)
		}

		if file, err = os.Create(filePath); err != nil {
			return ErrWriteFile(fileName, indexName, err)
		}
	}
	defer file.Close()

	// truncate file before writing
	err = file.Truncate(0)
	if err != nil {
		return ErrWriteFile(fileName, indexName, err)
	}

	jsonPage := jsonMapPage{
		&vals,
		&orderedKeys,
	}
	encoder := json.NewEncoder(file)
	err = encoder.Encode(jsonPage)
	if err != nil {
		return ErrWriteFile(fileName, indexName, err)
	}

	return nil
}

// ListPages lists the page files in the data directory
func (d FilesystemDriver) ListPages(indexName string) ([]string, error) {
	indexPath := path.Join(d.dataDir, indexName)
	files, err := ioutil.ReadDir(indexPath)
	if err != nil {
		if os.IsNotExist(err) {
			return []string{}, ErrNotExist(indexPath, indexName, err)
		}
		return []string{}, err
	}

	fileNames := []string{}
	for _, file := range files {
		fName := file.Name()
		// exclude lock files from results
		if isLockfile(fName) {
			continue
		}
		fileNames = append(fileNames, fName)
	}

	return sortFileNames(fileNames, d.pageExtension), nil
}

// CreateAutoIndex creates the folder for an auto index in the data dir
func (d FilesystemDriver) CreateAutoIndex(indexName string) error {
	indexPath := path.Join(d.dataDir, indexName)
	return os.Mkdir(indexPath, 0755)
}

// CreateMapIndex creates the folder for a map index in the data dir
func (d FilesystemDriver) CreateMapIndex(indexName string) error {
	indexPath := path.Join(d.dataDir, indexName)
	return os.Mkdir(indexPath, 0755)
}

// LockIndex creates a lockfile in the specified index
func (d FilesystemDriver) LockIndex(indexName string) error {
	log.Debugf("locking index %s for writes", indexName)
	lockExpires := time.Now().Add(d.lockDuration)
	expiresMillis := strconv.FormatInt(timeToMillis(lockExpires), 10)

	lockfileName := expiresMillis + d.pageExtension + lockfileExtension

	filePath := path.Join(d.dataDir, indexName, lockfileName)
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("writing lock file for index %s failed: %w", indexName, err)
	}

	log.Debugf("created lockfile %s", filePath)

	defer file.Close()
	return nil
}

// UnlockIndex deletes any lockfiles in an index
func (d FilesystemDriver) UnlockIndex(indexName string) error {
	log.Debugf("unlocking index %s for writes", indexName)
	globPattern := path.Join(d.dataDir, indexName, ("*" + lockfileExtension))
	fNames, err := filepath.Glob(globPattern)
	if err != nil {
		return fmt.Errorf("checking for lock files in index %s failed: %w", indexName, err)
	}

	for _, path := range fNames {
		log.Debugf("deleting lockfile %s", path)
		if err := os.Remove(path); err != nil {
			return fmt.Errorf("deleting lockfile in index %s failed: %w", indexName, err)
		}
	}

	return nil
}

// IndexIsLocked checks if an index is locked by another request process, returning the time at which the lock expires
func (d FilesystemDriver) IndexIsLocked(indexName string) (bool, time.Time, error) {
	log.Debugf("checking index %s for write locks", indexName)
	globPattern := path.Join(d.dataDir, indexName, ("*" + lockfileExtension))
	fNames, err := filepath.Glob(globPattern)
	if err != nil {
		return true, time.Time{}, fmt.Errorf("checking for lock files in index %s failed: %w", indexName, err)
	}

	// if lockfile(s) present, return max lock timestamp
	if len(fNames) > 0 {
		log.Debugf("found %d lockfiles in index %s", len(fNames), indexName)
		maxLockTs := time.Time{}
		for _, name := range fNames {
			ts, err := filenameToLockTimestamp(name)
			if err != nil {
				return true, maxLockTs.Add(d.lockDuration), fmt.Errorf("creating lock file for index %s failed: %w", indexName, err)
			}
			if ts.After(maxLockTs) {
				maxLockTs = ts
			}
		}

		expire := maxLockTs.Add(d.lockDuration)
		isLocked := maxLockTs.After(time.Now())
		log.Debugf("index %s locks expired at or before %s: locked? %v", indexName, expire.String(), isLocked)
		return isLocked, expire, nil
	}

	return false, time.Time{}, nil
}

// DropAutoIndex permanently deletes all the data and directory for an auto index
func (d FilesystemDriver) DropAutoIndex(indexName string) error {
	indexPath := path.Join(d.dataDir, indexName)
	if _, err := os.Stat(indexPath); os.IsNotExist(err) {
		return fmt.Errorf("failed dropping index '%s': directory does not exist", indexName)
	}
	return os.RemoveAll(indexPath)
}

// DropMapIndex permanently deletes all the data and directory for a map index
func (d FilesystemDriver) DropMapIndex(indexName string) error {
	indexPath := path.Join(d.dataDir, indexName)
	if _, err := os.Stat(indexPath); os.IsNotExist(err) {
		return fmt.Errorf("failed dropping index '%s': directory does not exist", indexName)
	}
	return os.RemoveAll(indexPath)
}
