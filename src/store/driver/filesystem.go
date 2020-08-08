package driver

import (
	"bufio"
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
	keys := make([]uint64, 0, pageSize)
	path := path.Join(d.dataDir, indexName, AddSuffixIfNotExist(fileName, d.pageExtension))

	pageFile, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return vals, keys, ErrNotExist(path, indexName, err)
		}
		return vals, keys, ErrReadFile(fileName, indexName, err)
	}
	defer pageFile.Close()

	scanner := bufio.NewScanner(pageFile)
	i := 0
	for scanner.Scan() {
		key, value, err := StringToKeyValue(scanner.Text())
		if err != nil {
			return vals, keys, fmt.Errorf("pagefile parsing failed: %w", err)
		}
		vals[key] = value
		keys = append(keys, key)
		i++
	}

	return vals, keys, nil
}

// ReadMapPage reads a file into a map page
func (d FilesystemDriver) ReadMapPage(fileName string, indexName string, pageSize int) (map[string]string, []string, error) {
	vals := map[string]string{}
	orderedKeys := []string{}
	filePath := path.Join(d.dataDir, indexName, AddSuffixIfNotExist(fileName, d.pageExtension))

	pageFile, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return vals, orderedKeys, ErrNotExist(filePath, indexName, err)
		}
		return vals, orderedKeys, ErrReadFile(fileName, indexName, err)
	}
	defer pageFile.Close()

	scanner := bufio.NewScanner(pageFile)
	for scanner.Scan() {
		key, value, err := StringToMapKeyValue(scanner.Text())
		if err != nil {
			return vals, orderedKeys, fmt.Errorf("pagefile parsing failed: %w", err)
		}
		vals[key] = value
		orderedKeys = append(orderedKeys, key)
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

	for _, key := range orderedKeys {
		line := fmt.Sprintf("%d:%s\n", key, vals[key])
		_, err = file.Write([]byte(line))
		if err != nil {
			return err
		}
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
		} else {
			return ErrReadFile(fileName, indexName, err)
		}

		if file, err = os.Create(filePath); err != nil {
			return ErrWriteFile(fileName, indexName, err)
		}
	}
	defer file.Close()

	for _, key := range orderedKeys {
		line := fmt.Sprintf("%s:%s\n", key, vals[key])
		_, err = file.Write([]byte(line))
		if err != nil {
			return fmt.Errorf("writing line to map index file '%s' in index '%s' failed: %w", fileName, indexName, err)
		}
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

	log.Debugf("found %d page files %v", len(fileNames), fileNames)

	return fileNames, nil
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
	currentMillis := strconv.FormatInt(MakeTimestamp(), 10)
	lockfileName := currentMillis + d.pageExtension + lockfileExtension

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
