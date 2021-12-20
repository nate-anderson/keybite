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
	_, err := os.Stat(dataDir)
	if err != nil {
		if os.IsNotExist(err) {
			return FilesystemDriver{}, errDataDirNotExist(dataDir, err)
		}
		return FilesystemDriver{}, errInternalDriverFailure("reading data directory", err)
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

	pageFile, err := d.openPageFile(indexName, fileName)
	if err != nil {
		return vals, orderedKeys, err
	}
	defer pageFile.Close()

	scanner := bufio.NewScanner(pageFile)
	i := 0
	for scanner.Scan() {
		key, value, err := stringToKeyValue(scanner.Text())
		if err != nil {
			return vals, orderedKeys, errBadIndexData(indexName, fileName, err)
		}
		vals[key] = unescapeNewlines(value)
		orderedKeys = append(orderedKeys, key)
		i++
	}

	return vals, orderedKeys, nil
}

// ReadMapPage reads a file into a map page
func (d FilesystemDriver) ReadMapPage(fileName string, indexName string, pageSize int) (map[string]string, []string, error) {
	vals := map[string]string{}
	orderedKeys := make([]string, 0, pageSize)

	pageFile, err := d.openPageFile(indexName, fileName)
	if err != nil {
		return vals, orderedKeys, err
	}
	defer pageFile.Close()

	scanner := bufio.NewScanner(pageFile)
	for scanner.Scan() {
		key, value, err := stringToMapKeyValue(scanner.Text())
		if err != nil {
			return vals, orderedKeys, errBadIndexData(indexName, fileName, err)
		}
		vals[key] = unescapeNewlines(value)
		orderedKeys = append(orderedKeys, key)
	}

	return vals, orderedKeys, nil
}

// WritePage persists a new or updated page as a file in the datadir
func (d FilesystemDriver) WritePage(vals map[uint64]string, orderedKeys []uint64, fileName string, indexName string) error {
	file, err := d.openOrCreatPageFileForWrite(indexName, fileName)
	defer file.Close()

	// truncate file before writing
	err = file.Truncate(0)
	if err != nil {
		return errInternalDriverFailure("truncating page file for update", err)
	}

	for _, key := range orderedKeys {
		line := fmt.Sprintf("%d:%s\n", key, escapeNewlines(vals[key]))
		_, err = file.Write([]byte(line))
		if err != nil {
			return errInternalDriverFailure("writing data to file", err)
		}
	}

	return nil
}

// WriteMapPage persists a new or updated map page as a file in the dataDir
func (d FilesystemDriver) WriteMapPage(vals map[string]string, orderedKeys []string, fileName string, indexName string) error {
	file, err := d.openOrCreatPageFileForWrite(indexName, fileName)
	defer file.Close()

	// truncate file before writing
	err = file.Truncate(0)
	if err != nil {
		return errInternalDriverFailure("truncating page file for update", err)
	}

	for _, key := range orderedKeys {
		line := fmt.Sprintf("%s:%s\n", key, escapeNewlines(vals[key]))
		_, err = file.Write([]byte(line))
		if err != nil {
			return errInternalDriverFailure("writing data to file", err)
		}
	}

	return nil
}

// ListPages lists the page files in the data directory
func (d FilesystemDriver) ListPages(indexName string, desc bool) ([]string, error) {
	indexPath := path.Join(d.dataDir, indexName)
	files, err := ioutil.ReadDir(indexPath)
	if err != nil {
		if os.IsNotExist(err) {
			return []string{}, errIndexNotExist(indexName, err)
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

	return sortFileNames(fileNames, d.pageExtension, desc), nil
}

// CreateAutoIndex creates the folder for an auto index in the data dir
func (d FilesystemDriver) CreateAutoIndex(indexName string) error {
	indexPath := path.Join(d.dataDir, indexName)
	err := os.Mkdir(indexPath, 0755)
	if err != nil {
		return errInternalDriverFailure("create auto index", err)
	}
	return nil
}

// CreateMapIndex creates the folder for a map index in the data dir
func (d FilesystemDriver) CreateMapIndex(indexName string) error {
	indexPath := path.Join(d.dataDir, indexName)
	err := os.Mkdir(indexPath, 0755)
	if err != nil {
		return errInternalDriverFailure("create map index", err)
	}
	return nil
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
		return errInternalDriverFailure("locking index", err)
	}

	defer file.Close()
	return nil
}

// UnlockIndex deletes any lockfiles in an index
func (d FilesystemDriver) UnlockIndex(indexName string) error {
	log.Debugf("unlocking index %s for writes", indexName)
	globPattern := path.Join(d.dataDir, indexName, ("*" + lockfileExtension))
	fNames, err := filepath.Glob(globPattern)
	if err != nil {
		return errInternalDriverFailure("reading index files for unlock", err)
	}

	for _, path := range fNames {
		log.Debugf("deleting lockfile %s", path)
		if err := os.Remove(path); err != nil {
			return errInternalDriverFailure("deleting lockfile", err)
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
		return true, time.Time{}, errInternalDriverFailure("listing index directory contents", err)
	}

	// if lockfile(s) present, return max lock timestamp
	if len(fNames) > 0 {
		log.Debugf("found %d lockfiles in index %s", len(fNames), indexName)
		maxLockTs := time.Time{}
		for _, name := range fNames {
			ts, err := filenameToLockTimestamp(name)
			if err != nil {
				return true, maxLockTs.Add(d.lockDuration), errInternalDriverFailure("creating lock file for index", err)
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
		return errIndexNotExist(indexName, err)
	}
	err := os.RemoveAll(indexPath)
	if err != nil {
		return errInternalDriverFailure("dropping auto index", err)
	}
	return nil
}

// DropMapIndex permanently deletes all the data and directory for a map index
func (d FilesystemDriver) DropMapIndex(indexName string) error {
	indexPath := path.Join(d.dataDir, indexName)
	if _, err := os.Stat(indexPath); os.IsNotExist(err) {
		return errIndexNotExist(indexName, err)
	}
	err := os.RemoveAll(indexPath)
	if err != nil {
		return errInternalDriverFailure("dropping map index", err)
	}
	return nil
}

// check if index directory exists in data dir
func (d FilesystemDriver) indexExists(indexName string) (bool, error) {
	_, err := os.Stat(path.Join(d.dataDir, indexName))
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, errInternalDriverFailure("reading index", err)
}

// helper for opening page file pointers
func (d FilesystemDriver) openPageFile(indexName, fileName string) (*os.File, error) {
	filePath := path.Join(d.dataDir, indexName, addSuffixIfNotExist(fileName, d.pageExtension))
	pageFile, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			// determine if index not exist or key not exist
			indexExists, err := d.indexExists(indexName)
			if err != nil {
				return pageFile, err
			}
			if !indexExists {
				return pageFile, errIndexNotExist(indexName, err)
			}
			return pageFile, errPageNotExist(indexName, fileName, err)
		}
		return pageFile, errInternalDriverFailure("reading index page", err)
	}
	return pageFile, nil
}

func (d FilesystemDriver) openOrCreatPageFileForWrite(indexName, fileName string) (*os.File, error) {
	filePath := path.Join(d.dataDir, indexName, addSuffixIfNotExist(fileName, d.pageExtension))
	file, err := os.OpenFile(filePath, os.O_RDWR, 0755)
	if err != nil {
		if os.IsNotExist(err) {
			file, err = os.Create(filePath)
			if err != nil {
				return file, errInternalDriverFailure("creating index page", err)
			}
		} else {
			return file, errInternalDriverFailure("reading index page", err)
		}
	}
	return file, nil
}
