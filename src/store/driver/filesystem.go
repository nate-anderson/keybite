package driver

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"keybite-http/util"
	"os"
	"path"
)

// FilesystemDriver enables writing and reading indexes from local filesystem
type FilesystemDriver struct {
	dataDir       string
	pageExtension string
}

// NewFilesystemDriver instantiates a new filesystem storage driver
func NewFilesystemDriver(dataDir string, pageExtension string) (FilesystemDriver, error) {
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		return FilesystemDriver{}, fmt.Errorf("no data directory named %s could be found", dataDir)
	}

	return FilesystemDriver{
		dataDir:       dataDir,
		pageExtension: pageExtension,
	}, nil
}

// ReadPage reads a file into a map
func (d FilesystemDriver) ReadPage(filename string, indexName string, pageSize int) (map[int64]string, error) {
	vals := make(map[int64]string, pageSize)
	path := path.Join(d.dataDir, indexName, util.AddSuffixIfNotExist(filename, d.pageExtension))

	pageFile, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return vals, ErrNotExist(path)
		}
		return vals, err
	}
	defer pageFile.Close()

	scanner := bufio.NewScanner(pageFile)
	for scanner.Scan() {
		key, value, err := util.StringToKeyValue(scanner.Text())
		if err != nil {
			return vals, err
		}
		vals[key] = value
	}

	return vals, nil
}

// ReadMapPage reads a file into a map page
func (d FilesystemDriver) ReadMapPage(fileName string, indexName string, pageSize int) (map[uint64]string, error) {
	vals := map[uint64]string{}
	filePath := path.Join(d.dataDir, indexName, util.AddSuffixIfNotExist(fileName, d.pageExtension))

	pageFile, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return vals, ErrNotExist(filePath)
		}
		return vals, err
	}
	defer pageFile.Close()

	scanner := bufio.NewScanner(pageFile)
	for scanner.Scan() {
		key, value, err := util.StringToMapKeyValue(scanner.Text())
		if err != nil {
			return vals, err
		}
		vals[key] = value
	}

	return vals, nil
}

// WritePage persists a new or updated page as a file in the datadir
func (d FilesystemDriver) WritePage(vals map[int64]string, filename string, indexName string) error {
	filePath := path.Join(d.dataDir, indexName, util.AddSuffixIfNotExist(filename, d.pageExtension))
	file, err := os.OpenFile(filePath, os.O_RDWR, 0755)
	if err != nil {
		if os.IsNotExist(err) {
			file, err = os.Create(filePath)
		} else {
			return err
		}
	}
	defer file.Close()

	for key, value := range vals {
		line := fmt.Sprintf("%d:%s\n", key, value)
		_, err = file.Write([]byte(line))
		if err != nil {
			return err
		}
	}

	return nil
}

// WriteMapPage persists a new or updated map page as a file in the dataDir
func (d FilesystemDriver) WriteMapPage(vals map[uint64]string, fileName string, indexName string) error {
	filePath := path.Join(d.dataDir, indexName, util.AddSuffixIfNotExist(fileName, d.pageExtension))
	file, err := os.OpenFile(filePath, os.O_RDWR, 0755)
	if err != nil {
		if !os.IsNotExist(err) {
			file, err = os.Create(filePath)
		} else {
			return err
		}

		if file, err = os.Create(filePath); err != nil {
			return err
		}
	}
	defer file.Close()

	for key, value := range vals {
		line := fmt.Sprintf("%d:%s\n", key, value)
		_, err = file.Write([]byte(line))
		if err != nil {
			return err
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
			return []string{}, ErrNotExist(indexPath)
		}
		return []string{}, err
	}

	fileNames := make([]string, len(files))
	for i, file := range files {
		fileNames[i] = file.Name()
	}

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
