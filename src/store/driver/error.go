package driver

import (
	"fmt"
)

const errNotExistMsg = "File or directory '%s' (index '%s') does not exist: %w"
const errReadFileMsg = "File or directory '%s' (index '%s') could not be read: %w"
const errWriteFileMsg = "File or directory '%s' (index '%s') could not be written: %w"
const errNoDataDirMsg = "Data directory at '%s' was inaccessible or could not be found: %w"

// FileError should be used accross drivers to indicate a file could not be found
type FileError struct {
	msgFmt    string
	filename  string
	indexname string
	original  error
}

// Error returns the error message
func (e FileError) Error() string {
	return fmt.Sprintf(e.msgFmt, e.filename, e.original)
}

// Unwrap the underlying error
func (e FileError) Unwrap() error { return e.original }

// IsNotExistError checks whether an error is a FileError
func IsNotExistError(err error) bool {
	fne, ok := err.(FileError)
	if !ok {
		return false
	}

	return fne.msgFmt == errNotExistMsg || fne.msgFmt == errNoDataDirMsg
}

// ErrNotExist returns a FileNotExist error
func ErrNotExist(filename, indexname string, original error) error {
	return FileError{
		msgFmt:    errNotExistMsg,
		filename:  filename,
		indexname: indexname,
		original:  original,
	}
}

// ErrReadFile returns an error due to failed read
func ErrReadFile(filename, indexname string, original error) error {
	return FileError{
		msgFmt:    errReadFileMsg,
		filename:  filename,
		indexname: indexname,
		original:  original,
	}
}

// ErrWriteFile returns an error due to failed write
func ErrWriteFile(filename, indexname string, original error) error {
	return FileError{
		msgFmt:    errWriteFileMsg,
		filename:  filename,
		indexname: indexname,
		original:  original,
	}
}

// ErrDataDirNotExist returns an error specific to a data directory not being readable
func ErrDataDirNotExist(dataDir string, original error) error {
	return FileError{
		msgFmt:   errNoDataDirMsg,
		filename: dataDir,
		original: original,
	}
}
