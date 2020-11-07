package driver

import (
	"fmt"
)

/*
 ### New Structured Errors
*/

// Error contains information related to a storage driver error
type Error struct {
	InternalErr error
	Message     string
	Code        string
}

func (e Error) Error() string {
	return e.Message
}

const (
	errCodeIndexNotExist          = "ERR_INDEX_NOT_EXIST"
	errCodeKeyNotExist            = "ERR_KEY_NOT_EXIST"
	errCodeIndexAlreadyExist      = "ERR_INDEX_ALREADY_EXIST"
	errCodeKeyAlreadyExist        = "ERR_KEY_ALREADY_EXIST"
	errCodeBadData                = "ERR_BAD_INDEX_DATA"
	errCodeInternalStorageFailure = "ERR_INTERNAL_DRIVER_FAILURE"
	errCodeDataDirNotExist        = "ERR_DATA_DIR_NOT_EXIST"
	errCodePageNotExist           = "ERR_PAGE_NOT_EXIST"
)

// ErrIndexNotExist indicates the requested index could not be found
func ErrIndexNotExist(indexName string, err error) Error {
	return Error{
		InternalErr: err,
		Message:     fmt.Sprintf("Index '%s' not found", indexName),
		Code:        errCodeIndexNotExist,
	}
}

// IsIndexNotExist indicates if an error is a missing index error
func IsIndexNotExist(err error) bool {
	e, ok := err.(Error)
	if ok && e.Code == errCodeIndexNotExist {
		return true
	}

	return false
}

// ErrAutoIndexKeyNotExist indicates an auto index key could not be found in the provided index
func ErrAutoIndexKeyNotExist(indexName string, key uint64, err error) Error {
	return Error{
		InternalErr: err,
		Message:     fmt.Sprintf("Key '%d' not found in index '%s'", key, indexName),
		Code:        errCodeKeyNotExist,
	}
}

// ErrMapIndexKeyNotExist indicates a map index key could not be found in the provided index
func ErrMapIndexKeyNotExist(indexName, key string, err error) Error {
	return Error{
		InternalErr: err,
		Message:     fmt.Sprintf("Key '%s' not found in index '%s'", key, indexName),
		Code:        errCodeKeyNotExist,
	}
}

// ErrIndexAlreadyExist indicates an index cannot be created because one already exists with this name
func ErrIndexAlreadyExist(indexName string, err error) Error {
	return Error{
		InternalErr: err,
		Message:     fmt.Sprintf("Index '%s' cannot be created: already exists", indexName),
		Code:        errCodeIndexAlreadyExist,
	}
}

// ErrMapIndexKeyAlreadyExist indicates a key cannot be inserted in a map index because it already exsits
func ErrMapIndexKeyAlreadyExist(indexName, key string, err error) Error {
	return Error{
		InternalErr: err,
		Message:     fmt.Sprintf("Key '%s' already exists in index '%s': cannot insert", key, indexName),
		Code:        errCodeKeyAlreadyExist,
	}
}

// ErrInternalDriverFailure describes an internal failure unrelated to user action
func ErrInternalDriverFailure(operationDescription string, err error) Error {
	return Error{
		InternalErr: err,
		Message:     fmt.Sprintf("Operation %s failed: internal driver failure", operationDescription),
		Code:        errCodeInternalStorageFailure,
	}
}

// ErrDataDirNotExist indicates the configured data directory does not exist
func ErrDataDirNotExist(dataDir string, err error) Error {
	return Error{
		InternalErr: err,
		Message:     fmt.Sprintf("Configured data directory '%s' could not be found", dataDir),
		Code:        errCodeDataDirNotExist,
	}
}

// ErrBadIndexData indicates an index page is corrupted
func ErrBadIndexData(indexName string, fileName string, err error) Error {
	return Error{
		InternalErr: err,
		Message:     fmt.Sprintf("File '%s' in index '%s' is corrupted and cannot be read", fileName, indexName),
		Code:        errCodeBadData,
	}
}

// ErrPageNotExist indicates the index exists, but the page does not
func ErrPageNotExist(indexName, pageName string, err error) Error {
	return Error{
		InternalErr: err,
		Message:     fmt.Sprintf("File '%s' not found in index '%s'", pageName, indexName),
		Code:        errCodePageNotExist,
	}
}

// IsPageNotExist indicates if an error is a missing page error
func IsPageNotExist(err error) bool {
	e, ok := err.(Error)
	if ok && e.Code == errCodePageNotExist {
		return true
	}

	return false
}
