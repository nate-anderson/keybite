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

func (e Error) Unwrap() error {
	return e.InternalErr
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

// errIndexNotExist indicates the requested index could not be found
func errIndexNotExist(indexName string, err error) Error {
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

// errAutoIndexKeyNotExist indicates an auto index key could not be found in the provided index
func errAutoIndexKeyNotExist(indexName string, key uint64, err error) Error {
	return Error{
		InternalErr: err,
		Message:     fmt.Sprintf("Key '%d' not found in index '%s'", key, indexName),
		Code:        errCodeKeyNotExist,
	}
}

// errMapIndexKeyNotExist indicates a map index key could not be found in the provided index
func errMapIndexKeyNotExist(indexName, key string, err error) Error {
	return Error{
		InternalErr: err,
		Message:     fmt.Sprintf("Key '%s' not found in index '%s'", key, indexName),
		Code:        errCodeKeyNotExist,
	}
}

// errIndexAlreadyExist indicates an index cannot be created because one already exists with this name
func errIndexAlreadyExist(indexName string, err error) Error {
	return Error{
		InternalErr: err,
		Message:     fmt.Sprintf("Index '%s' cannot be created: already exists", indexName),
		Code:        errCodeIndexAlreadyExist,
	}
}

// errMapIndexKeyAlreadyExist indicates a key cannot be inserted in a map index because it already exsits
func errMapIndexKeyAlreadyExist(indexName, key string, err error) Error {
	return Error{
		InternalErr: err,
		Message:     fmt.Sprintf("Key '%s' already exists in index '%s': cannot insert", key, indexName),
		Code:        errCodeKeyAlreadyExist,
	}
}

// errInternalDriverFailure describes an internal failure unrelated to user action
func errInternalDriverFailure(operationDescription string, err error) Error {
	return Error{
		InternalErr: err,
		Message:     fmt.Sprintf("Operation %s failed: internal driver failure", operationDescription),
		Code:        errCodeInternalStorageFailure,
	}
}

// errDataDirNotExist indicates the configured data directory does not exist
func errDataDirNotExist(dataDir string, err error) Error {
	return Error{
		InternalErr: err,
		Message:     fmt.Sprintf("Configured data directory '%s' could not be found", dataDir),
		Code:        errCodeDataDirNotExist,
	}
}

// errBadIndexData indicates an index page is corrupted
func errBadIndexData(indexName string, fileName string, err error) Error {
	return Error{
		InternalErr: err,
		Message:     fmt.Sprintf("File '%s' in index '%s' is corrupted and cannot be read", fileName, indexName),
		Code:        errCodeBadData,
	}
}

// errPageNotExist indicates the index exists, but the page does not
func errPageNotExist(indexName, pageName string, err error) Error {
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
