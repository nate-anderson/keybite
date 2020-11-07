package store

import (
	"fmt"
	"keybite/store/driver"
	"strconv"
)

// Error represents an index-level error
type Error struct {
	InternalError error
	IndexName     string
	Key           string
	message       string
	format        string
	Code          string
}

// Error returns message if no format present, or returns a formatted message,
// format should present key then index name
func (e Error) Error() string {
	// if key is present, assume formatted message
	if e.format != "" {
		if e.Key != "" {
			return fmt.Sprintf(e.format, e.Key, e.IndexName)
		}
		return fmt.Sprintf(e.format, e.IndexName)
	}
	return e.message
}

func (e Error) Unwrap() error {
	return e.InternalError
}

// most driver errors should be returned as-is, but the driver package is not aware
// of keys, so missing-key errors should be created at the store package level
const (
	errCodeIndexNotExist   = "ERR_INDEX_NOT_EXIST"
	errCodeKeyNotExist     = "ERR_KEY_NOT_EXIST"
	errCodeBadData         = "ERR_BAD_INDEX_DATA"
	errCodeInvalidMapKey   = "ERR_INVALID_MAP_KEY"
	errCodeKeyAlreadyExist = "ERR_KEY_ALREADY_EXIST"
)

// maybeMissingKeyError returns the driver error unless it is a missing-key error
func maybeMissingKeyError(indexName string, key interface{}, err error) error {
	if driver.IsPageNotExist(err) {
		return errKeyNotExist(indexName, key, err)
	}
	return err
}

// errKeyNotExist indicates
func errKeyNotExist(indexName string, key interface{}, err error) error {
	storeErr := Error{
		format:        "Key '%s' not found in index '%s'",
		IndexName:     indexName,
		InternalError: err,
		Code:          errCodeKeyNotExist,
	}
	switch value := key.(type) {
	case uint64:
		storeErr.Key = strconv.FormatUint(value, 10)
	case string:
		storeErr.Key = value
	default:
		return fmt.Errorf("encountered invalid key '%v' of type %T", value, value)
	}
	return storeErr
}

func errBadData(indexName, fileName string, err error) error {
	return Error{
		message:       fmt.Sprintf("File '%s' in index '%s' malformed or corrupt - unparseable file name", fileName, indexName),
		Code:          errCodeBadData,
		IndexName:     indexName,
		InternalError: err,
	}
}

func errInvalidMapKey(indexName, key string, err error) error {
	return Error{
		message:       fmt.Sprintf("Key '%s' is invalid: %s", key, err.Error()),
		Code:          errCodeInvalidMapKey,
		InternalError: err,
		IndexName:     indexName,
	}
}

func errKeyAlreadyExist(indexName, key string, err error) error {
	return Error{
		format:        "Key '%s' cannot be inserted into index '%s': already exists",
		Key:           key,
		IndexName:     indexName,
		InternalError: err,
		Code:          errCodeKeyAlreadyExist,
	}
}
