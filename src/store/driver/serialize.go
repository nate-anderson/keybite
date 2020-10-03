package driver

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// intermediate structs used for JSON marshalling/unmarshalling
type jsonAutoPage struct {
	Data        *map[uint64]string `json:"data"`
	OrderedKeys *[]uint64          `json:"orderedKeys"`
}

type jsonMapPage struct {
	Data        *map[string]string `json:"data"`
	OrderedKeys *[]string          `json:"orderedKeys"`
}

// NewPageReader constructs a page reader for an auto index page
func NewPageReader(vals map[uint64]string, orderedKeys []uint64) (io.Reader, error) {
	jsonPage := jsonAutoPage{
		&vals,
		&orderedKeys,
	}
	bytes, err := json.Marshal(jsonPage)
	if err != nil {
		return nil, fmt.Errorf("error forming JSON page body: %w", err)
	}

	return strings.NewReader(string(bytes)), nil
}

// NewMapPageReader constructs a page reader for a map page
func NewMapPageReader(vals map[string]string, orderedKeys []string) (io.Reader, error) {
	jsonPage := jsonMapPage{
		&vals,
		&orderedKeys,
	}
	bytes, err := json.Marshal(jsonPage)
	if err != nil {
		return nil, fmt.Errorf("error forming JSON page body: %w", err)
	}

	return strings.NewReader(string(bytes)), nil
}
