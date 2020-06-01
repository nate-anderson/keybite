package driver

import (
	"fmt"
	"io"
	"strings"
)

// NewPageReader constructs a page reader for an auto index page
func NewPageReader(vals map[int64]string) io.Reader {
	var body string
	for key, value := range vals {
		line := fmt.Sprintf("%d:%s\n", key, value)
		body += line
	}

	return strings.NewReader(body)
}

// NewMapPageReader constructs a page reader for a map index page
func NewMapPageReader(vals map[uint64]string) io.Reader {
	var body string
	for key, value := range vals {
		line := fmt.Sprintf("%d:%s\n", key, value)
		body += line
	}

	return strings.NewReader(body)
}
