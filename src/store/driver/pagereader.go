package driver

import (
	"fmt"
	"io"
	"strings"
)

// NewPageReader constructs a page reader for an auto index page
func NewPageReader(vals map[uint64]string) io.Reader {
	var body string
	for key, value := range vals {
		line := fmt.Sprintf("%d:%s\n", key, value)
		body += line
	}

	return strings.NewReader(body)
}

// NewMapPageReader constructs a page reader for a map page
func NewMapPageReader(vals map[string]string) io.Reader {
	var body string
	for key, value := range vals {
		line := fmt.Sprintf("%s:%s\n", key, value)
		body += line
	}

	return strings.NewReader(body)
}
