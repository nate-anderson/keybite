package store

import "os"

// CreateIndexDirectory creates a directory for a new index
func CreateIndexDirectory(path string) error {
	return os.Mkdir(path, 0755)
}
