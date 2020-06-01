package driver

import "fmt"

const errNotExistMsg = "File or directory does not exist"

// FileError should be used accross drivers to indicate a file could not be found
type FileError struct {
	msg      string
	filename string
}

func (e FileError) Error() string {
	return fmt.Sprintf("file %s does not exist: %s", e.filename, e.msg)
}

// IsNotExistError checks whether an error is a FileError
func IsNotExistError(err error) bool {
	fne, ok := err.(FileError)
	if !ok {
		return false
	}

	return fne.msg == errNotExistMsg
}

// ErrNotExist returns a FileNotExist error
func ErrNotExist(filename string) error {
	return FileError{
		msg:      errNotExistMsg,
		filename: filename,
	}
}
