package memdb

import "errors"

var (
	ErrNotImplemented = errors.New("Function not implemented")
	ErrAlreadyExists  = errors.New("Key already exists")
	ErrNonRootBucket  = errors.New("Could not dump non-root bucket")
)
