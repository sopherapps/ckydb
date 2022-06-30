package internal

import "errors"

var (
	ErrAlreadyRunning = errors.New("already running")
	ErrNotRunning     = errors.New("not running")
	ErrNotFound       = errors.New("not found")
	ErrCorruptedData  = errors.New("data in database is corrupt")
	ErrOutOfBounds    = errors.New("out of bounds")
)
