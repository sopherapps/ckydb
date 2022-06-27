package ckydb

import "errors"

var (
	ErrNotFound      = errors.New("not found")
	ErrCorruptedData = errors.New("data in database is corrupt")
)
