# Change Log

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

## [Unreleased]

## [0.0.4] - 2022-06-24

### Added

- `connect()` which provides the `Ckydb` controller context manager
- `ckydb.set()` which sets a key-value pair in the ckydb
- `ckydb.get()` which gets a value for a given key in the ckydb
- `ckydb.delete()` which deletes a given key-value pair from ckydb. This makes the keys unindexable but
  they still exist in the database
- `ckydb.clear()` resets the database, deleting all keys
- A background task to vacuum the database at user-defined interval. Vacuuming removes all key-value
pairs that are no longer unindexable due to being deleted

### Changed


### Fixed
