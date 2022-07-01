use std::io;
use crate::errors::{CorruptedDataError, NotFoundError};


/// `Store` trait represents the basic expectation for the internal store that accesses the file
/// system as well as stores data in memory
///
/// It must be able to do the basic [set], [get], [delete], and [clear] operations for
/// accessing and manipulating data in the database.
///
/// It must also be able to [load] the data from disk into memory, e.g. at start up
/// It should also be able to [vacuum] any keys that have been marked for deletion and are
/// thus no longer accessible
///
/// [set]: Storage::set
/// [get]: Storage::get
/// [delete]: Storage::delete
/// [clear]: Storage::clear
/// [load]: Storage::load
/// [vacuum]: Storage::vacuum
pub(crate) trait Storage {

    /// Loads the storage from disk
    ///
    /// # Errors
    /// - [io::Error] I/O errors e.g file permissions, missing files in case the database folder
    /// is not accessible
    ///
    /// [io::Error]: std::io::Error
    fn load(&self) -> Option<io::Error>;

    /// Adds or updates the value corresponding to the given key in store
    ///
    /// # Errors
    /// - [CorruptedDataError] in case the data on disk is inconsistent with that in memory
    ///
    /// [CorruptedDataError]: crate::errors::CorruptedDataError
    fn set(&self, key: &str, value: &str) -> Option<CorruptedDataError>;

    /// Retrieves the value corresponding to the given key
    ///
    /// # Errors
    /// - [NotFoundError] in case the key is not found in the store
    ///
    /// [NotFoundError]: crate::errors::NotFoundError
    fn get(&self, key: &str) -> Result<String, NotFoundError>;

    /// Removes the key-value pair corresponding to the passed key
    ///
    /// # Errors
    /// - [NotFoundError] in case the key is not found in the store
    ///
    /// [NotFoundError]: crate::errors::NotFoundError
    fn delete(&self, key: &str) -> Option<NotFoundError>;

    /// Resets the entire Store, and clears everything on disk
    ///
    /// # Errors
    /// - [io::Error] I/O errors e.g file permissions, missing files in case the database folder
    /// is not accessible
    ///
    /// [io::Error]: std::io::Error
    fn clear(&self) -> Option<io::Error>;

    /// Deletes all key-value pairs that have been previously marked for 'delete'
    /// when store.Delete(key) was called on them.
    ///
    /// # Errors
    /// - [io::Error] I/O errors e.g file permissions, missing files in case the database folder
    /// is not accessible
    ///
    /// [io::Error]: std::io::Error
    fn vacuum(&self) -> Option<io::Error>;
}

/// `Store` is the actual internal store that saves data both in memory and on disk
/// It implements the [Storage] trait
pub(crate) struct Store {

}

impl Store {
    /// Creates a new instance of Store
    ///
    /// `db_path` is the path to the folder to contain the database files.
    ///
    /// `max_file_size_kb` is the maximum size in kilobytes that the data files can be. Beyond that,
    ///
    /// # Errors
    /// - [io::Error] I/O errors e.g file permissions, missing files in case the `db_path` database folder
    /// is not accessible
    ///
    /// [io::Error]: std::io::Error
    pub(crate) fn new(db_path: &str, max_file_size_kb: f64) -> Result<Store, io::Error> {
        todo!()
    }
}

impl Storage for Store {
    fn load(&self) -> Option<io::Error> {
        todo!()
    }

    fn set(&self, key: &str, value: &str) -> Option<CorruptedDataError> {
        todo!()
    }

    fn get(&self, key: &str) -> Result<String, NotFoundError> {
        todo!()
    }

    fn delete(&self, key: &str) -> Option<NotFoundError> {
        todo!()
    }

    fn clear(&self) -> Option<io::Error> {
        todo!()
    }

    fn vacuum(&self) -> Option<io::Error> {
        todo!()
    }
}