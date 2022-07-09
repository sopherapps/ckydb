use crate::cache::{Cache, Caching};
use crate::constants::{
    DATA_FILE_EXT, DEL_FILENAME, INDEX_FILENAME, KEY_VALUE_SEPARATOR, LOG_FILE_EXT, TOKEN_SEPARATOR,
};
use crate::errors as ckydb;
use crate::errors::Error::{CorruptedDataError, NotFoundError};
use crate::strings::TokenizedString;
use crate::sync::Lock;
use crate::utils;
use std::collections::HashMap;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fs, io};

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
    fn load(&mut self) -> io::Result<()>;

    /// Adds or updates the value corresponding to the given key in store
    ///
    /// # Errors
    /// - [CorruptedDataError] in case the data on disk is inconsistent with that in memory
    ///
    /// [CorruptedDataError]: crate::errors::Error::CorruptedDataError
    fn set(&mut self, key: &str, value: &str) -> ckydb::Result<()>;

    /// Retrieves the value corresponding to the given key
    ///
    /// # Errors
    /// - [NotFoundError] in case the key is not found in the store
    /// - [CorruptedDataError] in case the data on disk is not
    /// consistent with that in memory
    ///
    /// [NotFoundError]: crate::errors::Error::NotFoundError
    /// [CorruptedDataError]: crate::errors::Error::CorruptedDataError
    fn get(&mut self, key: &str) -> ckydb::Result<String>;

    /// Removes the key-value pair corresponding to the passed key
    ///
    /// # Errors
    /// - [NotFoundError] in case the key is not found in the store
    /// - [CorruptedDataError] in case the data on disk is not
    /// consistent with that in memory
    ///
    /// [NotFoundError]: crate::errors::Error::NotFoundError
    /// [CorruptedDataError]: crate::errors::Error::CorruptedDataError
    fn delete(&mut self, key: &str) -> ckydb::Result<()>;

    /// Resets the entire Store, and clears everything on disk
    ///
    /// # Errors
    /// - [io::Error] I/O errors e.g file permissions, missing files in case the database folder
    /// is not accessible
    ///
    /// [io::Error]: std::io::Error
    fn clear(&mut self) -> io::Result<()>;

    /// Deletes all key-value pairs that have been previously marked for 'delete'
    /// when store.Delete(key) was called on them.
    ///
    /// # Errors
    /// - [io::Error] I/O errors e.g file permissions, missing files in case the database folder
    /// is not accessible
    ///
    /// [io::Error]: std::io::Error
    fn vacuum(&self) -> io::Result<()>;
}

/// `Store` is the actual internal store that saves data both in memory and on disk
/// It implements the [Storage] trait
pub(crate) struct Store {
    db_path: PathBuf,
    max_file_size_kb: f64,
    cache: Cache,
    memtable: TokenizedString,
    index: HashMap<String, String>,
    data_files: Vec<String>,
    current_log_file: String,
    current_log_file_path: PathBuf,
    del_file_path: PathBuf,
    index_file_path: PathBuf,
    cache_lock: Arc<Lock>,
    del_file_lock: Arc<Lock>,
}

impl Storage for Store {
    fn load(&mut self) -> io::Result<()> {
        fs::create_dir_all(self.db_path.clone())?;
        self.create_index_file_if_not_exists()?;
        self.create_del_file_if_not_exists()?;
        self.create_log_file_if_not_exists()?;
        self.vacuum()?;
        self.load_file_props_from_disk()?;
        self.load_index_from_disk()?;
        self.load_memtable_from_disk()
    }

    fn set(&mut self, key: &str, value: &str) -> ckydb::Result<()> {
        let (timestamped_key, is_new_key) = match self.get_timestamped_key(key) {
            Ok((t, i)) => (t, i),
            Err(err) => {
                self.remove_timestamped_key_for_key_if_exists(key).ok();
                let data = Some(format!("{}", err));
                return Err(CorruptedDataError { data });
            }
        };

        if let Err(err) = self.save_key_value_pair(&timestamped_key, value) {
            if is_new_key {
                self.delete_key_value_pair_if_exists(&timestamped_key).ok();
                self.remove_timestamped_key_for_key_if_exists(key).ok();
            } else if let Some(old_value) = err.get_data() {
                self.save_key_value_pair(&timestamped_key, &old_value).ok();
            }

            return Err(err);
        }

        if is_new_key {
            self.index.insert(key.to_string(), timestamped_key);
        }

        Ok(())
    }

    fn get(&mut self, key: &str) -> ckydb::Result<String> {
        let timestamped_key = self.index.get(key).ok_or(NotFoundError {
            key: "".to_string(),
        })?;

        let timestamped_key = timestamped_key.to_string();
        self.get_value_for_key(timestamped_key).or_else(|err| {
            Err(CorruptedDataError {
                data: Some(format!("error getting value: {}", err)),
            })
        })
    }

    // TODO: Deal with the key.to_string() call
    fn delete(&mut self, key: &str) -> ckydb::Result<()> {
        let timestamped_key = self.index.get(key).ok_or(NotFoundError {
            key: key.to_string(),
        })?;

        let lock = Arc::clone(&self.del_file_lock);
        if let Ok(_) = lock.lock() {
            utils::delete_key_values_from_file(&self.index_file_path, &vec![key.to_string()])?;
            let new_file_entry = format!("{}{}", timestamped_key, TOKEN_SEPARATOR);
            utils::append_to_file(&self.del_file_path, &new_file_entry)?;

            self.index.remove(key);

            return Ok(());
        }

        Err(CorruptedDataError {
            data: Some("failed to obtain lock on delete file".to_string()),
        })
    }

    fn clear(&mut self) -> io::Result<()> {
        self.index.clear();
        self.clear_disk()?;
        self.load()
    }

    fn vacuum(&self) -> io::Result<()> {
        let lock = Arc::clone(&self.del_file_lock);
        let res = match lock.lock() {
            Ok(_) => self.unlocked_vacuum(),
            Err(e) => Err(io::Error::new(ErrorKind::Other, e.to_string())),
        };

        res
    }
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
    pub(crate) fn new(db_path: &str, max_file_size_kb: f64) -> Store {
        let db_path = PathBuf::from(Path::new(db_path));
        let del_file_path = db_path.join(DEL_FILENAME);
        let index_file_path = db_path.join(INDEX_FILENAME);

        Store {
            db_path,
            max_file_size_kb,
            cache: Cache::new_empty(),
            memtable: Default::default(),
            index: Default::default(),
            data_files: vec![],
            current_log_file: "".to_string(),
            current_log_file_path: PathBuf::new(),
            del_file_path,
            index_file_path,
            cache_lock: Arc::new(Lock::new(1)),
            del_file_lock: Arc::new(Lock::new(1)),
        }
    }

    /// This does the actual vacuuming of removing keys that have benn marked for deletion
    /// However, it does not employ any lock on the files. As such, calling it directly without
    /// securing the del_file_lock might cause data races
    ///
    /// # Errors
    ///
    /// See [Store::get_keys_to_delete], [crate::utils::get_files_with_extensions],
    /// [crate::utils::delete_key_values_from_file] and [std::fs::write]
    fn unlocked_vacuum(&self) -> io::Result<()> {
        let file_exts_to_vacuum = vec![LOG_FILE_EXT, DATA_FILE_EXT];
        let keys_to_delete = self.get_keys_to_delete()?;

        if keys_to_delete.len() == 0 {
            return Ok(());
        }

        let files_to_vacuum = utils::get_files_with_extensions(&self.db_path, file_exts_to_vacuum)?;

        for filename in files_to_vacuum {
            let path = self.db_path.join(filename);
            utils::delete_key_values_from_file(&path, &keys_to_delete)?;
        }

        // Clear del file
        fs::write(&self.del_file_path, "")?;

        Ok(())
    }

    /// Creates a new index file if there is no index file in the database folder
    ///
    /// # Errors
    ///
    /// See [utils::create_file_if_not_exist]
    // #[inline]
    fn create_index_file_if_not_exists(&self) -> io::Result<()> {
        utils::create_file_if_not_exist(&self.index_file_path)
    }

    /// Creates a new del file if there is no del file in the database folder
    ///
    /// # Errors
    ///
    /// See [utils::create_file_if_not_exist]
    // #[inline]
    fn create_del_file_if_not_exists(&self) -> io::Result<()> {
        utils::create_file_if_not_exist(&self.del_file_path)
    }

    /// Creates a new log file if there is no .log file in the database folder
    ///
    /// # Errors
    ///
    /// See [utils::create_file_if_not_exist] and [Store::create_new_log_file]
    // #[inline]
    fn create_log_file_if_not_exists(&mut self) -> io::Result<()> {
        let extensions = vec![LOG_FILE_EXT];
        let log_files = utils::get_files_with_extensions(&self.db_path, extensions)?;

        if log_files.len() > 0 {
            self.current_log_file_path = self.db_path.join(&log_files[0]);
            return Ok(());
        }

        self.create_new_log_file()
    }

    /// loads the attributes that depend on the things in the folder
    ///
    /// # Errors
    ///
    /// See [crate::utils::get_file_names_in_folder]
    fn load_file_props_from_disk(&mut self) -> io::Result<()> {
        self.data_files.clear();

        let files_in_folder = utils::get_file_names_in_folder(&self.db_path)?;

        for filename in files_in_folder {
            let parts: Vec<&str> = filename.rsplitn(2, ".").collect();
            if parts.len() < 2 {
                continue;
            }

            let ext: &str = parts[0];
            let filename: &str = parts[1];

            if ext == LOG_FILE_EXT {
                self.current_log_file = filename.to_string()
            } else if ext == DATA_FILE_EXT {
                self.data_files.push(filename.to_string())
            }
        }

        self.data_files.sort();

        Ok(())
    }

    /// Loads the index from the index file
    ///
    /// # Error
    ///
    /// See [fs::read_to_string] and [utils::extract_key_values_from_str]
    // #[inline]
    fn load_index_from_disk(&mut self) -> io::Result<()> {
        let content = fs::read_to_string(&self.index_file_path)?;
        self.index = utils::extract_key_values_from_str(&content)?;
        Ok(())
    }

    /// Loads the memtable from the log file
    ///
    /// # Error
    ///
    /// See [fs::read_to_string] and [utils::extract_key_values_from_str]
    // #[inline]
    fn load_memtable_from_disk(&mut self) -> io::Result<()> {
        let content = fs::read_to_string(&self.current_log_file_path)?;
        self.memtable = TokenizedString::from(content);
        Ok(())
    }

    /// Creates a new log file basing on the current timestamp
    ///
    /// # Errors
    ///
    /// See [crate::utils::get_current_timestamp_str] and [utils::create_file_if_not_exist]
    fn create_new_log_file(&mut self) -> io::Result<()> {
        let log_file_name = utils::get_current_timestamp_str()?;
        let log_file_path = self
            .db_path
            .join(format!("{}.{}", log_file_name, LOG_FILE_EXT));

        utils::create_file_if_not_exist(&log_file_path)?;

        // update struct's props
        self.current_log_file = log_file_name;
        self.current_log_file_path = log_file_path;
        Ok(())
    }

    /// Reads the del file and gets the keys to be deleted
    ///
    /// # Errors
    ///
    /// See [fs::read_to_string]
    // #[inline]
    fn get_keys_to_delete(&self) -> io::Result<Vec<String>> {
        let content = fs::read_to_string(&self.del_file_path)?;
        Ok(utils::extract_tokens_from_str(&content))
    }

    /// Gets the timestamped key corresponding to the given key in the index
    /// If there is none, it creates a new timestamped key and adds it to the index file
    /// It returns a tuple of the key and a boolean of whether the key is new or not
    ///
    /// # Errors
    ///
    /// It will return a [CorruptedDataError] if it encounters any issues with creating timestamp
    /// or adding it to the index file
    ///
    /// [CorruptedDataError]: crate::errors::Error::CorruptedDataError
    fn get_timestamped_key(&mut self, key: &str) -> io::Result<(String, bool)> {
        if let Some(k) = self.index.get(key) {
            return Ok((k.to_string(), false));
        }

        let timestamp = utils::get_current_timestamp_str()?;
        let timestamped_key = format!("{}-{}", timestamp, key);
        let new_file_entry = format!(
            "{}{}{}{}",
            key, KEY_VALUE_SEPARATOR, timestamped_key, TOKEN_SEPARATOR
        );

        utils::append_to_file(&self.index_file_path, &new_file_entry)?;

        Ok((timestamped_key, true))
    }

    /// Removes the key and timestamped key from the index
    /// and the index file if it exists
    ///
    /// # Errors
    ///
    /// See [utils::delete_key_values_from_file]
    // #[inline]
    fn remove_timestamped_key_for_key_if_exists(&mut self, key: &str) -> io::Result<()> {
        if let Some(_) = self.index.get(key) {
            self.index.remove(key);
            utils::delete_key_values_from_file(&self.index_file_path, &vec![key.to_string()])?;
        }

        Ok(())
    }

    /// Saves the key value pair in memtable and log file if it is newer than log file
    /// or in cache and in the corresponding dataFile if the key is old
    ///
    /// # Error
    ///
    /// Retruns a [CorruptedDataError] if there is any issue with file IO as data is saved to disk
    ///
    /// [CorruptedDataError]: crate::errors::Error::CorruptedDataError
    fn save_key_value_pair(&mut self, timestamped_key: &str, value: &str) -> ckydb::Result<()> {
        let timestamped_key = timestamped_key.to_owned();
        let value = value.to_owned();

        if timestamped_key >= self.current_log_file {
            // FIXME: This to_string and map should not be done everytime even when there is no error
            let old_value = match self.memtable.get(&timestamped_key) {
                Ok(v) => Some(v.to_owned()),
                Err(_) => None,
            };

            return self
                .save_key_value_pair_to_memtable(&timestamped_key, &value)
                .or(Err(CorruptedDataError { data: old_value }));
        }

        let lock = Arc::clone(&self.cache_lock);
        if let Ok(_) = lock.lock() {
            if !self.cache.is_in_range(&timestamped_key) {
                self.load_cache_containing_key(&timestamped_key)
                    .or(Err(CorruptedDataError { data: None }))?;
            }

            let old_value = self.cache.data.get(&timestamped_key)?;
            let old_value = old_value.to_owned();
            return self
                .save_key_value_pair_to_cache(&timestamped_key, &value)
                .or(Err(CorruptedDataError {
                    data: Some(old_value),
                }));
        }

        Err(CorruptedDataError {
            data: Some("failed to get lock on cache".to_string()),
        })
    }

    /// Deletes the given key and its value from
    /// the index, the cache or the memtable, the log file or any data file
    ///
    /// # Errors
    ///
    /// See [Store::persist_cache_to_disk] and [utils::persist_map_data_to_file]
    // #[inline]
    fn delete_key_value_pair_if_exists(&mut self, key: &str) -> ckydb::Result<()> {
        if self.cache.is_in_range(key) {
            self.cache.remove(key)?;
            return match self.persist_cache_to_disk() {
                Err(err) => Err(ckydb::Error::from(err)),
                Ok(_) => Ok(()),
            };
        }

        if key.to_string() >= self.current_log_file {
            self.memtable.delete(key)?;
            let result = fs::write(&self.current_log_file_path, self.memtable.to_string());
            return match result {
                Err(err) => Err(ckydb::Error::from(err)),
                Ok(_) => Ok(()),
            };
        }

        Ok(())
    }

    /// Saves the key value pair to memtable and persists memtable
    /// to current log file
    ///
    /// # Errors
    ///
    /// See [crate::utils::persist_map_data_to_file] and [Store::roll_log_file_if_too_big]
    // #[inline]
    fn save_key_value_pair_to_memtable(
        &mut self,
        timestamped_key: &str,
        value: &str,
    ) -> io::Result<()> {
        self.memtable
            .insert(timestamped_key, value)
            .or_else(|e| Err(io::Error::new(ErrorKind::Other, e)))?;
        fs::write(&self.current_log_file_path, self.memtable.to_string())?;
        self.roll_log_file_if_too_big()
    }

    /// Saves the key value pair to cache and persists cache
    /// to corresponding data file
    ///
    /// # Errors
    ///
    /// See [Store::persist_cache_to_disk]
    // #[inline]
    fn save_key_value_pair_to_cache(
        &mut self,
        timestamped_key: &str,
        value: &str,
    ) -> io::Result<()> {
        self.cache
            .update(timestamped_key, value)
            .or_else(|e| Err(io::Error::new(ErrorKind::Other, e)))?;
        self.persist_cache_to_disk()
    }

    /// Loads the cache with data containing the timestampedKey
    ///
    /// # Errors
    ///
    /// A [crate::errors::CorruptedDataError] will be returned if the key does not fall in
    /// an of the ranges of timestamps represented by the data file names and the log file name.
    /// Other errors may occur as seen in
    /// [std::fs::read_to_string] and [utils::extract_key_values_from_str]
    // #[inline]
    fn load_cache_containing_key(&mut self, key: &str) -> io::Result<()> {
        let (start, end) = self
            .get_timestamp_range_for_key(key)
            .ok_or(io::Error::new(io::ErrorKind::InvalidData, key.to_string()))?;
        // get data from disk
        let file_path = self.db_path.join(format!("{}.{}", start, DATA_FILE_EXT));
        let content_str = fs::read_to_string(&file_path)?;

        self.cache = Cache::new(content_str, start, end);
        Ok(())
    }

    /// Rolls the current log file if it has exceeded the maximum size it should have
    ///
    /// # Errors
    ///
    /// See [crate::utils::get_file_size], [std::fs::rename] and [Store::create_new_log_file]
    fn roll_log_file_if_too_big(&mut self) -> io::Result<()> {
        let log_file_size = utils::get_file_size(&self.current_log_file_path)?;

        if log_file_size >= self.max_file_size_kb {
            let new_data_filename = format!("{}.{}", self.current_log_file, DATA_FILE_EXT);
            fs::rename(
                &self.current_log_file_path,
                self.db_path.join(&new_data_filename),
            )?;

            self.memtable = Default::default();
            self.data_files.push(self.current_log_file.clone());
            // endure the data files are sorted
            self.data_files.sort();
            self.create_new_log_file()?;
        }

        Ok(())
    }

    /// Persists the current cache to its corresponding data file
    ///
    /// # Errors
    ///
    /// See [crate::utils::persist_map_data_to_file]
    // #[inline]
    fn persist_cache_to_disk(&self) -> io::Result<()> {
        let data_file_path = self
            .db_path
            .join(format!("{}.{}", self.cache.start, DATA_FILE_EXT));
        fs::write(&data_file_path, &self.cache.data.to_string())
    }

    /// Returns the range of timestamps between which
    /// the key lies. The timestamps are got from the names of the data files and the current log file
    /// It will return None if there is no relevant timestamp range from the available data file names
    /// and log file names
    // #[inline]
    fn get_timestamp_range_for_key(&self, key: &str) -> Option<(String, String)> {
        let mut timestamps = self.data_files.clone();
        timestamps.push(self.current_log_file.clone());
        let key_as_string = key.to_string();

        for i in 1..timestamps.len() {
            let current = &timestamps[i];
            if *current > key_as_string {
                return Some((timestamps[i - 1].clone(), current.clone()));
            }
        }

        None
    }

    /// Gets the value corresponding to a given timestampedKey
    ///
    /// # Errors
    ///
    /// It will return [crate::errors::CorruptedDataError] if the data on disk is inconsistent
    /// with what is expected in memory e.g. if unable to load cache from disk, or cache or memtable
    /// don't contain the key yet they should contain it.
    ///
    /// Obviously [crate::errors::CorruptedDataError] has a very minute chance of happening
    // #[inline]
    fn get_value_for_key(&mut self, timestamped_key: String) -> ckydb::Result<String> {
        if timestamped_key >= self.current_log_file {
            let value = self.memtable.get(&timestamped_key)?;
            return Ok(value.to_string());
        }

        let lock = Arc::clone(&self.cache_lock);

        if let Ok(_) = lock.lock() {
            if !self.cache.is_in_range(&timestamped_key) {
                self.load_cache_containing_key(&timestamped_key)?;
            }

            let value = self.cache.get(&timestamped_key)?;

            return Ok(value.to_string());
        }

        Err(CorruptedDataError {
            data: Some("failed to get lock to cache".to_string()),
        })
    }

    /// Deletes all files in the database folder
    ///
    /// # Errors
    ///
    /// See [fs::remove_dir_all]
    // #[inline]
    fn clear_disk(&self) -> io::Result<()> {
        fs::remove_dir_all(&self.db_path)
    }
}

#[cfg(test)]
mod test {
    use crate::cache::{Cache, Caching};
    use crate::constants::{DEL_FILENAME, INDEX_FILENAME, KEY_VALUE_SEPARATOR, TOKEN_SEPARATOR};
    use crate::store::{Storage, Store};
    use crate::strings::TokenizedString;
    use crate::utils;
    use serial_test::serial;
    use std::collections::HashMap;
    use std::ffi::OsString;
    use std::fs;
    use std::path::Path;

    const DB_PATH: &str = "test_store_db";
    const MAX_FILE_SIZE_KB: f64 = 320.0 / 1024.0;
    const LOG_FILENAME: &str = "1655375171402014000.log";
    const DATA_FILES: [&str; 2] = ["1655375120328185000.cky", "1655375120328186000.cky"];
    const EMPTY_LIST: Vec<String> = vec![];

    #[test]
    #[serial]
    fn load_updates_memory_props_from_data_on_disk() {
        let expected_cache = Cache::new_empty();
        let expected_index = HashMap::from(
            [
                ("cow", "1655375120328185000-cow"),
                ("dog", "1655375120328185100-dog"),
                ("goat", "1655404770518678-goat"),
                ("hen", "1655404670510698-hen"),
                ("pig", "1655404770534578-pig"),
                ("fish", "1655403775538278-fish"),
            ]
            .map(|(k, v)| (k.to_string(), v.to_string())),
        );
        let expected_memtable = TokenizedString::from(String::from("1655404770518678-goat><?&(^#678 months$%#@*&^&1655404670510698-hen><?&(^#567 months$%#@*&^&1655404770534578-pig><?&(^#70 months$%#@*&^&1655403775538278-fish><?&(^#8990 months$%#@*&^&"));
        let expected_data_files = DATA_FILES
            .map(|filename| filename.trim_end_matches(".cky").to_string())
            .to_vec();
        let expected_current_log_file = LOG_FILENAME.trim_end_matches(".log").to_string();
        let mut store = Store::new(DB_PATH, MAX_FILE_SIZE_KB);
        let db_path = Path::new(DB_PATH);
        let log_file_path = db_path.join(LOG_FILENAME);
        let index_file_path = db_path.join(INDEX_FILENAME);
        let del_file_path = db_path.join(DEL_FILENAME);

        utils::clear_dummy_file_data_in_db(DB_PATH).expect("clears dummy data in db");
        utils::add_dummy_file_data_in_db(DB_PATH).expect("adds dummy data to db");
        store.load().expect("loads store");

        assert_eq!(expected_cache, store.cache);
        assert_eq!(expected_memtable, store.memtable);
        assert_eq!(expected_index, store.index);
        assert_eq!(expected_data_files, store.data_files);
        assert_eq!(expected_current_log_file, store.current_log_file);
        assert_eq!(log_file_path, store.current_log_file_path);
        assert_eq!(index_file_path, store.index_file_path);
        assert_eq!(del_file_path, store.del_file_path);
    }

    #[test]
    #[serial]
    fn load_creates_db_folder_with_del_and_index_files_if_not_exist() {
        let expected_cache = Cache::new_empty();
        let mut expected_files = [DEL_FILENAME, INDEX_FILENAME].map(String::from).to_vec();
        let mut store = Store::new(DB_PATH, MAX_FILE_SIZE_KB);
        let db_path = Path::new(DB_PATH);
        let index_file_path = db_path.join(INDEX_FILENAME);
        let del_file_path = db_path.join(DEL_FILENAME);
        let empty_map: HashMap<String, String> = Default::default();
        let empty_tokenized_string: TokenizedString = Default::default();

        utils::clear_dummy_file_data_in_db(DB_PATH).expect("clears dummy data in db");
        store.load().expect("loads store");

        let current_log_filename = format!("{}.log", store.current_log_file);
        expected_files.push(current_log_filename.clone());
        let expected_log_file_path = OsString::from(Path::new(DB_PATH).join(current_log_filename));
        let mut actual_files =
            utils::get_file_names_in_folder(DB_PATH).expect("get files in db folder");

        actual_files.sort();
        expected_files.sort();

        assert_eq!(expected_cache, store.cache);
        assert_ne!("".to_string(), store.current_log_file);
        assert_eq!(empty_map, store.index);
        assert_eq!(empty_tokenized_string, store.memtable);
        assert_eq!(EMPTY_LIST, store.data_files);
        assert_eq!(expected_files, actual_files);
        assert_eq!(index_file_path, store.index_file_path);
        assert_eq!(expected_log_file_path, store.current_log_file_path);
        assert_eq!(del_file_path, store.del_file_path);
    }

    #[test]
    #[serial]
    fn set_new_key_adds_key_value_to_memtable_and_index_and_log_files() {
        let (key, value) = ("New key", "foo");
        let mut store = Store::new(DB_PATH, MAX_FILE_SIZE_KB);
        let db_path = Path::new(DB_PATH);
        let index_file_path = db_path.join(INDEX_FILENAME);
        let log_file_path = db_path.join(LOG_FILENAME);

        utils::clear_dummy_file_data_in_db(DB_PATH).expect("clears dummy data in db");
        utils::add_dummy_file_data_in_db(DB_PATH).expect("adds dummy data to db");
        store.load().expect("loads store");
        store
            .set(key, value)
            .expect(&format!("set key: {}, value: {}", key, value));

        // expected
        let timestamped_key = store.index.get(key).unwrap();
        let expected_index_file_entry = format!(
            "{}{}{}{}",
            key, KEY_VALUE_SEPARATOR, timestamped_key, TOKEN_SEPARATOR
        );
        let expected_log_file_entry = format!(
            "{}{}{}{}",
            timestamped_key, KEY_VALUE_SEPARATOR, value, TOKEN_SEPARATOR
        );

        // actual
        let value_in_memtable = store.memtable.get(timestamped_key).unwrap();
        let index_file_content = fs::read_to_string(index_file_path).expect("read index file");
        let log_file_content = fs::read_to_string(log_file_path).expect("read log file");

        assert_eq!(value, value_in_memtable);
        assert!(index_file_content.contains(&expected_index_file_entry));
        assert!(log_file_content.contains(&expected_log_file_entry));
    }

    #[test]
    #[serial]
    fn set_same_recent_key_updates_value_in_memtable_and_log_file() {
        let (key, value, new_value) = ("New key", "foo", "hello-world");
        let mut store = Store::new(DB_PATH, MAX_FILE_SIZE_KB);
        let db_path = Path::new(DB_PATH);
        let index_file_path = db_path.join(INDEX_FILENAME);
        let log_file_path = db_path.join(LOG_FILENAME);

        utils::clear_dummy_file_data_in_db(DB_PATH).expect("clears dummy data in db");
        utils::add_dummy_file_data_in_db(DB_PATH).expect("adds dummy data to db");
        store.load().expect("loads store");
        store
            .set(key, value)
            .expect(&format!("set key: {}, value: {}", key, value));
        store
            .set(key, new_value)
            .expect(&format!("set key: {}, value: {}", key, new_value));

        // expected
        let timestamped_key = store.index.get(key).unwrap();
        let expected_index_file_entry = format!(
            "{}{}{}{}",
            key, KEY_VALUE_SEPARATOR, timestamped_key, TOKEN_SEPARATOR
        );
        let expected_log_file_entry = format!(
            "{}{}{}{}",
            timestamped_key, KEY_VALUE_SEPARATOR, new_value, TOKEN_SEPARATOR
        );

        // actual
        let value_in_memtable = store.memtable.get(timestamped_key).unwrap();
        let index_file_content = fs::read_to_string(index_file_path).expect("read index file");
        let log_file_content = fs::read_to_string(log_file_path).expect("read log file");

        assert_eq!(new_value, value_in_memtable);
        assert!(index_file_content.contains(&expected_index_file_entry));
        assert!(log_file_content.contains(&expected_log_file_entry));
    }

    #[test]
    #[serial]
    fn set_old_key_updates_value_in_cache_and_in_cky_file() {
        let (key, value) = ("cow", "foo-again");
        let db_path = Path::new(DB_PATH);
        let data_file_path = db_path.join(DATA_FILES[0]);
        let mut store = Store::new(DB_PATH, MAX_FILE_SIZE_KB);

        utils::clear_dummy_file_data_in_db(DB_PATH).expect("clears dummy data in db");
        utils::add_dummy_file_data_in_db(DB_PATH).expect("adds dummy data to db");
        store.load().expect("loads store");
        store
            .set(key, value)
            .expect(&format!("set key: {}, value: {}", key, value));

        // expected
        let timestamped_key = store.index.get(key).unwrap();
        let expected_data_file_entry =
            format!("{}{}{}", timestamped_key, KEY_VALUE_SEPARATOR, value);

        // actual
        let value_in_cache = store.cache.get(timestamped_key).unwrap();
        let data_file_content = fs::read_to_string(data_file_path).expect("read data file");

        assert_eq!(value, value_in_cache);
        assert!(data_file_content.contains(&expected_data_file_entry));
    }

    #[test]
    #[serial]
    fn get_new_key_gets_value_from_memtable() {
        let (key, expected_value) = ("fish", "8990 months");
        let mut store = Store::new(DB_PATH, MAX_FILE_SIZE_KB);

        utils::clear_dummy_file_data_in_db(DB_PATH).expect("clears dummy data in db");
        utils::add_dummy_file_data_in_db(DB_PATH).expect("adds dummy data to db");
        store.load().expect("loads store");

        // remove the database files to show data is got straight from memory
        utils::clear_dummy_file_data_in_db(DB_PATH).expect("clears dummy data in db");

        let actual_value = store.get(key).unwrap();
        assert_eq!(actual_value, expected_value);
    }

    #[test]
    #[serial]
    fn get_old_key_updates_cache_from_disk_and_gets_value_from_cache() {
        let (key, expected_value) = ("cow", "500 months");
        let expected_initial_cache = Cache::new_empty();
        let expected_final_cache = Cache::new(
            String::from("1655375120328185000-cow><?&(^#500 months$%#@*&^&1655375120328185100-dog><?&(^#23 months$%#@*&^&"),
            DATA_FILES[0].trim_end_matches(".cky").to_string(),
            DATA_FILES[1].trim_end_matches(".cky").to_string(),
        );

        let mut store = Store::new(DB_PATH, MAX_FILE_SIZE_KB);

        utils::clear_dummy_file_data_in_db(DB_PATH).expect("clears dummy data in db");
        utils::add_dummy_file_data_in_db(DB_PATH).expect("adds dummy data to db");
        store.load().expect("loads store");

        let initial_cache = store.cache.clone();
        let value = store.get(key).unwrap();
        let final_cache = store.cache.clone();

        assert_eq!(expected_value, value);
        assert_eq!(expected_initial_cache, initial_cache);
        assert_eq!(expected_final_cache, final_cache);
    }

    #[test]
    #[serial]
    fn get_old_key_again_gets_value_straight_from_cache() {
        let (key, expected_value) = ("cow", "500 months");
        let mut store = Store::new(DB_PATH, MAX_FILE_SIZE_KB);

        utils::clear_dummy_file_data_in_db(DB_PATH).expect("clears dummy data in db");
        utils::add_dummy_file_data_in_db(DB_PATH).expect("adds dummy data to db");
        store.load().expect("loads store");

        let _ = store.get(key).unwrap();

        // remove the database files to show data is got straight from memory on next get
        utils::clear_dummy_file_data_in_db(DB_PATH).expect("clears dummy data in db");

        let value = store.get(key).unwrap();

        assert_eq!(expected_value, value);
    }

    #[test]
    #[serial]
    fn get_non_existent_key_returns_not_found_error() {
        let key = "non-existent";
        let mut store = Store::new(DB_PATH, MAX_FILE_SIZE_KB);

        utils::clear_dummy_file_data_in_db(DB_PATH).expect("clears dummy data in db");
        store.load().expect("loads store");

        match store.get(key) {
            Ok(_) => panic!("error was expected"),
            Err(err) => assert!(err.to_string().contains("not found")),
        }
    }

    #[test]
    #[serial]
    fn delete_key_removes_key_from_index_and_adds_it_to_del_file() {
        let key = "pig";
        let expected_index = HashMap::from([
            (String::from("cow"), String::from("1655375120328185000-cow")),
            (String::from("dog"), String::from("1655375120328185100-dog")),
            (String::from("goat"), String::from("1655404770518678-goat")),
            (String::from("hen"), String::from("1655404670510698-hen")),
            (String::from("fish"), String::from("1655403775538278-fish")),
        ]);
        let expected_keys_marked_for_delete = vec!["1655404770534578-pig"];
        let mut store = Store::new(DB_PATH, MAX_FILE_SIZE_KB);
        let db_path = Path::new(DB_PATH);
        let index_file_path = db_path.join(INDEX_FILENAME);
        let del_file_path = db_path.join(DEL_FILENAME);

        utils::clear_dummy_file_data_in_db(DB_PATH).expect("clears dummy data in db");
        utils::add_dummy_file_data_in_db(DB_PATH).expect("adds dummy data in db");
        store.load().expect("loads store");
        store.delete(key).expect(&format!("delete {}", key));

        let idx_file_content = fs::read_to_string(index_file_path).expect("read index file");
        let del_file_content = fs::read_to_string(del_file_path).expect("read del file");
        let map_from_idx_file = utils::extract_key_values_from_str(&idx_file_content)
            .expect("extract key values from index");
        let list_from_del_file = utils::extract_tokens_from_str(&del_file_content);

        match store.get(key) {
            Ok(_) => panic!("error was expected"),
            Err(err) => assert!(err.to_string().contains("not found")),
        }

        assert_eq!(expected_index, map_from_idx_file);
        assert_eq!(expected_keys_marked_for_delete, list_from_del_file);
        assert_eq!(expected_index, store.index);
    }

    #[test]
    #[serial]
    fn delete_non_existent_key_returns_not_found_error() {
        let key = "non-existent";
        let mut store = Store::new(DB_PATH, MAX_FILE_SIZE_KB);

        utils::clear_dummy_file_data_in_db(DB_PATH).expect("clears dummy data in db");
        store.load().expect("loads store");

        match store.delete(key) {
            Ok(_) => panic!("error was expected"),
            Err(err) => assert!(err.to_string().contains("not found")),
        }
    }

    #[test]
    #[serial]
    fn clear_deletes_all_data_on_disk_and_resets_memory_props() {
        let expected_cache = Cache::new_empty();
        let mut expected_files = vec![DEL_FILENAME.to_string(), INDEX_FILENAME.to_string()];
        let mut store = Store::new(DB_PATH, MAX_FILE_SIZE_KB);
        let db_path = Path::new(DB_PATH);
        let index_file_path = db_path.join(INDEX_FILENAME);
        let del_file_path = db_path.join(DEL_FILENAME);
        let empty_map: HashMap<String, String> = Default::default();
        let empty_list: Vec<String> = Default::default();
        let empty_tokenized_string: TokenizedString = Default::default();

        utils::clear_dummy_file_data_in_db(DB_PATH).expect("clears dummy data in db");
        utils::add_dummy_file_data_in_db(DB_PATH).expect("adds dummy data in db");
        store.load().expect("loads store");
        store.clear().expect("clear");

        let current_log_filename = format!("{}.log", store.current_log_file);
        let expected_current_log_file_path = db_path.join(&current_log_filename);
        expected_files.push(current_log_filename);
        let mut actual_files =
            utils::get_file_names_in_folder(db_path).expect("get files in db folder");
        expected_files.sort();
        actual_files.sort();

        assert_eq!(expected_cache, store.cache);
        assert_ne!("".to_string(), store.current_log_file);
        assert_eq!(empty_map, store.index);
        assert_eq!(empty_tokenized_string, store.memtable);
        assert_eq!(empty_list, store.data_files);
        assert_eq!(expected_files, actual_files);
        assert_eq!(index_file_path, store.index_file_path);
        assert_eq!(expected_current_log_file_path, store.current_log_file_path);
        assert_eq!(del_file_path, store.del_file_path);
    }

    #[test]
    #[serial]
    fn vacuum_removes_keys_and_values_listed_in_del_file_from_log_and_cky_files() {
        let expected_log_file_content = String::from("1655404770518678-goat><?&(^#678 months$%#@*&^&1655404670510698-hen><?&(^#567 months$%#@*&^&1655404770534578-pig><?&(^#70 months$%#@*&^&1655403775538278-fish><?&(^#8990 months$%#@*&^&");
        let expected_data_contents = vec![
            "1655375120328185000-cow><?&(^#500 months$%#@*&^&1655375120328185100-dog><?&(^#23 months$%#@*&^&".to_string(), "".to_string(),
        ];
        let expected_del_file_content = "".to_string();
        let db_path = Path::new(DB_PATH);
        let data_file_paths = DATA_FILES.map(|f| db_path.join(f));
        let log_file_path = db_path.join(LOG_FILENAME);
        let del_file_path = db_path.join(DEL_FILENAME);
        let store = Store::new(DB_PATH, MAX_FILE_SIZE_KB);

        if let Err(err) = utils::clear_dummy_file_data_in_db(DB_PATH) {
            panic!("error clearing dummy data: {}", err);
        }

        if let Err(err) = utils::add_dummy_file_data_in_db(DB_PATH) {
            panic!("error adding dummy data: {}", err);
        }

        if let Err(err) = store.vacuum() {
            panic!("error vacuuming: {}", err);
        }

        let data_file_content =
            data_file_paths.map(|path| fs::read_to_string(path).expect("read data file"));
        let log_file_content = fs::read_to_string(log_file_path).expect("read log file");
        let del_file_content = fs::read_to_string(del_file_path).expect("read log file");

        assert_eq!(expected_log_file_content, log_file_content);
        assert_eq!(expected_del_file_content, del_file_content);
        assert_eq!(expected_data_contents, data_file_content);
    }

    #[test]
    #[serial]
    fn vacuum_does_nothing_if_del_file_is_empty() {
        let expected_log_file_content = String::from("1655404770518678-goat><?&(^#678 months$%#@*&^&1655404670510698-hen><?&(^#567 months$%#@*&^&1655404770534578-pig><?&(^#70 months$%#@*&^&1655403775538278-fish><?&(^#8990 months$%#@*&^&1655403795838278-foo><?&(^#890 months$%#@*&^&");
        let expected_data_contents = vec![
            "1655375120328185000-cow><?&(^#500 months$%#@*&^&1655375120328185100-dog><?&(^#23 months$%#@*&^&".to_string(), "1655375171402014000-bar><?&(^#foo$%#@*&^&".to_string(),
        ];
        let expected_del_file_content = "".to_string();
        let db_path = Path::new(DB_PATH);
        let data_file_paths = DATA_FILES.map(|f| db_path.join(f));
        let log_file_path = db_path.join(LOG_FILENAME);
        let del_file_path = db_path.join(DEL_FILENAME);
        let store = Store::new(DB_PATH, MAX_FILE_SIZE_KB);

        if let Err(err) = utils::clear_dummy_file_data_in_db(DB_PATH) {
            panic!("error clearing dummy data: {}", err);
        }

        if let Err(err) = utils::add_dummy_file_data_in_db(DB_PATH) {
            panic!("error adding dummy data: {}", err);
        }

        // clear delete file
        fs::write(&del_file_path, "").expect("clear delete file");

        if let Err(err) = store.vacuum() {
            panic!("error vacuuming: {}", err);
        }

        let data_file_content =
            data_file_paths.map(|path| fs::read_to_string(path).expect("read data file"));
        let log_file_content = fs::read_to_string(log_file_path).expect("read log file");
        let del_file_content = fs::read_to_string(del_file_path).expect("read log file");

        assert_eq!(expected_log_file_content, log_file_content);
        assert_eq!(expected_del_file_content, del_file_content);
        assert_eq!(expected_data_contents, data_file_content);
    }
}
