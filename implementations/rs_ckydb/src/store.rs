use crate::cache::Cache;
use crate::constants::{DATA_FILE_EXT, DEL_FILENAME, INDEX_FILENAME, LOG_FILE_EXT};
use crate::errors::{CorruptedDataError, NotFoundError};
use crate::utils;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
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
    /// [CorruptedDataError]: crate::errors::CorruptedDataError
    fn set(&self, key: &str, value: &str) -> Result<(), CorruptedDataError>;

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
    fn delete(&self, key: &str) -> Result<(), NotFoundError>;

    /// Resets the entire Store, and clears everything on disk
    ///
    /// # Errors
    /// - [io::Error] I/O errors e.g file permissions, missing files in case the database folder
    /// is not accessible
    ///
    /// [io::Error]: std::io::Error
    fn clear(&self) -> io::Result<()>;

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
    memtable: HashMap<String, String>,
    index: HashMap<String, String>,
    data_files: Vec<String>,
    current_log_file: String,
    current_log_file_path: PathBuf,
    del_file_path: PathBuf,
    index_file_path: PathBuf,
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

    fn set(&self, key: &str, value: &str) -> Result<(), CorruptedDataError> {
        todo!()
    }

    fn get(&self, key: &str) -> Result<String, NotFoundError> {
        todo!()
    }

    fn delete(&self, key: &str) -> Result<(), NotFoundError> {
        todo!()
    }

    fn clear(&self) -> io::Result<()> {
        todo!()
    }

    fn vacuum(&self) -> io::Result<()> {
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
        }
    }

    /// Creates a new index file if there is no index file in the database folder
    ///
    /// # Errors
    ///
    /// See [utils::create_file_if_not_exist]
    fn create_index_file_if_not_exists(&self) -> io::Result<()> {
        utils::create_file_if_not_exist(&self.index_file_path)
    }

    /// Creates a new del file if there is no del file in the database folder
    ///
    /// # Errors
    ///
    /// See [utils::create_file_if_not_exist]
    fn create_del_file_if_not_exists(&self) -> io::Result<()> {
        utils::create_file_if_not_exist(&self.del_file_path)
    }

    /// Creates a new log file if there is no .log file in the database folder
    ///
    /// # Errors
    ///
    /// See [utils::create_file_if_not_exist] and [Store::create_new_log_file]
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
    fn load_memtable_from_disk(&mut self) -> io::Result<()> {
        let content = fs::read_to_string(&self.current_log_file_path)?;
        self.memtable = utils::extract_key_values_from_str(&content)?;
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
    fn get_keys_to_delete(&self) -> io::Result<Vec<String>> {
        let content = fs::read_to_string(&self.del_file_path)?;
        Ok(utils::extract_tokens_from_str(&content))
    }
}

#[cfg(test)]
mod test {
    use crate::cache::Cache;
    use crate::constants::{DEL_FILENAME, INDEX_FILENAME};
    use crate::store::{Storage, Store};
    use crate::utils;
    use serial_test::serial;
    use std::collections::HashMap;
    use std::ffi::OsString;
    use std::fs;
    use std::path::Path;

    const DB_PATH: &str = "test_store_db";
    const VACUUM_INTERVAL_SEC: f64 = 2.0;
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
        let expected_memtable = HashMap::from(
            [
                ("1655404770518678-goat", "678 months"),
                ("1655404670510698-hen", "567 months"),
                ("1655404770534578-pig", "70 months"),
                ("1655403775538278-fish", "8990 months"),
            ]
            .map(|(k, v)| (k.to_string(), v.to_string())),
        );
        let expected_data_files = DATA_FILES
            .map(|filename| filename.trim_end_matches(".cky").to_string())
            .to_vec();
        let expected_current_log_file = LOG_FILENAME.trim_end_matches(".log").to_string();
        let mut store = Store::new(DB_PATH, MAX_FILE_SIZE_KB);
        let db_path = Path::new(DB_PATH);
        let log_file_path = db_path.join(LOG_FILENAME);
        let index_file_path = db_path.join(INDEX_FILENAME);
        let del_file_path = db_path.join(DEL_FILENAME);

        if let Err(err) = utils::clear_dummy_file_data_in_db(DB_PATH) {
            panic!("error removing dummy data: {}", err)
        }

        if let Err(err) = utils::add_dummy_file_data_in_db(DB_PATH) {
            panic!("error adding dummy data: {}", err)
        }

        if let Err(err) = store.load() {
            panic!("error loading store: {}", err)
        }

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

        if let Err(err) = utils::clear_dummy_file_data_in_db(DB_PATH) {
            panic!("error removing dummy data: {}", err)
        }

        if let Err(err) = store.load() {
            panic!("error loading store: {}", err)
        }

        let current_log_filename = format!("{}.log", store.current_log_file);
        expected_files.push(current_log_filename.clone());
        let expected_log_file_path = OsString::from(Path::new(DB_PATH).join(current_log_filename));
        let mut actual_files: Vec<String> = vec![];

        match utils::get_file_names_in_folder(DB_PATH) {
            Ok(files) => actual_files = files,
            Err(err) => panic!("error getting files in folder: {}", err),
        };

        actual_files.sort();
        expected_files.sort();

        assert_eq!(expected_cache, store.cache);
        assert_ne!("".to_string(), store.current_log_file);
        assert_eq!(empty_map, store.index);
        assert_eq!(empty_map, store.memtable);
        assert_eq!(EMPTY_LIST, store.data_files);
        assert_eq!(expected_files, actual_files);
        assert_eq!(index_file_path, store.index_file_path);
        assert_eq!(expected_log_file_path, store.current_log_file_path);
        assert_eq!(del_file_path, store.del_file_path);
    }

    #[test]
    #[serial]
    fn set_new_key_adds_key_value_to_memtable_and_index_and_log_files() {}

    #[test]
    #[serial]
    fn set_same_recent_key_updates_value_in_memtable_and_log_file() {}

    #[test]
    #[serial]
    fn set_old_key_updates_value_in_cache_and_in_cky_file() {}

    #[test]
    #[serial]
    fn get_new_key_gets_value_from_memtable() {}

    #[test]
    #[serial]
    fn get_old_key_updates_cache_from_disk_and_gets_value_from_cache() {}

    #[test]
    #[serial]
    fn get_old_key_again_gets_value_straight_from_cache() {}

    #[test]
    #[serial]
    fn get_non_existent_key_returns_not_found_error() {}

    #[test]
    #[serial]
    fn delete_key_removes_key_from_index_and_adds_it_to_del_file() {}

    #[test]
    #[serial]
    fn delete_non_existent_key_returns_not_found_error() {}

    #[test]
    #[serial]
    fn clear_deletes_all_data_on_disk_and_resets_memory_props() {}

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
