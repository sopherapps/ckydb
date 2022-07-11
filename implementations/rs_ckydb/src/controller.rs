use crate::errors as ckydb;
use crate::store::{Storage, Store};
use std::io;

/// `Controller` trait represents the basic expectation for the public API for the database
///
/// It must be able to do the basic [set], [get], [delete], and [clear] operations for
/// accessing and manipulating data in the database.
///
/// It must also be able to [open] the connection to the underlying database, so as to start
/// some house-cleaning background tasks. It should also [close] the connection, and stop
/// the background tasks
///
/// [set]: Controller::set
/// [get]: Controller::get
/// [delete]: Controller::delete
/// [clear]: Controller::clear
/// [open]: Controller::open
/// [close]: Controller::close
pub trait Controller {
    /// Loads the store and starts the background tasks
    ///
    /// # Errors
    /// - [io::Error] I/O errors e.g file permissions, missing files in case the database folder
    /// is not accessible
    ///
    /// [io::Error]: std::io::Error
    fn open(&mut self) -> io::Result<()>;

    /// Stops all background tasks
    ///
    /// # Errors
    /// - [io::Error] I/O errors e.g file permissions, missing files in case the database folder
    /// is not accessible
    ///
    /// [io::Error]: std::io::Error
    fn close(&mut self) -> io::Result<()>;

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
    /// - [CorruptedDataError] in case the data on disk is inconsistent with that in memory
    ///
    /// [NotFoundError]: crate::errors::Error::NotFoundError
    /// [CorruptedDataError]: crate::errors::Error::CorruptedDataError
    fn get(&mut self, key: &str) -> ckydb::Result<String>;

    /// Removes the key-value pair corresponding to the passed key
    ///
    /// # Errors
    /// - [NotFoundError] in case the key is not found in the store
    /// - [CorruptedDataError] in case the data on disk is inconsistent with that in memory
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
}

/// `Ckydb` is the public API for the database.
/// It implements the [Controller] trait as well as the [Drop] trait
pub struct Ckydb {
    store: Store,
    is_open: bool,
}

impl Ckydb {
    /// Creates a new instance of Ckydb, loading the internal store
    ///
    /// # Errors
    /// - [io::Error] I/O errors e.g file permissions, missing files in case the `db_path` database folder
    /// is not accessible
    ///
    /// [io::Error]: std::io::Error
    fn new(db_path: &str, max_file_size_kb: f64, _vacuum_interval_sec: f64) -> io::Result<Ckydb> {
        let mut store = Store::new(db_path, max_file_size_kb);

        store.load().and(Ok(Ckydb {
            store,
            is_open: false,
        }))
    }
}

impl Controller for Ckydb {
    fn open(&mut self) -> io::Result<()> {
        if self.is_open {
            return Ok(());
        }

        self.is_open = true;

        Ok(())
    }

    fn close(&mut self) -> io::Result<()> {
        if !self.is_open {
            return Ok(());
        }

        self.is_open = false;
        Ok(())
    }

    fn set(&mut self, key: &str, value: &str) -> ckydb::Result<()> {
        self.store.set(key, value)
    }

    fn get(&mut self, key: &str) -> ckydb::Result<String> {
        self.store.get(key)
    }

    fn delete(&mut self, key: &str) -> ckydb::Result<()> {
        self.store.delete(key)
    }

    fn clear(&mut self) -> io::Result<()> {
        self.store.clear()
    }
}

impl Drop for Ckydb {
    fn drop(&mut self) {
        self.close().unwrap_or(());
    }
}

/// Connects to the Ckydb instance, initializing it with its background tasks and returns it.
/// `max_file_size_kb` is the maximum file size permitted for the database files. Make sure it fits in RAM.
/// `vacuum_interval_sec` is the time between [vacuuming] cycles for the database.
///
/// # Errors
/// - [io::Error] I/O errors e.g file permissions, missing files in case the database folder
/// is not accessible
///
/// [io::Error]: std::io::Error
/// [vacuuming]: crate::store::Storage::vacuum
pub fn connect(
    db_path: &str,
    max_file_size_kb: f64,
    vacuum_interval_sec: f64,
) -> io::Result<Ckydb> {
    let mut db = Ckydb::new(db_path, max_file_size_kb, vacuum_interval_sec)?;
    db.open().and(Ok(db))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{constants, utils};
    use serial_test::serial;
    use std::collections::HashMap;

    const DB_PATH: &str = "test_controller_db";
    const VACUUM_INTERVAL_SEC: f64 = 2.0;
    const MAX_FILE_SIZE_KB: f64 = 320.0 / 1024.0;
    const TEST_RECORDS: [(&str, &str); 7] = [
        ("hey", "English"),
        ("hi", "English"),
        ("salut", "French"),
        ("bonjour", "French"),
        ("hola", "Spanish"),
        ("oi", "Portuguese"),
        ("mulimuta", "Runyoro"),
    ];

    #[test]
    #[serial]
    fn connect_should_call_open() {
        let db = connect_to_test_db(DB_PATH, MAX_FILE_SIZE_KB, VACUUM_INTERVAL_SEC)
            .unwrap_or_else(|err| panic!("{}", err));
        assert!(db.is_open);
    }

    #[test]
    #[serial]
    fn close_should_stop_all_tasks() {
        let mut db = connect_to_test_db(DB_PATH, MAX_FILE_SIZE_KB, VACUUM_INTERVAL_SEC).unwrap();
        db.close().expect("closing db");
        assert!(!db.is_open);
    }

    #[test]
    #[serial]
    fn set_new_key_should_add_key_value_to_store() {
        let mut db =
            connect_to_test_db(DB_PATH, MAX_FILE_SIZE_KB * 2.5, VACUUM_INTERVAL_SEC).unwrap();

        for (k, v) in &TEST_RECORDS {
            if let Err(err) = db.set(*k, *v) {
                panic!("error setting items: {}", err);
            };
        }

        for (k, v) in &TEST_RECORDS {
            match db.get(*k) {
                Ok(value) => assert_eq!(value, (*v).to_string()),
                Err(err) => panic!("error getting items: {}", err),
            }
        }
    }

    #[test]
    #[serial]
    fn set_old_key_should_update_old_key_value() {
        let mut old_records = HashMap::from(TEST_RECORDS);

        let updates = HashMap::from([
            ("hey", "Jane"),
            ("hi", "John"),
            ("salut", "Jean"),
            ("oi", "Ronaldo"),
            ("mulimuta", "Aliguma"),
        ]);

        let mut db = connect_to_test_db(DB_PATH, MAX_FILE_SIZE_KB, VACUUM_INTERVAL_SEC).unwrap();

        for (k, v) in &old_records {
            if let Err(err) = db.set(*k, *v) {
                panic!("error setting items: {}", err);
            };
        }

        for (k, v) in &updates {
            match db.set(*k, *v) {
                Ok(_) => {
                    old_records.remove(k);
                }
                Err(err) => panic!("error setting items: {}", err),
            };
        }

        for (k, v) in &updates {
            match db.get(*k) {
                Ok(value) => assert_eq!(*v, value),
                Err(err) => panic!("error getting items: {}", err),
            };
        }

        for (k, v) in &old_records {
            match db.get(*k) {
                Ok(value) => assert_eq!(*v, value),
                Err(err) => panic!("error getting items: {}", err),
            };
        }
    }

    #[test]
    #[serial]
    fn get_old_key_should_return_value_for_key_in_store() {
        let (key, value) = ("cow", "500 months");
        utils::clear_dummy_file_data_in_db(DB_PATH).expect("clear dummy data");
        utils::add_dummy_file_data_in_db(DB_PATH).expect("add dummy data");
        let mut db = connect(DB_PATH, MAX_FILE_SIZE_KB, VACUUM_INTERVAL_SEC).expect("connect");

        match db.get(key) {
            Ok(v) => assert_eq!(value.to_string(), v),
            Err(err) => panic!("error getting items: {}", err),
        }
    }

    #[test]
    #[serial]
    fn get_old_key_again_should_get_value_from_memory_cache() {
        let (key, value) = ("cow", "500 months");

        utils::clear_dummy_file_data_in_db(DB_PATH).expect("clear dummy data");
        utils::add_dummy_file_data_in_db(DB_PATH).expect("add dummy data");
        let mut db = connect(DB_PATH, MAX_FILE_SIZE_KB, VACUUM_INTERVAL_SEC).expect("connect");

        if let Err(err) = db.get(key) {
            panic!("error getting items: {}", err);
        }

        // remove the files to ensure data is got from memory only
        if let Err(err) = utils::clear_dummy_file_data_in_db(DB_PATH) {
            panic!("error deleting files: {}", err)
        }

        match db.get(key) {
            Ok(v) => assert_eq!(value.to_string(), v),
            Err(err) => panic!("error getting items: {}", err),
        }
    }

    #[test]
    #[serial]
    fn get_newly_inserted_key_should_get_from_memory_memtable() {
        let (key, value) = ("hello", "world");

        let mut db = connect_to_test_db(DB_PATH, MAX_FILE_SIZE_KB, VACUUM_INTERVAL_SEC).unwrap();

        if let Err(err) = db.set(key, value) {
            panic!("error getting items: {}", err);
        }

        // remove the files to ensure data is got from memory only
        if let Err(err) = utils::clear_dummy_file_data_in_db(DB_PATH) {
            panic!("error deleting files: {}", err)
        }

        match db.get(key) {
            Ok(v) => assert_eq!(value.to_string(), v),
            Err(err) => panic!("error getting items: {}", err),
        }
    }

    #[test]
    #[serial]
    fn delete_should_remove_key_value_from_store() {
        let mut old_records = HashMap::from(TEST_RECORDS);
        let keys_to_delete = ["hey", "salut"];

        let mut db = connect_to_test_db(DB_PATH, MAX_FILE_SIZE_KB, VACUUM_INTERVAL_SEC).unwrap();

        for (k, v) in &old_records {
            if let Err(err) = db.set(*k, *v) {
                panic!("error setting items: {}", err);
            };
        }

        for k in &keys_to_delete {
            match db.delete(*k) {
                Ok(_) => {
                    old_records.remove(*k);
                }
                Err(err) => panic!("error deleting items: {}", err),
            }
        }

        for (k, v) in &old_records {
            match db.get(*k) {
                Ok(value) => assert_eq!(*v, value),
                Err(err) => panic!("error getting items: {}", err),
            };
        }

        for k in &keys_to_delete {
            match db.get(*k) {
                Ok(_) => panic!("key: {} unexpected", k),
                Err(err) => assert!(err.to_string().contains("not found")),
            }
        }
    }

    #[test]
    #[serial]
    fn clear_should_remove_all_key_values_from_store() {
        let mut db = connect_to_test_db(DB_PATH, MAX_FILE_SIZE_KB, VACUUM_INTERVAL_SEC).unwrap();

        for (k, v) in &TEST_RECORDS {
            if let Err(err) = db.set(*k, *v) {
                panic!("error setting items: {}", err);
            };
        }

        if let Err(err) = db.clear() {
            panic!("error clearing db: {}", err)
        }

        for (k, _) in &TEST_RECORDS {
            match db.get(*k) {
                Ok(_) => panic!("key: {} unexpected", k),
                Err(err) => assert!(err.to_string().contains("not found")),
            }
        }
    }

    #[test]
    #[serial]
    fn vacuum_task_should_run_at_defined_interval() {
        let mut db =
            connect_to_test_db(DB_PATH, MAX_FILE_SIZE_KB * 2.5, VACUUM_INTERVAL_SEC).unwrap();

        for (k, v) in &TEST_RECORDS {
            db.set(*k, *v).expect("set key");
        }

        for i in 0..TEST_RECORDS.len() {
            let (k, _) = TEST_RECORDS[i];
            db.delete(k).expect("delete key");

            let idx_file_contents_post_vacuum =
                utils::read_files_with_extension(DB_PATH, "idx").unwrap();
            let del_file_contents_post_vacuum =
                utils::read_files_with_extension(DB_PATH, "del").unwrap();
            let log_file_contents_post_vacuum =
                utils::read_files_with_extension(DB_PATH, "log").unwrap();

            if i != 5 {
                assert!(!idx_file_contents_post_vacuum[0].contains(k));
                assert!(del_file_contents_post_vacuum[0].contains(k));
                assert!(log_file_contents_post_vacuum[0].contains(k));
            } else {
                assert!(!idx_file_contents_post_vacuum[0].contains(k));
                assert!(!del_file_contents_post_vacuum[0].contains(k));
                assert!(!log_file_contents_post_vacuum[0].contains(k));
            }
        }
    }

    #[test]
    #[serial]
    fn log_file_should_be_turned_to_cky_file_when_it_exceeds_max_size() {
        let mut pre_roll_data: Vec<HashMap<String, String>> = Vec::with_capacity(3);
        let post_roll_data = HashMap::from([("hey", "English"), ("hi", "English")]);

        if let Err(err) = utils::clear_dummy_file_data_in_db(DB_PATH) {
            panic!("error clearing test db disk data: {}", err)
        }

        let mut db = connect(DB_PATH, MAX_FILE_SIZE_KB, VACUUM_INTERVAL_SEC).unwrap();

        for i in 0..3 {
            let mut data: HashMap<String, String> = HashMap::with_capacity(TEST_RECORDS.len());

            for (k, v) in &TEST_RECORDS {
                let key = format!("{}-{}", *k, i);
                let value = (*v).to_string();

                if let Err(err) = db.set(&key, &value) {
                    panic!("error setting items: {}", err)
                }

                data.insert(key, value);
            }

            pre_roll_data.push(data);
        }

        for (k, v) in &post_roll_data {
            if let Err(err) = db.set(*k, *v) {
                panic!("error setting items: {}", err);
            }
        }

        let mut cky_file_contents_post_roll =
            utils::read_files_with_extension(DB_PATH, "cky").unwrap();
        let log_file_contents_post_roll = utils::read_files_with_extension(DB_PATH, "log").unwrap();
        cky_file_contents_post_roll.sort();

        assert_eq!(pre_roll_data.len(), cky_file_contents_post_roll.len());
        for i in 0..pre_roll_data.len() {
            for (k, v) in &pre_roll_data[i] {
                let key_value_pair = format!("{}{}{}", *k, constants::KEY_VALUE_SEPARATOR, *v);
                assert!(cky_file_contents_post_roll[i].contains(&key_value_pair));
            }
        }

        for (k, v) in &post_roll_data {
            let key_value_pair = format!("{}{}{}", *k, constants::KEY_VALUE_SEPARATOR, *v);
            assert!(log_file_contents_post_roll[0].contains(&key_value_pair));
        }
    }

    /// Connects to the test database; first clearing out any dummy data
    ///
    /// # Errors
    ///
    /// - File IO errors due to db_path say being inaccessible or permissions not given
    fn connect_to_test_db(
        db_path: &str,
        max_file_size_kb: f64,
        vacuum_interval_sec: f64,
    ) -> io::Result<Ckydb> {
        utils::clear_dummy_file_data_in_db(db_path)?;
        // utils::add_dummy_file_data_in_db(db_path)?;
        connect(db_path, max_file_size_kb, vacuum_interval_sec)
    }
}
