use crate::errors::{CorruptedDataError, NotFoundError};
use crate::store::{Storage, Store};
use crate::task::Task;
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
    fn open(&self) -> io::Result<()>;

    /// Stops all background tasks
    ///
    /// # Errors
    /// - [io::Error] I/O errors e.g file permissions, missing files in case the database folder
    /// is not accessible
    ///
    /// [io::Error]: std::io::Error
    fn close(&self) -> io::Result<()>;

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
}

/// `Ckydb` is the public API for the database.
/// It implements the [Controller] trait as well as the [Drop] trait
pub struct Ckydb {
    tasks: Option<Vec<Task>>,
    store: Store,
    vacuum_interval_sec: f64,
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
    fn new(db_path: &str, max_file_size_kb: f64, vacuum_interval_sec: f64) -> io::Result<Ckydb> {
        let mut store = Store::new(db_path, max_file_size_kb);

        store.load().and(Ok(Ckydb {
            tasks: None,
            store,
            vacuum_interval_sec,
            is_open: false,
        }))
    }
}

impl Controller for Ckydb {
    fn open(&self) -> io::Result<()> {
        todo!()
    }

    fn close(&self) -> io::Result<()> {
        todo!()
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
    let db = Ckydb::new(db_path, max_file_size_kb, vacuum_interval_sec)?;
    db.open().and(Ok(db))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::Worker;
    use crate::{constants, utils};
    use serial_test::serial;
    use std::collections::HashMap;
    use std::thread::sleep;
    use std::time::Duration;

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

    #[serial]
    #[test]
    fn connect_should_call_open() {
        let mut db = connect_to_test_db(DB_PATH, MAX_FILE_SIZE_KB, VACUUM_INTERVAL_SEC).unwrap();

        let tasks = db.tasks.take().unwrap_or(Vec::with_capacity(0));
        assert!(tasks.len() > 0);
        tasks.into_iter().for_each(|task| {
            assert!(task.is_running());
            task.stop().unwrap_or(());
        });
    }

    #[serial]
    #[test]
    fn open_should_start_all_tasks() {
        let mut db = Ckydb::new(DB_PATH, MAX_FILE_SIZE_KB, VACUUM_INTERVAL_SEC).unwrap();

        if let Err(err) = db.open() {
            panic!("error opening db: {}", err);
        }

        let tasks = db.tasks.take().unwrap_or(Vec::with_capacity(0));
        assert!(tasks.len() > 0);
        tasks.into_iter().for_each(|task| {
            assert!(task.is_running());
            task.stop().unwrap_or(());
        });
    }

    #[serial]
    #[test]
    fn close_should_stop_all_tasks() {
        let mut db = connect_to_test_db(DB_PATH, MAX_FILE_SIZE_KB, VACUUM_INTERVAL_SEC).unwrap();

        if let Err(err) = db.close() {
            panic!("error closing db: {}", err);
        }

        let tasks = db.tasks.take().unwrap_or(Vec::with_capacity(0));

        assert!(tasks.len() > 0);
        tasks.into_iter().for_each(|task| {
            assert!(!task.is_running());
        });
    }

    #[serial]
    #[test]
    fn set_new_key_should_add_key_value_to_store() {
        let db = connect_to_test_db(DB_PATH, MAX_FILE_SIZE_KB, VACUUM_INTERVAL_SEC).unwrap();

        for (k, v) in &TEST_RECORDS {
            if let Err(err) = db.set(*k, *v) {
                panic!("error setting keys: {}", err);
            };
        }

        for (k, v) in &TEST_RECORDS {
            match db.get(*k) {
                Ok(value) => assert_eq!(value, (*v).to_string()),
                Err(err) => panic!("error getting keys: {}", err),
            }
        }
    }

    #[serial]
    #[test]
    fn set_old_key_should_update_old_key_value() {
        let mut old_records = HashMap::from(TEST_RECORDS);

        let updates = HashMap::from([
            ("hey", "Jane"),
            ("hi", "John"),
            ("salut", "Jean"),
            ("oi", "Ronaldo"),
            ("mulimuta", "Aliguma"),
        ]);

        let db = connect_to_test_db(DB_PATH, MAX_FILE_SIZE_KB, VACUUM_INTERVAL_SEC).unwrap();

        for (k, v) in &old_records {
            if let Err(err) = db.set(*k, *v) {
                panic!("error setting keys: {}", err);
            };
        }

        for (k, v) in &updates {
            match db.set(*k, *v) {
                Ok(_) => {
                    old_records.remove(k);
                }
                Err(err) => panic!("error setting keys: {}", err),
            };
        }

        for (k, v) in &updates {
            match db.get(*k) {
                Ok(value) => assert_eq!(*v, value),
                Err(err) => panic!("error getting keys: {}", err),
            };
        }

        for (k, v) in &old_records {
            match db.get(*k) {
                Ok(value) => assert_eq!(*v, value),
                Err(err) => panic!("error getting keys: {}", err),
            };
        }
    }

    #[serial]
    #[test]
    fn get_old_key_should_return_value_for_key_in_store() {
        let (key, value) = ("cow", "500 months");

        let db = connect_to_test_db(DB_PATH, MAX_FILE_SIZE_KB, VACUUM_INTERVAL_SEC).unwrap();

        match db.get(key) {
            Ok(v) => assert_eq!(value.to_string(), v),
            Err(err) => panic!("error getting keys: {}", err),
        }
    }

    #[serial]
    #[test]
    fn get_old_key_again_should_get_value_from_memory_cache() {
        let (key, value) = ("cow", "500 months");

        let db = connect_to_test_db(DB_PATH, MAX_FILE_SIZE_KB, VACUUM_INTERVAL_SEC).unwrap();

        if let Err(err) = db.get(key) {
            panic!("error getting keys: {}", err);
        }

        // remove the files to ensure data is got from memory only
        if let Err(err) = utils::clear_dummy_file_data_in_db(DB_PATH) {
            panic!("error deleting files: {}", err)
        }

        match db.get(key) {
            Ok(v) => assert_eq!(value.to_string(), v),
            Err(err) => panic!("error getting keys: {}", err),
        }
    }

    #[serial]
    #[test]
    fn get_newly_inserted_key_should_get_from_memory_memtable() {
        let (key, value) = ("hello", "world");

        let db = connect_to_test_db(DB_PATH, MAX_FILE_SIZE_KB, VACUUM_INTERVAL_SEC).unwrap();

        if let Err(err) = db.set(key, value) {
            panic!("error getting keys: {}", err);
        }

        // remove the files to ensure data is got from memory only
        if let Err(err) = utils::clear_dummy_file_data_in_db(DB_PATH) {
            panic!("error deleting files: {}", err)
        }

        match db.get(key) {
            Ok(v) => assert_eq!(value.to_string(), v),
            Err(err) => panic!("error getting keys: {}", err),
        }
    }

    #[serial]
    #[test]
    fn delete_should_remove_key_value_from_store() {
        let mut old_records = HashMap::from(TEST_RECORDS);
        let keys_to_delete = ["hey", "salut"];

        let db = connect_to_test_db(DB_PATH, MAX_FILE_SIZE_KB, VACUUM_INTERVAL_SEC).unwrap();

        for (k, v) in &old_records {
            if let Err(err) = db.set(*k, *v) {
                panic!("error setting keys: {}", err);
            };
        }

        for k in &keys_to_delete {
            match db.delete(*k) {
                Ok(_) => {
                    old_records.remove(*k);
                }
                Err(err) => panic!("error deleting keys: {}", err),
            }
        }

        for (k, v) in &old_records {
            match db.get(*k) {
                Ok(value) => assert_eq!(*v, value),
                Err(err) => panic!("error getting keys: {}", err),
            };
        }

        for k in &keys_to_delete {
            match db.get(*k) {
                Ok(_) => panic!("key: {} unexpected", k),
                Err(err) => assert!(err.to_string().contains("not found")),
            }
        }
    }

    #[serial]
    #[test]
    fn clear_should_remove_all_key_values_from_store() {
        let db = connect_to_test_db(DB_PATH, MAX_FILE_SIZE_KB, VACUUM_INTERVAL_SEC).unwrap();

        for (k, v) in &TEST_RECORDS {
            if let Err(err) = db.set(*k, *v) {
                panic!("error setting keys: {}", err);
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

    #[serial]
    #[test]
    fn vacuum_task_should_run_at_defined_interval() {
        let key_to_delete = "salut";
        let db = connect_to_test_db(DB_PATH, MAX_FILE_SIZE_KB, VACUUM_INTERVAL_SEC).unwrap();

        for (k, v) in &TEST_RECORDS {
            if let Err(err) = db.set(*k, *v) {
                panic!("error setting keys: {}", err);
            };
        }

        if let Err(err) = db.delete(key_to_delete) {
            panic!("error deleting keys: {}", err)
        }

        let idx_file_contents_pre_vacuum =
            utils::read_files_with_extension(DB_PATH, "idx").unwrap();
        let del_file_contents_pre_vacuum =
            utils::read_files_with_extension(DB_PATH, "del").unwrap();
        let log_file_contents_pre_vacuum =
            utils::read_files_with_extension(DB_PATH, "log").unwrap();

        sleep(Duration::from_secs_f64(VACUUM_INTERVAL_SEC));

        let idx_file_contents_post_vacuum =
            utils::read_files_with_extension(DB_PATH, "idx").unwrap();
        let del_file_contents_post_vacuum =
            utils::read_files_with_extension(DB_PATH, "del").unwrap();
        let log_file_contents_post_vacuum =
            utils::read_files_with_extension(DB_PATH, "log").unwrap();

        // before vacuum
        assert!(!idx_file_contents_pre_vacuum[0].contains(key_to_delete));
        assert!(del_file_contents_pre_vacuum[0].contains(key_to_delete));
        assert!(log_file_contents_pre_vacuum[0].contains(key_to_delete));
        // after vacuum
        assert!(!idx_file_contents_post_vacuum[0].contains(key_to_delete));
        assert!(!del_file_contents_post_vacuum[0].contains(key_to_delete));
        assert!(!log_file_contents_post_vacuum[0].contains(key_to_delete));
    }

    #[serial]
    #[test]
    fn log_file_should_be_turned_to_cky_file_when_it_exceeds_max_size() {
        let mut pre_roll_data: Vec<HashMap<String, String>> = Vec::with_capacity(3);
        let post_roll_data = HashMap::from([("hey", "English"), ("hi", "English")]);

        if let Err(err) = utils::clear_dummy_file_data_in_db(DB_PATH) {
            panic!("error clearing test db disk data: {}", err)
        }

        let db = connect(DB_PATH, MAX_FILE_SIZE_KB, VACUUM_INTERVAL_SEC).unwrap();

        for i in 0..3 {
            let mut data: HashMap<String, String> = HashMap::with_capacity(TEST_RECORDS.len());

            for (k, v) in &TEST_RECORDS {
                let key = format!("{}-{}", *k, i);
                let value = (*v).to_string();

                if let Err(err) = db.set(&key, &value) {
                    panic!("error setting keys: {}", err)
                }

                data.insert(key, value);
            }

            pre_roll_data.push(data);
        }

        for (k, v) in &post_roll_data {
            if let Err(err) = db.set(*k, *v) {
                panic!("error setting keys: {}", err);
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

    /// Connects to the test database; first clearing out any dummy data then adding it afresh
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
        utils::add_dummy_file_data_in_db(db_path)?;
        connect(db_path, max_file_size_kb, vacuum_interval_sec)
    }
}
