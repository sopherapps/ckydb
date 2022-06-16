"""Tests for the ckydb.Store class"""
import os
import shutil
import unittest
from datetime import datetime

import ckydb.__store

asset_folder = os.path.join(os.path.dirname(__file__), "assets")
dummy_db_folder = os.path.join(asset_folder, "db")
db_folder = os.path.join(asset_folder, "test_store_db")


class TestStore(unittest.TestCase):
    """Tests for the ckydb.Store class"""

    def setUp(self) -> None:
        """initialize some common variables"""
        self.store = ckydb.Store(db_folder, max_file_size_kb=1, should_sanitize=False)
        self.log_filename = "1655375171402014000.log"
        self.index_filename = "index.idx"
        self.del_filename = "delete.del"
        self.data_files = sorted([
            "1655375120328185000.cky",
            "1655375120328186000.cky",
        ])

    def tearDown(self) -> None:
        """Clean up"""
        self.__clear_dummy_db_data()

    def test_load(self):
        """load should initialize the db_folder from disk"""
        expected_cache = ckydb.Cache()
        expected_index = {
            "cow": "1655375120328185000-cow",
            "dog": "1655375120328185100-dog",
            "goat": "1655304770518678-goat",
            "hen": "1655304670510698-hen",
            "pig": "1655304770534578-pig",
            "fish": "1655303775538278-fish",
        }
        expected_memtable = {
            "1655304770518678-goat": "678 months",
            "1655304670510698-hen": "567 months",
            "1655304770534578-pig": "70 months",
            "1655303775538278-fish": "8990 months",
        }
        expected_data_files = [file.rstrip(".cky") for file in self.data_files]
        expected_current_log_file = self.log_filename.rstrip(".log")

        self.__add_dummy_db_data()
        self.store.load()

        self.assertEqual(expected_cache, self.store._cache)
        self.assertEqual(expected_current_log_file, self.store._current_log_file, )
        self.assertDictEqual(expected_index, self.store._index, )
        self.assertDictEqual(expected_memtable, self.store._memtable, )
        self.assertListEqual(expected_data_files, self.store._data_files, )

    def test_load_empty_db(self):
        """load creates the db folder if not exists, and adds .idx, .del and .log files"""
        expected_cache = ckydb.Cache()
        expected_files = [self.del_filename, self.index_filename]

        self.store.load()
        expected_files.append(f"{self.store._current_log_file}.log")
        expected_files.sort()
        files_in_db_folder = os.listdir(db_folder)
        files_in_db_folder.sort()

        self.assertEqual(expected_cache, self.store._cache)
        self.assertIsNot(self.store._current_log_file, "")
        self.assertDictEqual({}, self.store._index)
        self.assertDictEqual({}, self.store._memtable)
        self.assertListEqual([], self.store._data_files)
        self.assertEqual(expected_files, files_in_db_folder)

    def test_clear(self):
        """clear should reset all properties and delete all data on disk"""
        expected_cache = ckydb.Cache()
        expected_files = [self.del_filename, self.index_filename]

        self.__add_dummy_db_data()
        self.store.load()
        self.store.clear()
        expected_files.append(f"{self.store._current_log_file}.log")
        expected_files.sort()
        files_in_db_folder = os.listdir(db_folder)
        files_in_db_folder.sort()

        self.assertEqual(expected_cache, self.store._cache)
        self.assertIsNot(self.store._current_log_file, "")
        self.assertDictEqual({}, self.store._index)
        self.assertDictEqual({}, self.store._memtable)
        self.assertListEqual([], self.store._data_files)
        self.assertEqual(expected_files, files_in_db_folder)

    def test_set(self):
        """set should add key-value to memtable and log file, key-timestamped_key to index"""
        key, value = datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "foo"
        log_file_path = os.path.join(db_folder, self.log_filename)
        index_file_path = os.path.join(db_folder, self.index_filename)
        token = self.store._token_separator
        key_value_separator = self.store._key_value_separator

        self.__add_dummy_db_data()
        self.store.load()
        self.store.set(k=key, v=value)
        timestamped_key = self.store._index[key]
        expected_index_file_entry = f"{token}{key}{key_value_separator}{timestamped_key}"
        expected_log_file_entry = f"{token}{timestamped_key}{key_value_separator}{value}"
        value_in_memtable = self.store._memtable[timestamped_key]
        index_file_content = self.__read_to_str(index_file_path)
        log_file_content = self.__read_to_str(log_file_path)

        self.assertEqual(value, value_in_memtable)
        self.assertIn(expected_index_file_entry, index_file_content)
        self.assertIn(expected_log_file_entry, log_file_content)

    def test_set_same_key(self):
        """set same key should overwrite key-value on log (.log) file"""
        key, value, new_value = datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "foo", "bar"
        log_file_path = os.path.join(db_folder, self.log_filename)
        token = self.store._token_separator
        key_value_separator = self.store._key_value_separator

        self.__add_dummy_db_data()
        self.store.load()
        self.store.set(k=key, v=value)
        self.store.set(k=key, v=new_value)
        timestamped_key = self.store._index[key]
        expected_log_file_entry = f"{token}{timestamped_key}{key_value_separator}{new_value}"
        value_in_memtable = self.store._memtable[timestamped_key]
        log_file_content = self.__read_to_str(log_file_path)

        self.assertEqual(new_value, value_in_memtable)
        self.assertIn(expected_log_file_entry, log_file_content)

    def test_set_old_key(self):
        """set same old key should overwrite key-value on data (.cky) file"""
        key, value = "foo", "bar"
        data_file_path = os.path.join(db_folder, self.data_files[-1])
        token = self.store._token_separator
        key_value_separator = self.store._key_value_separator

        self.__add_dummy_db_data()
        self.store.load()
        self.store.set(k=key, v=value)
        timestamped_key = self.store._index[key]
        expected_data_file_entry = f"{token}{timestamped_key}{key_value_separator}{value}"
        value_in_memtable = self.store._memtable[timestamped_key]
        data_file_content = self.__read_to_str(data_file_path)

        self.assertEqual(value, value_in_memtable)
        self.assertIn(expected_data_file_entry, data_file_content)

    def test_get_recent_key(self):
        """get should return value directly from memtable for recent key, no update to cache"""
        # to test, one can delete the log file first
        pass

    def test_get_old_key(self):
        """get of old key should update cache with all data from selected old data file,
        and then return the value"""
        pass

    def test_get_old_key_again(self):
        """get of old key again should return value directly from cache, no update to cache"""
        # to test, one can delete the data_file from the db first
        pass

    def test_delete(self):
        """delete should remove key-value from index and log file;
        transfer key-timestamped_key from index (and .idx file) to .del file"""
        pass

    def test_delete_old_key(self):
        """delete should remove key-value from index and data (.cky) file;
        transfer key-timestamped_key from index (and .idx file) to .del file"""
        pass

    def test_vacuum(self):
        """vacuum should delete all marked-for-delete key-values from .cky and .log files"""
        expected_log_file_content = "1655304770518678-goat><?&(^#678 months$%#@*&^&1655304670510698-hen><?&(^#567 months$%#@*&^&1655304770534578-pig><?&(^#70 months$%#@*&^&1655303775538278-fish><?&(^#8990 months$%#@*&^&"
        expected_data_file_content = ["1655375120328185000-cow><?&(^#500 months$%#@*&^&1655375120328185100-dog><?&(^#23 months$%#@*&^&", ""]
        log_file_path = os.path.join(db_folder, self.log_filename)
        data_file_paths = [os.path.join(db_folder, file) for file in self.data_files]

        self.__add_dummy_db_data()
        self.store.vacuum()
        data_file_content = [self.__read_to_str(data_file_path) for data_file_path in data_file_paths]
        log_file_content = self.__read_to_str(log_file_path)

        self.assertEqual(expected_log_file_content, log_file_content)
        self.assertListEqual(expected_data_file_content, data_file_content)

    def test_should_sanitize_true(self):
        """should sanitize key and value in .log file;
         return unsanitized key-value on get and delete key-value given unsanitized key"""
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        store = ckydb.Store(db_path=db_folder, max_file_size_kb=1, should_sanitize=True)
        token = store._token_separator
        key_value_separator = store._key_value_separator
        key, value = f"{now}-{key_value_separator}", f"{token}_foo"

        self.__add_dummy_db_data()
        store.load()
        store.set(k=key, v=value)

        self.assertEqual(value, store.get(key))
        store.delete(key)
        self.assertRaises(ckydb.exc.NotFoundError, store.get)

    def test_max_file_size_kb(self):
        """should transform .log file to .cky if size of .log file comes close to max_file_size"""
        pass

    @staticmethod
    def __add_dummy_db_data():
        """Adds dummy db data to the current database's folder"""
        os.makedirs(db_folder, exist_ok=True)
        for file in os.listdir(dummy_db_folder):
            shutil.copy2(os.path.join(dummy_db_folder, file), db_folder)

    @staticmethod
    def __clear_dummy_db_data():
        """Removes all dummy db data found in the current database's folder"""
        shutil.rmtree(db_folder, ignore_errors=True)

    @staticmethod
    def __read_to_str(file_path: str) -> str:
        """Reads the contents at the given file path into a string"""
        with open(file_path) as file:
            return "\n".join(file.readlines())


if __name__ == '__main__':
    unittest.main()
