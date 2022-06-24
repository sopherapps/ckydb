import os
import time
import unittest

import ckydb

db_folder = os.path.join(os.path.dirname(__file__), "assets", "test_ckydb_db")


class TestCkydb(unittest.TestCase):
    """Tests for the public API of ckydb"""

    def setUp(self) -> None:
        self.keys = ["hey", "hi", "salut", "bonjour", "hola", "oi", "mulimuta"]
        self.values = ["English", "English", "French", "French", "Spanish", "Portuguese", "Runyoro"]
        self.db = ckydb.Ckydb(db_folder, vacuum_interval_sec=2)
        self.db.start()

    def tearDown(self) -> None:
        self.db.clear()
        self.db.close()

    def test_default_connect(self):
        """connect returns a Ckydb instance with all the defaults"""
        with ckydb.connect(db_folder) as got, ckydb.Ckydb(db_folder) as expected:
            self.assertEqual(got, expected)

    def test_connect_with_custom_options(self):
        """connect returns a Ckydb instance with all the passed custom options"""
        max_file_size_kb = 80000
        vacuum_interval_sec = 9000
        conn1 = ckydb.connect(db_folder,
                              max_file_size_kb=max_file_size_kb,
                              vacuum_interval_sec=vacuum_interval_sec)
        conn2 = ckydb.Ckydb(db_folder,
                            max_file_size_kb=max_file_size_kb,
                            vacuum_interval_sec=vacuum_interval_sec)

        with conn1 as got, conn2 as expected:
            self.assertEqual(got, expected)

    def test_ckydb_set(self):
        """Sets the given keys in the database"""
        for k, v in zip(self.keys, self.values):
            self.db.set(k, v)

        for i, k in enumerate(self.keys):
            self.assertEqual(self.values[i], self.db.get(k))

    def test_ckydb_set_same_key(self):
        """Setting the same key more than once overwrites its value"""
        new_values = ["Jane", "John", "Jean", "Marie", "Santos", "Ronaldo", "Aliguma"]

        for k, v in zip(self.keys, self.values):
            self.db.set(k, v)

        for k, v in zip(self.keys, new_values):
            self.db.set(k, v)

        for i, k in enumerate(self.keys):
            assert new_values[i] == self.db.get(k)

    def test_ckydb_delete(self):
        """Deleting removes the given key"""
        for k, v in zip(self.keys, self.values):
            self.db.set(k, v)

        for k in self.keys[:2]:
            self.db.delete(k)

        for k, v in zip(self.keys[2:], self.values[2:]):
            self.assertEqual(v, self.db.get(k))

        for k in self.keys[:2]:
            self.assertRaises(ckydb.exc.NotFoundError, self.db.get, k)

    def test_ckydb_clear(self):
        """Clears all the data in the database"""
        for k, v in zip(self.keys, self.values):
            self.db.set(k, v)

        self.db.clear()

        for k in self.keys:
            self.assertRaises(ckydb.exc.NotFoundError, self.db.get, k)

    def test_vacuum_cycles(self):
        """vacuum should be called on the store instance every vacuum_interval_sec seconds"""
        for k, v in zip(self.keys, self.values):
            self.db.set(k, v)

        key_to_delete = self.keys[1]

        self.db.delete(key_to_delete)
        got_idx_content_pre_vacuum = self.__read_to_str("index.idx")
        got_del_content_pre_vacuum = self.__read_to_str("delete.del")
        got_log_content_pre_vacuum = self.__read_to_str(self._log_filename)
        time.sleep(4)
        got_idx_content_post_vacuum = self.__read_to_str("index.idx")
        got_del_content_post_vacuum = self.__read_to_str("delete.del")
        got_log_content_post_vacuum = self.__read_to_str(self._log_filename)

        self.assertNotIn(key_to_delete, got_idx_content_pre_vacuum)
        self.assertIn(key_to_delete, got_del_content_pre_vacuum)
        self.assertIn(key_to_delete, got_log_content_pre_vacuum)
        self.assertNotIn(key_to_delete, got_idx_content_post_vacuum)
        self.assertNotIn(key_to_delete, got_del_content_post_vacuum)
        self.assertNotIn(key_to_delete, got_log_content_post_vacuum)

    def test_roll_log_cycles(self):
        """roll_log should be called on the store instance when the log goes beyond the max_file_size_kb size"""
        # copy data that exceeds the given limit into the log file and wait for a minute
        # check that a new .cky file has been created and an empty .log file has also been created
        # copy little data this time and check to see that no new file was created
        # copy big data again into the log file and check to see that a new .cky file has been created, as
        # well as a new .log file
        pass

    @property
    def _log_filename(self):
        return [file for file in os.listdir(db_folder) if file.endswith(".log")][0]

    @staticmethod
    def __read_to_str(file_name: str) -> str:
        """Reads the contents at the given file name in the db folder into a string"""
        file_path = os.path.join(db_folder, file_name)
        with open(file_path) as file:
            return "\n".join(file.readlines())


if __name__ == '__main__':
    unittest.main()
