import os
import unittest

import ckydb

db_folder = os.path.join(os.path.dirname(__file__), "assets", "test_ckydb_db")


class TestCkydb(unittest.TestCase):
    """Tests for the public API of ckydb"""

    def setUp(self) -> None:
        self.keys = ["hey", "hi", "salut", "bonjour", "hola", "oi", "mulimuta"]
        self.values = ["English", "English", "French", "French", "Spanish", "Portuguese", "Runyoro"]
        self.db = ckydb.Ckydb(db_folder)

    def test_default_connect(self):
        """connect returns a Ckydb instance with all the defaults"""
        self.assertEqual(ckydb.connect(db_folder), ckydb.Ckydb(db_folder))

    def test_connect_with_custom_options(self):
        """connect returns a Ckydb instance with all the passed custom options"""
        max_file_size_kb = 80000
        vacuum_interval_sec = 9000
        should_sanitize = True

        got = ckydb.connect(
            db_folder, max_file_size_kb=max_file_size_kb,
            vacuum_interval_sec=vacuum_interval_sec,
            should_sanitize=should_sanitize)

        expected = ckydb.Ckydb(
            db_folder, max_file_size_kb=max_file_size_kb,
            vacuum_interval_sec=vacuum_interval_sec,
            should_sanitize=should_sanitize)

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


if __name__ == '__main__':
    unittest.main()
