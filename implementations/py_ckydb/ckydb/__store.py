"""
Module containing the actual store representation
"""
import os
import re
from typing import Dict, List, Tuple


class Store:
    """The actual representation of the data store"""
    _token_separator = "$%#@*&^&"
    _key_value_separator = "><?&(^#"
    __index_filename = "index.idx"
    __del_filename = "delete.del"

    def __init__(self, db_path: str, max_file_size_kb: int, should_sanitize: bool):
        self.__db_path = db_path
        self.__max_file_size_kb = max_file_size_kb
        self.__should_sanitize = should_sanitize

        # defaults
        self._cache: Cache = Cache()
        self._index: Dict[str, str] = {}
        self._memtable: Dict[str, str] = {}
        self._data_files: List[str] = []
        self._current_log_file: str = ""

    def __eq__(self, other) -> bool:
        return (self.__db_path == other.__db_path
                and self.__max_file_size_kb == other.__max_file_size_kb
                and self.__should_sanitize == other.__should_sanitize
                and self._memtable == other._memtable
                and self._cache == other._cache
                and self._index == other._index
                and self._data_files == other._data_files
                and self._current_log_file == other._current_log_file
                )

    def load(self):
        """
        Loads the database from disk and updates its in-memory state
        """
        self.__create_log_file()
        self.__create_del_file()
        self.__create_idx_file()
        self.vacuum()
        self.__load_from_disk()

    def set(self, k: str, v: str):
        """
        Sets the given key k with the value v

        :param k: the key for the given value
        :param v: the value to set
        """
        pass

    def get(self, k: str) -> str:
        """
        Gets the value corresponding to the given key k

        :param k: the key of value to be retrieved
        :return: the value for key k
        :raises NotFoundError: if value is not found
        :raises CorruptDataError: if data in database is corrupted
        """
        pass

    def delete(self, k: str):
        """
        Deletes the value for the given key k

        :param k: the key of value to be deleted
        :raises NotFoundError: if value is not found
        :raises CorruptDataError: if data in database is corrupted
        """
        pass

    def clear(self):
        """
        Clears all the data in the database including that on disk and that
        in memory
        """
        pass

    def vacuum(self):
        """
        Deletes all keys marked for deletion from the
        ".cky" and ".log" files and then removes them from
        ".del" file
        """
        keys_to_delete = self.__get_keys_to_delete()
        if len(keys_to_delete) == 0:
            return

        data_files = os.listdir(self.__db_path)
        for file in data_files:
            if file == self.__del_filename or file == self.__index_filename:
                continue

            path = os.path.join(self.__db_path, file)
            self.__delete_key_values_from_file(path, keys_to_delete)

    def __delete_key_values_from_file(self, path: str, keys: List[str]):
        """
        Deletes the key-value pairs in the file at `path` for the given keys
        :param path: path to file
        :param keys: keys whose key-value pairs are to be deleted
        """
        with open(path) as f:
            content = "\n".join(f.readlines())

        key_value_pairs = content.split(self._token_separator)

        for key in keys:
            key_value_pairs = [kv for kv in key_value_pairs if key not in kv]

        content = self._token_separator.join(key_value_pairs)

        with open(path, "w") as f:
            f.write(content)

    def __get_keys_to_delete(self) -> List[str]:
        """
        Gets the list of keys to delete, as recorded in the del file
        :return:
        """
        try:
            del_file_path = os.path.join(self.__db_path, self.__del_filename)
            with open(del_file_path) as f:
                content = "\n".join(f.readlines())

            return content.split(self._token_separator)
        except FileNotFoundError:
            return []

    def __create_log_file(self):
        """
        Creates a new ".log" file if not exist
        :return:
        """
        pass

    def __create_del_file(self):
        """
        Creates a new ".del" file if not exist
        :return:
        """
        pass

    def __create_idx_file(self):
        """
        Creates a new ".idx" file if not exist
        :return:
        """
        pass

    def __transform_log_file_to_data_file(self, log_file_path) -> str:
        """
        Transforms a given log file into a data file
        :param log_file_path: the path to the log file
        :return: str - the path to the new log file
        """
        pass

    def __load_from_disk(self):
        """
        Loads the properties of this store from the data found on disk
        It updates properties like cache, memtable, index, data_files
        """
        pass

    def __clear_disk(self):
        """
        Clears all data on disk
        """
        pass

    def __sanitize_key_value_pair(self, key: str, value: str) -> Tuple[str, str]:
        """
        Escapes `token_separator` instances in
        the key and value pair and returns the pair

        :param key: - the unsanitized key
        :param value: - the unsanitized value
        :return: (str, str) - the sanitized (key, value) pair
        """
        pass

    def __desanitize_key_value_pair(self, key: str, value: str) -> Tuple[str, str]:
        """
        Restores the escaped `token_separator` instances in the
        key and value pair and returns the pair

        :param key: - the sanitized key
        :param value: - the sanitized value
        :return: (str, str) - the unsanitized (key, value) pair
        """
        pass

    def __generate_timestamped_key_pair(self, key: str) -> Tuple[str, str]:
        """
        Generates a new key, timestamped_key pair

        :param key: - the key to be timestamped
        :return: (str, str) - the (key, timestamped_key) pair
        """
        pass

    def __update_sorted_data_files_list(self):
        """
        Updates the sorted data_files list property from the
        list of ".cky" files in the database on disk
        """
        pass

    def __mark_key_for_deletion(self, key: str):
        """
        Removes the key from the in-memory index,
        and removes it from the ".idx" file
        and appends it to the ".del" file
        :param key: - the key to be marked for deletion
        """
        pass


class Cache:
    """
    The cache holding the latest data for the given time range
    """

    def __init__(self, data=None, start: str = "0", stop: str = "0"):
        self.__data = {} if data is None else data
        self.__start = int(start)
        self.__stop = int(stop)

    def is_in_range(self, timestamp: str) -> bool:
        """
        Checks to determine whether the given timestamp is in the cache's range
        :param timestamp:
        :return: whether it is in range or not
        """
        pass

    def __eq__(self, other) -> bool:
        return (
                self.__data == other.__data
                and self.__stop == other.__stop
                and self.__start == other.__start)
