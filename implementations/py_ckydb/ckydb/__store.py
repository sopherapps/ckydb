"""
Module containing the actual store representation
"""
import os
import shutil
import time
from pathlib import Path
from typing import Dict, List, Tuple, Optional

from ckydb.__exc import CorruptedDataError, NotFoundError

_DEFAULT_TOKEN_SEPARATOR = "$%#@*&^&"
_DEFAULT_KEY_VALUE_SEPARATOR = "><?&(^#"


class Store:
    """The actual representation of the data store"""
    _token_separator = _DEFAULT_TOKEN_SEPARATOR
    _key_value_separator = _DEFAULT_KEY_VALUE_SEPARATOR
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
        self.__create_db_folder()
        self.__create_log_file()
        self.__create_del_file()
        self.__create_idx_file()
        self.vacuum()
        self.__load_file_props_from_disk()
        self.__load_index_from_disk()
        self.__load_memtable_from_disk()

    def set(self, k: str, v: str):
        """
        Sets the given key k with the value v

        :param k: the key for the given value
        :param v: the value to set
        """
        timestamped_key = None

        try:
            if self.__should_sanitize:
                k, v = self.__sanitize_key_value_pair(key=k, value=v)

            timestamped_key = self.__get_timestamped_key(k)
            self.__save_key_value_pair(key=timestamped_key, value=v)
        except Exception:
            self.__delete_key_value_pair(key=timestamped_key)
            self.__remove_timestamped_key(k)

    def get(self, k: str) -> str:
        """
        Gets the value corresponding to the given key k

        :param k: the key of value to be retrieved
        :return: the value for key k
        :raises NotFoundError: if value is not found
        :raises CorruptDataError: if data in database is corrupted
        """
        if self.__should_sanitize:
            k, _ = self.__sanitize_key_value_pair(key=k, value="")

        timestamped_key = self._index.get(k, None)
        if timestamped_key is None:
            raise NotFoundError()

        value = self.__get_value_for_key(timestamped_key)
        if value is None:
            raise CorruptedDataError()

        return value

    def delete(self, k: str):
        """
        Deletes the value for the given key k

        :param k: the key of value to be deleted
        :raises NotFoundError: if value is not found
        :raises CorruptDataError: if data in database is corrupted
        """
        self.__mark_key_for_deletion(k)

    def clear(self):
        """
        Clears all the data in the database including that on disk and that
        in memory
        """
        self.__clear_disk()
        self.load()

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

        with open(self.__del_file_path, "w") as f:
            pass

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

    def __save_key_value_pair(self, key: str, value: str):
        """
        Saves the given key value pair in memtable and in the log file
        :param key:
        :param value:
        """
        if key >= self._current_log_file:
            self._memtable[key] = value
            self.__persist_memtable_to_disk()

        elif self._cache.is_in_range(key):
            self._cache.update(key, value)
            self.__persist_cache_to_disk()

        else:
            timestamp_range = self.__get_timestamp_range_for_key(key)
            if timestamp_range is None:
                raise CorruptedDataError()

            self.__load_cache_for_timestamp_range(timestamp_range)
            self._cache.update(key, value)
            self.__persist_cache_to_disk()

    def __delete_key_value_pair(self, key: str):
        """
        Removes the given key value pair for the given key in memtable and in the log file
        :param key: the timestamped key to delete
        """
        if self._cache.is_in_range(key):
            self._cache.remove(key)
            self.__persist_cache_to_disk()
        elif key >= self._current_log_file:
            self._memtable.pop(key)
            self.__persist_memtable_to_disk()

    def __get_keys_to_delete(self) -> List[str]:
        """
        Gets the list of keys to delete, as recorded in the del file
        :return:
        """
        del_file_path = os.path.join(self.__db_path, self.__del_filename)
        with open(del_file_path) as f:
            content = "\n".join(f.readlines())

        return content.rstrip(self._token_separator).split(self._token_separator)

    def __create_log_file(self):
        """
        Creates a new ".log" file if not exist
        :return:
        """
        log_files = [file for file in os.listdir(self.__db_path) if file.endswith(".log")]

        if len(log_files) == 0:
            log_filename = f"{time.time_ns()}"
            log_file_path = os.path.join(self.__db_path, f"{log_filename}.log")
            Path(log_file_path).touch()
            self._current_log_file = log_filename

    def __create_del_file(self):
        """
        Creates a new ".del" file if not exist
        :return:
        """
        if self.__del_filename not in os.listdir(self.__db_path):
            Path(self.__del_file_path).touch()

    def __create_idx_file(self):
        """
        Creates a new ".idx" file if not exist
        :return:
        """
        if self.__index_filename not in os.listdir(self.__db_path):
            Path(self.__index_file_path).touch()

    def __create_db_folder(self):
        """
        Creates the db folder if not exists
        """
        os.makedirs(self.__db_path, exist_ok=True)

    def __transform_log_file_to_data_file(self, log_file_path) -> str:
        """
        Transforms a given log file into a data file
        :param log_file_path: the path to the log file
        :return: str - the path to the new log file
        """
        pass

    def __load_memtable_from_disk(self):
        """Loads the memtable from the current log .log file"""
        self._memtable = self.__get_key_value_pairs_from_file(f"{self._current_log_file}.log")

    def __load_index_from_disk(self):
        """Loads the index from the index .idx file"""
        self._index = self.__get_key_value_pairs_from_file(self.__index_filename)

    def __get_key_value_pairs_from_file(self, filename: str) -> Dict[str, str]:
        """
        Extracts the key-value pairs saved in the given file

        :param filename: - the filename within the db folder
        :return: - the key-value pairs as a dictionary
        """
        file = os.path.join(self.__db_path, filename)

        with open(file) as f:
            content = "\n".join(f.readlines())

        if content == "":
            return {}

        key_value_pairs = content.rstrip(self._token_separator).split(self._token_separator)
        return dict(kv.split(self._key_value_separator) for kv in key_value_pairs)

    def __load_file_props_from_disk(self):
        """
        Updates the __data_files and the __current_log_file from disk
        """
        self._data_files = []
        self._current_log_file = ""

        files = os.listdir(self.__db_path)
        for file in files:
            if file.endswith(".log"):
                self._current_log_file = file.rstrip(".log")
            if file.endswith(".cky"):
                self._data_files.append(file.rstrip(".cky"))

        self._data_files.sort()

    def __clear_disk(self):
        """
        Clears all data on disk
        """
        shutil.rmtree(self.__db_path, ignore_errors=True)

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

    def __get_timestamped_key(self, key: str) -> str:
        """
        Gets the timestamped key from index or generates one if not exists and adds it to index file

        :param key: - the key to be timestamped
        :return: str - the timestamped key
        """
        timestamped_key = self._index.get(key, None)
        if timestamped_key is None:
            timestamped_key = f"{time.time_ns()}-{key}"
            self._index[key] = timestamped_key

            with open(self.__index_file_path, "a") as f:
                f.write(f"{key}{self._key_value_separator}{timestamped_key}{self._token_separator}")

        return timestamped_key

    def __remove_timestamped_key(self, key: str):
        """
        Reverse of __get_timestamped_key
        Removes the key from index and from index file

        :param key: - the key to be removed from the index file
        """
        timestamped_key = self._index.pop(key, None)
        if timestamped_key is None:
            return

        with open(self.__index_file_path, "r+") as f:
            content = "\n".join(f.readlines())
            key_timestamped_key_pair = f"{key}{self._key_value_separator}{timestamped_key}{self._token_separator}"
            f.write(content.replace(key_timestamped_key_pair, ""))

    def __persist_memtable_to_disk(self):
        """Persists the current memtable to disk"""
        self.__persist_data_to_file(data=self._memtable, filename=f"{self._current_log_file}.log")

    def __persist_cache_to_disk(self):
        """Persists the current cache to disk"""
        self.__persist_data_to_file(data=self._cache.data, filename=f"{self._cache.start}.cky")

    def __persist_data_to_file(self, data: Dict[str, str], filename: str):
        """
        Persists the given data into the file within the database folder,
        overwriting the older data

        :param data: the new data
        :param filename: the name of the file within the database folder
        """
        content = ""
        data_file_path = os.path.join(self.__db_path, filename)

        for key, value in data.items():
            content += f"{key}{self._key_value_separator}{value}{self._token_separator}"

        with open(data_file_path, "w") as f:
            f.write(content)

    def __get_timestamp_range_for_key(self, key: str) -> Optional[Tuple[str, str]]:
        """
        Returns the range of timestamps within which the given key falls.
        This range corresponds to the data_files and the log_file names which are actually timestamps
        :param key:
        :return:
        """
        timestamps = sorted([*self._data_files, self._current_log_file])

        for i, timestamp in enumerate(timestamps):
            if timestamp > key and i > 0:
                return timestamps[i - 1], timestamp

        return None

    def __load_cache_for_timestamp_range(self, timestamp_range: Tuple[str, str]):
        """
        Loads the _cache for the given timestamp range where the lower limit of the range
        is the name of the data file whose data is to be loaded into the disk.
        The upper limit is used to just update the cache's end property

        :param timestamp_range:
        """
        data = self.__get_key_value_pairs_from_file(f"{timestamp_range[0]}.cky")
        self._cache = Cache(data=data, start=timestamp_range[0], end=timestamp_range[1])

    def __get_value_for_key(self, timestamped_key: str) -> Optional[str]:
        """
        Returns the value for the given key. It will return None if value is not found
        :param timestamped_key:
        :return: (Optional[str]) the value for the given key
        """
        if timestamped_key >= self._current_log_file:
            return self._memtable.get(timestamped_key, None)
        elif self._cache.is_in_range(timestamped_key):
            return self._cache.data.get(timestamped_key, None)
        else:
            timestamp_range = self.__get_timestamp_range_for_key(timestamped_key)
            if timestamp_range is None:
                return None

            self.__load_cache_for_timestamp_range(timestamp_range)
            return self._cache.data.get(timestamped_key, None)

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
        :raises NotFoundError: if key is not in index
        """
        timestamped_key = self._index.pop(key, None)
        if timestamped_key is None:
            raise NotFoundError()

        self.__persist_data_to_file(self._index, self.__index_filename)

        with open(self.__del_file_path, "a") as f:
            f.write(f"{timestamped_key}{self._token_separator}")

    @property
    def __index_file_path(self):
        return os.path.join(self.__db_path, self.__index_filename)

    @property
    def __del_file_path(self):
        return os.path.join(self.__db_path, self.__del_filename)

    @property
    def __log_file_path(self):
        return os.path.join(self.__db_path, f"{self._current_log_file}.log")


class Cache:
    """
    The cache holding the latest data for the given time range
    """

    def __init__(self,
                 data=None,
                 start: str = "0",
                 end: str = "0"):
        self.__data = {} if data is None else data
        self.__start = start
        self.__end = end

    def is_in_range(self, timestamp: str) -> bool:
        """
        Checks to determine whether the given timestamp is in the cache's range
        :param timestamp:
        :return: whether it is in range or not
        """
        return self.__start <= timestamp <= self.__end

    def update(self, key: str, value: str):
        """
        Update the given key with the given value in the cache
        :param key: the key to be updated
        :param value: the new value for th key
        """
        self.__data[key] = value

    def remove(self, key: str):
        """
        Removes the given key from the cache
        :param key:
        """
        self.__data.pop(key)

    @property
    def data(self):
        return self.__data

    @property
    def start(self):
        return self.__start

    @property
    def end(self):
        return self.__end

    def __eq__(self, other) -> bool:
        return (
                self.__data == other.__data
                and self.__end == other.__end
                and self.__start == other.__start
        )
