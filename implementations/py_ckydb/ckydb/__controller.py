import os

from ckydb.__store import Store


class Ckydb:
    """
    The Ckydb controller class that receives the data queries
    and returns the appropriate data

    If db_path passed is not accessible, it will throw an error.
    """

    def __init__(self, db_path: str, max_file_size_kb=(4 * 1024), vacuum_interval_sec=(5 * 60)):
        self.__max_file_size_kb = max_file_size_kb
        self.__vacuum_interval_sec = vacuum_interval_sec
        self.__store = Store(db_path=db_path)
        self.__store.load()
        self.__start_vacuum_cycles(store=self.__store)

    def set(self, k: str, v: str):
        """
        Sets the given key k with the value v

        :param k: the key for the given value
        :param v: the value to set
        """
        return self.__store.set(k=k, v=v)

    def get(self, k: str) -> str:
        """
        Gets the value corresponding to the given key k

        :param k: the key of value to be retrieved
        :return: the value for key k
        :raises NotFoundError: if value is not found
        :raises CorruptDataError: if data in database is corrupted
        """
        return self.__store.get(k)

    def delete(self, k: str):
        """
        Deletes the value for the given key k

        :param k: the key of value to be deleted
        :raises NotFoundError: if value is not found
        :raises CorruptDataError: if data in database is corrupted
        """
        return self.__store.delete(k)

    def clear(self):
        """
        Clears all the data in the database
        """
        return self.__store.clear()

    def __start_vacuum_cycles(self, store: Store):
        """
        Initializes the background tasks to clean stale data in the store

        :param store: the store to be vacuumed
        """
        pass

    def __stop_vacuum_cycles(self):
        """
        Halts the execution of the background tasks that do the vacuuming
        """
        pass

    def __del__(self):
        self.__stop_vacuum_cycles()

    def __eq__(self, other) -> bool:
        return (
                self.__store == other.__store
                and self.__vacuum_interval_sec == other.__vacuum_interval_sec
                and self.__max_file_size_kb == other.__max_file_size_kb
        )
