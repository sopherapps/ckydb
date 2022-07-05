import multiprocessing as mp
from multiprocessing.synchronize import Lock, Event
from typing import Optional

from ckydb.__store import Store


class Ckydb:
    """
    The Ckydb controller class that receives the data queries
    and returns the appropriate data

    If db_path passed is not accessible, it will throw an error.
    """

    def __init__(self, db_path: str, max_file_size_kb=(4 * 1024), vacuum_interval_sec=(5 * 60)):
        self.__vacuum_interval_sec = vacuum_interval_sec
        self.__store = Store(db_path=db_path, max_file_size_kb=max_file_size_kb)
        self.__db_path = db_path
        self.__store.load()
        self.__mut_lock: mp.synchronize.Lock = mp.Lock()
        self.__vacuum_process: Optional[mp.Process] = None
        self.__is_open = False
        self.__exit_event = mp.Event()

    def start(self):
        if not self.__is_open:
            self.__start_vacuum_cycles()
            self.__is_open = True

    def close(self):
        if self.__is_open:
            self.__exit_event.set()
            self.__vacuum_process.join()
            self.__is_open = False
            self.__exit_event.clear()

    def set(self, k: str, v: str):
        """
        Sets the given key k with the value v

        :param k: the key for the given value
        :param v: the value to set
        """
        with self.__mut_lock:
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
        with self.__mut_lock:
            return self.__store.delete(k)

    def clear(self):
        """
        Clears all the data in the database
        """
        with self.__mut_lock:
            return self.__store.clear()

    def __start_vacuum_cycles(self):
        """
        Initializes the background tasks to clean stale data in the store
        """
        self.__vacuum_process = mp.Process(
            target=vacuum_at_intervals,
            kwargs=dict(db_path=self.__db_path,
                        interval=self.__vacuum_interval_sec,
                        exit_event=self.__exit_event,
                        lock=self.__mut_lock))
        self.__vacuum_process.start()

    def __del__(self):
        self.close()

    def __eq__(self, other) -> bool:
        return (
                self.__store == other.__store
                and self.__vacuum_interval_sec == other.__vacuum_interval_sec
        )

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def vacuum_at_intervals(db_path: str, interval: int, exit_event: Event, lock: Lock):
    """
    Vacuums the store at the given db path at the given interval

    :param db_path: path to the database folder
    :param interval: interval for vacuuming in seconds
    :param exit_event: the controller event for stopping all background tasks
    :param lock: the lock to help synchronize the access of database files
    """
    store = Store(db_path=db_path)

    while not exit_event.is_set():
        exit_event.wait(interval)

        with lock:
            store.load()
            store.vacuum()
