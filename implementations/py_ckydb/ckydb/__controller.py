import multiprocessing as mp
from enum import Enum
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
        self.__max_file_size_kb = max_file_size_kb
        self.__vacuum_interval_sec = vacuum_interval_sec
        self.__store = Store(db_path=db_path)
        self.__db_path = db_path
        self.__store.load()
        self.__lock: mp.synchronize.Lock = mp.Lock()
        self.__vacuum_process: Optional[mp.Process] = None
        self.__roll_log_process: Optional[mp.Process] = None
        self.__is_open = False
        self.__exit_event = mp.Event()

    def start(self):
        if not self.__is_open:
            self.__start_vacuum_cycles()
            self.__start_roll_log_cycles()
            self.__is_open = True

    def close(self):
        if self.__is_open:
            self.__exit_event.set()
            self.__vacuum_process.join()
            self.__roll_log_process.join()
            self.__is_open = False
            self.__exit_event.clear()

    def set(self, k: str, v: str):
        """
        Sets the given key k with the value v

        :param k: the key for the given value
        :param v: the value to set
        """
        with self.__lock:
            return self.__store.set(k=k, v=v)

    def get(self, k: str) -> str:
        """
        Gets the value corresponding to the given key k

        :param k: the key of value to be retrieved
        :return: the value for key k
        :raises NotFoundError: if value is not found
        :raises CorruptDataError: if data in database is corrupted
        """
        with self.__lock:
            return self.__store.get(k)

    def delete(self, k: str):
        """
        Deletes the value for the given key k

        :param k: the key of value to be deleted
        :raises NotFoundError: if value is not found
        :raises CorruptDataError: if data in database is corrupted
        """
        with self.__lock:
            return self.__store.delete(k)

    def clear(self):
        """
        Clears all the data in the database
        """
        with self.__lock:
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
                        lock=self.__lock))
        self.__vacuum_process.start()

    def __start_roll_log_cycles(self):
        """
        Initializes the background tasks to check log file size and convert it to .cky if it has exceeded
        max file size
        """
        self.__roll_log_process = mp.Process(
            target=roll_log_at_intervals,
            kwargs=dict(db_path=self.__db_path,
                        exit_event=self.__exit_event,
                        lock=self.__lock,
                        max_file_size_in_kb=self.__max_file_size_kb))
        self.__roll_log_process.start()

    def __del__(self):
        self.close()

    def __eq__(self, other) -> bool:
        return (
                self.__store == other.__store
                and self.__vacuum_interval_sec == other.__vacuum_interval_sec
                and self.__max_file_size_kb == other.__max_file_size_kb
        )

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class Signal(Enum):
    STOP = 1


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

        store.load()
        with lock:
            store.vacuum()


def roll_log_at_intervals(db_path: str, max_file_size_in_kb: int, exit_event: Event, lock: Lock):
    """
    Checks the log file size every 5 seconds and rolls it if it is greater or equal to the max file size

    :param db_path: path to the database folder
    :param max_file_size_in_kb: the maximum file size in kilobytes
    :param exit_event: the controller event for stopping all background tasks
    :param lock: the lock to help synchronize the access of database files
    """
    store = Store(db_path=db_path)

    while not exit_event.is_set():
        exit_event.wait(5)

        store.load()
        with lock:
            if store.log_file_size >= max_file_size_in_kb:
                store.roll_log()
