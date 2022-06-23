"""Module containing connection utilities for ckydb"""
from .__controller import Ckydb


def connect(db_path: str, max_file_size_kb=(4 * 1024), vacuum_interval_sec=(5 * 60)):
    """
    Connects to the ckydb at the db_path, creating it if it does not exist

    :param db_path: the path to the database folder
    :param max_file_size_kb: the maximum size in kilobytes of each file in database. default: 4MBs
    :param vacuum_interval_sec: the interval in seconds for the scheduled background task
    to remove stale deleted data
    :return: the Ckydb instance
    """
    return Ckydb(db_path=db_path, max_file_size_kb=max_file_size_kb, vacuum_interval_sec=vacuum_interval_sec)
