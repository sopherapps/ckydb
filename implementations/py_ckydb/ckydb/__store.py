"""
Module containing the actual store representation
"""


class Store:
    """The actual representation of the data store"""

    def __init__(self, db_path: str, max_file_size_kb: int, should_sanitize: bool):
        self.db_path = db_path
        self.max_file_size_kb = max_file_size_kb
        self.should_sanitize = should_sanitize

    def __eq__(self, other) -> bool:
        return (self.db_path == other.db_path
                and self.max_file_size_kb == other.max_file_size_kb
                and self.should_sanitize == other.should_sanitize)
