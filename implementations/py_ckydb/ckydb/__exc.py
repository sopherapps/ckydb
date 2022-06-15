class NotFoundError(Exception):
    """Error when a key is not found in the database"""
    pass


class CorruptedDataError(Exception):
    """Error when data in the database is corrupted"""
    pass
