# core/exceptions.py

class DataCollectorException(Exception):
    """Base exception class for all custom exceptions in the project."""
    pass

class ConfigurationError(DataCollectorException):
    """Raised for errors related to the configuration file."""
    pass

class DataCollectionError(DataCollectorException):
    """Raised for errors during data collection."""
    pass

class StorageError(DataCollectorException):
    """Raised for errors during data storage."""
    pass

class CaptchaError(DataCollectorException):
    """Raised for errors during CAPTCHA solving."""
    pass