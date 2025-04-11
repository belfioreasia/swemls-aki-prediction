# ------------------------- Custom System Exceptions -------------------------
# Custom Exceptions for the system that allow for better error handling.

class SigtermException(Exception):
    """Custom Exception raised when sigterm signal is received."""
    pass

class SigintException(Exception):
    """Custom Exception raised when sigint signal is received."""
    pass