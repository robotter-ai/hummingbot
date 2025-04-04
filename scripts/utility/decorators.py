import asyncio
import inspect
import logging
import os
from functools import wraps


# Configure logger with a file handler
def setup_logger(name="app", log_file="app.log", level=logging.DEBUG):
    """Set up and configure a logger that writes to a specific file"""
    _logger = logging.getLogger(name)
    _logger.setLevel(level)

    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    # Create file handler and set formatter
    file_handler = logging.FileHandler(log_file, mode="a")
    file_handler.setLevel(level)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(formatter)

    # Add handler to logger if not already added
    if not _logger.handlers:
        _logger.addHandler(file_handler)

    return _logger


# Initialize logger
_logger = setup_logger(name="DEBUG", log_file="logs/debug.log")


class Logger:
    def log(self, level, message, frame=None, _object=None):
        """Generic logging method that adds frame information if provided"""
        frame_info = ""
        if frame:
            frame_info = f" [{frame.f_code.co_filename}:{frame.f_lineno}]"

        log_message = f"{frame_info}: {message}"

        if level == logging.DEBUG:
            _logger.debug(log_message)
        elif level == logging.INFO:
            _logger.info(log_message)
        elif level == logging.WARNING:
            _logger.warning(log_message)
        elif level == logging.ERROR:
            _logger.error(log_message)
        elif level == logging.CRITICAL:
            _logger.critical(log_message)

    def debug(self, message, frame=None, _object=None):
        """Log a debug message"""
        self.log(logging.DEBUG, message, frame, _object)

    def info(self, message, frame=None, _object=None):
        """Log an info message"""
        self.log(logging.INFO, message, frame, _object)

    def warning(self, message, frame=None, _object=None):
        """Log a warning message"""
        self.log(logging.WARNING, message, frame, _object)

    def error(self, message, frame=None, _object=None):
        """Log an error message"""
        self.log(logging.ERROR, message, frame, _object)

    def critical(self, message, frame=None, _object=None):
        """Log a critical message"""
        self.log(logging.CRITICAL, message, frame, _object)


logger = Logger()


def sync_logged_method(method):
    @wraps(method)
    def wrapper(*args, **kwargs):
        frame = inspect.currentframe().f_back

        logger.debug(f"""Starting {method.__name__}...""", frame=frame)
        try:
            result = method(*args, **kwargs)

            logger.debug(
                f"""Successfully executed {method.__name__}.""",
                # object={
                # 	"args": args,
                # 	"kwargs": kwargs,
                # 	"result": result
                # }
                frame=frame,
            )

            return result
        except Exception as exception:
            logger.debug(
                f"""Exception raised in {method.__name__}: {exception}.""",
                # object={
                # 	"args": args,
                # 	"kwargs": kwargs,
                # 	"exception": exception
                # }
                frame=frame,
            )

            raise

    return wrapper


def async_logged_method(method):
    @wraps(method)
    async def wrapper(*args, **kwargs):
        frame = inspect.currentframe().f_back

        logger.debug(f"""Starting {method.__name__}...""", frame=frame)
        try:
            result = await method(*args, **kwargs)

            logger.debug(
                f"""Successfully executed {method.__name__}.""",
                # object={
                # 	"args": args,
                # 	"kwargs": kwargs,
                # 	"result": result
                # }
                frame=frame,
            )

            return result
        except Exception as exception:
            logger.debug(
                f"""Exception raised in {method.__name__}: {exception}.""",
                # object={
                # 	"args": args,
                # 	"kwargs": kwargs,
                # 	"exception": exception
                # }
                frame=frame,
            )

            raise

    return wrapper


def logged_class(cls):
    for attr, method in cls.__dict__.items():
        if callable(method):
            if asyncio.iscoroutinefunction(method):
                setattr(cls, attr, async_logged_method(method))
            else:
                setattr(cls, attr, sync_logged_method(method))

    return cls
