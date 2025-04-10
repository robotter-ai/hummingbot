import asyncio
import inspect
import logging
import traceback
from functools import wraps
from logging import DEBUG
from pathlib import Path
from typing import Any

from core.properties import properties
from core.telegram.telegram import telegram
from core.utils import dump, escape_html
from singleton.singleton import ThreadSafeSingleton


@ThreadSafeSingleton
class Logger(object):
    def __init__(self):
        self.level = properties.get("logging.level")
        self.levels = properties.get("logging.levels")
        self.telegram_level: bool = properties.get("telegram.level")
        self.use_telegram: bool = properties.get("logging.use_telegram")

        directory = properties.get("logging.directory")
        Path(directory).mkdir(parents=True, exist_ok=True)

        format = properties.get("logging.format")

        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)

        for level in self.levels:
            file_handler = logging.FileHandler(f"{directory}/{str(logging.getLevelName(level)).lower()}.log", mode="a")
            file_handler.setLevel(level)

            # Create a filter to only log messages of a specific level
            class SpecificLevelFilter(logging.Filter):
                def __init__(self, level):
                    super().__init__()
                    self.__level = level

                def filter(self, logRecord):
                    return logRecord.levelno == self.__level

            file_handler.addFilter(SpecificLevelFilter(level))
            file_handler.setFormatter(logging.Formatter(format))
            logger.addHandler(file_handler)

        file_handler = logging.FileHandler(f"{directory}/all.log", mode="a")
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(logging.Formatter(format))
        logger.addHandler(file_handler)

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(logging.Formatter(format))
        stream_handler.setLevel(self.level)
        logger.addHandler(stream_handler)

    def log(self, level: int, message: str = "", object: Any = None, prefix: str = "", frame: Any = None):
        if not frame:
            frame = inspect.currentframe().f_back

        filename = frame.f_code.co_filename.removeprefix(f"""{properties.get("root_path")}/""")
        line_number = frame.f_lineno
        function_name = frame.f_code.co_name

        if object:
            message = f"{message}:\n{dump(object)}"

        message = f"{prefix} {filename}:{line_number} {function_name}: {message}"

        logging.log(level, message)

        if self.use_telegram and level >= self.level and level >= self.telegram_level:
            if level >= logging.ERROR and not "/cc " in message:
                message += f"\n/cc {telegram.admins}"

            message = escape_html(message)

            telegram.send(message)

    def ignore_exception(self, exception: Exception, prefix: str = "", frame=inspect.currentframe().f_back):
        formatted_exception = traceback.format_exception(type(exception), exception, exception.__traceback__)
        formatted_exception = "\n".join(formatted_exception)

        message = f"""Ignored exception: {type(exception).__name__} {str(exception)}:\n{formatted_exception}"""

        self.log(logging.ERROR, prefix=prefix, message=message, frame=frame)


logger = Logger.instance()


# class Logger:
#     # noinspection PyMethodMayBeStatic
#     def debug(self, message, frame=None, _object=None):
#         frame_info = ""
#         if frame:
#             frame_info = f" [{frame.f_code.co_filename}:{frame.f_lineno}]"
#
#         with open("logs/logs_amm_portfolio_manager.log", "a") as log_file:
#             print(f"DEBUG{frame_info}: {message}", file=log_file)
#
#
# logger = Logger()


def automatic_retry_with_timeout(retries=1, delay=0, timeout=None):
    def decorator(function):
        async def wrapper(*args, **kwargs):
            errors = []
            number_of_retries = range(1, retries + 1)
            for i in range(retries):
                try:
                    result = await asyncio.wait_for(function(*args, **kwargs), timeout=timeout)

                    return result
                except Exception as exception:
                    if i == number_of_retries:
                        error = traceback.format_exception(exception)
                    else:
                        error = str(exception)

                    errors.append("".join(error))

                    await asyncio.sleep(delay)

            error_message = f"Function failed after {retries} attempts. Here are the errors:\n" + "\n".join(errors)

            raise Exception(error_message)

        return wrapper

    return decorator


def log_function_call(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        from core.logger import logger

        frame = inspect.currentframe().f_back

        # fully_qualified_name = f"{func.__module__}.{func.__qualname__}"
        fully_qualified_name = func.__qualname__

        logger.log(logging.DEBUG, f"{fully_qualified_name} input", {"args": args, "kwargs": kwargs}, frame=frame)

        try:
            output = func(*args, **kwargs)

            logger.log(logging.DEBUG, f"{fully_qualified_name} output", output, frame=frame)
            return output
        except Exception as exception:
            logger.log(logging.DEBUG, f"{fully_qualified_name} exception", exception, frame=frame)

            raise

    return wrapper


def log_function_exception(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        from core.logger import logger

        frame = inspect.currentframe().f_back
        fully_qualified_name = func.__qualname__

        try:
            return func(*args, **kwargs)
        except Exception as exception:
            formatted_exception = traceback.format_exception(type(exception), exception, exception.__traceback__)
            formatted_exception = "\n".join(formatted_exception)

            logger.log(logging.DEBUG, f"{fully_qualified_name} input", {"args": args, "kwargs": kwargs}, frame=frame)
            logger.log(logging.DEBUG, f"{fully_qualified_name} exception", formatted_exception, frame=frame)
            raise

    return wrapper


def log_class_exceptions(cls):
    for name, method in inspect.getmembers(cls, inspect.isfunction):
        setattr(cls, name, log_function_exception(method))

    return cls


def log(level: int, message: str = "", object: Any = None):
    from core.logger import logger

    logger.log(level=level, message=message, object=object, frame=inspect.currentframe().f_back.f_back)


def sync_logged_method(method):
    @wraps(method)
    def wrapper(*args, **kwargs):
        frame = inspect.currentframe().f_back

        # fully_qualified_name = f"{func.__module__}.{func.__qualname__}"
        fully_qualified_name = func.__qualname__

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
