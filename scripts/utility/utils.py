import asyncio
import inspect
import logging
import time
import traceback
from functools import wraps
from pathlib import Path
from typing import Any, Dict


def dump(target: Any):
    try:
        if isinstance(target, str):
            return target

        if isinstance(target, Dict):
            return str(target)

        return str(target)
    except (Exception,):
        return target


class Logger(object):
    def __init__(
        self,
        path: str = "logs/logs_hummingbot.log",
        level: int = logging.DEBUG,
        format: str = "%(asctime)s %(levelname)s %(message)s",
    ):
        self._root_path = Path(__file__).parent.parent.parent

        logger = logging.getLogger()

        logger.setLevel(level)

        # file_handler = logging.FileHandler(path, mode="a")
        # file_handler.setLevel(level)
        # file_handler.setFormatter(logging.Formatter(format))
        # logger.addHandler(file_handler)

        # stream_handler = logging.StreamHandler()
        # stream_handler.setFormatter(logging.Formatter(format))
        # stream_handler.setLevel(level)
        # logger.addHandler(stream_handler)

    def log(self, level: int, message: str = "", object: Any = None, prefix: str = "", frame: Any = None):
        if not frame:
            frame = inspect.currentframe().f_back

        filename = frame.f_code.co_filename.removeprefix(f"""{self._root_path}/""")
        line_number = frame.f_lineno
        function_name = frame.f_code.co_name

        if object:
            message = f"{message}:\n{dump(object)}"

        message = f"{prefix} {filename}:{line_number} {function_name}: {message}\n\n"

        logging.log(level, message)

    def debug(self, message: str = "", object: Any = None, prefix: str = "", frame: Any = None):
        self.log(logging.DEBUG, message, object, prefix, frame)

    def info(self, message: str = "", object: Any = None, prefix: str = "", frame: Any = None):
        self.log(logging.INFO, message, object, prefix, frame)

    def warning(self, message: str = "", object: Any = None, prefix: str = "", frame: Any = None):
        self.log(logging.WARNING, message, object, prefix, frame)

    def error(self, message: str = "", object: Any = None, prefix: str = "", frame: Any = None):
        self.log(logging.ERROR, message, object, prefix, frame)

    def critical(self, message: str = "", object: Any = None, prefix: str = "", frame: Any = None):
        self.log(logging.CRITICAL, message, object, prefix, frame)

    def ignore_exception(
        self, exception: Exception, message: str = "", prefix: str = "", frame=inspect.currentframe().f_back
    ):
        formatted_exception = traceback.format_exception(type(exception), exception, exception.__traceback__)
        formatted_exception = "\n".join(formatted_exception)

        message = f"""{message.join("\n") if message else ""}Ignored exception: {type(exception).__name__} {str(exception)}:\n{formatted_exception}"""

        self.log(logging.ERROR, prefix=prefix, message=message, frame=frame)


def run_with_retry_and_timeout(retries=1, delay=0, timeout=None):
    def decorator(function):
        async def wrapper(*args, **kwargs):
            errors = []
            number_of_retries = range(1, retries + 2)
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


def sync_logged_method(method, logger: Logger):
    @wraps(method)
    def wrapper(*args, **kwargs):
        frame = inspect.currentframe().f_back

        # fully_qualified_name = f"{func.__module__}.{func.__qualname__}"
        fully_qualified_name = method.__qualname__

        logger.debug(f"""Starting {fully_qualified_name}...""", {"args": args, "kwargs": kwargs}, frame=frame)

        try:
            result = method(*args, **kwargs)

            logger.debug(
                f"""Successfully executed {fully_qualified_name}.""",
                object={
                    # "args": args,
                    # "kwargs": kwargs,
                    "result": result
                },
                frame=frame,
            )

            return result
        except Exception as exception:
            formatted_exception = traceback.format_exception(type(exception), exception, exception.__traceback__)
            formatted_exception = "\n".join(formatted_exception)

            logger.debug(
                f"Exception raised in {fully_qualified_name}: {exception}\n{formatted_exception}",
                object={
                    # "args": args,
                    # "kwargs": kwargs,
                    # "exception": exception
                },
                frame=frame,
            )

            raise

    return wrapper


def async_logged_method(method, logger: Logger):
    @wraps(method)
    async def wrapper(*args, **kwargs):
        frame = inspect.currentframe().f_back

        # fully_qualified_name = f"{func.__module__}.{func.__qualname__}"
        fully_qualified_name = method.__qualname__

        logger.debug(f"""Starting {fully_qualified_name}...""", {"args": args, "kwargs": kwargs}, frame=frame)

        try:
            result = await method(*args, **kwargs)

            logger.debug(
                f"""Successfully executed {fully_qualified_name}.""",
                object={
                    # "args": args,
                    # "kwargs": kwargs,
                    "result": result
                },
                frame=frame,
            )

            return result
        except Exception as exception:
            formatted_exception = traceback.format_exception(type(exception), exception, exception.__traceback__)
            formatted_exception = "\n".join(formatted_exception)

            logger.debug(
                f"Exception raised in {fully_qualified_name}: {exception}\n{formatted_exception}",
                object={
                    # "args": args,
                    # "kwargs": kwargs,
                    # "exception": exception
                },
                frame=frame,
            )

            raise

    return wrapper


def logged_class(
    cls=None, logger: Logger = None, allowed_methods: list[str] = None, disallowed_methods: list[str] = None
):
    def decorator(cls):
        for attr, method in cls.__dict__.items():
            if not callable(method):
                continue

            # Skip if method is in disallowed list
            if disallowed_methods and attr in disallowed_methods:
                continue

            # Skip if allowed methods are specified and method is not in allowed list
            if allowed_methods and attr not in allowed_methods:
                continue

            if asyncio.iscoroutinefunction(method):
                setattr(cls, attr, async_logged_method(method, logger))
            else:
                setattr(cls, attr, sync_logged_method(method, logger))
        return cls

    # If called with @logged_class
    if cls is not None:
        return decorator(cls)

    # If called with @logged_class(logger=...)
    return decorator


class CacheManager:
    _cache = {}

    @classmethod
    def get_cache_key(cls, function, args, kwargs):
        args_key = str(args)
        kwargs_key = str(sorted(kwargs.items()))
        return f"{function.__qualname__}:{args_key}:{kwargs_key}"

    @classmethod
    def get_cached_value(cls, key):
        if key in cls._cache:
            cached_data = cls._cache[key]
            if cached_data["expiration_time"] > time.time():
                return cached_data["value"]
            del cls._cache[key]
        return None

    @classmethod
    def set_cached_value(cls, key, value, ttl):
        cls._cache[key] = {"value": value, "expiration_time": time.time() + ttl}

    @classmethod
    def clear_cache(cls):
        cls._cache.clear()


def cached(ttl: int = 60):
    def decorator(function):
        @wraps(function)
        async def async_wrapper(*args, **kwargs):
            force_refresh = kwargs.pop("force_refresh", False)
            cache_key = CacheManager.get_cache_key(function, args, kwargs)

            if not force_refresh:
                cached_value = CacheManager.get_cached_value(cache_key)
                if cached_value is not None:
                    return cached_value

            result = await function(*args, **kwargs)
            CacheManager.set_cached_value(cache_key, result, ttl)
            return result

        @wraps(function)
        def sync_wrapper(*args, **kwargs):
            force_refresh = kwargs.pop("force_refresh", False)
            cache_key = CacheManager.get_cache_key(function, args, kwargs)

            if not force_refresh:
                cached_value = CacheManager.get_cached_value(cache_key)
                if cached_value is not None:
                    return cached_value

            result = function(*args, **kwargs)
            CacheManager.set_cached_value(cache_key, result, ttl)
            return result

        if asyncio.iscoroutinefunction(function):
            return async_wrapper
        return sync_wrapper

    return decorator
