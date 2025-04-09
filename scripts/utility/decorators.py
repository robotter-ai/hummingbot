import asyncio
import inspect
from functools import wraps


class Logger:
    # noinspection PyMethodMayBeStatic
    def debug(self, message, frame=None, _object=None):
        frame_info = ""
        if frame:
            frame_info = f" [{frame.f_code.co_filename}:{frame.f_lineno}]"

        with open("logs/logs_amm_portfolio_manager.log", "a") as log_file:
            print(f"DEBUG{frame_info}: {message}", file=log_file)


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
