"""
AMM Portfolio Manager Base Strategy

This module provides the base class for AMM arbitrage strategies, implementing common
functionality for portfolio management, data updates, and arbitrage execution.

Key Features:
  - Common database structure and management
  - Shared portfolio tracking and profit calculation
  - Unified data update mechanisms
  - Abstract arbitrage execution methods
  - Common utility functions and helpers
"""

import asyncio
import inspect
import json
import logging
import os
import time
import traceback
from abc import ABC, abstractmethod
from decimal import Decimal
from enum import Enum
from functools import wraps
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from pydantic.v1 import Field, validator

from hummingbot.client.config.config_data_types import BaseClientModel
from hummingbot.client.settings import GatewayConnectionSetting
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.in_flight_order import OrderState
from hummingbot.core.event.events import TradeType
from hummingbot.core.gateway.gateway_http_client import GatewayHttpClient
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase

# ==============================================================================
# Constants
# ==============================================================================
DECIMAL_ZERO = Decimal("0")
DECIMAL_ONE_PERCENT = Decimal("0.01")
DECIMAL_TEN_PERCENT = Decimal("0.1")
DECIMAL_TWENTY_FIVE_PERCENT = Decimal("0.25")
DECIMAL_FIFTY_PERCENT = Decimal("0.5")
DECIMAL_SEVENTY_FIVE_PERCENT = Decimal("0.75")
DECIMAL_ONE_HUNDRED_PERCENT = Decimal("1")
DECIMAL_ONE = Decimal("1")
DECIMAL_ONE_HUNDRED = Decimal("100")
DECIMAL_NOT_A_NUMBER = Decimal("NaN")
DECIMAL_POSITIVE_INFINITY = Decimal("Infinity")
DECIMAL_NEGATIVE_INFINITY = Decimal("-Infinity")

REQUEST_RETRIES = 3
REQUEST_DELAY = 1  # seconds
REQUEST_TIMEOUT = 60  # seconds

SWAP_OPERATION_RETRIES = 60

LOCK_ACQUISITION_TIMEOUT = 5  # seconds


# ==============================================================================
# Global Configuration and Database Schema
# ==============================================================================

# noinspection SpellCheckingInspection
configuration: Dict[str, Any] = {
    "globals": {
        "maximum_slippage_percentage": None,  # Maximum allowed price slippage when executing trades (Ex.: 0.5 for 0.5%)
        "minimum_profitability_percentage": None,  # Minimum profit threshold required to execute an arbitrage (Ex.: 1 for 1%)
        "arbitrage_check_interval_seconds": "60",  # Time interval between subsequent arbitrage opportunity checks
        "minimum_trade_amount": None,  # Minimum trade amount
        "maximum_trade_amount": None,  # Maximum trade amount
        "time_delay_between_arbitrages": "1",  # Delay in seconds between consecutive arbitrage executions
        "transaction_confirmation_delay": "2",  # Delay in seconds before checking for transaction confirmation
        "balance_update_delay": "3",  # Delay in seconds to wait for wallet balances to update after a trade
        "transaction_polling_interval": "2",  # Time interval between checks for transaction confirmation status
        "data_update_intervals": {
            "wallet": "60",  # Update wallet balance data every x seconds
            "token": "60",  # Update token price data every x seconds
            "pool": "60",  # Update pool statistics data every x seconds
        },
        "use_async_data_updates": False,  # Whether to update data asynchronously or synchronously
        "main_quote_token": "USDT",  # Main quote token used for price references
        "strategy_type": "CONNECTORS_ARBITRAGE",  # Strategy type identifier for arbitrage strategy
    },
    "connections": {
        # "hydration": {
        #     "mainnet": {
        #         "hydration": {
        #             "native_token_symbol": "HDX",
        #             "fee_payment_token_symbol": "HDX",
        #             "wallets": [
        #                 os.environ["HYDRATION_MAINNET_WALLET_ADDRESS"]
        #             ],
        #             "pools": [],
        #         }
        #     },
        # },
        # "solana": {
        #     "mainnet-beta": {
        #         "raydium": {
        #             "native_token_symbol": "SOL",
        #             "fee_payment_token_symbol": "SOL",
        #             "wallets": [
        #                 os.environ["SOLANA_MAINNET_BETA_RAYDIUM_WALLET_ADDRESS"]
        #             ],
        #             "pools": [],
        #         }
        #     }
        # },
    },
    "token_pairs": [],  # List of token pairs to track
    "token_triads": ["USDC/HDX/USDT"],  # List of token triads to track
}


def dump(target: Any):
    try:
        if isinstance(target, str):
            return target

        # noinspection PyBroadException,PyUnusedLocal
        try:
            if isinstance(target, Dict):
                return json.dumps(target, indent=2, sort_keys=True, check_circular=True)
        except (Exception,):
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
            self, exception: BaseException, message: str = "", prefix: str = "", frame=inspect.currentframe().f_back
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


# ==============================================================================
# Enumerators
# ==============================================================================

class StrategyType(Enum):
    """Enum for different types of arbitrage strategies"""
    CONNECTORS_ARBITRAGE = "CONNECTORS_ARBITRAGE"  # Arbitrage between different connectors
    TRIANGULAR_ARBITRAGE = "TRIANGULAR_ARBITRAGE"  # Triangular arbitrage inside the same connector

    @staticmethod
    def get_by_id(identifier: str):
        for strategy_type in StrategyType:
            if strategy_type.value == identifier:
                return strategy_type

        raise ValueError(f"Unknown strategy type: {identifier}")


class PoolType(Enum):
    """Enum for different types of liquidity pools"""

    # Hydration
    XYK = "Xyk"
    STABLE = "Stableswap"
    OMNIPOOL = "Omnipool"
    LBP = "Lbp"

    # Raydium
    AMM = "amm"
    CPMM = "cpmm"
    CLMM = "clmm"

    UNKNOWN = "unknown"


# ==============================================================================
# Database Lock Context Manager
# ==============================================================================
class DatabaseLock:
    """
    Simplified context manager for database with timeout.
    """

    # noinspection PyShadowingNames
    def __init__(self, lock: asyncio.Lock, logger: logging.Logger, caller: str):
        self.lock = lock
        self.logger = logger
        self.caller = caller
        self._acquired = False

    async def __aenter__(self) -> bool:
        """Acquires the lock with timeout."""
        try:
            self._acquired = await asyncio.wait_for(self.lock.acquire(), timeout=LOCK_ACQUISITION_TIMEOUT)

            return self._acquired
        except asyncio.TimeoutError:
            self.logger.warning(f"Timeout while trying to acquire lock for {self.caller}")

            return False
        except Exception as e:
            self.logger.error(f"Error acquiring lock for {self.caller}: {str(e)}")

            return False

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Releases the lock."""
        if self._acquired:
            try:
                self.lock.release()
                self._acquired = False
            except Exception as e:
                self.logger.error(f"Error releasing lock from {self.caller}: {str(e)}")
                self._acquired = False

    @property
    def is_acquired(self) -> bool:
        """Checks if the lock is currently acquired."""
        return self._acquired


# ==============================================================================
# Pydantic Models for Strategy Configuration
# ==============================================================================
class WalletConfig(BaseClientModel):
    """Model representing wallet configuration."""

    address: str = Field(...)


class PoolConfig(BaseClientModel):
    """Model representing pool configuration."""

    address: str = Field(...)


class ConnectorConfig(BaseClientModel):
    """Model for connector configuration."""

    wallets: List[str] = Field(default_factory=list)
    pools: List[str] = Field(default_factory=list)


class NetworkConfig(BaseClientModel):
    """Model for network configuration."""

    connectors: Dict[str, ConnectorConfig] = Field(default_factory=dict)


class ChainConfig(BaseClientModel):
    """Model for chain configuration."""

    networks: Dict[str, NetworkConfig] = Field(default_factory=dict)


class DataUpdateIntervals(BaseClientModel):
    """Update intervals for dynamic data."""

    wallet: int = Field(default=60)
    token: int = Field(default=60)
    pool: int = Field(default=60)

    # noinspection PyMethodParameters
    @validator("wallet", "token", "pool", allow_reuse=True)
    def validate_positive_interval(cls, value):
        if value <= 0:
            raise ValueError("Update interval must be positive")
        return value


class GlobalConfig(BaseClientModel):
    """Global parameters for the strategy."""

    maximum_slippage_percentage: Decimal = Field(default=Decimal("0.5"))
    minimum_profitability_percentage: Decimal = Field(default=DECIMAL_ONE)
    arbitrage_check_interval_seconds: int = Field(default=60)
    minimum_trade_amount: Decimal = Field(default=Decimal("0.1"))
    maximum_trade_amount: Decimal = Field(default=Decimal("10"))
    time_delay_between_arbitrages: int = Field(default=1)
    transaction_confirmation_delay: int = Field(default=1)
    balance_update_delay: int = Field(default=3)
    transaction_polling_interval: int = Field(default=1)
    data_update_intervals: DataUpdateIntervals = Field(default_factory=DataUpdateIntervals)
    use_async_data_updates: bool = Field(default=True)
    main_quote_token: str = Field(default="USDC")
    strategy_type: str = Field(default=StrategyType.CONNECTORS_ARBITRAGE.value)


class AMMPortfolioManagerConfiguration(BaseClientModel):
    """
    Configuration model for the AMM Portfolio Manager strategy.

    Mirrors the global configuration, connections and token list.
    """

    script_file_name: str = Field(default_factory=lambda: os.path.basename(__file__))
    globals: GlobalConfig = Field(default_factory=GlobalConfig)
    chains: Dict[str, ChainConfig] = Field(default_factory=dict)
    token_pairs: List[str] = Field(default_factory=list)
    token_triads: List[str] = Field(default_factory=list)

    class Config:
        arbitrary_types_allowed = True


# ==============================================================================
# Logger Initialization
# ==============================================================================
logger = Logger(path="logs/logs_amm_portfolio_base.log", level=logging.DEBUG)
# TODO - HANDLE LOG INSTANCE CORRECTLY !!!


# ==============================================================================
# AMMPortfolioManagerBase Strategy Class
# ==============================================================================
# @logged_class(logger=logger, disallowed_methods=["on_tick"])
class AMMPortfolioManagerBase(ScriptStrategyBase, ABC):
    """
    AMM Portfolio Manager Base Strategy

    This is the base class for AMM arbitrage strategies, providing common functionality
    for portfolio management, data updates, and arbitrage execution. Specific arbitrage
    strategies should inherit from this class and implement their own arbitrage execution
    methods.

    The base class handles:
    - Database initialization and management
    - Portfolio tracking and profit calculation
    - Data updates for wallets, tokens, and pools
    - Common utility functions and helpers
    - Abstract arbitrage execution methods
    """

    # Configuration attributes
    # Example structure)
    # database: Dict[str, Any] = {
    #     "connections": {
    #         "hydration": {
    #             "mainnet": {
    #                 "hydration": {
    #                     "wallets": {
    #                         "<wallet_address>": {
    #                             "internal_id": "<chain>/<network>/<connector>/<wallet_address>",
    #                             "chain": "<chain>",
    #                             "network": "<network>",
    #                             "connector": "<connector>",
    #                             "tokens": {
    #                                 "<token_symbol>": {
    #                                     "balances": {
    #                                         "free": "<free_token_balance>",
    #                                         "locked": {
    #                                             "total": "<locked_token_balance>",
    #                                             "liquidity": {
    #                                                 "total": "<liquidity_token_balance>",
    #                                                 "pools": {
    #                                                     "<pool_address>": "<pool_token_balance>"
    #                                                 }
    #                                             }
    #                                         },
    #                                         "total": "<token_balance>"
    #                                     }
    #                                 }
    #                             },
    #                             "pools": {
    #                                 "<pool_address>": {
    #                                     "shares": "<pool_shares>",
    #                                     "token_list": ["token_1_symbol", "token_2_symbol"],
    #                                     "tokens": {
    #                                         "<token_1_symbol>": {
    #                                             "balance": "<pool_token_1_balance>",
    #                                             "prices": {
    #                                                 "<token_2_symbol>": "<token_2_price_relative_to_token_1>",
    #                                             },
    #                                         },
    #                                         "<token_2_symbol>": {
    #                                             "balance": "<pool_token_2_balance>",
    #                                             "prices": {
    #                                                 "<token_1_symbol>": "<token_1_price_relative_to_token_2>",
    #                                             },
    #                                         },
    #                                     },
    #                                     "impermanent_loss": "<pool_impermanent_loss>",
    #                                 }
    #                             }
    #                         }
    #                     },
    #                     "tokens": {
    #                         "<token_symbol>": {
    #                             "internal_id": "<chain>/<network>/<connector>/<token_address>",
    #                             "address": "<token_address>",
    #                             "chain": "<chain>",
    #                             "network": "<network>",
    #                             "connector": "<connector>",
    #                             "symbol": "<token_symbol>",
    #                             "name": "<token_name>",
    #                             "decimals": "<token_decimals>",
    #                             "price": "<token_price>"
    #                         }
    #                     },
    #                     "pools": {
    #                         "<pool_address>": {
    #                             "internal_id": "<chain>/<network>/<connector>/<pool_address>",
    #                             "address": "<pool_address>",
    #                             "chain": "<chain>",
    #                             "network": "<network>",
    #                             "connector": "<connector>",
    #                             "type": "<pool_type>",
    #                             "tokens_list": ["token_1_symbol", "token_2_symbol"],
    #                             "tokens": {
    #                                 "<token_1_symbol>": {
    #                                     "price": "<token_1_price>",
    #                                 },
    #                                 "<token_2_symbol>": {
    #                                     "price": "<token_2_price>",
    #                                 },
    #                             },
    #                             "annual_percentage_rate": "<pool_annual_percentage_rate>",
    #                             "total_value_locked": "<pool_total_value_locked>",
    #                             "volume": {
    #                                 "24h": "<pool_24h_volume>",
    #                             }
    #                         }
    #                     }
    #                 },
    #             }
    #         }
    #     },
    #     "arbitrage_opportunities": [],
    #     "execution_history": [],
    #     "maps": {
    #         "pools_by_tokens": {},  # Format: "token1/token2" -> [pool_internal_id1, pool_internal_id2, ...]
    #         "wallets_by_pool": {},  # Format: pool_internal_id -> [wallet_internal_id1, wallet_internal_id2, ...]
    #         "pools_by_wallet": {},  # Format: wallet_internal_id -> [pool_internal_id1, pool_internal_id2, ...]
    #     }
    # }
    _database: Dict[str, Any] = {}
    _configuration: Optional[Dict[str, Any]] = None
    _gateway_is_ready: bool = False
    _gateway_http_client: Optional[GatewayHttpClient] = None
    _all_gateway_connections: List[Dict[str, Any]] = []
    _logger: Optional[Logger] = None

    # Strategy parameters
    _last_arbitrage_check_time: float = 0
    _maximum_slippage_percentage: Decimal = DECIMAL_ZERO
    _minimum_profitability_percentage: Decimal = DECIMAL_ZERO
    _arbitrage_check_interval_seconds: int = 60
    _minimum_trade_amount: Decimal = DECIMAL_ZERO
    _maximum_trade_amount: Decimal = DECIMAL_ZERO
    _time_delay_between_arbitrages: int = 1
    _transaction_confirmation_delay: int = 1
    _balance_update_delay: int = 3
    _transaction_polling_interval: int = 1
    _maximum_transaction_confirmation_timeout: int = 60
    _strategy_type: StrategyType = None

    # Data update control
    _data_update_task: Optional[asyncio.Task] = None
    _data_update_intervals: Dict[str, int] = {"wallet": 60, "token": 60, "pool": 60}
    _last_wallet_update_time: float = 0
    _last_token_update_time: float = 0
    _last_pool_update_time: float = 0
    _use_async_data_updates: bool = True
    _main_quote_token: str = "USDC"

    # Token data storage
    _token_pairs: List[str] = []
    _token_triads: List[str] = []
    _token_symbols: List[str] = []

    # State control
    _initialized: bool = False  # Indicates if database was initialized
    _initializing: bool = False  # Indicates if database is being initialized
    _is_running: bool = False  # Indicates if an update is in progress
    _database_lock: asyncio.Lock = None

    def __init__(self, connectors: Dict[str, ConnectorBase], configuration: Dict[str, Any], logger: Optional[Logger] = None):
        """
        Initializes the strategy: loads configuration and prepares database.

        Args:
            connectors: Dictionary of available connectors.
        """
        logger.info("Initializing AMM Portfolio Manager strategy")

        super().__init__(connectors)

        self._configuration = configuration
        self._logger = logger
        self._database_lock = asyncio.Lock()

    async def _initialize(self):
        """
        Configures gateway client, database structure and asynchronous updates.
        """
        logger.info("Initializing database")

        try:
            self._initializing = True

            self._database: Dict[str, Any] = {
                "connections": {},
                "arbitrage_opportunities": [],
                "execution_history": [],
                "maps": {
                    "pools_by_tokens": {},  # "token1/token2" -> list of pool internal IDs
                    "wallets_by_pool": {},  # pool internal ID -> list of wallet internal IDs
                    "pools_by_wallet": {},  # wallet internal ID -> list of pool internal IDs
                },
            }

            self._database_lock = asyncio.Lock()

            # Gateway client initialization
            self._all_gateway_connections = GatewayConnectionSetting.load()
            self._gateway_http_client = GatewayHttpClient.get_instance()

            # Load configuration
            global_configurations = self._configuration.get("globals", {})

            # Strategy parameters
            self._maximum_slippage_percentage = Decimal(global_configurations["maximum_slippage_percentage"])
            self._minimum_profitability_percentage = Decimal(global_configurations["minimum_profitability_percentage"])
            self._arbitrage_check_interval_seconds = int(global_configurations["arbitrage_check_interval_seconds"])
            self._minimum_trade_amount = Decimal(global_configurations["minimum_trade_amount"])
            self._maximum_trade_amount = Decimal(global_configurations["maximum_trade_amount"])
            self._time_delay_between_arbitrages = int(global_configurations["time_delay_between_arbitrages"])
            self._transaction_confirmation_delay = int(global_configurations["transaction_confirmation_delay"])
            self._balance_update_delay = int(global_configurations["balance_update_delay"])
            self._transaction_polling_interval = int(global_configurations["transaction_polling_interval"])
            self._use_async_data_updates = bool(global_configurations["use_async_data_updates"])
            self._main_quote_token = global_configurations["main_quote_token"]
            self._strategy_type = StrategyType.get_by_id(global_configurations.get("strategy_type", StrategyType.CONNECTORS_ARBITRAGE).value)

            # Configure update intervals
            data_update_intervals = global_configurations["data_update_intervals"]
            if data_update_intervals:
                self._data_update_intervals = {
                    "wallet": int(data_update_intervals["wallet"]),
                    "token": int(data_update_intervals["token"]),
                    "pool": int(data_update_intervals["pool"]),
                }

            # Configure transaction confirmation timeout
            if self._transaction_confirmation_delay > 0:
                self._maximum_transaction_confirmation_timeout = self._transaction_confirmation_delay * 5

            self._token_pairs = self._configuration.get("token_pairs", [])
            self._token_triads = self._configuration.get("token_triads", [])
            self._token_symbols = self._get_all_token_symbols()

            await self._check_gateway_status()

            # Initialize database
            await self._initialize_database_structure()

            # Start update task if configured
            if self._use_async_data_updates:
                self._start_data_update_task()

            logger.info("Database initialized")
        except Exception as exception:
            logger.info("Database initialization failed")

            raise exception
        finally:
            self._initializing = False

    def _start_data_update_task(self):
        """Starts the asynchronous data update task."""
        if self._data_update_task is not None:
            self._data_update_task.cancel()
        self._data_update_task = asyncio.ensure_future(self._run_data_update_loop())
        logger.info("Data update task started")

    async def _run_data_update_loop(self):
        """
        Asynchronous loop to update dynamic data with appropriate intervals.
        Avoids excessive updates by respecting configured time intervals.
        """
        try:
            # Variables for time control between updates
            last_update_time = 0
            minimum_update_interval = min(self._data_update_intervals.values())

            while True:
                current_time = time.time()
                time_since_last_update = current_time - last_update_time

                # Update database only if minimum interval has passed
                if time_since_last_update >= minimum_update_interval:
                    if self._initialized:
                        try:
                            self._is_running = True
                            await self._update_database()
                            last_update_time = current_time
                        except Exception as exception:
                            logger.ignore_exception(exception, "Error during database update")
                        finally:
                            self._is_running = False
                    else:
                        logger.warning("Database not initialized. Skipping update...")

                # Wait appropriate interval before checking again
                # Use at least 1 seconds or half the smallest configured interval
                sleep_time = max(1.0, minimum_update_interval / 2.0)
                await asyncio.sleep(sleep_time)
        except asyncio.CancelledError as exception:
            logger.ignore_exception(exception, "Data update task cancelled")
        except Exception as exception:
            logger.ignore_exception(exception, "Critical error in update loop")

            raise

    def on_tick(self):
        """Called on each tick to execute the strategy."""
        if not self._initialized and not self._initializing:
            asyncio.ensure_future(self._initialize())

        if self._initialized and not self._is_running:
            # Execute arbitrage strategy at configured interval
            current_time = time.time()
            if current_time - self._last_arbitrage_check_time >= self._arbitrage_check_interval_seconds:
                self._last_arbitrage_check_time = current_time

                asyncio.ensure_future(self._async_on_tick())

    async def _async_on_tick(self):
        """
        Main execution method. The strategy executes only if the database
        has been updated and the gateway is available.
        """
        try:
            logger.info("Starting a new strategy cycle")

            self._is_running = True

            await self._check_gateway_status()

            # Update database only if not using asynchronous updates
            if not self._use_async_data_updates:
                await self._update_database()

            await self._run_arbitrage_strategy()
        except Exception as exception:
            raise exception
        finally:
            logger.info("Strategy cycle completed")

            self._is_running = False

    async def _check_gateway_status(self):
        """
        Verifies the availability of the gateway by sending a ping.
        Sets _gateway_is_ready to True if the gateway responds, otherwise False.
        """
        try:
            while True:
                if not self._gateway_is_ready:
                    ping_result = await self._gateway_ping_gateway()
                    if ping_result:
                        self._gateway_is_ready = True
                    else:
                        self._gateway_is_ready = False
                        logger.warning("Ping of gateway did not return response.")

                    if not self._gateway_is_ready:
                        logger.warning("Gateway not ready. Trying again in 5 seconds...")
                        await asyncio.sleep(5)

                        continue
                else:
                    break
        except Exception as exception:
            self._gateway_is_ready = False
            logger.ignore_exception(exception, "Error during gateway status check")

    async def _run_arbitrage_strategy(self):
        """
        Executes the arbitrage strategy:
         - Discovers promising token pairs or triads based on strategy type
         - Validates each opportunity
         - Executes arbitrage trades for opportunities that pass validation
        """
        logger.info(f"Executing arbitrage strategy: {self._strategy_type.value}")

        # Choose strategy based on configuration
        if self._strategy_type.value == StrategyType.CONNECTORS_ARBITRAGE.value:
            await self._run_connectors_arbitrage()
        elif self._strategy_type.value == StrategyType.TRIANGULAR_ARBITRAGE.value:
            await self._run_triangular_arbitrage()
        else:
            logger.warning(f"Unknown strategy type: {self._strategy_type.value}")

        logger.info("Arbitrage strategy cycle completed.")

    @abstractmethod
    async def _run_connectors_arbitrage(self):
        """
        Abstract method for executing connectors arbitrage strategy.
        This method should be implemented by specific arbitrage strategies
        that handle connectors arbitrage between different connectors.
        """
        pass

    @abstractmethod
    async def _run_triangular_arbitrage(self):
        """
        Abstract method for executing triangular arbitrage strategy.
        This method should be implemented by specific arbitrage strategies
        that handle triangular arbitrage within a single connector.
        """
        pass

    # --------------------------------------------------------------------------
    # Permanent Database Initialization and Mapping Methods
    # --------------------------------------------------------------------------
    async def _initialize_database_structure(self):
        """
        Builds the database structure.
        For each chain, network and connector, stores static information about wallets, tokens and pools.
        """
        if self._initialized:
            logger.info("Database already initialized, skipping...")

            return

        # Initializes basic structure of the database
        for chain_name, chain_configuration in self._configuration.get("connections", {}).items():
            self._database["connections"].setdefault(chain_name, {})

            for network_name, network_configuration in chain_configuration.items():
                self._database["connections"][chain_name].setdefault(network_name, {})

                for connector_name, connector_configuration in network_configuration.items():
                    self._database["connections"][chain_name][network_name].setdefault(
                        connector_name,
                        {
                            "fee_payment_token_symbol": connector_configuration.get("fee_payment_token_symbol"),
                            "fee_payment_token_amount": connector_configuration.get("fee_payment_token_amount"),
                            "native_token_symbol": connector_configuration.get("native_token_symbol"),
                            "wallets": {},
                            "tokens": {},
                            "pools": {},
                        },
                    )

        # Initialize tokens, pools, and wallets in order
        await self._initialize_token_information()
        await self._initialize_pool_information()
        await self._initialize_wallet_information()

        # Initializes database maps
        await self._update_database_maps()

        # Update initial dynamic data
        await self._update_token_information()
        await self._update_pool_information()
        await self._update_wallet_balances()

        # Marks the database as initialized
        self._initialized = True

    async def _initialize_token_information(self):
        """
        Initializes static token information (address, name, decimals)
        for all tokens defined in the configuration.
        """

        for chain_name, chain_configuration in self._configuration.get("connections", {}).items():
            for network_name, network_configuration in chain_configuration.items():
                for connector_name, connector_configuration in network_configuration.items():
                    connection = self._database["connections"][chain_name][network_name][connector_name]

                    # Retrieves token information from the gateway
                    token_response = await self._gateway_get_tokens(
                        chain_name, network_name, self._token_symbols
                    )

                    if token_response and "tokens" in token_response:
                        tokens = token_response["tokens"]

                        # Initialize each found token
                        for token in tokens:
                            token_symbol = token.get("symbol")
                            if token_symbol in self._token_symbols:
                                # Create or update token info with static data
                                connection["tokens"][token_symbol] = {
                                    "internal_id": f"{chain_name}/{network_name}/{connector_name}/{token_symbol}",
                                    "address": token.get("address"),
                                    "chain": chain_name,
                                    "network": network_name,
                                    "connector": connector_name,
                                    "symbol": token_symbol,
                                    "name": token.get("name", token_symbol),
                                    "decimals": token.get("decimals"),
                                    "price": None,  # Price is dynamic and will be updated separately
                                }

    async def _initialize_pool_information(self):
        """
        Initializes pool information by following this approach:
        1. First add pools explicitly specified in the configuration
        2. Then find pools by using the token pairs from the configuration
        3. Update detailed information for all found pools

        All pools are assumed to have exactly 2 tokens.
        """

        # We don't need pools for triangular arbitrage
        if self._strategy_type.value == StrategyType.TRIANGULAR_ARBITRAGE.value:
            return

        for chain_name, chain_configuration in self._configuration.get("connections", {}).items():
            for network_name, network_configuration in chain_configuration.items():
                for connector_name, connector_configuration in network_configuration.items():
                    connection = self._database["connections"][chain_name][network_name][connector_name]
                    pool_addresses = set()

                    # 1. Collect pools from configuration
                    for pool_address in connector_configuration.get("pools", []):
                        pool_addresses.add(pool_address)

                    # 2. Process each token pair
                    for token_pair in self._token_pairs:
                        base_token, quote_token = self._extract_token_symbols_from_token_pair(token_pair)

                        # For Raydium, use the AMM list-pools endpoint with configured pools
                        if connector_name.lower() == "raydium":
                            list_pools_response = await self._gateway_list_pools(
                                connector_name,
                                network_name,
                                [PoolType.AMM.value],
                                [base_token, quote_token],
                                use_config_pools=True,  # Use configured pools for Raydium
                            )
                        else:
                            # For other connectors (like Hydration), keep existing behavior
                            list_pools_response = await self._gateway_list_pools(
                                connector_name,
                                network_name,
                                [PoolType.XYK.value, PoolType.STABLE.value, PoolType.AMM.value],
                                [base_token, quote_token],
                            )

                        if not list_pools_response or not list_pools_response.get("pools"):
                            logger.warning(f"No pools found for {token_pair} on {connector_name} {network_name}")
                            continue

                        # Add found pools to our collection
                        for pool_information in list_pools_response.get("pools", []):
                            pool_addresses.add(pool_information.get("address"))

                    # 3. Update detailed information for all pools
                    for pool_address in pool_addresses:
                        # Get detailed pool information
                        detailed_pool_info = await self._gateway_get_pool_info(
                            connector_name, network_name, pool_address
                        )

                        if not detailed_pool_info:
                            logger.error(f"Failed to retrieve detailed pool information for {pool_address}")
                            continue

                        # Create or update pool with detailed information
                        base_token = self._get_token_by_address(
                            chain_name, network_name, connector_name, detailed_pool_info["baseTokenAddress"]
                        )
                        quote_token = self._get_token_by_address(
                            chain_name, network_name, connector_name, detailed_pool_info["quoteTokenAddress"]
                        )
                        pool_tokens = [base_token.get("symbol"), quote_token.get("symbol")]

                        internal_id = f"{chain_name}/{network_name}/{connector_name}/{pool_address}"

                        # For Raydium, force AMM type
                        pool_type = PoolType.AMM.value if connector_name.lower() == "raydium" else detailed_pool_info.get("poolType")

                        connection["pools"][internal_id] = {
                            "internal_id": internal_id,
                            "address": pool_address,
                            "chain": chain_name,
                            "network": network_name,
                            "connector": connector_name,
                            "type": pool_type,
                            "tokens_list": pool_tokens,
                            "tokens": {},
                            "annual_percentage_rate": None,
                            "total_value_locked": None,
                            "impermanent_loss": None,
                            "volume": {"24h": None},
                        }

                        # Initialize tokens structure in pool
                        for token_symbol in pool_tokens:
                            connection["pools"][internal_id]["tokens"][token_symbol] = {
                                "balance": None,
                                "prices": {},
                            }

    async def _initialize_wallet_information(self):
        """
        Initializes wallet information for all configured wallets,
        setting up the basic structure for tokens and pools.
        """
        for chain_name, chain_configuration in self._configuration.get("connections", {}).items():
            for network_name, network_configuration in chain_configuration.items():
                for connector_name, connector_configuration in network_configuration.items():
                    connection = self._database["connections"][chain_name][network_name][connector_name]

                    # Add wallets from configuration
                    for wallet_address in connector_configuration.get("wallets", []):
                        wallet_internal_id = f"{chain_name}/{network_name}/{connector_name}/{wallet_address}"

                        if wallet_internal_id not in connection["wallets"]:
                            connection["wallets"][wallet_internal_id] = {
                                "internal_id": wallet_internal_id,
                                "address": wallet_address,
                                "chain": chain_name,
                                "network": network_name,
                                "connector": connector_name,
                                "tokens": {},
                                "pools": {},
                            }

                            # Pre-initialize token structures for all configured tokens
                            for token_symbol in self._token_symbols:
                                connection["wallets"][wallet_internal_id]["tokens"][token_symbol] = {
                                    "balances": {
                                        "free": None,  # Dynamic data
                                        "locked": {
                                            "total": None,  # Dynamic data
                                            "liquidity": {
                                                "total": None,  # Dynamic data
                                                "pools": {},  # Dynamic data
                                            },
                                        },
                                        "total": None,  # Dynamic data
                                    }
                                }

                            # Link wallet to pools it's eligible for
                            for pool_internal_id in connection["pools"]:
                                connection["wallets"][wallet_internal_id]["pools"][pool_internal_id] = {
                                    "shares": None,  # Dynamic data
                                    "tokens": {},  # Dynamic data
                                    "impermanent_loss": None,  # Dynamic data
                                }

                                # Link pool to wallet in maps
                                self._database["maps"].setdefault("wallets_by_pool", {}).setdefault(
                                    pool_internal_id, []
                                ).append(wallet_internal_id)
                                self._database["maps"].setdefault("pools_by_wallet", {}).setdefault(
                                    wallet_internal_id, []
                                ).append(pool_internal_id)

    # noinspection PyMethodMayBeStatic
    async def _update_database_maps(self):
        """
        Rebuilds the mapping dictionaries:
         - pools_by_tokens: "token1/token2" -> list of pool internal IDs
         - wallets_by_pool: pool internal ID -> list of wallet internal IDs
         - pools_by_wallet: wallet internal ID -> list of pool internal IDs
        """
        maps = self._database["maps"]
        # Clears existing maps
        maps["pools_by_tokens"].clear()
        maps["wallets_by_pool"].clear()
        maps["pools_by_wallet"].clear()

        # Rebuilds maps
        for chain_name, chain_configuration in self._database["connections"].items():
            for network_name, network_configuration in chain_configuration.items():
                for connector_name, connector_configuration in network_configuration.items():
                    # Maps pools by tokens
                    for pool_internal_id, pool_configuration in connector_configuration["pools"].items():
                        tokens_list = pool_configuration.get("tokens_list", [])
                        for base_token_index in range(len(tokens_list)):
                            for quote_token_index in range(base_token_index + 1, len(tokens_list)):
                                base_token, quote_token = tokens_list[base_token_index], tokens_list[quote_token_index]
                                key = f"{base_token}/{quote_token}"
                                maps["pools_by_tokens"].setdefault(key, []).append(pool_internal_id)

                    # Maps wallets to pools and vice versa
                    for wallet_internal_id, wallet_configuration in connector_configuration["wallets"].items():
                        for pool_internal_id in wallet_configuration.get("pools", {}):
                            maps["pools_by_wallet"].setdefault(wallet_internal_id, []).append(pool_internal_id)
                            maps["wallets_by_pool"].setdefault(pool_internal_id, []).append(wallet_internal_id)

    # --------------------------------------------------------------------------
    # Dynamic Database Update Methods
    # --------------------------------------------------------------------------
    async def _update_database(self):
        """
        Updates dynamic parts of the database:
         - Pool statistics (APR, TVL, volume, token prices)
         - Wallet balances and positions in pools
         - Token information (price, decimals)

        Updates database maps only if any update has been performed.
        """
        logger.info("Updating database")

        try:
            current_time = time.time()

            # Update intervals configured
            wallet_interval = self._data_update_intervals["wallet"]
            token_interval = self._data_update_intervals["token"]
            pool_interval = self._data_update_intervals["pool"]

            # Flag to indicate if any update has been performed
            updates_performed = False

            async with DatabaseLock(self._database_lock, logger, "_update_database") as lock_acquired:
                if not lock_acquired:
                    logger.warning("Unable to acquire lock for update, skipping this cycle...")

                    return

                # Token update
                if (not hasattr(self, "_last_token_update_time")) or (
                    current_time - self._last_token_update_time >= token_interval
                ):
                    self._last_token_update_time = current_time
                    await self._update_token_information()
                    updates_performed = True

                # Pool update
                if (not hasattr(self, "_last_pool_update_time")) or (
                    current_time - self._last_pool_update_time >= pool_interval
                ):
                    self._last_pool_update_time = current_time
                    await self._update_pool_information()
                    updates_performed = True

                # Wallet update
                if (not hasattr(self, "_last_wallet_update_time")) or (
                    current_time - self._last_wallet_update_time >= wallet_interval
                ):
                    self._last_wallet_update_time = current_time
                    await self._update_wallet_balances()
                    updates_performed = True

                # Updates maps only if any update has been performed
                if updates_performed:
                    await self._update_database_maps()

            logger.info("Database updated")
        except Exception as exception:
            logger.error("Database update failed")

            raise exception

    async def _update_pool_information(self):
        """
        Updates dynamic pool statistics (APR, TVL, volume, token prices)
        by querying the gateway. All pools are assumed to have exactly 2 tokens,
        with the first being the base token and the second being the quote token.
        """
        for chain_name, chain_configuration in self._database["connections"].items():
            for network_name, network_configuration in chain_configuration.items():
                for connector_name, connector_configuration in network_configuration.items():
                    for pool_internal_id, pool_configuration in connector_configuration["pools"].items():
                        # Get updated pool information from gateway
                        pool_address = pool_configuration["address"]
                        pool_information = await self._gateway_get_pool_info(connector_name, network_name, pool_address)

                        if not pool_information:
                            raise Exception(f"Failed to retrieve pool information for {pool_internal_id}")

                        # Update dynamic values
                        pool_configuration["annual_percentage_rate"] = pool_information.get("feePct")
                        pool_configuration["total_value_locked"] = pool_information.get("total_value_locked")
                        pool_configuration["volume"]["24h"] = pool_information.get("volume", {}).get("24h")

                        # Get token symbols - assume first is base, second is quote
                        base_token = pool_configuration["tokens_list"][0]
                        quote_token = pool_configuration["tokens_list"][1]

                        # Update token prices if available
                        if "price" in pool_information:
                            price = Decimal(str(pool_information.get("price", 0)))

                            # Update base token price in terms of quote token
                            if base_token in pool_configuration["tokens"]:
                                pool_configuration["tokens"][base_token].setdefault("prices", {})
                                pool_configuration["tokens"][base_token]["prices"][quote_token] = price

                            # Update quote token price in terms of base token
                            if quote_token in pool_configuration["tokens"]:
                                pool_configuration["tokens"][quote_token].setdefault("prices", {})
                                pool_configuration["tokens"][quote_token]["prices"][base_token] = (
                                    DECIMAL_ONE / price if price != DECIMAL_ZERO else None
                                )

                        # Update token balances in pool
                        if "baseTokenAmount" in pool_information and base_token in pool_configuration["tokens"]:
                            pool_configuration["tokens"][base_token]["balance"] = Decimal(
                                str(pool_information.get("baseTokenAmount", "0"))
                            )

                        if "quoteTokenAmount" in pool_information and quote_token in pool_configuration["tokens"]:
                            pool_configuration["tokens"][quote_token]["balance"] = Decimal(
                                str(pool_information.get("quoteTokenAmount", "0"))
                            )

    async def _update_wallet_balances(self):
        """
        Updates dynamic wallet data: balances and positions in pools
        by querying the gateway.
        """
        for chain_name, chain_configuration in self._database["connections"].items():
            for network_name, network_configuration in chain_configuration.items():
                for connector_name, connector_configuration in network_configuration.items():
                    for wallet_internal_id, wallet_configuration in connector_configuration["wallets"].items():
                        wallet_address = wallet_configuration["address"]

                        wallet_balances = await self._gateway_get_balances(
                            chain_name, network_name, wallet_address, self._token_symbols
                        )

                        for token_symbol, balance in wallet_balances.get("balances", {}).items():
                            if token_symbol not in wallet_configuration["tokens"]:
                                wallet_configuration["tokens"][token_symbol] = {
                                    "balances": {
                                        "free": None,
                                        "locked": {
                                            "total": None,
                                            "liquidity": {
                                                "total": None,
                                                "pools": {},
                                            },
                                        },
                                        "total": None,
                                    }
                                }

                            # Updates free and total balance
                            wallet_configuration["tokens"][token_symbol]["balances"]["free"] = balance
                            wallet_configuration["tokens"][token_symbol]["balances"]["total"] = balance

    async def _update_token_information(self):
        """
        Updates dynamic token information (price)
        by querying the gateway.
        """
        for chain_name, chain_configuration in self._database["connections"].items():
            for network_name, network_configuration in chain_configuration.items():
                for connector_name, connector_configuration in network_configuration.items():
                    for token_symbol in connector_configuration["tokens"]:
                        if token_symbol == self._main_quote_token:
                            connector_configuration["tokens"][token_symbol]["price"] = DECIMAL_ONE

                            continue

                        quote_swap_response = await self._gateway_quote_swap(
                            network_name,
                            connector_name,
                            token_symbol,
                            self._main_quote_token,
                            DECIMAL_ONE,
                            TradeType.SELL,
                            self._maximum_slippage_percentage,
                        )

                        if quote_swap_response and "price" in quote_swap_response:
                            connector_configuration["tokens"][token_symbol]["price"] = Decimal(
                                str(quote_swap_response["price"])
                            )

    # --------------------------------------------------------------------------
    # Arbitrage Opportunity Discovery and Trade Execution Methods
    # --------------------------------------------------------------------------
    def _find_most_promising_token_pairs(self) -> List[Dict[str, Any]]:
        """
        Identifies promising token pairs
        based on current pool data.

        Returns:
            List of dictionaries of token pairs including metrics
            like price variance.
        """
        promising_pairs = []

        # Process each configured token pair
        for token_pair in self._token_pairs:
            base_token, quote_token = self._extract_token_symbols_from_token_pair(token_pair)

            # Finds pools containing both tokens
            pools = self._find_pools_with_token_pair(base_token, quote_token)

            # Only considers pairs with at least 2 pools (necessary for arbitrage)
            if len(pools) >= 2:
                # Calculates metrics for the pair
                total_volume = sum(Decimal(str(pool.get("volume", {}).get("24h", 0) or 0)) for pool in pools)
                total_liquidity = sum(Decimal(str(pool.get("total_value_locked", 0) or 0)) for pool in pools)

                # Collects prices from all pools
                prices = []
                for pool in pools:
                    price = self._get_token_pair_relative_price_in_pool(pool, base_token, quote_token)
                    if price is not None:
                        prices.append(price)

                # Calculates price variance if there are at least 2 valid prices
                price_variance_percentage = DECIMAL_ZERO
                if len(prices) >= 2 and min(prices) > DECIMAL_ZERO:
                    price_variance_percentage = (max(prices) - min(prices)) / min(prices) * DECIMAL_ONE_HUNDRED

                # Adds the pair to the promising list
                promising_pairs.append(
                    {
                        "base_token": base_token,
                        "quote_token": quote_token,
                        "pools_count": len(pools),
                        "total_volume": total_volume,
                        "total_liquidity": total_liquidity,
                        "price_variance_percentage": price_variance_percentage,
                    }
                )

        # Orders pairs by price variance (descending), pool count and volume
        promising_pairs.sort(
            key=lambda promising_pair: (
                promising_pair["price_variance_percentage"],
                promising_pair["pools_count"],
                promising_pair["total_volume"],
            ),
            reverse=True,
        )

        return promising_pairs

    def _find_pools_with_token_pair(self, base_token: str, quote_token: str) -> List[Dict[str, Any]]:
        """
        Searches the database for pools containing both tokens.

        Args:
            base_token: Symbol of the base token.
            quote_token: Symbol of the quote token.
        Returns:
            List of dictionaries of pools.
        """
        result: List[Dict[str, Any]] = []

        key1 = f"{base_token}/{quote_token}"
        key2 = f"{quote_token}/{base_token}"

        pool_ids = []
        if key1 in self._database["maps"]["pools_by_tokens"]:
            pool_ids.extend(self._database["maps"]["pools_by_tokens"][key1])
        if key2 in self._database["maps"]["pools_by_tokens"]:
            pool_ids.extend(self._database["maps"]["pools_by_tokens"][key2])

        # If IDs found in map, searches corresponding pools
        if pool_ids:
            for chain_information in self._database["connections"].values():
                for network_information in chain_information.values():
                    for connector_information in network_information.values():
                        for pool_information in connector_information.get("pools", {}).values():
                            if pool_information.get("internal_id") in pool_ids:
                                result.append(pool_information)
            return result

        # Fallback: searches directly in all pools (less efficient)
        for chain_information in self._database["connections"].values():
            for network_information in chain_information.values():
                for connector_information in network_information.values():
                    for pool_information in connector_information.get("pools", {}).values():
                        tokens_list = pool_information.get("tokens_list", [])
                        if base_token in tokens_list and quote_token in tokens_list:
                            result.append(pool_information)

        return result

    # noinspection PyMethodMayBeStatic
    def _get_token_by_address(
        self, chain_name: str, network_name: str, connector_name: str, address: str
    ) -> Optional[Dict[str, Any]]:
        """
        Retrieves token information by address.
        """
        tokens = [
            token
            for token in self._database["connections"]
            .get(chain_name, {})
            .get(network_name, {})
            .get(connector_name, {})
            .get("tokens", {})
            .values()
            if token.get("address") == address
        ]

        return tokens[0] if tokens else None

    # noinspection PyMethodMayBeStatic
    def _get_token_pair_relative_price_in_pool(
        self, pool: Dict[str, Any], base_token: str, quote_token: str
    ) -> Optional[Decimal]:
        """
        Retrieves relative price for a token pair from pool data.

        Args:
            base_token: Symbol of the base token.
            quote_token: Symbol of the quote token.
            pool: Dictionary of the pool.
        Returns:
            Price as Decimal if available; otherwise, None.
        """
        if "tokens" in pool and base_token in pool["tokens"]:
            token_information = pool["tokens"][base_token]
            if "prices" in token_information and quote_token in token_information["prices"]:
                return token_information["prices"][quote_token]

        return None

    async def _calculate_price_difference_percentage(
        self, base_token: str, quote_token: str, pool_1: Dict[str, Any], pool_2: Dict[str, Any]
    ) -> Optional[Decimal]:
        """
        Calculates price difference percentage between two pools.

        Args:
            base_token: Symbol of the base token.
            quote_token: Symbol of the quote token.
            pool_1: First pool.
            pool_2: Second pool.
        Returns:
            Price difference percentage as Decimal, or None.
        """
        price_1 = self._get_token_pair_relative_price_in_pool(pool_1, base_token, quote_token)
        price_2 = self._get_token_pair_relative_price_in_pool(pool_2, base_token, quote_token)

        if price_1 is None or price_2 is None or price_1 == DECIMAL_ZERO:
            return None

        return ((price_2 - price_1) / price_1) * DECIMAL_ONE_HUNDRED

    # noinspection PyMethodMayBeStatic

    def calculate_portfolio_profit(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculates the profit of the portfolio.
        """
        final_value = (
            data["buy"]["base"]["balance"]["final"] * data["buy"]["base"]["price"]
            + data["buy"]["quote"]["balance"]["final"] * data["buy"]["quote"]["price"]
            + data["sell"]["base"]["balance"]["final"] * data["sell"]["base"]["price"]
            + data["sell"]["quote"]["balance"]["final"] * data["sell"]["quote"]["price"]
        )
        initial_value = (
            data["buy"]["base"]["balance"]["initial"] * data["buy"]["base"]["price"]
            + data["buy"]["quote"]["balance"]["initial"] * data["buy"]["quote"]["price"]
            + data["sell"]["base"]["balance"]["initial"] * data["sell"]["base"]["price"]
            + data["sell"]["quote"]["balance"]["initial"] * data["sell"]["quote"]["price"]
        )

        data["profit"] = {
            "absolute": final_value - initial_value,
            "percentage": ((final_value - initial_value) / initial_value) * DECIMAL_ONE_HUNDRED,
        }

        return data

    # noinspection PyMethodMayBeStatic
    async def _get_total_token_balance_from_all_wallets(self, token: str) -> Decimal:
        """
        Sums the free balance of a token in all wallets in the database.

        Args:
            token: Symbol of the token.
        Returns:
            Total available balance as Decimal.
        """
        total = DECIMAL_ZERO

        for chain_information in self._database["connections"].values():
            for network_information in chain_information.values():
                for connector_information in network_information.values():
                    for wallet_information in connector_information.get("wallets", {}).values():
                        balance = wallet_information.get("tokens", {}).get(token, {}).get("balances", {}).get("free", 0)
                        if balance:
                            total += Decimal(str(balance))

        return total

    async def _get_wallets_for_pool(self, pool: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Retrieves wallet addresses associated with a given pool.

        Args:
            pool: Pool dictionary.
        Returns:
            List of wallet addresses.
        """
        pool_id = pool.get("internal_id")
        mapping = self._database["maps"].get("wallets_by_pool", {})
        if pool_id in mapping:
            wallets_internal_ids = mapping[pool_id]
            chain_name, network_name, connector_name, _ = pool_id.split("/")

            wallets = []

            for wallet_internal_id in wallets_internal_ids:
                wallet = self._database["connections"][chain_name][network_name][connector_name]["wallets"][
                    wallet_internal_id
                ]
                wallets.append(wallet)

            return wallets

        # Fallback: search configuration
        for chain_configuration in self._configuration.get("connections", {}).values():
            for network_configuration in chain_configuration.values():
                for connector_configuration in network_configuration.values():
                    if pool_id in connector_configuration.get("pools", {}).keys():
                        return connector_configuration.get("wallets", {})

        return []

    async def _wait_for_transaction_confirmation(
        self, chain: str, network: str, transaction_hash: str, maximum_timeout: int = None
    ) -> bool:
        """
        Waits for a transaction to be confirmed via polling the gateway.

        Args:
            chain: Chain identifier.
            network: Network identifier.
            transaction_hash: Transaction hash.
            maximum_timeout: Maximum time to wait (seconds), default is class setting.
        Returns:
            True if confirmed; otherwise, False.
        """
        if maximum_timeout is None:
            maximum_timeout = self._maximum_transaction_confirmation_timeout
        start_time = time.time()
        while time.time() - start_time < maximum_timeout:
            try:
                transaction_status = await self._gateway_poll_transaction(chain, network, transaction_hash)
                if transaction_status and transaction_status.get("txStatus") == 1:
                    logger.info(f"Transaction {transaction_hash} confirmed!")

                    return True
                if transaction_status and transaction_status.get("txStatus") == -1:
                    logger.error(f"Transaction {transaction_hash} failed: {transaction_status}")

                    return False

                await asyncio.sleep(self._transaction_polling_interval)
            except Exception as exception:
                logger.ignore_exception(exception, f"Error polling transaction {transaction_hash}")
                await asyncio.sleep(self._transaction_polling_interval)

        logger.warning(f"Transaction {transaction_hash} confirmation timed out after {maximum_timeout} seconds")

        return False

    # noinspection PyMethodMayBeStatic
    def _extract_token_symbols_from_token_pair(self, pair: str) -> Tuple[str, str]:
        """
        Extracts base and quote token symbols from a token pair string.
        """
        tokens = pair.split("/")

        if len(tokens) != 2:
            raise ValueError(f"Invalid token pair: {pair}. Expected format: TOKEN1/TOKEN2")

        return tokens[0], tokens[1]

    # noinspection PyMethodMayBeStatic
    def _extract_token_symbols_from_token_triad(self, triad: str) -> Tuple[str, str, str]:
        """
        Extracts tokens from a token triad string.

        Args:
            triad: Token triad string in format "TOKEN1/TOKEN2/TOKEN3"
        Returns:
            Tuple of (token1, token2, token3)
        """
        tokens = triad.split("/")

        if len(tokens) != 3:
            raise ValueError(f"Invalid token triad: {triad}. Expected format: TOKEN1/TOKEN2/TOKEN3")

        return tokens[0], tokens[1], tokens[2]

    def _get_all_token_symbols(self) -> List[str]:
        """
        Extracts individual tokens from token pairs or token triads depending on the strategy type.

        Returns:
            List of unique token symbols.
        """
        tokens_set = set()
        tokens_list = []

        if self._strategy_type.value == StrategyType.CONNECTORS_ARBITRAGE.value:
            for pair in self._token_pairs:
                tokens_list = pair.split("/")

                if len(tokens_list) != 2:
                    raise ValueError(f"Invalid token pair: {pair}. Expected format: TOKEN1/TOKEN2")
        elif self._strategy_type.value == StrategyType.TRIANGULAR_ARBITRAGE.value:
            for triad in self._token_triads:
                tokens_list = triad.split("/")

                if len(tokens_list) != 3:
                    raise ValueError(f"Invalid token triad: {triad}. Expected format: TOKEN1/TOKEN2/TOKEN3")

        for token in tokens_list:
            tokens_set.add(token)

        return list(tokens_set)

    def _find_pools_for_token_triad(self, token1: str, token2: str, token3: str) -> Dict[str, List[Dict[str, Any]]]:
        """
        Searches the database for pools containing each pair of tokens in the triad.
        Returns a dictionary with pools for each pair in the triad.

        Args:
            token1: First token symbol
            token2: Second token symbol
            token3: Third token symbol
        Returns:
            Dictionary with pools for each segment of the triangular path:
            {
                "1/2": [pools containing token1 and token2],
                "2/3": [pools containing token2 and token3],
                "3/1": [pools containing token3 and token1]
            }
        """
        result = {
            "1/2": self._find_pools_with_token_pair(token1, token2),
            "2/3": self._find_pools_with_token_pair(token2, token3),
            "3/1": self._find_pools_with_token_pair(token3, token1)
        }

        return result

    async def _get_wallet_for_chain_network_connector(self, chain: str, network: str, connector: str) -> Optional[Dict[str, Any]]:
        """
        Gets a wallet for the specified chain/network/connector combination.

        Args:
            chain: Blockchain chain
            network: Network name
            connector: Connector name

        Returns:
            Wallet information dictionary or None if not found
        """
        if (chain in self._database["connections"] and
            network in self._database["connections"][chain] and
                connector in self._database["connections"][chain][network]):

            wallets = list(self._database["connections"][chain][network][connector].get("wallets", {}).values())
            if wallets:
                return wallets[0]  # Return the first wallet found

        return None

    @run_with_retry_and_timeout(retries=SWAP_OPERATION_RETRIES, delay=REQUEST_DELAY, timeout=REQUEST_TIMEOUT)
    async def _order_tracker_fetch_order(self, connector: ConnectorBase, client_order_id: str):
        """Fetches an order from the order tracker."""
        order = connector._order_tracker.fetch_order(client_order_id)

        if order is None:
            raise Exception(f"Order not found: {client_order_id}")

        if order.current_state not in [OrderState.FILLED]:
            raise Exception(f"Order not filled: {client_order_id}. Current state: {order.current_state}")

        return order

    # --------------------------------------------------------------------------
    # Gateway Methods (using retry/timeout)
    # --------------------------------------------------------------------------
    @run_with_retry_and_timeout(retries=REQUEST_RETRIES, delay=REQUEST_DELAY, timeout=REQUEST_TIMEOUT)
    async def _gateway_ping_gateway(self):
        """Pings the gateway server to verify connectivity."""
        return await self._gateway_http_client.ping_gateway()

    @run_with_retry_and_timeout(retries=REQUEST_RETRIES, delay=REQUEST_DELAY, timeout=REQUEST_TIMEOUT)
    async def _gateway_get_pool_info(self, connector: str, network: str, pool_address: str):
        """
        Gets pool information from the gateway.
        """
        try:
            # Get pool info using the AMM endpoint
            response = await self._gateway_http_client.api_request(
                "get",
                f"connectors/{connector}/amm/pool-info",
                {
                    "network": network,
                    "poolAddress": pool_address
                }
            )
            return response
        except Exception as e:
            logger.error(f"Error getting pool info: {str(e)}")
            return None

    @run_with_retry_and_timeout(retries=REQUEST_RETRIES, delay=REQUEST_DELAY, timeout=REQUEST_TIMEOUT)
    async def _gateway_get_tokens(
            self, chain: str, network: str, token_symbols: Optional[Union[str, List[str]]] = None
    ):
        """Retrieves token information from the gateway."""
        return await self._gateway_http_client.get_tokens(chain, network, token_symbols)

    @run_with_retry_and_timeout(retries=REQUEST_RETRIES, delay=REQUEST_DELAY, timeout=REQUEST_TIMEOUT)
    async def _gateway_get_balances(
            self, chain: str, network: str, address: str, token_symbols: Optional[Union[str, List[str]]] = None
    ):
        """Retrieves token balances for a wallet address from the gateway."""
        return await self._gateway_http_client.get_balances(chain, network, address, token_symbols)

    @run_with_retry_and_timeout(retries=REQUEST_RETRIES, delay=REQUEST_DELAY, timeout=REQUEST_TIMEOUT)
    async def _gateway_quote_swap(
            self,
            network: str,
            connector: str,
            base_asset: str,
            quote_asset: str,
            amount: Decimal,
            side: TradeType,
            slippage_percentage: Decimal,
            pool_address: Optional[str] = None,
    ):
        """Requests a swap quote from the gateway."""
        return await self._gateway_http_client.amm_quote_swap(
            network=network,
            connector=connector,
            base_asset=base_asset,
            quote_asset=quote_asset,
            amount=amount,
            side=side,
            slippage_percentage=slippage_percentage,
            pool_address=pool_address,
        )

    @run_with_retry_and_timeout(retries=REQUEST_RETRIES, delay=REQUEST_DELAY, timeout=REQUEST_TIMEOUT)
    async def _gateway_execute_swap(
            self,
            network: str,
            connector: str,
            wallet_address: str,
            base_asset: str,
            quote_asset: str,
            side: TradeType,
            amount: Decimal,
            slippage_percentage: Decimal,
            pool_address: Optional[str] = None,
    ):
        """Executes a swap transaction via the gateway."""
        return await self._gateway_http_client.amm_execute_swap(
            network=network,
            connector=connector,
            wallet_address=wallet_address,
            base_asset=base_asset,
            quote_asset=quote_asset,
            side=side,
            amount=amount,
            slippage_percentage=slippage_percentage,
            pool_address=pool_address,
        )

    @run_with_retry_and_timeout(retries=REQUEST_RETRIES, delay=REQUEST_DELAY, timeout=REQUEST_TIMEOUT)
    async def _gateway_poll_transaction(self, chain: str, network: str, tx_hash: str):
        """Polls the transaction status from the gateway."""
        return await self._gateway_http_client.get_transaction_status(chain, network, tx_hash)

    @run_with_retry_and_timeout(retries=REQUEST_RETRIES, delay=REQUEST_DELAY, timeout=REQUEST_TIMEOUT)
    async def _gateway_list_pools(
            self,
            connector: str,
            network: str,
            types: Optional[List[str]] = None,
            token_symbols: Optional[List[str]] = None,
            token_addresses: Optional[List[str]] = None,
            max_number_of_pages: int = 3,
            use_official_tokens: bool = True,
            use_config_pools: bool = False,
            fail_silently: bool = False,
    ) -> Dict[str, Any]:
        """
        Lists available pools for a given connector and network.

        Args:
            connector: The connector/protocol (e.g., "raydium")
            network: The network to use (e.g., "mainnet")
            types: List of pool types to filter by
            token_symbols: List of tokens to filter pools by
            token_addresses: List of token addresses to filter pools by
            max_number_of_pages: Maximum number of pages to fetch
            use_official_tokens: Whether to use official tokens only
            use_config_pools: Whether to use pool addresses from configuration
            fail_silently: Whether to fail silently on error

        Returns:
            Dictionary containing pools information
        """
        return await self._gateway_http_client.amm_list_pools(
            connector=connector,
            network=network,
            types=types,
            token_symbols=token_symbols,
            token_addresses=token_addresses,
            max_number_of_pages=max_number_of_pages,
            use_official_tokens=use_official_tokens,
            use_config_pools=use_config_pools,
            fail_silently=fail_silently,
        )
