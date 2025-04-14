"""
AMM Robust Position Manager Strategy

This module implements an arbitrage strategy that monitors liquidity pools
across multiple chains, identifies arbitrage opportunities, and executes trades
to profit from price discrepancies.

Key Features:
  - Asynchronous periodic updates using asyncio tasks.
  - Permanent (static) database information is built once; dynamic fields
    (wallet balances, token prices, pool statistics, etc.) are updated periodically.
  - The database schema exactly follows the provided specification.
  - Gateway API calls are wrapped with retry/timeout decorators.
  - All strategy and helper methods (arbitrage discovery, validation, execution)
    are fully implemented.
  - The arbitrage strategy runs only after the dynamic database has been fully updated at least once.
"""

import asyncio
import logging
import os
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional, Union

from pydantic.v1 import Field, validator

# Hummingbot and utility imports (assumed available in your environment)
from hummingbot.client.config.config_data_types import BaseClientModel
from hummingbot.client.settings import GatewayConnectionSetting
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.event.events import TradeType
from hummingbot.core.gateway.gateway_http_client import GatewayHttpClient
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase
from scripts.utility.utils import Logger, logged_class, run_with_retry_and_timeout

# ==============================================================================
# Constants for Decimal Calculations and Gateway HTTP Settings
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

GATEWAY_REQUEST_RETRIES = 3
GATEWAY_REQUEST_DELAY = 1  # seconds
GATEWAY_REQUEST_TIMEOUT = 30  # seconds
LOCK_ACQUISITION_TIMEOUT = 5  # seconds


# ==============================================================================
# Database Lock Context Manager
# ==============================================================================
class DatabaseLock:
    """
    Simplified context manager for database with timeout.
    """

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
# Global Configuration and Database Schema
# ==============================================================================

configuration: Dict[str, Any] = {
    "globals": {
        "maximum_slippage_percentage": "0.5",  # 0.5% allowed slippage
        "minimum_profitability_percentage": "-1",  # Profit threshold (e.g., "1" for 1%)
        "arbitrage_check_interval_seconds": "60",  # Time between arbitrage checks
        "minimum_trade_amount": "0.1",  # Minimum trade amount
        "time_delay_between_arbitrages": "1",  # Delay between arbitrage trades
        "transaction_confirmation_delay": "2",  # Delay for transaction confirmation polling
        "transaction_polling_interval": "2",  # Polling interval for transaction confirmation
        "data_update_intervals": {
            "wallet": "60",  # Update wallet data every 60 seconds
            "token": "300",  # Update token data every 300 seconds
            "pool": "30",  # Update pool data every 120 seconds
        },
        "use_async_data_updates": True,
    },
    "connections": {
        "polkadot": {
            "mainnet": {
                "hydration": {
                    "wallets": ["5HKTQCEWuuA9bJEqFbAEsbwFQfEe5tXrbZXWj7yQpuxVSHKt"],
                    "pools": [],
                }
            },
        },
        "solana": {
            "mainnet-beta": {
                "raydium": {
                    "wallets": ["7pWpBM8xtVHJq7C4BBivumfBZAC2J8XndTWvmg9GGXDb"],
                    "pools": [],
                }
            }
        },
    },
    "tokens": ["USDC", "USDT"],
}

# The database schema per specification.
database: Dict[str, Any] = {
    "connections": {},
    "arbitrage_opportunities": [],
    "execution_history": [],
    "maps": {
        "pools_by_tokens": {},  # "token1/token2" -> list of pool internal IDs
        "wallets_by_pool": {},  # pool internal ID -> list of wallet internal IDs
        "pools_by_wallet": {},  # wallet internal ID -> list of pool internal IDs
    },
}


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
    token: int = Field(default=300)
    pool: int = Field(default=120)

    # noinspection PyMethodParameters
    @validator("wallet", "token", "pool", allow_reuse=True)
    def validate_positive_interval(cls, v):
        if v <= 0:
            raise ValueError("Update interval must be positive")
        return v


class GlobalConfig(BaseClientModel):
    """Global parameters for the strategy."""

    maximum_slippage_percentage: Decimal = Field(default=Decimal("0.5"))
    minimum_profitability_percentage: Decimal = Field(default=Decimal("-1"))
    arbitrage_check_interval_seconds: int = Field(default=60)
    minimum_trade_amount: Decimal = Field(default=Decimal("0.1"))
    time_delay_between_arbitrages: int = Field(default=1)
    transaction_confirmation_delay: int = Field(default=2)
    transaction_polling_interval: int = Field(default=2)
    data_update_intervals: DataUpdateIntervals = Field(default_factory=DataUpdateIntervals)
    use_async_data_updates: bool = Field(default=True)


class AMMPortfolioManagerConfiguration(BaseClientModel):
    """
    Configuration model for the AMM Robust Position Manager strategy.

    Mirrors the global configuration, connections and token list.
    """

    script_file_name: str = Field(default_factory=lambda: os.path.basename(__file__))
    globals: GlobalConfig = Field(default_factory=GlobalConfig)
    chains: Dict[str, ChainConfig] = Field(default_factory=dict)
    tokens: List[str] = Field(default_factory=list)

    class Config:
        arbitrary_types_allowed = True


# ==============================================================================
# Logger Initialization
# ==============================================================================
logger = Logger(path="logs/logs_amm_portfolio_manager.py", level=logging.DEBUG)


# ==============================================================================
# AMMPortfolioManager Strategy Class
# ==============================================================================
@logged_class(logger=logger, disallowed_methods=["on_tick"])
class AMMPortfolioManager(ScriptStrategyBase):
    """
    AMM Robust Position Manager Strategy - Refactored version

    This strategy monitors AMM pools, discovers arbitrage opportunities
    and executes paired trades between pools with differing prices.
    Uses asynchronous tasks for dynamic data updates.
    """

    # Configuration attributes
    markets: Dict[str, Any] = {}
    _configuration: Optional[Dict[str, Any]] = None
    _gateway_is_ready: bool = False
    _gateway_http_client: Optional[GatewayHttpClient] = None
    _all_gateway_connections: List[Dict[str, Any]] = []

    # Strategy parameters
    _last_arbitrage_check_time: float = 0
    _maximum_slippage_percentage: Decimal = DECIMAL_ZERO
    _minimum_profitability_percentage: Decimal = DECIMAL_ZERO
    _arbitrage_check_interval_seconds: int = 60
    _minimum_trade_amount: Decimal = DECIMAL_ZERO
    _time_delay_between_arbitrages: int = 1
    _transaction_confirmation_delay: int = 2
    _transaction_polling_interval: int = 2
    _maximum_transaction_confirmation_timeout: int = 60

    # Data update control
    _data_update_task: Optional[asyncio.Task] = None
    _data_update_intervals: Dict[str, int] = {"wallet": 60, "token": 300, "pool": 120}
    _last_wallet_update_time: float = 0
    _last_token_update_time: float = 0
    _last_pool_update_time: float = 0
    _use_async_data_updates: bool = True

    # State control
    _db_initialized: bool = False  # Indicates if database was initialized
    _is_updating: bool = False  # Indicates if an update is in progress
    _db_lock: asyncio.Lock = None

    def __init__(self, connectors: Dict[str, ConnectorBase]):
        """
        Initializes the strategy: loads configuration and prepares database.

        Args:
            connectors: Dictionary of available connectors.
        """
        super().__init__(connectors)
        self._db_lock = asyncio.Lock()
        safe_ensure_future(self._initialize())

    async def _initialize(self):
        """
        Configures gateway client, database structure and asynchronous updates.
        """
        self.logger().info("Initializing AMM arbitrage strategy")

        # Gateway client initialization
        self._gateway_http_client = GatewayHttpClient.get_instance()
        self._all_gateway_connections = GatewayConnectionSetting.load()

        # Load configuration
        self._configuration = configuration
        globals_conf = self._configuration.get("globals", {})

        # Strategy parameters
        self._maximum_slippage_percentage = Decimal(globals_conf.get("maximum_slippage_percentage", "0.5"))
        self._minimum_profitability_percentage = Decimal(globals_conf.get("minimum_profitability_percentage", "-1"))
        self._arbitrage_check_interval_seconds = int(globals_conf.get("arbitrage_check_interval_seconds", "60"))
        self._minimum_trade_amount = Decimal(globals_conf.get("minimum_trade_amount", "0.1"))
        self._time_delay_between_arbitrages = int(globals_conf.get("time_delay_between_arbitrages", "1"))
        self._transaction_confirmation_delay = int(globals_conf.get("transaction_confirmation_delay", "2"))
        self._transaction_polling_interval = int(globals_conf.get("transaction_polling_interval", "2"))
        self._use_async_data_updates = bool(globals_conf.get("use_async_data_updates", True))

        # Configure update intervals
        data_update_intervals = globals_conf.get("data_update_intervals", {})
        if data_update_intervals:
            self._data_update_intervals = {
                "wallet": int(data_update_intervals.get("wallet", 60)),
                "token": int(data_update_intervals.get("token", 300)),
                "pool": int(data_update_intervals.get("pool", 120)),
            }

        # Configure transaction confirmation timeout
        if self._transaction_confirmation_delay > 0:
            self._maximum_transaction_confirmation_timeout = self._transaction_confirmation_delay * 5

        # Initialize database
        await self._initialize_database_structure()

        # Start update task if configured
        if self._use_async_data_updates:
            self._start_data_update_task()

        # Check gateway status
        await self._check_gateway_status()

        self.logger().info("Strategy initialization completed")

    def _start_data_update_task(self):
        """Starts the asynchronous data update task."""
        if self._data_update_task is not None:
            self._data_update_task.cancel()
        self._data_update_task = safe_ensure_future(self._run_data_update_loop())
        self.logger().info("Data update task started")

    async def _run_data_update_loop(self):
        """
        Asynchronous loop to update dynamic data with appropriate intervals.
        Avoids excessive updates by respecting configured time intervals.
        """
        try:
            # Variables for time control between updates
            last_update_time = 0
            min_update_interval = min(self._data_update_intervals.values())

            while True:
                # Check gateway status when needed
                if not self._gateway_is_ready:
                    await self._check_gateway_status()
                    if not self._gateway_is_ready:
                        self.logger().warning("Gateway not ready. Trying again in 5 seconds...")
                        await asyncio.sleep(5)
                        continue

                current_time = time.time()
                time_since_last_update = current_time - last_update_time

                # Update database only if minimum interval has passed
                if time_since_last_update >= min_update_interval:
                    if self._db_initialized:
                        try:
                            self._is_updating = True
                            await self._update_database()
                            last_update_time = current_time
                        except Exception as e:
                            self.logger().error(f"Error during database update: {str(e)}")
                        finally:
                            self._is_updating = False
                    else:
                        self.logger().warning("Database not initialized. Skipping update.")

                # Wait appropriate interval before checking again
                # Use at least 5 seconds or half the smallest configured interval
                sleep_time = max(5, min_update_interval / 2)
                await asyncio.sleep(sleep_time)

        except asyncio.CancelledError:
            self.logger().info("Data update task cancelled")
        except Exception as e:
            self.logger().error(f"Critical error in update loop: {str(e)}")
            raise

    def on_tick(self):
        """Called on each tick to execute the strategy."""
        if not self._is_updating and self._db_initialized:
            safe_ensure_future(self._async_on_tick())

    async def _async_on_tick(self):
        """
        Main execution method. The strategy executes only if the database
        has been updated and the gateway is available.
        """
        try:
            self._is_updating = True

            # Update database only if not using asynchronous updates
            if not self._use_async_data_updates:
                await self._update_database()

            # Execute strategy only if gateway is ready
            if not self._gateway_is_ready:
                self.logger().warning("Gateway not ready. Skipping arbitrage check.")
                return

            # Execute arbitrage strategy at configured interval
            current_time = time.time()
            if current_time - self._last_arbitrage_check_time >= self._arbitrage_check_interval_seconds:
                self._last_arbitrage_check_time = current_time
                await self._run_arbitrage_strategy()
        except Exception as e:
            self.logger().error(f"Error during strategy execution: {str(e)}")
        finally:
            self._is_updating = False

    # --------------------------------------------------------------------------
    # Missing Methods Implementation
    # --------------------------------------------------------------------------
    async def _check_gateway_status(self):
        """
        Verifies the availability of the gateway by sending a ping.
        Sets _gateway_is_ready to True if the gateway responds, otherwise False.
        """
        try:
            ping_result = await self._gateway_ping_gateway()
            if ping_result:
                self._gateway_is_ready = True
                self.logger().info("Gateway is online.")
            else:
                self._gateway_is_ready = False
                self.logger().warning("Ping of gateway did not return response.")
        except Exception as e:
            self._gateway_is_ready = False
            self.logger().error(f"Gateway status check failed: {str(e)}")

    async def _run_arbitrage_strategy(self):
        """
        Executes the arbitrage strategy:
         - Discovers promising token pairs
         - Validates each opportunity
         - Executes arbitrage trades for opportunities that pass validation
        """
        self.logger().info("Executing arbitrage strategy: searching for opportunities...")

        # Finds promising token pairs
        promising_pairs = self._find_most_promising_token_pairs()
        if not promising_pairs:
            self.logger().info("No promising token pair found.")
            return

        # Searches for arbitrage opportunities in promising pairs
        opportunities = await self._find_arbitrage_opportunities(promising_pairs)
        if not opportunities:
            self.logger().info("No arbitrage opportunity found.")
            return

        self.logger().info(f"Found {len(opportunities)} arbitrage opportunities.")

        # Validates and executes each opportunity
        for opp in opportunities:
            # Registers the opportunity in the database
            if "arbitrage_opportunities" not in database:
                database["arbitrage_opportunities"] = []
            database["arbitrage_opportunities"].append(opp)

            # Validates the opportunity
            self.logger().info(
                f"Validating opportunity: {opp['base_token']}/{opp['quote_token']} with difference of {opp['price_difference_percentage']:.2f}%"
            )
            valid = await self._validate_opportunity(opp)

            if valid:
                # Executes arbitrage if valid
                self.logger().info(f"Executing arbitrage: {opp['base_token']}/{opp['quote_token']}")
                success = await self._execute_arbitrage(opp)

                if success:
                    self.logger().info("Arbitrage executed successfully.")
                else:
                    self.logger().warning("Arbitrage execution failed.")

                # Waits the configured delay between arbitrages
                if self._time_delay_between_arbitrages > 0:
                    await asyncio.sleep(self._time_delay_between_arbitrages)
            else:
                self.logger().info("Opportunity invalidated after detailed validation.")

        self.logger().info("Arbitrage strategy cycle completed.")

    # --------------------------------------------------------------------------
    # Permanent Database Initialization and Mapping Methods
    # --------------------------------------------------------------------------
    async def _initialize_database_structure(self):
        """
        Builds the database structure according to the provided schema.
        For each chain, network and connector, stores static information about wallets, tokens and pools.
        """
        if self._db_initialized:
            self.logger().info("Database already initialized")
            return

        self.logger().info("Starting database initialization...")

        try:
            # Initializes basic structure of the database
            for chain, chain_conf in self._configuration.get("connections", {}).items():
                database["connections"].setdefault(chain, {})

                for network, net_conf in chain_conf.items():
                    database["connections"][chain].setdefault(network, {})

                    for connector, conn_conf in net_conf.items():
                        # Initializes structure for the connector
                        database["connections"][chain][network].setdefault(
                            connector,
                            {
                                "wallets": {},
                                "tokens": {},
                                "pools": {},
                            },
                        )

                        hydration = database["connections"][chain][network][connector]

                        # Adds wallets
                        for wallet_addr in conn_conf.get("wallets", []):
                            if wallet_addr not in hydration["wallets"]:
                                hydration["wallets"][wallet_addr] = {
                                    "internal_id": f"{chain}/{network}/{connector}/{wallet_addr}",
                                    "chain": chain,
                                    "network": network,
                                    "connector": connector,
                                    "tokens": {},
                                    "pools": {},
                                }

                        # Adds pools
                        for pool_addr in conn_conf.get("pools", []):
                            if pool_addr not in hydration["pools"]:
                                hydration["pools"][pool_addr] = {
                                    "internal_id": f"{chain}/{network}/{connector}/{pool_addr}",
                                    "address": pool_addr,
                                    "chain": chain,
                                    "network": network,
                                    "connector": connector,
                                    "type": "unknown",  # Will be updated later
                                    "tokens_list": self._configuration.get("tokens", []),
                                    "tokens": {},
                                    "annual_percentage_rate": None,
                                    "total_value_locked": None,
                                    "impermanent_loss": None,
                                    "volume": {"24h": None},
                                }

                        # Adds tokens
                        for token_sym in self._configuration.get("tokens", []):
                            if token_sym not in hydration["tokens"]:
                                hydration["tokens"][token_sym] = {
                                    "internal_id": f"{chain}/{network}/{connector}/{token_sym}",
                                    "address": None,  # Will be updated from gateway
                                    "chain": chain,
                                    "network": network,
                                    "connector": connector,
                                    "symbol": token_sym,
                                    "name": None,
                                    "decimals": None,
                                    "price": None,
                                }

            # Adds hardcoded test pools
            self._add_hardcoded_test_pools()

            # Initializes database maps
            await self._update_database_maps()

            # Marks the database as initialized
            self._db_initialized = True
            self.logger().info("Database structure initialization completed successfully")

        except Exception as e:
            self.logger().error(f"Error initializing database: {str(e)}")
            self._db_initialized = False
            raise

    def _add_hardcoded_test_pools(self):
        """
        Adds hardcoded test pools.
        # TODO: Make parametrizable or remove after tests.
        """
        self.logger().info("Adding hardcoded test pools")

        # Specific pools for test in different networks
        test_pools = {
            "polkadot": {
                "mainnet": {
                    "hydration": [
                        "7LVGEVLFXpsCCtnsvhzkSMQARU7gRVCtwMckG7u7d3V6FVvG",  # USDC-USDT pool
                        # "7JP6TvcH5x31TsbC6qVJHEhsW7UNmpREMZuLBpK2bG1goJRS",  # USDC-DAI pool (commented)
                    ]
                }
            },
            "solana": {
                "mainnet-beta": {
                    "raydium": [
                        "2EXiumdi14E9b8Fy62QcA5Uh6WdHS2b38wtSxp72Mibj",  # USDC-USDT pool
                        "7TbGqz32RsuwXbXY7EyBCiAnMbJq1gm1wKmfjQjuwoyF",  # USDC-USDT pool alternative
                    ]
                }
            },
        }

        # Adds hardcoded pools to the database
        for chain, chain_data in test_pools.items():
            if chain in database["connections"]:
                for network, network_data in chain_data.items():
                    if network in database["connections"][chain]:
                        for connector, pool_addresses in network_data.items():
                            if connector in database["connections"][chain][network]:
                                for pool_addr in pool_addresses:
                                    if pool_addr not in database["connections"][chain][network][connector]["pools"]:
                                        # Defines token pair for this pool (based on comment or name)
                                        token_list = ["USDC", "USDT"]  # By default, we assume USDC-USDT
                                        if "DAI" in pool_addr or "DAI" in str(locals()):
                                            token_list = ["USDC", "DAI"]

                                        database["connections"][chain][network][connector]["pools"][pool_addr] = {
                                            "internal_id": f"{chain}/{network}/{connector}/{pool_addr}",
                                            "address": pool_addr,
                                            "chain": chain,
                                            "network": network,
                                            "connector": connector,
                                            "type": "unknown",  # Will be updated from gateway
                                            "tokens_list": token_list,
                                            "tokens": {},
                                            "annual_percentage_rate": None,
                                            "total_value_locked": None,
                                            "impermanent_loss": None,
                                            "volume": {"24h": None},
                                        }
                                        self.logger().info(
                                            f"Test pool added: {pool_addr} ({token_list[0]}-{token_list[1]})"
                                        )

        # Checks if pools were added
        total_pools = sum(
            len(c.get("pools", {})) for n in database["connections"].values() for cn in n.values() for c in cn.values()
        )
        self.logger().info(f"Total of {total_pools} pools in database after adding test pools")

    async def _update_database_maps(self):
        """
        Rebuilds the mapping dictionaries:
         - pools_by_tokens: "token1/token2" -> list of pool internal IDs
         - wallets_by_pool: pool internal ID -> list of wallet internal IDs
         - pools_by_wallet: wallet internal ID -> list of pool internal IDs
        """
        self.logger().info("Updating database maps...")

        try:
            db_maps = database["maps"]
            # Clears existing maps
            db_maps["pools_by_tokens"].clear()
            db_maps["wallets_by_pool"].clear()
            db_maps["pools_by_wallet"].clear()

            # Rebuilds maps
            for chain, chain_conf in database["connections"].items():
                for network, net_conf in chain_conf.items():
                    for connector, hydration in net_conf.items():
                        # Maps pools by tokens
                        for pool_addr, pool in hydration["pools"].items():
                            tokens_list = pool.get("tokens_list", [])
                            for i in range(len(tokens_list)):
                                for j in range(i + 1, len(tokens_list)):
                                    key = f"{tokens_list[i]}/{tokens_list[j]}"
                                    db_maps["pools_by_tokens"].setdefault(key, []).append(pool["internal_id"])

                        # Maps wallets to pools and vice versa
                        for wallet_addr, wallet in hydration["wallets"].items():
                            wallet_id = wallet["internal_id"]
                            for pool_addr in wallet.get("pools", {}).keys():
                                db_maps["pools_by_wallet"].setdefault(wallet_id, []).append(pool_addr)
                                db_maps["wallets_by_pool"].setdefault(pool_addr, []).append(wallet_id)

            self.logger().info("Database map update completed")
        except Exception as e:
            self.logger().error(f"Error in database map update: {str(e)}")
            raise

    # --------------------------------------------------------------------------
    # Dynamic Database Update Methods
    # --------------------------------------------------------------------------
    async def _update_database(self):
        """
        Updates dynamic parts of the database:
         - Pool statistics (APR, TVL, volume, token prices)
         - Wallet balances and positions in pools
         - Token information (price, decimals, name)

        Updates database maps only if any update has been performed.
        """
        self.logger().info("Starting database update...")
        current_time = time.time()

        # Update intervals configured
        wallet_interval = self._data_update_intervals.get("wallet", 60)
        token_interval = self._data_update_intervals.get("token", 300)
        pool_interval = self._data_update_intervals.get("pool", 120)

        # Flag to indicate if any update has been performed
        updates_performed = False

        # Simplified lock usage - only one acquisition for entire update
        # TODO: Refactor to remove lock usage if causing problems
        async with DatabaseLock(self._db_lock, self.logger(), "_update_database") as lock_acquired:
            if not lock_acquired:
                self.logger().warning("Unable to acquire lock for update, skipping this cycle")
                return

            try:
                # Token update (less frequent)
                if (not hasattr(self, "_last_token_update_time")) or (
                    current_time - self._last_token_update_time >= token_interval
                ):
                    self.logger().info(f"Updating token information (interval: {token_interval}s)...")
                    self._last_token_update_time = current_time
                    await self._update_token_information()
                    updates_performed = True

                # Pool update (medium frequency)
                if (not hasattr(self, "_last_pool_update_time")) or (
                    current_time - self._last_pool_update_time >= pool_interval
                ):
                    self.logger().info(f"Updating pool information (interval: {pool_interval}s)...")
                    self._last_pool_update_time = current_time
                    await self._update_pool_information()
                    updates_performed = True

                # Wallet update (more frequent)
                if (not hasattr(self, "_last_wallet_update_time")) or (
                    current_time - self._last_wallet_update_time >= wallet_interval
                ):
                    self.logger().info(f"Updating wallet balances (interval: {wallet_interval}s)...")
                    self._last_wallet_update_time = current_time
                    await self._update_wallet_balances()
                    updates_performed = True

                # Updates maps only if any update has been performed
                if updates_performed:
                    self.logger().info("Updating database maps after changes...")
                    await self._update_database_maps()
                else:
                    self.logger().info("No updates performed in this cycle. Skipping map update.")

                # First time the method is executed, marks the database as initialized
                if not self._db_initialized:
                    self._db_initialized = True
                    self.logger().info("Database marked as initialized after first update")

                self.logger().info("Database update completed successfully")
            except Exception as e:
                self.logger().error(f"Error during database update: {str(e)}")
                raise

    async def _update_pool_information(self):
        """
        Updates dynamic pool statistics (APR, TVL, volume, token prices)
        by querying the gateway.
        """
        self.logger().info("Updating pool information...")

        try:
            for chain, chain_conf in database["connections"].items():
                for network, net_conf in chain_conf.items():
                    for connector, hydration in net_conf.items():
                        for pool_addr, pool in hydration["pools"].items():
                            try:
                                pool_info = await self._gateway_get_pool_info(connector, network, pool_addr)

                                if pool_info:
                                    # Updates pool fundamental data
                                    pool["annual_percentage_rate"] = pool_info.get("annual_percentage_rate")
                                    pool["total_value_locked"] = pool_info.get("total_value_locked")
                                    pool["volume"]["24h"] = pool_info.get("volume", {}).get("24h")

                                    # Updates token information in pool
                                    if "tokens" in pool_info:
                                        pool["tokens"] = pool_info["tokens"]

                                    # Updates pool type
                                    if pool_info.get("type"):
                                        pool["type"] = pool_info.get("type")
                            except Exception as e:
                                self.logger().error(f"Failed to update pool {pool_addr}: {str(e)}")

            self.logger().info("Pool information update completed")
        except Exception as e:
            self.logger().error(f"Error in pool information update: {str(e)}")
            raise

    async def _update_wallet_balances(self):
        """
        Updates dynamic wallet data: balances and positions in pools
        by querying the gateway.
        """
        self.logger().info("Updating wallet balances...")

        try:
            for chain, chain_conf in database["connections"].items():
                for network, net_conf in chain_conf.items():
                    for connector, hydration in net_conf.items():
                        for wallet_addr, wallet in hydration["wallets"].items():
                            try:
                                wallet_info = await self._gateway_get_balances(chain, network, wallet_addr)

                                if wallet_info and "tokens" in wallet_info:
                                    # Updates token information
                                    for token_sym, bal_data in wallet_info["tokens"].items():
                                        wallet["tokens"][token_sym] = {
                                            "balances": {
                                                "free": bal_data.get("free"),
                                                "locked": {
                                                    "total": bal_data.get("locked"),
                                                    "liquidity": {
                                                        "total": bal_data.get("liquidity", {}).get("total"),
                                                        "pools": bal_data.get("liquidity", {}).get("pools", {}),
                                                    },
                                                },
                                                "total": bal_data.get("total"),
                                            }
                                        }

                                # Updates pool information for wallet
                                if "pools" in wallet_info:
                                    wallet["pools"] = wallet_info["pools"]
                            except Exception as e:
                                self.logger().error(f"Failed to update wallet {wallet_addr}: {str(e)}")

            self.logger().info("Wallet balances update completed")
        except Exception as e:
            self.logger().error(f"Error in wallet balances update: {str(e)}")
            raise

    async def _update_token_information(self):
        """
        Updates static token information (price, decimals, name)
        by querying the gateway.
        """
        self.logger().info("Updating token information...")

        try:
            for chain_name, chain in database["connections"].items():
                for network_name, network in chain.items():
                    for connector_name, connector in network.items():
                        try:
                            # Retrieves token information from the gateway
                            token_response = await self._gateway_get_tokens(
                                chain_name, network_name, self._configuration.get("tokens", [])
                            )

                            if token_response and "tokens" in token_response:
                                tokens = token_response["tokens"]

                                # Updates each found token
                                for token in tokens:
                                    if token.get("symbol") in connector["tokens"]:
                                        connector["tokens"][token.get("symbol")].update(
                                            {
                                                "address": token.get("address"),
                                                "name": token.get("name"),
                                                "decimals": token.get("decimals"),
                                                "price": token.get("price"),
                                            }
                                        )
                        except Exception as e:
                            self.logger().error(
                                f"Failed to update tokens for {chain_name}/{network_name}/{connector_name}: {str(e)}"
                            )

            self.logger().info("Token information update completed")
        except Exception as e:
            self.logger().error(f"Error in token information update: {str(e)}")
            raise

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
        tokens = self._configuration.get("tokens", [])
        promising_pairs = []

        # Generates all token pair combinations
        for i in range(len(tokens)):
            for j in range(i + 1, len(tokens)):
                token1, token2 = tokens[i], tokens[j]

                # Finds pools containing both tokens
                pools = self._find_pools_with_token_pair(token1, token2)

                # Only considers pairs with at least 2 pools (necessary for arbitrage)
                if len(pools) >= 2:
                    # Calculates metrics for the pair
                    total_volume = sum(Decimal(str(p.get("volume", {}).get("24h", 0) or 0)) for p in pools)
                    total_liquidity = sum(Decimal(str(p.get("total_value_locked", 0) or 0)) for p in pools)

                    # Collects prices from all pools
                    prices = []
                    for pool in pools:
                        price = self._get_cached_token_price_in_pool(token1, token2, pool)
                        if price is not None:
                            prices.append(price)

                    # Calculates price variance if there are at least 2 valid prices
                    price_variance = Decimal("0")
                    if len(prices) >= 2 and min(prices) > DECIMAL_ZERO:
                        price_variance = (max(prices) - min(prices)) / min(prices) * DECIMAL_ONE_HUNDRED

                    # Adds the pair to the promising list
                    promising_pairs.append(
                        {
                            "token1": token1,
                            "token2": token2,
                            "pools_count": len(pools),
                            "total_volume": total_volume,
                            "total_liquidity": total_liquidity,
                            "price_variance": price_variance,
                        }
                    )

        # Orders pairs by price variance (descending), pool count and volume
        promising_pairs.sort(key=lambda x: (x["price_variance"], x["pools_count"], x["total_volume"]), reverse=True)

        # Returns the 5 most promising pairs
        return promising_pairs[:5]

    def _find_pools_with_token_pair(self, token1: str, token2: str) -> List[Dict[str, Any]]:
        """
        Searches the database for pools containing both tokens.

        Args:
            token1: Symbol of the first token.
            token2: Symbol of the second token.
        Returns:
            List of dictionaries of pools.
        """
        result = []

        # First tries to use map for quick search
        key1 = f"{token1}/{token2}"
        key2 = f"{token2}/{token1}"

        pool_ids = []
        if key1 in database["maps"]["pools_by_tokens"]:
            pool_ids.extend(database["maps"]["pools_by_tokens"][key1])
        if key2 in database["maps"]["pools_by_tokens"]:
            pool_ids.extend(database["maps"]["pools_by_tokens"][key2])

        # If IDs found in map, searches corresponding pools
        if pool_ids:
            for chain_data in database["connections"].values():
                for network_data in chain_data.values():
                    for connector_data in network_data.values():
                        for pool in connector_data.get("pools", {}).values():
                            if pool.get("internal_id") in pool_ids:
                                result.append(pool)
            return result

        # Fallback: searches directly in all pools (less efficient)
        for chain_data in database["connections"].values():
            for network_data in chain_data.values():
                for connector_data in network_data.values():
                    for pool in connector_data.get("pools", {}).values():
                        tokens_list = pool.get("tokens_list", [])
                        if token1 in tokens_list and token2 in tokens_list:
                            result.append(pool)

        return result

    def _get_cached_token_price_in_pool(
        self, base_token: str, quote_token: str, pool: Dict[str, Any]
    ) -> Optional[Decimal]:
        """
        Retrieves cached price for a token pair from pool data.

        Args:
            base_token: Symbol of the base token.
            quote_token: Symbol of the quote token.
            pool: Dictionary of the pool.
        Returns:
            Price as Decimal if available; otherwise, None.
        """
        try:
            if "tokens" in pool and base_token in pool["tokens"]:
                token_info = pool["tokens"][base_token]
                if "prices" in token_info and quote_token in token_info["prices"]:
                    return Decimal(str(token_info["prices"][quote_token]))
        except (KeyError, TypeError):
            return None
        return None

    async def _calculate_price_difference_percentage(
        self, base_token: str, quote_token: str, pool1: Dict[str, Any], pool2: Dict[str, Any]
    ) -> Optional[Decimal]:
        """
        Calculates price difference percentage between two pools.

        Args:
            base_token: Symbol of the base token.
            quote_token: Symbol of the quote token.
            pool1: First pool.
            pool2: Second pool.
        Returns:
            Price difference percentage as Decimal, or None.
        """
        price1 = self._get_cached_token_price_in_pool(base_token, quote_token, pool1)
        price2 = self._get_cached_token_price_in_pool(base_token, quote_token, pool2)

        if price1 is None or price2 is None or price1 == DECIMAL_ZERO:
            return None

        return ((price2 - price1) / price1) * DECIMAL_ONE_HUNDRED

    async def _find_arbitrage_opportunities(self, promising_pairs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Finds arbitrage opportunities based on promising token pairs.

        Args:
            promising_pairs: List of dictionaries of token pairs.
        Returns:
            List of dictionaries of arbitrage opportunities.
        """
        opportunities = []
        min_profitability = self._minimum_profitability_percentage

        # Extracts token pairs from dictionaries
        token_pairs = [(pair["token1"], pair["token2"]) for pair in promising_pairs]

        for base_token, quote_token in token_pairs:
            # Finds all pools containing both tokens
            pools = self._find_pools_with_token_pair(base_token, quote_token)

            # Needs at least 2 pools for arbitrage
            if len(pools) < 2:
                continue

            # Compares each pair of pools for price differences
            for i in range(len(pools)):
                for j in range(i + 1, len(pools)):
                    # Calculates price difference percentage between pools
                    diff = await self._calculate_price_difference_percentage(
                        base_token, quote_token, pools[i], pools[j]
                    )

                    # Skips if unable to calculate difference
                    if diff is None:
                        continue

                    # Checks if difference is greater than minimum profitability
                    if abs(diff) > min_profitability:
                        # Determines which pool is buy and which is sell
                        if diff > 0:
                            buy_pool, sell_pool = pools[i], pools[j]
                        else:
                            buy_pool, sell_pool = pools[j], pools[i]

                        # Creates opportunity record
                        opportunity = {
                            "base_token": base_token,
                            "quote_token": quote_token,
                            "buy_pool": buy_pool,
                            "sell_pool": sell_pool,
                            "price_difference_percentage": abs(diff),
                            "timestamp": time.time(),
                        }

                        opportunities.append(opportunity)
                        self.logger().info(
                            f"Arbitrage opportunity found: {base_token}/{quote_token} "
                            f"difference {abs(diff):.2f}% between {buy_pool.get('address')} and {sell_pool.get('address')}"
                        )

        return opportunities

    async def _validate_opportunity(self, opportunity: Dict[str, Any]) -> bool:
        """
        Validates an arbitrage opportunity by checking available balances and
        simulating quotes.

        Args:
            opportunity: Dictionary of arbitrage opportunity.
        Returns:
            True if opportunity is valid and expected profit meets threshold;
            otherwise, False.
        """
        base_token = opportunity["base_token"]
        quote_token = opportunity["quote_token"]
        buy_pool = opportunity["buy_pool"]
        sell_pool = opportunity["sell_pool"]

        # Checks if there is available balance
        available_balance = await self._get_total_token_balance_from_all_wallets(base_token)
        if available_balance < self._minimum_trade_amount:
            self.logger().info(f"Insufficient balance of {base_token}: {available_balance}")
            return False

        # Calculates ideal trade amount
        trade_amount = await self._calculate_optimal_trade_amount(opportunity, available_balance)
        if not trade_amount or trade_amount <= DECIMAL_ZERO:
            self.logger().info("Invalid optimal trade amount")
            return False

        max_slippage = self._maximum_slippage_percentage

        try:
            # Simulates buy: base_token -> quote_token
            buy_quote = await self._get_quote_swap(
                buy_pool, base_token, quote_token, trade_amount, TradeType.SELL, max_slippage
            )
            if not buy_quote or "estimatedAmountOut" not in buy_quote:
                self.logger().info(f"Buy quote unavailable for pool {buy_pool.get('address')}")
                return False

            expected_quote = Decimal(str(buy_quote["estimatedAmountOut"]))

            # Simulates sell: quote_token -> base_token
            sell_quote = await self._get_quote_swap(
                sell_pool, quote_token, base_token, expected_quote, TradeType.SELL, max_slippage
            )
            if not sell_quote or "estimatedAmountOut" not in sell_quote:
                self.logger().info(f"Sell quote unavailable for pool {sell_pool.get('address')}")
                return False

            # Calculates expected profit
            expected_return = Decimal(str(sell_quote["estimatedAmountOut"]))
            expected_profit = expected_return - trade_amount
            profit_percentage = (expected_profit / trade_amount) * DECIMAL_ONE_HUNDRED

            # Updates opportunity with calculated values
            opportunity.update(
                {
                    "trade_amount": trade_amount,
                    "expected_quote_token": expected_quote,
                    "expected_base_token_return": expected_return,
                    "expected_profit": expected_profit,
                    "expected_profit_percentage": profit_percentage,
                }
            )

            # Checks if opportunity meets minimum profitability
            if profit_percentage < self._minimum_profitability_percentage:
                self.logger().info(
                    f"Opportunity not profitable after slippage: "
                    f"{profit_percentage:.2f}% < {self._minimum_profitability_percentage}%"
                )
                return False

            self.logger().info(
                f"Opportunity validated: {base_token}/{quote_token} expected profit {profit_percentage:.2f}%"
            )
            return True

        except Exception as e:
            self.logger().error(f"Error during opportunity validation: {str(e)}")
            return False

    async def _calculate_optimal_trade_amount(self, opportunity: Dict[str, Any], max_available: Decimal) -> Decimal:
        """
        Determines the optimal trade amount by simulating profits at different sizes.

        Args:
            opportunity: Arbitrage opportunity dictionary.
            max_available: Maximum available balance of the base token.
        Returns:
            Optimal trade amount as Decimal.
        """
        min_amount = self._minimum_trade_amount
        test_amounts = [
            min_amount,
            max_available * DECIMAL_TEN_PERCENT,
            max_available * DECIMAL_TWENTY_FIVE_PERCENT,
            max_available * DECIMAL_FIFTY_PERCENT,
            max_available * DECIMAL_SEVENTY_FIVE_PERCENT,
            max_available,
        ]
        best_amount = DECIMAL_ZERO
        best_profit_pct = DECIMAL_NEGATIVE_INFINITY
        for amount in sorted(test_amounts):
            if amount > max_available:
                continue
            profit_pct = await self._simulate_arbitrage_profit(opportunity, amount)
            if profit_pct > best_profit_pct:
                best_profit_pct = profit_pct
                best_amount = amount
        return best_amount if best_amount > DECIMAL_ZERO else min_amount

    async def _simulate_arbitrage_profit(self, opportunity: Dict[str, Any], amount: Decimal) -> Decimal:
        """
        Simulates expected profit percentage for a given trade amount.

        Args:
            opportunity: Arbitrage opportunity dictionary.
            amount: Proposed trade amount.
        Returns:
            Expected profit percentage as Decimal.
        """
        base_token = opportunity["base_token"]
        quote_token = opportunity["quote_token"]
        buy_pool = opportunity["buy_pool"]
        sell_pool = opportunity["sell_pool"]
        max_slippage = self._maximum_slippage_percentage
        try:
            buy_quote = await self._get_quote_swap(
                buy_pool, base_token, quote_token, amount, TradeType.SELL, max_slippage
            )
            if not buy_quote or "estimatedAmountOut" not in buy_quote:
                return DECIMAL_NEGATIVE_INFINITY
            expected_quote = Decimal(str(buy_quote["estimatedAmountOut"]))
            sell_quote = await self._get_quote_swap(
                sell_pool, quote_token, base_token, expected_quote, TradeType.SELL, max_slippage
            )
            if not sell_quote or "estimatedAmountOut" not in sell_quote:
                return DECIMAL_NEGATIVE_INFINITY
            expected_return = Decimal(str(sell_quote["estimatedAmountOut"]))
            profit = expected_return - amount
            return (profit / amount) * DECIMAL_ONE_HUNDRED if amount > DECIMAL_ZERO else DECIMAL_ZERO
        except Exception as e:
            self.logger().error(f"Error simulating arbitrage profit: {str(e)}")
            return DECIMAL_NEGATIVE_INFINITY

    async def _execute_arbitrage(self, opportunity: Dict[str, Any]) -> bool:
        """
        Executes an arbitrage trade by performing sequential swaps.

        Args:
            opportunity: Arbitrage opportunity dictionary.
        Returns:
            True if the trade is executed successfully with profit; otherwise, False.
        """
        base_token = opportunity["base_token"]
        quote_token = opportunity["quote_token"]
        buy_pool = opportunity["buy_pool"]
        sell_pool = opportunity["sell_pool"]
        trade_amount = opportunity["trade_amount"]
        expected_quote = opportunity["expected_quote_token"]

        self.logger().info(f"Executing arbitrage for {base_token}/{quote_token}")

        # Gets wallet addresses for each pool.
        buy_wallets = await self._get_wallet_addresses_for_pool(buy_pool)
        sell_wallets = await self._get_wallet_addresses_for_pool(sell_pool)
        if not buy_wallets:
            self.logger().error(f"No wallet found for buy pool {buy_pool.get('address')}")
            return False
        if not sell_wallets:
            self.logger().error(f"No wallet found for sell pool {sell_pool.get('address')}")
            return False
        buy_wallet = buy_wallets[0]
        sell_wallet = sell_wallets[0]

        try:
            # First swap (buy pool): base_token -> quote_token.
            self.logger().info(
                f"Step 1: Swapping {trade_amount} {base_token} for {quote_token} in pool {buy_pool.get('address')}"
            )
            initial_buy = await self._gateway_get_balances(
                buy_pool.get("chain"), buy_pool.get("network"), buy_wallet, [base_token, quote_token]
            )
            if not initial_buy or "balances" not in initial_buy:
                self.logger().error(f"Failed to get initial balances for wallet {buy_wallet}")
                return False
            init_base = Decimal(str(initial_buy["balances"].get(base_token, 0)))
            init_quote = Decimal(str(initial_buy["balances"].get(quote_token, 0)))
            if init_base < trade_amount:
                self.logger().info(f"Insufficient balance in {buy_wallet}: {init_base} {base_token}")
                return False

            first_swap = await self._post_execute_swap(
                pool=buy_pool,
                wallet_address=buy_wallet,
                base_token=base_token,
                quote_token=quote_token,
                amount=trade_amount,
                side=TradeType.SELL,
                slippage_percentage=self._maximum_slippage_percentage,
            )
            if not first_swap or "signature" not in first_swap:
                self.logger().error(f"First swap failed in wallet {buy_wallet}")
                return False
            self.logger().info(f"First swap signature: {first_swap['signature']}")
            confirmed1 = await self._wait_for_transaction_confirmation(
                buy_pool.get("chain"), buy_pool.get("network"), first_swap["signature"]
            )
            if not confirmed1:
                self.logger().error("First swap transaction not confirmed")
                return False

            updated_buy = await self._gateway_get_balances(
                buy_pool.get("chain"), buy_pool.get("network"), buy_wallet, [quote_token]
            )
            if not updated_buy or "balances" not in updated_buy:
                self.logger().error("Failed to get updated buy wallet balances")
                return False
            updated_quote = Decimal(str(updated_buy["balances"].get(quote_token, 0)))
            quote_received = updated_quote - init_quote
            second_swap_amount = quote_received if quote_received > DECIMAL_ZERO else expected_quote
            if quote_received <= 0:
                self.logger().warning(
                    f"Actual quote received undetermined; using expected: {expected_quote} {quote_token}"
                )

            # Second swap (sell pool): quote_token -> base_token.
            self.logger().info(
                f"Step 2: Swapping {second_swap_amount} {quote_token} to {base_token} in pool {sell_pool.get('address')}"
            )
            initial_sell = await self._gateway_get_balances(
                sell_pool.get("chain"), sell_pool.get("network"), sell_wallet, [base_token]
            )
            if not initial_sell or "balances" not in initial_sell:
                self.logger().error(f"Failed to get initial sell wallet balances for {sell_wallet}")
                return False
            init_sell_base = Decimal(str(initial_sell["balances"].get(base_token, 0)))
            second_swap = await self._post_execute_swap(
                pool=sell_pool,
                wallet_address=sell_wallet,
                base_token=quote_token,
                quote_token=base_token,
                amount=second_swap_amount,
                side=TradeType.SELL,
                slippage_percentage=self._maximum_slippage_percentage,
            )
            if not second_swap or "signature" not in second_swap:
                self.logger().error(f"Second swap failed in wallet {sell_wallet}")
                return False
            self.logger().info(f"Second swap signature: {second_swap['signature']}")
            confirmed2 = await self._wait_for_transaction_confirmation(
                sell_pool.get("chain"), sell_pool.get("network"), second_swap["signature"]
            )
            if not confirmed2:
                self.logger().error("Second swap transaction not confirmed")
                return False

            updated_sell = await self._gateway_get_balances(
                sell_pool.get("chain"), sell_pool.get("network"), sell_wallet, [base_token]
            )
            if not updated_sell or "balances" not in updated_sell:
                self.logger().error("Failed to get updated sell wallet balance")
                return False
            final_sell_base = Decimal(str(updated_sell["balances"].get(base_token, 0)))
            profit = final_sell_base - init_sell_base
            profit_pct = (profit / trade_amount) * DECIMAL_ONE_HUNDRED if trade_amount > DECIMAL_ZERO else Decimal("0")
            trade_record = {
                "timestamp": time.time(),
                "buy_wallet": buy_wallet,
                "sell_wallet": sell_wallet,
                "base_token": base_token,
                "quote_token": quote_token,
                "buy_pool": buy_pool.get("address"),
                "sell_pool": sell_pool.get("address"),
                "trade_amount": trade_amount,
                "quote_received": quote_received,
                "profit": profit,
                "profit_percentage": profit_pct,
                "first_swap_tx": first_swap["signature"],
                "second_swap_tx": second_swap["signature"],
            }
            database["execution_history"].append(trade_record)
            if profit > 0:
                self.logger().info(f"Arbitrage trade successful! Profit: {profit} {base_token} ({profit_pct:.2f}%)")
                return True
            else:
                self.logger().warning(
                    f"Arbitrage trade executed with no profit/loss: {profit} {base_token} ({profit_pct:.2f}%)"
                )
                return False
        except Exception as e:
            self.logger().error(f"Error during arbitrage execution: {str(e)}")
            return False

    async def _get_total_token_balance_from_all_wallets(self, token: str) -> Decimal:
        """
        Sums the free balance of a token in all wallets in the database.

        Args:
            token: Symbol of the token.
        Returns:
            Total available balance as Decimal.
        """
        total = DECIMAL_ZERO

        # We don't need lock here, just reading data
        for chain_data in database["connections"].values():
            for network_data in chain_data.values():
                for connector_data in network_data.values():
                    for wallet in connector_data.get("wallets", {}).values():
                        try:
                            bal = wallet.get("tokens", {}).get(token, {}).get("balances", {}).get("free", 0)
                            if bal:
                                total += Decimal(str(bal))
                        except Exception:
                            continue

        return total

    async def _get_wallet_addresses_for_pool(self, pool: Dict[str, Any]) -> List[str]:
        """
        Retrieves wallet addresses associated with a given pool.

        Args:
            pool: Pool dictionary.
        Returns:
            List of wallet addresses.
        """
        pool_id = pool.get("address")
        async with self._db_lock:
            mapping = database["maps"].get("wallets_by_pool", {})
            if pool_id in mapping:
                return mapping[pool_id]
            # Fallback: search configuration
            for chain_conf in self._configuration.get("connections", {}).values():
                for net_conf in chain_conf.values():
                    for connector, conf in net_conf.items():
                        if pool.get("address") in conf.get("pools", []):
                            return conf.get("wallets", [])
            return []

    async def _get_quote_swap(
        self,
        pool: Dict[str, Any],
        base_token: str,
        quote_token: str,
        amount: Decimal,
        side: TradeType,
        slippage_percentage: Decimal,
    ) -> Optional[Dict[str, Any]]:
        """
        Gets a swap quote for a given pool and token pair using the gateway.

        Args:
            pool: Pool dictionary containing network, connector, and address.
            base_token: Token to swap from.
            quote_token: Token to swap to.
            amount: Amount to swap.
            side: Trade side.
            slippage_percentage: Allowed slippage.
        Returns:
            Quote dictionary with 'estimatedAmountOut', or None if unavailable.
        """
        network = pool.get("network")
        connector = pool.get("connector")
        pool_address = pool.get("address")
        try:
            return await self._gateway_quote_swap(
                network, connector, base_token, quote_token, amount, side, slippage_percentage, pool_address
            )
        except Exception as e:
            self.logger().error(f"Error getting swap quote: {str(e)}")
            return None

    async def _post_execute_swap(
        self,
        pool: Dict[str, Any],
        wallet_address: str,
        base_token: str,
        quote_token: str,
        amount: Decimal,
        side: TradeType,
        slippage_percentage: Decimal,
    ) -> Optional[Dict[str, Any]]:
        """
        Executes a swap transaction via the gateway.

        Args:
            pool: Pool dictionary.
            wallet_address: Wallet address to use.
            base_token: Token to swap from.
            quote_token: Token to swap to.
            amount: Amount to swap.
            side: Trade side.
            slippage_percentage: Allowed slippage.
        Returns:
            Dictionary with transaction details (e.g., 'signature') or None.
        """
        network = pool.get("network")
        connector = pool.get("connector")
        pool_address = pool.get("address")
        try:
            return await self._gateway_execute_swap(
                network,
                connector,
                wallet_address,
                base_token,
                quote_token,
                side,
                amount,
                slippage_percentage,
                pool_address,
            )
        except Exception as e:
            self.logger().error(f"Error executing swap: {str(e)}")
            return None

    async def _wait_for_transaction_confirmation(
        self, chain: str, network: str, tx_hash: str, max_timeout: int = None
    ) -> bool:
        """
        Waits for a transaction to be confirmed via polling the gateway.

        Args:
            chain: Chain identifier.
            network: Network identifier.
            tx_hash: Transaction hash.
            max_timeout: Maximum time to wait (seconds), default is class setting.
        Returns:
            True if confirmed; otherwise, False.
        """
        if max_timeout is None:
            max_timeout = self._maximum_transaction_confirmation_timeout
        start_time = time.time()
        while time.time() - start_time < max_timeout:
            try:
                tx_status = await self._gateway_poll_transaction(chain, network, tx_hash)
                if tx_status and tx_status.get("txStatus") == 1:
                    self.logger().info(f"Transaction {tx_hash} confirmed!")
                    return True
                if tx_status and tx_status.get("txStatus") == -1:
                    self.logger().error(f"Transaction {tx_hash} failed: {tx_status}")
                    return False
                await asyncio.sleep(self._transaction_polling_interval)
            except Exception as e:
                self.logger().error(f"Error polling transaction {tx_hash}: {str(e)}")
                await asyncio.sleep(self._transaction_polling_interval)
        self.logger().warning(f"Transaction {tx_hash} confirmation timed out after {max_timeout} seconds")
        return False

    # --------------------------------------------------------------------------
    # Gateway Helper Methods (using retry/timeout)
    # --------------------------------------------------------------------------
    @run_with_retry_and_timeout(
        retries=GATEWAY_REQUEST_RETRIES, delay=GATEWAY_REQUEST_DELAY, timeout=GATEWAY_REQUEST_TIMEOUT
    )
    async def _gateway_ping_gateway(self):
        """Pings the gateway server to verify connectivity."""
        return await self._gateway_http_client.ping_gateway()

    @run_with_retry_and_timeout(
        retries=GATEWAY_REQUEST_RETRIES, delay=GATEWAY_REQUEST_DELAY, timeout=GATEWAY_REQUEST_TIMEOUT
    )
    async def _gateway_get_pool_info(self, connector: str, network: str, pool_address: str):
        """Retrieves pool details from the gateway."""
        return await self._gateway_http_client.amm_pool_info(connector, network, pool_address)

    @run_with_retry_and_timeout(
        retries=GATEWAY_REQUEST_RETRIES, delay=GATEWAY_REQUEST_DELAY, timeout=GATEWAY_REQUEST_TIMEOUT
    )
    async def _gateway_get_tokens(
        self, chain: str, network: str, token_symbols: Optional[Union[str, List[str]]] = None
    ):
        """Retrieves token information from the gateway."""
        return await self._gateway_http_client.get_tokens(chain, network, token_symbols)

    @run_with_retry_and_timeout(
        retries=GATEWAY_REQUEST_RETRIES, delay=GATEWAY_REQUEST_DELAY, timeout=GATEWAY_REQUEST_TIMEOUT
    )
    async def _gateway_get_balances(
        self, chain: str, network: str, address: str, token_symbols: Optional[Union[str, List[str]]] = None
    ):
        """Retrieves token balances for a wallet address from the gateway."""
        return await self._gateway_http_client.get_balances(chain, network, address, token_symbols)

    @run_with_retry_and_timeout(
        retries=GATEWAY_REQUEST_RETRIES, delay=GATEWAY_REQUEST_DELAY, timeout=GATEWAY_REQUEST_TIMEOUT
    )
    async def _gateway_quote_swap(
        self,
        network: str,
        connector: str,
        base_asset: str,
        quote_asset: str,
        amount: Decimal,
        side: TradeType,
        slippage_percentage: Decimal,
        pool_address: str,
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

    @run_with_retry_and_timeout(
        retries=GATEWAY_REQUEST_RETRIES, delay=GATEWAY_REQUEST_DELAY, timeout=GATEWAY_REQUEST_TIMEOUT
    )
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
        pool_address: str,
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

    @run_with_retry_and_timeout(
        retries=GATEWAY_REQUEST_RETRIES, delay=GATEWAY_REQUEST_DELAY, timeout=GATEWAY_REQUEST_TIMEOUT
    )
    async def _gateway_poll_transaction(self, chain: str, network: str, tx_hash: str):
        """Polls the transaction status from the gateway."""
        return await self._gateway_http_client.get_transaction_status(chain, network, tx_hash)


# ==============================================================================
# End of Module
