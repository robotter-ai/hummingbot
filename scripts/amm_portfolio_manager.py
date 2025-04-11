import asyncio
import logging
import os
import threading
import time
from decimal import Decimal
from enum import Enum
from itertools import permutations
from typing import Any, Dict, List, Optional, Union

from pydantic.v1 import Field, validator

from hummingbot.client.config.config_data_types import BaseClientModel
from hummingbot.client.settings import GatewayConnectionSetting
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.event.events import TradeType
from hummingbot.core.gateway.gateway_http_client import GatewayHttpClient
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase
from scripts.utility.utils import Logger, logged_class, run_with_retry_and_timeout

# Constants for Decimal calculations
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

# Constants for Gateway HTTP client retries
GATEWAY_REQUEST_RETRIES = 3
GATEWAY_REQUEST_DELAY = 1  # seconds
GATEWAY_REQUEST_TIMEOUT = 30  # seconds


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


configuration: Dict[str, Any] = {
    "globals": {
        "maximum_slippage_percentage": "0.5",  # 0.5 means 0.5%, or 0.005, in the code
        "minimum_profitability_percentage": "-1",  # 1 means 1%, or 0.01, in the code
        "arbitrage_check_interval_seconds": "60",  # Time between arbitrage checks
        "minimum_trade_amount": "0.1",  # Minimum amount to consider for a trade
        "time_delay_between_arbitrages": "1",  # Time delay between arbitrage trades
        "transaction_confirmation_delay": "2",  # Time delay between transaction confirmation
        "transaction_polling_interval": "2",  # Time delay between transaction polling
        "data_update_intervals": {
            "wallet": "60",  # Update wallet data every 60 seconds
            "token": "300",  # Update token data every 300 seconds
            "pool": "120",  # Update pool data every 120 seconds
        },
    },
    "connections": {
        "polkadot": {
            "mainnet": {
                "hydration": {
                    "wallets": [
                        "5HKTQCEWuuA9bJEqFbAEsbwFQfEe5tXrbZXWj7yQpuxVSHKt",
                    ],
                    "pools": [
                        # "7JRrXBpB1K2JUapwojTYLZPoMvLPMQUDyiEyJb5hj7wad1of",  # XyK / Isolated pool
                        # "7L53bUTBbfuj14UpdCNPwmgzzHSsrsTWBHX5pys32mVWM3C1",  # Omni pool
                        "7LVGEVLFXpsCCtnsvhzkSMQARU7gRVCtwMckG7u7d3V6FVvG",  # Stable pool
                        # "<pool_address>",  # LBP pool
                    ],
                }
            },
        },
        "solana": {
            "mainnet-beta": {
                "raydium": {
                    "wallets": [
                        "7pWpBM8xtVHJq7C4BBivumFbzAC2J8XndTWvmg9GGXDb",
                    ],
                    "pools": [
                        "2EXiumdi14E9b8Fy62QcA5Uh6WdHS2b38wtSxp72Mibj",
                        "3NeUgARDmFgnKtkJLqUcEUNCfknFCcGsFfMJCtx6bAgx"
                        # "G7mw1d83ismcQJKkzt62Ug4noXCjVhu3eV7U5EMgge6Z",  # XyK / Isolated pool
                        # "<pool_address>",  # Omni pool
                        # "<pool_address>",  # Stable pool
                        # "<pool_address>",  # LBP pool
                    ],
                }
            }
        },
    },
    "tokens": [
        "USDC",
        "USDT",
        # "DOT",
        # "HDX",
    ],
}

database: Dict[str, Any] = {
    "connections": {},
    "arbitrage_opportunities": [],
    "execution_history": [],
    "maps": {
        "pools_by_tokens": {},  # Format: "token1/token2" -> [pool_internal_id1, pool_internal_id2, ...]
        "wallets_by_pool": {},  # Format: pool_internal_id -> [wallet_internal_id1, wallet_internal_id2, ...]
        "pools_by_wallet": {},  # Format: wallet_internal_id -> [pool_internal_id1, pool_internal_id2, ...]
    },
}


class WalletConfig(BaseClientModel):
    """Configuration for a wallet"""

    address: str = Field(...)


class PoolConfig(BaseClientModel):
    """Configuration for a pool"""

    address: str = Field(...)


class ConnectorConfig(BaseClientModel):
    """Configuration for a connector with wallets and pools"""

    wallets: List[str] = Field(default_factory=list)
    pools: List[str] = Field(default_factory=list)


class NetworkConfig(BaseClientModel):
    """Configuration for a network with connectors"""

    connectors: Dict[str, ConnectorConfig] = Field(default_factory=dict)


class ChainConfig(BaseClientModel):
    """Configuration for a chain with networks"""

    networks: Dict[str, NetworkConfig] = Field(default_factory=dict)


class DataUpdateIntervals(BaseClientModel):
    """Configuration for data update intervals"""

    wallet: int = Field(default=60)
    token: int = Field(default=300)
    pool: int = Field(default=120)

    @validator("wallet", "token", "pool")
    def validate_positive_interval(cls, v):
        if v <= 0:
            raise ValueError("Update interval must be positive")
        return v


class GlobalConfig(BaseClientModel):
    """Global configuration parameters"""

    maximum_slippage_percentage: Decimal = Field(default=Decimal("0.5"))
    minimum_profitability_percentage: Decimal = Field(default=Decimal("-1"))
    arbitrage_check_interval_seconds: int = Field(default=60)
    minimum_trade_amount: Decimal = Field(default=Decimal("0.1"))
    time_delay_between_arbitrages: int = Field(default=1)
    transaction_confirmation_delay: int = Field(default=2)
    transaction_polling_interval: int = Field(default=2)
    data_update_intervals: DataUpdateIntervals = Field(default_factory=DataUpdateIntervals)


class AMMRobustPositionManagerConfiguration(BaseClientModel):
    """
    Configuration for the AMM Robust Position Manager strategy.

    This model reflects the structure of the global configuration object that controls
    various aspects of the strategy's behavior including slippage tolerances,
    update intervals, and connection details.
    """

    script_file_name: str = Field(default_factory=lambda: os.path.basename(__file__))
    globals: GlobalConfig = Field(default_factory=GlobalConfig)
    chains: Dict[str, ChainConfig] = Field(default_factory=dict)
    tokens: List[str] = Field(default_factory=list)

    class Config:
        arbitrary_types_allowed = True


logger = Logger(path="logs/logs_amm_portfolio_manager.py", level=logging.DEBUG)


@logged_class(logger=logger)
class AMMRobustPositionManager(ScriptStrategyBase):
    """
    AMM Robust Position Manager - A strategy for managing and optimizing positions across AMM pools.

    This strategy monitors multiple AMM pools across different chains and networks, identifies
    arbitrage opportunities, and executes trades to capitalize on price discrepancies.

    Key features:
    - Multi-chain and multi-pool support
    - Efficient data caching to minimize API calls
    - Separate threads for data collection and strategy execution
    - Configurable parameters for risk management
    - Advanced opportunity validation with slippage simulation
    """

    markets: Dict[str, Any] = {}
    _configuration = None
    _gateway_is_ready = False
    _gateway_http_client: Optional[GatewayHttpClient] = None
    _all_gateway_connections: List[Dict[str, Any]] = []
    _last_arbitrage_check_time = 0
    _maximum_slippage_percentage: Decimal = DECIMAL_ZERO
    _minimum_profitability_percentage: Decimal = DECIMAL_ZERO
    _arbitrage_check_interval_seconds: Decimal = DECIMAL_ZERO
    _minimum_trade_amount: Decimal = DECIMAL_ZERO
    _time_delay_between_arbitrages: Decimal = DECIMAL_ZERO
    _transaction_confirmation_delay: Decimal = DECIMAL_ZERO
    _maximum_transaction_confirmation_timeout: int = 60
    _balance_cache: Dict[str, Dict[str, Any]] = {}
    _data_update_thread = None
    _stop_threads = False
    _data_update_intervals = {"wallet": 60, "token": 300, "pool": 120}
    _last_wallet_update_time = 0
    _last_token_update_time = 0
    _last_pool_update_time = 0
    _thread_lock = threading.Lock()

    def __init__(self, connectors: Dict[str, ConnectorBase]):
        """
        Initialize the AMM Robust Position Manager strategy.

        Args:
            connectors: Dictionary of available connectors
        """
        super().__init__(connectors)
        self._initialize()

    def _initialize(self, _strategy_configuration: AMMRobustPositionManagerConfiguration = None):
        """
        Initialize the strategy with configuration parameters.

        Args:
            _strategy_configuration: Optional configuration to use instead of the default
        """
        self._gateway_http_client = GatewayHttpClient.get_instance()

        # Set configuration
        self._configuration = configuration

        # Use the global configuration variable to initialize global values
        self._maximum_slippage_percentage = Decimal(
            self._configuration["globals"].get("maximum_slippage_percentage", DECIMAL_ZERO)
        )
        self._minimum_profitability_percentage = Decimal(
            self._configuration["globals"].get("minimum_profitability_percentage", DECIMAL_ZERO)
        )
        self._arbitrage_check_interval_seconds = Decimal(
            self._configuration["globals"].get("arbitrage_check_interval_seconds", DECIMAL_ZERO)
        )
        self._minimum_trade_amount = Decimal(self._configuration["globals"].get("minimum_trade_amount", DECIMAL_ZERO))
        self._time_delay_between_arbitrages = Decimal(
            self._configuration["globals"].get("time_delay_between_arbitrages", DECIMAL_ZERO)
        )
        self._transaction_confirmation_delay = Decimal(
            self._configuration["globals"].get("transaction_confirmation_delay", DECIMAL_ZERO)
        )
        self._transaction_polling_interval = int(
            self._configuration["globals"].get("transaction_polling_interval", DECIMAL_ZERO)
        )

        # Initialize data update intervals
        data_update_intervals = self._configuration["globals"].get("data_update_intervals", {})
        if data_update_intervals:
            self._data_update_intervals = {
                "wallet": int(data_update_intervals.get("wallet", 60)),
                "token": int(data_update_intervals.get("token", 300)),
                "pool": int(data_update_intervals.get("pool", 120)),
            }

        # Set transaction confirmation timeout to 5x the configured delay as a safety measure
        # If the configured delay is 0, use the default timeout (60 seconds)
        if self._transaction_confirmation_delay > 0:
            self._maximum_transaction_confirmation_timeout = int(self._transaction_confirmation_delay) * 5

        self._all_gateway_connections = GatewayConnectionSetting.load()

        self._log_initialization()

        # Start data update thread
        self._start_data_update_thread()

    def _log_initialization(self):
        """Log the initialization of the strategy"""
        self.logger().info(f"Starting {self.__class__.__name__} strategy")

    def _start_data_update_thread(self):
        """Start a separate thread for data collection and updates"""
        self._stop_threads = False
        self._data_update_thread = threading.Thread(target=self._run_data_update_loop)
        self._data_update_thread.daemon = True
        self._data_update_thread.start()
        self.logger().info("Data update thread started")

    def _run_data_update_loop(self):
        """Run the data update loop in a separate thread"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            while not self._stop_threads:
                # Check gateway status first
                if not self._gateway_is_ready:
                    loop.run_until_complete(self._check_gateway_status())
                    if not self._gateway_is_ready:
                        time.sleep(5)  # Wait before retrying
                        continue

                # Update database with latest information
                loop.run_until_complete(self._update_database())

                # Sleep for a short interval before checking again
                time.sleep(1)
        except Exception as e:
            self.logger().error(f"Error in data update thread: {str(e)}")
        finally:
            loop.close()

    def on_tick(self):
        """
        Strategy execution that runs on each tick.
        This method triggers the strategy logic.
        """
        safe_ensure_future(self._async_on_tick())

    async def _async_on_tick(self):
        """Main strategy execution logic that runs on each tick"""
        # We don't need to check gateway status here as it's done in the data update thread
        if not self._gateway_is_ready:
            return

        # Check if it's time to run arbitrage check
        current_time = Decimal(time.time())
        arbitrage_check_interval = self._arbitrage_check_interval_seconds

        if current_time - self._last_arbitrage_check_time >= arbitrage_check_interval:
            self._last_arbitrage_check_time = current_time
            await self._run_arbitrage_strategy()

    def stop(self):
        """Stop the strategy and clean up resources"""
        self._stop_threads = True
        if self._data_update_thread and self._data_update_thread.is_alive():
            self._data_update_thread.join(timeout=10)
        super().stop()

    async def _run_arbitrage_strategy(self):
        """
        Execute the arbitrage strategy.
        This method finds and validates arbitrage opportunities across pools.
        """
        self.logger().info("Checking for arbitrage opportunities...")

        # Find the most promising token pairs first for optimization
        promising_pairs = self._find_most_promising_token_pairs()

        if not promising_pairs:
            self.logger().info("No promising token pairs found")
            return

        self.logger().info(f"Found {len(promising_pairs)} promising token pairs")

        # Find arbitrage opportunities using promising pairs
        opportunities = await self._find_arbitrage_opportunities(promising_pairs)

        if not opportunities:
            self.logger().info("No arbitrage opportunities found")
            return

        self.logger().info(f"Found {len(opportunities)} potential arbitrage opportunities")

        # Validate and execute opportunities
        for opportunity in opportunities:
            if await self._validate_opportunity(opportunity):
                await self._execute_arbitrage(opportunity)
                # Wait between arbitrages
                if self._time_delay_between_arbitrages > 0:
                    await asyncio.sleep(float(self._time_delay_between_arbitrages))

    def _find_most_promising_token_pairs(self):
        """
        Identify token pairs with highest arbitrage potential.

        Returns:
            List[Dict]: List of token pairs sorted by potential
        """
        tokens = self._configuration["tokens"]
        promising_pairs = []

        for i in range(len(tokens)):
            for j in range(i + 1, len(tokens)):
                token1, token2 = tokens[i], tokens[j]
                pools = self._find_pools_with_token_pair(token1, token2)

                if len(pools) >= 2:  # Need at least 2 pools for arbitrage
                    # Check liquidity and volume to determine potential
                    total_volume = sum(Decimal(str(p.get("volume", {}).get("24h", 0) or 0)) for p in pools)
                    total_liquidity = sum(Decimal(str(p.get("total_value_locked", 0) or 0)) for p in pools)

                    # Calculate price variance between pools
                    prices = []
                    for pool in pools:
                        price = self._get_cached_token_price_in_pool(token1, token2, pool)
                        if price is not None:
                            prices.append(price)

                    price_variance = 0
                    if len(prices) >= 2:
                        min_price = min(prices)
                        max_price = max(prices)
                        if min_price > 0:
                            price_variance = (max_price - min_price) / min_price * 100

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

        # Sort by price variance, pool count, and volume
        promising_pairs.sort(key=lambda x: (x["price_variance"], x["pools_count"], x["total_volume"]), reverse=True)
        return promising_pairs[:5]  # Return top 5 most promising pairs

    def _get_cached_token_price_in_pool(self, base_token, quote_token, pool):
        """
        Get token price from cached pool data.

        Args:
            base_token: Base token symbol
            quote_token: Quote token symbol
            pool: Pool data dictionary

        Returns:
            Decimal: Token price or None if not available
        """
        try:
            if (
                "tokens" in pool
                and base_token in pool["tokens"]
                and "prices" in pool["tokens"][base_token]
                and quote_token in pool["tokens"][base_token]["prices"]
            ):
                return pool["tokens"][base_token]["prices"][quote_token]
        except (KeyError, TypeError):
            pass
        return None

    async def _find_arbitrage_opportunities(self, promising_pairs=None) -> List[Dict[str, Any]]:
        """
        Find arbitrage opportunities across pools and tokens.

        Args:
            promising_pairs: Optional list of promising token pairs to check

        Returns:
            List[Dict]: List of arbitrage opportunities
        """
        opportunities = []
        minimum_profitability_percentage = self._minimum_profitability_percentage

        # Use provided promising pairs or get all token pairs
        if promising_pairs:
            token_pairs = [(pair["token1"], pair["token2"]) for pair in promising_pairs]
        else:
            tokens = self._configuration["tokens"]
            token_pairs = [(tokens[i], tokens[j]) for i in range(len(tokens)) for j in range(i + 1, len(tokens))]

        # Check each token pair for arbitrage opportunities
        for base_token, quote_token in token_pairs:
            # Find pools containing both tokens
            relevant_pools = self._find_pools_with_token_pair(base_token, quote_token)

            if len(relevant_pools) < 2:
                continue

            # Check for arbitrage between different pools
            for pool_idx in range(len(relevant_pools)):
                pool_1 = relevant_pools[pool_idx]
                for pool_2_idx in range(pool_idx + 1, len(relevant_pools)):
                    pool_2 = relevant_pools[pool_2_idx]

                    # Calculate price difference
                    price_difference_percentage = await self._calculate_price_difference_percentage(
                        base_token, quote_token, pool_1, pool_2
                    )

                    if price_difference_percentage is None:
                        continue

                    # Check if difference exceeds profitability threshold
                    if abs(price_difference_percentage) > minimum_profitability_percentage:
                        # Determine which pool to buy from and which to sell to
                        buy_pool, sell_pool = (pool_1, pool_2) if price_difference_percentage > 0 else (pool_2, pool_1)

                        opportunity = {
                            "base_token": base_token,
                            "quote_token": quote_token,
                            "buy_pool": buy_pool,
                            "sell_pool": sell_pool,
                            "price_difference_percentage": price_difference_percentage,
                            "timestamp": time.time(),
                        }

                        opportunities.append(opportunity)

                        self.logger().info(
                            f"Found arbitrage opportunity: {base_token}/{quote_token} with "
                            f"{price_difference_percentage:.2f}% difference between "
                            f"{buy_pool['address']} and {sell_pool['address']}"
                        )

        # Update the database with opportunities
        with self._thread_lock:
            database["arbitrage_opportunities"] = opportunities

        return opportunities

    async def _validate_opportunity(self, opportunity: Dict[str, Any]) -> bool:
        """
        Validate an arbitrage opportunity by checking slippage and available balances.

        Args:
            opportunity: Dictionary containing arbitrage opportunity details

        Returns:
            bool: True if opportunity is valid, False otherwise
        """
        base_token = opportunity["base_token"]
        quote_token = opportunity["quote_token"]
        buy_pool = opportunity["buy_pool"]
        sell_pool = opportunity["sell_pool"]

        # Get token balances
        base_token_balance = await self._get_total_token_balance_from_all_wallets(base_token)

        # Check if we have enough balance
        minimum_trade_amount = self._minimum_trade_amount
        if base_token_balance < minimum_trade_amount:
            self.logger().info(f"Insufficient balance of {base_token} for arbitrage: {base_token_balance}")
            return False

        # Calculate optimal trade amount
        trade_amount = await self._calculate_optimal_trade_amount(opportunity, base_token_balance)

        if not trade_amount or trade_amount <= DECIMAL_ZERO:
            self.logger().info("Optimal trade amount calculation resulted in zero or negative amount")
            return False

        # Check slippage for both trades
        maximum_slippage_percentage = self._maximum_slippage_percentage

        try:
            # Get quote for buying quote_token with base_token in buy_pool
            buy_quote = await self._get_quote_swap(
                buy_pool,
                base_token,
                quote_token,
                trade_amount,
                TradeType.SELL,  # Selling base_token to buy quote_token
                maximum_slippage_percentage,
            )

            if not buy_quote or "estimatedAmountOut" not in buy_quote:
                self.logger().info(
                    f"Failed to get buy quote for {base_token}/{quote_token} in pool {buy_pool['address']}"
                )
                return False

            expected_quote_token = Decimal(str(buy_quote["estimatedAmountOut"]))

            # Get quote for selling quote_token for base_token in sell_pool
            sell_quote = await self._get_quote_swap(
                sell_pool,
                quote_token,
                base_token,
                expected_quote_token,
                TradeType.SELL,  # Selling quote_token to get back base_token
                maximum_slippage_percentage,
            )

            if not sell_quote or "estimatedAmountOut" not in sell_quote:
                self.logger().info(
                    f"Failed to get sell quote for {quote_token}/{base_token} in pool {sell_pool['address']}"
                )
                return False

            expected_base_token_return = Decimal(str(sell_quote["estimatedAmountOut"]))

            # Calculate expected profit
            expected_profit = expected_base_token_return - trade_amount
            expected_profit_percentage = (expected_profit / trade_amount) * DECIMAL_ONE_HUNDRED

            # Add profit details to the opportunity
            opportunity["trade_amount"] = trade_amount
            opportunity["expected_quote_token"] = expected_quote_token
            opportunity["expected_base_token_return"] = expected_base_token_return
            opportunity["expected_profit"] = expected_profit
            opportunity["expected_profit_percentage"] = expected_profit_percentage

            # Check if still profitable after slippage
            minimum_profitability_percentage = self._minimum_profitability_percentage

            if expected_profit_percentage < minimum_profitability_percentage:
                self.logger().info(
                    f"Arbitrage opportunity no longer profitable after slippage: "
                    f"{expected_profit_percentage:.2f}% < {minimum_profitability_percentage}%"
                )
                return False

            self.logger().info(
                f"Validated arbitrage opportunity: {base_token}/{quote_token} with "
                f"expected profit of {expected_profit_percentage:.2f}%"
            )
            return True

        except Exception as e:
            self.logger().error(f"Error validating opportunity: {str(e)}")
            return False

    async def _calculate_optimal_trade_amount(self, opportunity: Dict[str, Any], max_available: Decimal) -> Decimal:
        """
        Calculate the optimal amount to trade based on slippage considerations.

        Args:
            opportunity: Dictionary containing arbitrage opportunity details
            max_available: Maximum available balance

        Returns:
            Decimal: Optimal trade amount
        """
        minimum_trade_amount = self._minimum_trade_amount

        # Try different trade amounts to find the optimal one
        test_amounts = [
            minimum_trade_amount,
            max_available * DECIMAL_TEN_PERCENT,
            max_available * DECIMAL_TWENTY_FIVE_PERCENT,
            max_available * DECIMAL_FIFTY_PERCENT,
            max_available * DECIMAL_SEVENTY_FIVE_PERCENT,
            max_available,
        ]

        best_amount = DECIMAL_ZERO
        best_profit_percentage = DECIMAL_NEGATIVE_INFINITY

        # Use a faster search approach
        # First check min and max amounts to determine if scaling is worth it
        min_profit = await self._simulate_arbitrage_profit(opportunity, minimum_trade_amount)
        max_profit = await self._simulate_arbitrage_profit(opportunity, max_available)

        # If profit decreases with size, use minimum amount
        if min_profit >= max_profit:
            return minimum_trade_amount

        # If profit increases with size, try intermediate amounts
        for amount in sorted(test_amounts):
            if amount > max_available:
                continue

            # Skip amounts that are too close to previously tested ones
            if best_amount > DECIMAL_ZERO and abs(amount - best_amount) / best_amount < DECIMAL_TEN_PERCENT:
                continue

            profit_percentage = await self._simulate_arbitrage_profit(opportunity, amount)

            if profit_percentage > best_profit_percentage:
                best_profit_percentage = profit_percentage
                best_amount = amount

        return best_amount if best_amount > DECIMAL_ZERO else minimum_trade_amount

    async def _execute_arbitrage(self, opportunity: Dict[str, Any]) -> bool:
        """Execute an arbitrage trade between two pools using their respective wallet addresses"""
        base_token = opportunity["base_token"]
        quote_token = opportunity["quote_token"]
        buy_pool = opportunity["buy_pool"]
        sell_pool = opportunity["sell_pool"]
        trade_amount = opportunity["trade_amount"]
        expected_quote_token = opportunity["expected_quote_token"]

        self.logger().info(f"Executing arbitrage trade: {base_token}/{quote_token}")

        # Get wallet address for buy pool
        buy_pool_wallet_addresses = self._get_wallet_addresses_for_pool(buy_pool)
        buy_pool_wallet_address = buy_pool_wallet_addresses[0] if buy_pool_wallet_addresses else None
        if not buy_pool_wallet_address:
            self.logger().error(f"No wallet address found for buy pool {buy_pool['address']}")
            return False

        # Get wallet address for sell pool
        sell_pool_wallet_addresses = self._get_wallet_addresses_for_pool(sell_pool)
        sell_pool_wallet_address = sell_pool_wallet_addresses[0] if sell_pool_wallet_addresses else None
        if not sell_pool_wallet_address:
            self.logger().error(f"No wallet address found for sell pool {sell_pool['address']}")
            return False

        # Track overall success
        overall_success = False
        maximum_slippage_percentage = self._maximum_slippage_percentage

        # Execute first trade with buy pool wallet
        self.logger().info(f"Attempting first swap with wallet: {buy_pool_wallet_address}")

        try:
            # Record initial balance for buy pool wallet
            initial_wallet_balances = await self._gateway_get_balances(
                buy_pool.get("chain"), buy_pool.get("network"), buy_pool_wallet_address, [base_token, quote_token]
            )

            if not initial_wallet_balances or "balances" not in initial_wallet_balances:
                self.logger().error(f"Failed to get initial balance for wallet {buy_pool_wallet_address}")
                return False

            initial_base_token_balance = Decimal(str(initial_wallet_balances["balances"].get(base_token, 0)))
            initial_quote_token_balance = Decimal(str(initial_wallet_balances["balances"].get(quote_token, 0)))

            # Check if wallet has sufficient balance
            if initial_base_token_balance < trade_amount:
                self.logger().info(
                    f"Wallet {buy_pool_wallet_address} has insufficient balance: {initial_base_token_balance} {base_token}"
                )
                return False

            self.logger().info(
                f"Step 1: Swap {trade_amount} {base_token} for {quote_token} in buy pool {buy_pool['address']}"
            )

            # Execute first swap: base_token -> quote_token in buy_pool
            first_swap_result = await self._post_execute_swap(
                pool=buy_pool,
                wallet_address=buy_pool_wallet_address,
                base_token=base_token,
                quote_token=quote_token,
                amount=trade_amount,
                side=TradeType.SELL,  # Selling base_token to buy quote_token
                slippage_percentage=maximum_slippage_percentage,
            )

            if not first_swap_result or "signature" not in first_swap_result:
                self.logger().error(
                    f"First swap failed for wallet {buy_pool_wallet_address}: {base_token} -> {quote_token}"
                )
                return False

            self.logger().info(f"First swap completed with transaction signature: {first_swap_result['signature']}")

            # Wait for first transaction to be confirmed using polling
            first_transaction_confirmation = await self._wait_for_transaction_confirmation(
                buy_pool.get("chain"), buy_pool.get("network"), first_swap_result["signature"]
            )

            if not first_transaction_confirmation:
                self.logger().error(f"First swap transaction confirmation failed for {first_swap_result['signature']}")
                return False

            # Get quote_token balance after first swap
            updated_wallet_balances = await self._gateway_get_balances(
                buy_pool.get("chain"), buy_pool.get("network"), buy_pool_wallet_address, [quote_token]
            )

            if not updated_wallet_balances or "balances" not in updated_wallet_balances:
                self.logger().error(f"Failed to get updated balance for wallet {buy_pool_wallet_address}")
                return False

            quote_token_balance_after_swap = Decimal(str(updated_wallet_balances["balances"].get(quote_token, 0)))

            # Calculate actual amount of quote_token received from the first swap
            quote_token_received = quote_token_balance_after_swap - initial_quote_token_balance

            # Use the actual amount received for the second swap or the expected amount if there's an issue
            second_swap_amount = quote_token_received if quote_token_received > 0 else expected_quote_token

            if quote_token_received <= 0:
                self.logger().warning(
                    f"Couldn't determine actual quote token received, using expected amount: {expected_quote_token} {quote_token}"
                )

            # Execute second swap: quote_token -> base_token in sell_pool with sell pool wallet
            self.logger().info(
                f"Step 2: Swap {second_swap_amount} {quote_token} back to {base_token} in sell pool {sell_pool['address']} using wallet {sell_pool_wallet_address}"
            )

            # Record initial balance for sell pool wallet
            initial_sell_wallet_balances = await self._gateway_get_balances(
                sell_pool.get("chain"), sell_pool.get("network"), sell_pool_wallet_address, [base_token]
            )

            if not initial_sell_wallet_balances or "balances" not in initial_sell_wallet_balances:
                self.logger().error(f"Failed to get initial balance for sell wallet {sell_pool_wallet_address}")
                return False

            initial_sell_base_token_balance = Decimal(str(initial_sell_wallet_balances["balances"].get(base_token, 0)))

            second_swap_result = await self._post_execute_swap(
                pool=sell_pool,
                wallet_address=sell_pool_wallet_address,
                base_token=quote_token,
                quote_token=base_token,
                amount=second_swap_amount,
                side=TradeType.SELL,  # Selling quote_token to get back base_token
                slippage_percentage=maximum_slippage_percentage,
            )

            if not second_swap_result or "signature" not in second_swap_result:
                self.logger().error(
                    f"Second swap failed for wallet {sell_pool_wallet_address}: {quote_token} -> {base_token}"
                )
                return False

            self.logger().info(f"Second swap completed with transaction signature: {second_swap_result['signature']}")

            # Wait for second transaction to be confirmed using polling
            second_transaction_confirmation = await self._wait_for_transaction_confirmation(
                sell_pool.get("chain"), sell_pool.get("network"), second_swap_result["signature"]
            )

            if not second_transaction_confirmation:
                self.logger().error(
                    f"Second swap transaction confirmation failed for {second_swap_result['signature']}"
                )
                return False

            # Calculate actual profit by checking final balance
            final_buy_wallet_balances = await self._gateway_get_balances(
                buy_pool.get("chain"), buy_pool.get("network"), buy_pool_wallet_address, [base_token]
            )

            final_sell_wallet_balances = await self._gateway_get_balances(
                sell_pool.get("chain"), sell_pool.get("network"), sell_pool_wallet_address, [base_token]
            )

            if not final_buy_wallet_balances or "balances" not in final_buy_wallet_balances:
                self.logger().error(f"Failed to get final balance for buy wallet {buy_pool_wallet_address}")
                return False

            if not final_sell_wallet_balances or "balances" not in final_sell_wallet_balances:
                self.logger().error(f"Failed to get final balance for sell wallet {sell_pool_wallet_address}")
                return False

            final_buy_base_token_balance = Decimal(str(final_buy_wallet_balances["balances"].get(base_token, 0)))
            final_sell_base_token_balance = Decimal(str(final_sell_wallet_balances["balances"].get(base_token, 0)))

            # Calculate profit for the buy wallet (negative indicates amount spent)
            buy_wallet_profit = final_buy_base_token_balance - initial_base_token_balance

            # Calculate profit for the sell wallet (positive indicates amount gained)
            sell_wallet_profit = final_sell_base_token_balance - initial_sell_base_token_balance

            # Total profit is the sum of both changes
            actual_profit = buy_wallet_profit + sell_wallet_profit

            # Calculate profit percentage based on initial investment
            actual_profit_percentage = (actual_profit / trade_amount) * DECIMAL_ONE_HUNDRED if trade_amount > 0 else 0

            # Record trade result in execution history
            trade_result = {
                "timestamp": time.time(),
                "buy_wallet_address": buy_pool_wallet_address,
                "sell_wallet_address": sell_pool_wallet_address,
                "base_token": base_token,
                "quote_token": quote_token,
                "buy_pool": buy_pool["address"],
                "sell_pool": sell_pool["address"],
                "trade_amount": trade_amount,
                "quote_amount_received": quote_token_received,
                "buy_wallet_balance_change": buy_wallet_profit,
                "sell_wallet_balance_change": sell_wallet_profit,
                "profit": actual_profit,
                "profit_percentage": actual_profit_percentage,
                "first_swap_tx": first_swap_result["signature"],
                "second_swap_tx": second_swap_result["signature"],
            }

            database["execution_history"].append(trade_result)

            if actual_profit > 0:
                self.logger().info(
                    f"Arbitrage trade successful! Profit: {actual_profit} {base_token} ({actual_profit_percentage:.2f}%)"
                )
                overall_success = True
            else:
                self.logger().warning(
                    f"Arbitrage trade completed with loss: {actual_profit} {base_token} ({actual_profit_percentage:.2f}%)"
                )

        except Exception as e:
            self.logger().error(
                f"Error executing arbitrage between wallets {buy_pool_wallet_address} and {sell_pool_wallet_address}: {str(e)}"
            )
            return False

        return overall_success

    async def _get_quote_swap(
        self,
        pool: Dict[str, Any],
        base_token: str,
        quote_token: str,
        amount: Decimal,
        side: TradeType,
        slippage_percentage: Decimal,
    ):
        """
        Get a quote for swapping tokens in a pool.

        Args:
            pool: Dictionary containing pool configuration parameters
            base_token: Symbol of the base token
            quote_token: Symbol of the quote token
            amount: Amount to swap as Decimal
            side: Trade side (BUY or SELL)
            slippage_percentage: Maximum acceptable slippage as a Decimal

        Returns:
            Dictionary containing swap quote information
        """
        network = pool.get("network")
        connector = pool.get("connector")
        pool_address = pool.get("address")

        if not all([network, connector, pool_address]):
            return None

        try:
            # Use a cache key based on all parameters
            cache_key = f"quote_{network}_{connector}_{pool_address}_{base_token}_{quote_token}_{amount}_{side.name}_{slippage_percentage}"

            # Check if we have this quote cached
            if hasattr(self, "_quote_cache") and cache_key in self._quote_cache:
                cached_data = self._quote_cache[cache_key]
                # Only use cache if it's recent (last 10 seconds)
                if time.time() - cached_data["timestamp"] < 10:
                    return cached_data["data"]

            # Get fresh quote
            quote_result = await self._gateway_quote_swap(
                network,
                connector,
                base_token,
                quote_token,
                amount,
                side,
                slippage_percentage,
                pool_address,
            )

            # Cache the result
            if not hasattr(self, "_quote_cache"):
                self._quote_cache = {}

            self._quote_cache[cache_key] = {"data": quote_result, "timestamp": time.time()}

            return quote_result
        except Exception as e:
            self.logger().error(f"Error getting quote for swap: {str(e)}")
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
    ):
        """
        Execute a token swap in the specified pool.

        Args:
            pool: Dictionary containing pool configuration parameters
            wallet_address: Address of the wallet executing the swap
            base_token: Symbol of the base token
            quote_token: Symbol of the quote token
            amount: Amount to swap
            side: Trade side (BUY or SELL)
            slippage_percentage: Maximum acceptable slippage

        Returns:
            Dictionary containing swap execution result
        """
        network = pool.get("network")
        connector = pool.get("connector")
        pool_address = pool.get("address")

        if not all([network, connector, pool_address, wallet_address]):
            return None

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
        Wait for a transaction to be confirmed using the poll mechanism.

        Args:
            chain: Chain identifier
            network: Network identifier
            tx_hash: Transaction hash to poll
            max_timeout: Maximum time to wait for confirmation in seconds

        Returns:
            bool: True if transaction was confirmed, False otherwise
        """
        if max_timeout is None:
            max_timeout = self._maximum_transaction_confirmation_timeout

        start_time = time.time()
        polling_interval = self._transaction_polling_interval

        self.logger().info(f"Waiting for transaction {tx_hash} to be confirmed...")

        while time.time() - start_time < max_timeout:
            try:
                # Poll transaction status
                tx_status = await self._gateway_poll_transaction(chain, network, tx_hash)

                # Check if transaction is confirmed (successful)
                if tx_status and tx_status.get("txStatus") == 1:
                    self.logger().info(f"Transaction {tx_hash} confirmed successfully!")
                    return True

                # If transaction has failed
                if tx_status and tx_status.get("txStatus") == -1:
                    self.logger().error(f"Transaction {tx_hash} failed with status: {tx_status}")
                    return False

                # Wait before polling again
                await asyncio.sleep(polling_interval)

            except Exception as e:
                self.logger().error(f"Error while polling transaction {tx_hash}: {str(e)}")
                await asyncio.sleep(polling_interval)

        self.logger().warning(f"Transaction {tx_hash} confirmation timed out after {max_timeout} seconds")

        return False

    async def _calculate_price_difference_percentage(
        self, base_token: str, quote_token: str, pool_1: Dict[str, Any], pool_2: Dict[str, Any]
    ) -> Optional[Decimal]:
        """
        Calculate price difference between two pools for a token pair.

        Args:
            base_token: Base token symbol
            quote_token: Quote token symbol
            pool_1: First pool data
            pool_2: Second pool data

        Returns:
            Decimal: Price difference as percentage
        """
        # Try to get prices from cache first
        price_1 = self._get_cached_token_price_in_pool(base_token, quote_token, pool_1)
        price_2 = self._get_cached_token_price_in_pool(base_token, quote_token, pool_2)

        # If not in cache, fetch from API
        if price_1 is None:
            price_1 = await self._get_token_price_in_pool(base_token, quote_token, pool_1)

        if price_2 is None:
            price_2 = await self._get_token_price_in_pool(base_token, quote_token, pool_2)

        if price_1 is None:
            self.logger().warning(f"Failed to get price for {base_token}/{quote_token} in pool {pool_1['address']}")
            return None

        if price_2 is None:
            self.logger().warning(f"Failed to get price for {base_token}/{quote_token} in pool {pool_2['address']}")
            return None

        if price_1 == DECIMAL_ZERO:
            self.logger().warning(f"Price for {base_token}/{quote_token} in pool {pool_1['address']} is 0")
            return None

        if price_2 == DECIMAL_ZERO:
            self.logger().warning(f"Price for {base_token}/{quote_token} in pool {pool_2['address']} is 0")
            return None

        # Calculate price difference percentage
        price_difference = DECIMAL_ONE_HUNDRED * ((price_2 - price_1) / price_1)

        return price_difference

    async def _get_token_price_in_pool(
        self, base_token: str, quote_token: str, pool: Dict[str, Any]
    ) -> Optional[Decimal]:
        """
        Get the price of quote_token in terms of base_token in the given pool.

        Args:
            base_token: Base token symbol
            quote_token: Quote token symbol
            pool: Pool data

        Returns:
            Decimal: Token price
        """
        try:
            # First check if we have the price cached in the pool data
            cached_price = self._get_cached_token_price_in_pool(base_token, quote_token, pool)
            if cached_price is not None:
                return cached_price

            # If not cached, get quote for a small amount to determine price
            maximum_slippage_percentage = self._maximum_slippage_percentage

            # Use a small fixed amount for consistent price estimations
            quote = await self._get_quote_swap(
                pool, base_token, quote_token, DECIMAL_ONE, TradeType.SELL, maximum_slippage_percentage
            )

            if not quote or "estimatedAmountOut" not in quote:
                return None

            expected_out = Decimal(str(quote["estimatedAmountOut"]))

            # Cache the price in the pool data structure
            chain = pool.get("chain")
            network = pool.get("network")
            connector = pool.get("connector")
            pool_address = pool.get("address")

            if (
                chain
                and network
                and connector
                and pool_address
                and chain in database["connections"]
                and network in database["connections"][chain]
                and connector in database["connections"][chain][network]
                and "pools" in database["connections"][chain][network][connector]
                and pool_address in database["connections"][chain][network][connector]["pools"]
            ):
                pool_data = database["connections"][chain][network][connector]["pools"][pool_address]

                if "tokens" not in pool_data:
                    pool_data["tokens"] = {}

                if base_token not in pool_data["tokens"]:
                    pool_data["tokens"][base_token] = {}

                if "prices" not in pool_data["tokens"][base_token]:
                    pool_data["tokens"][base_token]["prices"] = {}

                pool_data["tokens"][base_token]["prices"][quote_token] = expected_out

            return expected_out
        except Exception as e:
            self.logger().error(f"Error getting token price in pool: {str(e)}")
            return None

    def format_status(self) -> str:
        """
        Format the strategy status for display.

        Returns:
            str: Formatted status string
        """
        if not self._gateway_is_ready:
            return "Gateway is not ready. Please check connection."

        # Get recent arbitrage opportunities
        opportunities = database.get("arbitrage_opportunities", [])
        recent_opportunities = [
            opportunity for opportunity in opportunities if time.time() - opportunity.get("timestamp", 0) < 3600
        ]  # Last hour

        # Get recent executions
        executions = database.get("execution_history", [])
        recent_executions = [
            execution for execution in executions if time.time() - execution.get("timestamp", 0) < 3600
        ]  # Last hour

        # Calculate profit statistics
        total_profit = DECIMAL_ZERO
        for ex in executions:
            profit = ex.get("profit")
            if profit is not None:
                total_profit += Decimal(str(profit))

        # Get cached token balances
        balances_summary = []
        tokens = self._configuration["tokens"]
        if tokens:
            for token in tokens:
                try:
                    balance = self._get_total_token_balance_from_cached_sources(token)
                except Exception as exception:
                    logger.ignore_exception(exception)
                    balance = DECIMAL_ZERO
                balances_summary.append(f"{token}: {balance:.4f}")

        # Format status message
        current_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        status = [
            "AMM Robust Position Manager Status:",
            f"Time: {current_time}",
            f"Gateway Status: {'Ready' if self._gateway_is_ready else 'Not Ready'}",
            f"Opportunities Found (last hour): {len(recent_opportunities)}",
            f"Trades Executed (last hour): {len(recent_executions)}",
            f"Total Profit: {total_profit:.4f}",
            "\nToken Balances:",
            *balances_summary,
        ]

        if recent_executions:
            status.append("\nRecent Trades:")
            for ex in reversed(recent_executions[-5:]):  # Show last 5 trades, most recent first
                token = ex.get("base_token", "")
                profit = ex.get("profit")
                profit_percentage = ex.get("profit_percentage")
                timestamp = ex.get("timestamp", 0)
                time_str = time.strftime("%H:%M:%S", time.localtime(timestamp))

                if profit is not None and profit_percentage is not None:
                    profit_decimal = Decimal(str(profit))
                    profit_percentage_decimal = Decimal(str(profit_percentage))
                    status.append(f"  {time_str} - {token}: {profit_decimal:.4f} ({profit_percentage_decimal:.2f}%)")

        return "\n".join(status)

    def _get_total_token_balance_from_cached_sources(self, token_symbol):
        """
        Get total token balance from all cached wallet data.

        Args:
            token_symbol: Token symbol

        Returns:
            Decimal: Total token balance
        """
        total = DECIMAL_ZERO

        # Check cache for all keys containing this token
        for key, data in self._balance_cache.items():
            if key.endswith(f"_{token_symbol}"):
                total += data["balance"]

        return total

    # noinspection PyMethodMayBeStatic
    def _generate_token_permutations(self, tokens: List[str]) -> List[str]:
        """
        Generate all possible permutations of token pairs for a given list of tokens.

        Args:
            tokens: List of token symbols

        Returns:
            List[str]: List of token pair strings (e.g., "TOKEN1/TOKEN2")
        """
        token_pairs = []

        for permutation in permutations(tokens, len(tokens)):
            # Join tokens with '/' to create the key
            token_key = "/".join(permutation)
            token_pairs.append(token_key)

        return token_pairs

    # noinspection PyMethodMayBeStatic
    def _find_pools_with_token_pair(self, base_token: str, quote_token: str) -> List[Dict[str, Any]]:
        """
        Find pools that contain both tokens.

        Args:
            base_token: Base token symbol
            quote_token: Quote token symbol

        Returns:
            List[Dict]: List of pool data dictionaries
        """
        matching_pools = []
        token_key = f"{base_token}/{quote_token}"

        # Get pool internal IDs from the map
        pool_internal_ids = database["maps"]["pools_by_tokens"].get(token_key, [])

        # Convert internal IDs to full pool information
        for pool_internal_id in pool_internal_ids:
            chain, network, connector, pool_address = pool_internal_id.split("/")
            if (
                chain in database["connections"]
                and network in database["connections"][chain]
                and connector in database["connections"][chain][network]
                and "pools" in database["connections"][chain][network][connector]
                and pool_address in database["connections"][chain][network][connector]["pools"]
            ):
                matching_pools.append(database["connections"][chain][network][connector]["pools"][pool_address])

        return matching_pools

    # noinspection PyMethodMayBeStatic
    def _get_wallet_addresses_for_pool(self, pool: Dict[str, Any]) -> Optional[List[str]]:
        """
        Get wallet addresses that can be used for a specific pool.

        Args:
            pool: Pool data dictionary

        Returns:
            Optional[List[str]]: List of wallet addresses or None
        """
        pool_internal_id = pool.get("internal_id")
        if not pool_internal_id:
            return None

        # Get wallet internal IDs from the map
        wallet_internal_ids = database["maps"]["wallets_by_pool"].get(pool_internal_id, [])
        if not wallet_internal_ids:
            return None

        # Convert internal IDs to wallet addresses
        wallet_addresses = []
        for wallet_internal_id in wallet_internal_ids:
            chain, network, connector, wallet_address = wallet_internal_id.split("/")
            if (
                chain in database["connections"]
                and network in database["connections"][chain]
                and connector in database["connections"][chain][network]
                and "wallets" in database["connections"][chain][network][connector]
                and wallet_address in database["connections"][chain][network][connector]["wallets"]
            ):
                wallet_addresses.append(wallet_address)

        return wallet_addresses if wallet_addresses else None

    async def _get_total_token_balance_from_all_wallets(self, token_symbol: str) -> Decimal:
        """
        Get token balance across all wallets.

        Args:
            token_symbol: Token symbol

        Returns:
            Decimal: Total token balance
        """
        # First check if we have recent cached balances
        total_from_cache = self._get_total_token_balance_from_cached_sources(token_symbol)
        if total_from_cache > DECIMAL_ZERO:
            return total_from_cache

        # If not in cache, fetch from gateway
        total_balance = DECIMAL_ZERO

        for chain_name, chain in self._configuration["connections"].items():
            for network_name, network in chain.items():
                for connector_name, connector in network.items():
                    for wallet_address in connector["wallets"]:
                        balances = await self._gateway_get_balances(
                            chain_name, network_name, wallet_address, [token_symbol]
                        )

                        if balances and "balances" in balances:
                            balance = balances["balances"].get(token_symbol)
                            if balance is not None:
                                wallet_balance = Decimal(str(balance))
                                total_balance += wallet_balance

                                # Update cache
                                cache_key = f"{chain_name}_{network_name}_{wallet_address}_{token_symbol}"
                                self._balance_cache[cache_key] = {"balance": wallet_balance, "timestamp": time.time()}

        return total_balance

    async def _check_gateway_status(self):
        """
        Check if Gateway server is online and verify wallet connections.
        This method sets _gateway_is_ready flag based on connectivity.
        """
        # Skip if gateway is already verified as ready
        if self._gateway_is_ready:
            return

        self.logger().info("Checking Gateway server status...")
        try:
            if await self._gateway_ping_gateway():
                self._gateway_is_ready = True
                self.logger().info("Gateway server is online!")
                await self._verify_wallet_connections()
            else:
                self._set_gateway_as_not_ready(
                    "Gateway server is offline! Make sure Gateway is running before using this strategy."
                )
        except Exception as exception:
            self._set_gateway_as_not_ready(f"Error connecting to Gateway server: {str(exception)}")

    def _set_gateway_as_not_ready(self, error_message: str):
        """
        Set gateway as not ready with appropriate error message.

        Args:
            error_message: Error message to log
        """
        self._gateway_is_ready = False
        self.logger().error(error_message)

    async def _verify_wallet_connections(self):
        """
        Verify wallet connections for all configured pools.
        This method validates that wallets exist and are properly connected.
        """
        if not self._all_gateway_connections or len(self._all_gateway_connections) == 0:
            self.logger().error("No wallet connections found. Please connect a wallet using 'gateway connect'.")
            return

        # Check connections for each chain/network/connector in configuration
        for chain, chain_data in self._configuration["connections"].items():
            for network, network_data in chain_data.items():
                for connector, connector_data in network_data.items():
                    # Find matching gateway connection
                    gateway_connection = next(
                        (
                            conn
                            for conn in self._all_gateway_connections
                            if conn.get("chain") == chain
                            and conn.get("network") == network
                            and conn.get("connector") == connector
                        ),
                        None,
                    )

                    if not gateway_connection:
                        self.logger().error(
                            f"No gateway connection found for {chain}/{connector}/{network}. "
                            f"Please connect using 'gateway connect'."
                        )
                    else:
                        self.logger().info(
                            f"Found wallet connection for {chain}/{connector}/{network}: "
                            f"{gateway_connection.get('wallet_address')}"
                        )

    async def _link_wallets_and_pools(self, chain, network, connector):
        """
        Create mappings between wallets and pools.

        Args:
            chain: Chain identifier
            network: Network identifier
            connector: Connector identifier
        """
        # Get the wallets and pools for this connector
        wallets = self._configuration["connections"][chain][network][connector].get("wallets", [])
        pools = self._configuration["connections"][chain][network][connector].get("pools", [])

        # No pools to process
        if not pools:
            return

        for wallet_address in wallets:
            # Create the wallet's internal_id
            wallet_internal_id = f"{chain}/{network}/{connector}/{wallet_address}"

            # Ensure the wallet has a pools structure
            if wallet_address not in database["connections"][chain][network][connector]["wallets"]:
                continue

            if "pools" not in database["connections"][chain][network][connector]["wallets"][wallet_address]:
                database["connections"][chain][network][connector]["wallets"][wallet_address]["pools"] = {}

            # For each pool, link it to this wallet
            for pool_address in pools:
                # Create the pool's internal_id
                pool_internal_id = f"{chain}/{network}/{connector}/{pool_address}"

                # Add this pool to the wallet's pools list if it's not already there
                if (
                    pool_address
                    not in database["connections"][chain][network][connector]["wallets"][wallet_address]["pools"]
                ):
                    # Initialize the pool entry in the wallet's structure
                    database["connections"][chain][network][connector]["wallets"][wallet_address]["pools"][
                        pool_address
                    ] = {"shares": DECIMAL_ZERO, "tokens": {}, "impermanent_loss": DECIMAL_ZERO}

                    # Get the pool's tokens if available
                    if (
                        pool_address in database["connections"][chain][network][connector]["pools"]
                        and "tokens_list" in database["connections"][chain][network][connector]["pools"][pool_address]
                    ):
                        pool_tokens = database["connections"][chain][network][connector]["pools"][pool_address][
                            "tokens_list"
                        ]

                        # Initialize token balances to zero
                        for token_symbol in pool_tokens:
                            database["connections"][chain][network][connector]["wallets"][wallet_address]["pools"][
                                pool_address
                            ]["tokens"][token_symbol] = DECIMAL_ZERO

                    # Update liquidity token information in wallet tokens structure
                    for token_symbol in self._configuration["tokens"]:
                        # Ensure token structure exists
                        if (
                            token_symbol
                            not in database["connections"][chain][network][connector]["wallets"][wallet_address][
                                "tokens"
                            ]
                        ):
                            continue

                        if (
                            "balances"
                            not in database["connections"][chain][network][connector]["wallets"][wallet_address][
                                "tokens"
                            ][token_symbol]
                        ):
                            continue

                        # Initialize or update the liquidity pools mapping for this token
                        token_balances = database["connections"][chain][network][connector]["wallets"][wallet_address][
                            "tokens"
                        ][token_symbol]["balances"]
                        if "locked" not in token_balances:
                            token_balances["locked"] = {
                                "total": DECIMAL_ZERO,
                                "liquidity": {"total": DECIMAL_ZERO, "pools": {}},
                            }
                        elif "liquidity" not in token_balances["locked"]:
                            token_balances["locked"]["liquidity"] = {"total": DECIMAL_ZERO, "pools": {}}
                        elif "pools" not in token_balances["locked"]["liquidity"]:
                            token_balances["locked"]["liquidity"]["pools"] = {}

                        # Add pool to token's liquidity pools with zero balance
                        token_balances["locked"]["liquidity"]["pools"][pool_address] = DECIMAL_ZERO

                # Add to the pools_by_wallet map
                if wallet_internal_id not in database["maps"]["pools_by_wallet"]:
                    database["maps"]["pools_by_wallet"][wallet_internal_id] = []

                if pool_internal_id not in database["maps"]["pools_by_wallet"][wallet_internal_id]:
                    database["maps"]["pools_by_wallet"][wallet_internal_id].append(pool_internal_id)

                # Add to the wallets_by_pool map
                if pool_internal_id not in database["maps"]["wallets_by_pool"]:
                    database["maps"]["wallets_by_pool"][pool_internal_id] = []

                if wallet_internal_id not in database["maps"]["wallets_by_pool"][pool_internal_id]:
                    database["maps"]["wallets_by_pool"][pool_internal_id].append(wallet_internal_id)

    # Gateway API wrapper methods with retry and timeout
    @run_with_retry_and_timeout(
        retries=GATEWAY_REQUEST_RETRIES, delay=GATEWAY_REQUEST_DELAY, timeout=GATEWAY_REQUEST_TIMEOUT
    )
    async def _gateway_ping_gateway(self):
        """
        Ping the gateway server to check if it's online.

        Returns:
            bool: True if gateway is online, False otherwise
        """
        return await self._gateway_http_client.ping_gateway()

    @run_with_retry_and_timeout(
        retries=GATEWAY_REQUEST_RETRIES, delay=GATEWAY_REQUEST_DELAY, timeout=GATEWAY_REQUEST_TIMEOUT
    )
    async def _gateway_get_pools(self, connector: str, network: str):
        """
        Fetch all available pools for a connector.

        Args:
            connector: Connector identifier
            network: Network identifier

        Returns:
            Dict: Pool information
        """
        return await self._gateway_http_client.amm_list_pools(connector, network)

    @run_with_retry_and_timeout(
        retries=GATEWAY_REQUEST_RETRIES, delay=GATEWAY_REQUEST_DELAY, timeout=GATEWAY_REQUEST_TIMEOUT
    )
    async def _gateway_get_pool_info(self, connector: str, network: str, pool_address: str):
        """
        Get detailed information about a liquidity pool.

        Args:
            connector: Connector identifier
            network: Network identifier
            pool_address: Pool address

        Returns:
            Dict: Pool information
        """
        return await self._gateway_http_client.amm_pool_info(connector, network, pool_address)

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
        """
        Get a quote for swapping tokens in a pool.

        Args:
            network: Network identifier
            connector: Connector identifier
            base_asset: Base token symbol
            quote_asset: Quote token symbol
            amount: Amount to swap
            side: Trade side (BUY or SELL)
            slippage_percentage: Maximum acceptable slippage
            pool_address: Pool address

        Returns:
            Dict: Swap quote information
        """
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
        """
        Execute a token swap in the specified pool.

        Args:
            network: Network identifier
            connector: Connector identifier
            wallet_address: Wallet address
            base_asset: Base token symbol
            quote_asset: Quote token symbol
            side: Trade side (BUY or SELL)
            amount: Amount to swap
            slippage_percentage: Maximum acceptable slippage
            pool_address: Pool address

        Returns:
            Dict: Swap execution result
        """
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
        """
        Poll for transaction status.

        Args:
            chain: Chain identifier
            network: Network identifier
            tx_hash: Transaction hash

        Returns:
            Dict: Transaction status information
        """
        return await self._gateway_http_client.get_transaction_status(chain, network, tx_hash)

    @run_with_retry_and_timeout(
        retries=GATEWAY_REQUEST_RETRIES, delay=GATEWAY_REQUEST_DELAY, timeout=GATEWAY_REQUEST_TIMEOUT
    )
    async def _gateway_get_tokens(
        self, chain: str, network: str, token_symbols: Optional[Union[str, List[str]]] = None
    ):
        """
        Get token information.

        Args:
            chain: Chain identifier
            network: Network identifier
            token_symbols: Optional token symbols to filter

        Returns:
            Dict: Token information
        """
        return await self._gateway_http_client.get_tokens(chain, network, token_symbols)

    @run_with_retry_and_timeout(
        retries=GATEWAY_REQUEST_RETRIES, delay=GATEWAY_REQUEST_DELAY, timeout=GATEWAY_REQUEST_TIMEOUT
    )
    async def _gateway_get_balances(
        self, chain: str, network: str, address: str, token_symbols: Optional[Union[str, List[str]]] = None
    ):
        """
        Get token balances for a wallet address.

        Args:
            chain: Chain identifier
            network: Network identifier
            address: Wallet address
            token_symbols: Optional token symbols to filter

        Returns:
            Dict: Balance information
        """
        return await self._gateway_http_client.get_balances(chain, network, address, token_symbols)

    @run_with_retry_and_timeout(
        retries=GATEWAY_REQUEST_RETRIES, delay=GATEWAY_REQUEST_DELAY, timeout=GATEWAY_REQUEST_TIMEOUT
    )
    async def _gateway_get_status(self):
        """
        Get the status of the Gateway server.

        Returns:
            Dict: Server status information
        """
        return await self._gateway_http_client.get_gateway_status()

    @run_with_retry_and_timeout(
        retries=GATEWAY_REQUEST_RETRIES, delay=GATEWAY_REQUEST_DELAY, timeout=GATEWAY_REQUEST_TIMEOUT
    )
    async def _gateway_get_config(self, chain_or_connector: Optional[str] = None):
        """
        Get Gateway configuration settings.

        Args:
            chain_or_connector: Optional chain or connector to filter

        Returns:
            Dict: Gateway configuration
        """
        return await self._gateway_http_client.get_configuration(chain_or_connector)

    @run_with_retry_and_timeout(
        retries=GATEWAY_REQUEST_RETRIES, delay=GATEWAY_REQUEST_DELAY, timeout=GATEWAY_REQUEST_TIMEOUT
    )
    async def _gateway_get_network_status(self, chain: str, network: str):
        """
        Get chain status.

        Args:
            chain: Chain identifier
            network: Network identifier

        Returns:
            Dict: Chain status information
        """
        return await self._gateway_http_client.get_network_status(chain, network)

    async def _get_chain_tokens(self, chain: str, network: str, token_symbols: Optional[Union[str, List[str]]] = None):
        """
        Get token information for a specific chain and network.

        Args:
            chain: Chain identifier
            network: Network identifier
            token_symbols: Optional token symbols to filter

        Returns:
            Dict: Token information
        """
        return await self._gateway_get_tokens(chain, network, token_symbols)

    async def _get_token_price(self, token_symbol: str, chain: str, network: str) -> Optional[Decimal]:
        """
        Get the price of a token from a price feed or API.

        Args:
            token_symbol: Token symbol
            chain: Chain identifier
            network: Network identifier

        Returns:
            Optional[Decimal]: Token price or None if unavailable
        """
        try:
            # This is a placeholder implementation
            # In a real implementation, you would fetch the price from a price feed or API
            # For simplicity, we'll return None to indicate price is not available
            return None
        except Exception as e:
            self.logger().error(f"Error fetching price for {token_symbol}: {str(e)}")
            return None
