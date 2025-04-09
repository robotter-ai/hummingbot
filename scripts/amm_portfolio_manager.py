import asyncio
import os
import time
from decimal import Decimal
from enum import Enum
from itertools import permutations
from typing import Any, Dict, List, Optional, Union

# noinspection PyUnresolvedReferences
from pydantic.v1 import Field

from hummingbot.client.config.config_data_types import BaseClientModel
from hummingbot.client.settings import GatewayConnectionSetting
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.event.events import TradeType
from hummingbot.core.gateway.gateway_http_client import GatewayHttpClient
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase
from scripts.utility.decorators import logged_class

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
DECIMAL_INFINITY = Decimal("Infinity")


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
        "minimum_profitability_percentage": "0.01",  # 1 means 1%, or 0.01, in the code
        "arbitrage_check_interval_seconds": "60",  # Time between arbitrage checks
        "minimum_trade_amount": "1",  # Minimum amount to consider for a trade
        "time_delay_between_arbitrages": "1",  # Time delay between arbitrage trades
        "transaction_confirmation_delay": "2",  # Time delay between transaction confirmation
        "transaction_polling_interval": "2",  # Time delay between transaction polling
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

# Example structure)
# database: Dict[str, Any] = {
#     "connections": {
#         "polkadot": {
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
#                                     "tokens": {
#                                         "<token_symbol>": "<pool_token_balance>",
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


class AMMRobustPositionManagerConfiguration(BaseClientModel):
    script_file_name: str = Field(default_factory=lambda: os.path.basename(__file__))


# noinspection PyShadowingNames
@logged_class
class AMMRobustPositionManager(ScriptStrategyBase):
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

    def __init__(self, connectors: Dict[str, ConnectorBase]):
        super().__init__(connectors)

        self._initialize()

    def _initialize(self, _strategy_configuration: AMMRobustPositionManagerConfiguration = None):
        """Initialize the strategy with configuration parameters."""
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

        # Set transaction confirmation timeout to 5x the configured delay as a safety measure
        # If the configured delay is 0, use the default timeout (60 seconds)
        if self._transaction_confirmation_delay > 0:
            self._maximum_transaction_confirmation_timeout = int(self._transaction_confirmation_delay) * 5

        self._all_gateway_connections = GatewayConnectionSetting.load()

        self._log_initialization()

    def _log_initialization(self):
        self.logger().info(f"Starting {self.__class__.__name__} strategy")

    def on_tick(self):
        safe_ensure_future(self._async_on_tick())

    async def _async_on_tick(self):
        """Main strategy execution logic that runs on each tick"""
        # First check gateway status
        if not self._gateway_is_ready:
            await self._check_gateway_status()
            if not self._gateway_is_ready:
                return

        # Update database with latest information
        await self._update_database()

        # Check if it's time to run arbitrage check
        current_time = Decimal(time.time())
        arbitrage_check_interval = self._arbitrage_check_interval_seconds

        if current_time - self._last_arbitrage_check_time >= arbitrage_check_interval:
            self._last_arbitrage_check_time = current_time
            await self._run_arbitrage_strategy()

    async def _run_arbitrage_strategy(self):
        """Execute the arbitrage strategy"""
        self.logger().info("Checking for arbitrage opportunities...")

        opportunities = await self._find_arbitrage_opportunities()

        for opportunity in opportunities:
            if await self._validate_opportunity(opportunity):
                await self._execute_arbitrage(opportunity)

    async def _find_arbitrage_opportunities(self) -> List[Dict[str, Any]]:
        """Find arbitrage opportunities across pools and tokens"""
        opportunities = []

        # Get all available tokens and pools from the database
        tokens = self._configuration["tokens"]

        # Minimum profit percentage required for arbitrage
        minimum_profitability_percentage = self._minimum_profitability_percentage

        # Iterate through all token pairs and pool combinations
        for token_idx in range(len(tokens)):
            base_token = tokens[token_idx]
            for quote_idx in range(token_idx + 1, len(tokens)):
                quote_token = tokens[quote_idx]
                # Find pools that contain both tokens
                relevant_pools = self._find_pools_with_token_pair(base_token, quote_token)

                # Check for arbitrage opportunities between different pools
                for pool_idx in range(len(relevant_pools)):
                    pool_1 = relevant_pools[pool_idx]
                    for pool_2_idx in range(pool_idx + 1, len(relevant_pools)):
                        pool_2 = relevant_pools[pool_2_idx]
                        # Calculate price difference between the two pools
                        price_difference_percentage = await self._calculate_price_difference_percentage(
                            base_token, quote_token, pool_1, pool_2
                        )

                        if price_difference_percentage is None:
                            continue

                        # If price difference exceeds minimum profit threshold
                        if abs(price_difference_percentage) > minimum_profitability_percentage:
                            # Determine which pool to buy from and which to sell to
                            buy_pool, sell_pool = (
                                (pool_1, pool_2) if price_difference_percentage > 0 else (pool_2, pool_1)
                            )

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
                                f"Found arbitrage opportunity: {base_token}/{quote_token} with {price_difference_percentage:.2f}% difference "
                                f"between {buy_pool['address']} and {sell_pool['address']}"
                            )

        # Update the database with found opportunities
        database["arbitrage_opportunities"] = opportunities

        return opportunities

    async def _validate_opportunity(self, opportunity: Dict[str, Any]) -> bool:
        """Validate an arbitrage opportunity by checking slippage and available balances"""
        base_token = opportunity["base_token"]
        quote_token = opportunity["quote_token"]
        buy_pool = opportunity["buy_pool"]
        sell_pool = opportunity["sell_pool"]

        # Get token balances
        base_token_balance = await self._get_total_token_balance_from_all_wallets(base_token)

        # Check if we have enough balance for the trade
        minimum_trade_amount = self._minimum_trade_amount
        if base_token_balance < minimum_trade_amount:
            self.logger().info(f"Insufficient balance of {base_token} for arbitrage: {base_token_balance}")
            return False

        # Calculate optimal trade amount (considering slippage)
        trade_amount = await self._calculate_optimal_trade_amount(opportunity, base_token_balance)

        if not trade_amount or trade_amount <= DECIMAL_ZERO:
            self.logger().info("Optimal trade amount calculation resulted in an invalid or non-positive amount")
            return False

        # Check slippage for both trades
        maximum_slippage_percentage = self._maximum_slippage_percentage

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
            self.logger().info(f"Failed to get buy quote for {base_token}/{quote_token} in pool {buy_pool['address']}")
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

        # Validate that the opportunity is still profitable after slippage
        minimum_profitability_percentage = self._minimum_profitability_percentage
        if expected_profit_percentage < minimum_profitability_percentage:
            self.logger().info(
                f"Arbitrage opportunity no longer profitable after slippage: {expected_profit_percentage:.2f}% < {minimum_profitability_percentage}%"
            )
            return False

        self.logger().info(
            f"Validated arbitrage opportunity: {base_token}/{quote_token} with expected profit of {expected_profit_percentage:.2f}%"
        )
        return True

    async def _execute_arbitrage(self, opportunity: Dict[str, Any]) -> bool:
        """Execute an arbitrage trade for each available wallet"""
        base_token = opportunity["base_token"]
        quote_token = opportunity["quote_token"]
        buy_pool = opportunity["buy_pool"]
        sell_pool = opportunity["sell_pool"]
        trade_amount = opportunity["trade_amount"]

        self.logger().info(f"Executing arbitrage trade: {base_token}/{quote_token}")

        # Get wallet addresses for buy pool
        wallet_addresses = self._get_wallet_addresses_for_pool(buy_pool)
        if not wallet_addresses:
            self.logger().error(f"No wallet address found for pool {buy_pool['address']}")
            return False

        # Convert to list if it's a single address
        if not isinstance(wallet_addresses, list):
            wallet_addresses = [wallet_addresses]

        if len(wallet_addresses) == 0:
            self.logger().error(f"Empty wallet list for pool {buy_pool['address']}")
            return False

        # Track overall success
        overall_success = False
        maximum_slippage_percentage = self._maximum_slippage_percentage

        # Execute trades for each wallet
        for wallet_address in wallet_addresses:
            self.logger().info(f"Attempting arbitrage with wallet: {wallet_address}")

            try:
                # Record initial balance for this wallet
                initial_wallet_balances = await self._post_chain_balances(
                    buy_pool.get("chain"), buy_pool.get("network"), wallet_address, [base_token]
                )

                if not initial_wallet_balances or "balances" not in initial_wallet_balances:
                    self.logger().error(f"Failed to get initial balance for wallet {wallet_address}")
                    continue

                initial_base_token_balance = Decimal(str(initial_wallet_balances["balances"].get(base_token, 0)))

                # Check if wallet has sufficient balance
                if initial_base_token_balance < trade_amount:
                    self.logger().info(
                        f"Wallet {wallet_address} has insufficient balance: {initial_base_token_balance} {base_token}"
                    )
                    continue

                self.logger().info(
                    f"Step 1: Swap {trade_amount} {base_token} for {quote_token} in pool {buy_pool['address']}"
                )

                # Execute first swap: base_token -> quote_token in buy_pool
                first_swap_result = await self._post_execute_swap(
                    pool=buy_pool,
                    wallet_address=wallet_address,
                    base_token=base_token,
                    quote_token=quote_token,
                    amount=trade_amount,
                    side=TradeType.SELL,  # Selling base_token to buy quote_token
                    slippage_percentage=maximum_slippage_percentage,
                )

                if not first_swap_result or "signature" not in first_swap_result:
                    self.logger().error(f"First swap failed for wallet {wallet_address}: {base_token} -> {quote_token}")
                    continue

                self.logger().info(f"First swap completed with transaction signature: {first_swap_result['signature']}")

                # Wait for first transaction to be confirmed using polling
                first_transaction_confirmation = await self._wait_for_transaction_confirmation(
                    buy_pool.get("chain"), buy_pool.get("network"), first_swap_result["signature"]
                )

                if not first_transaction_confirmation:
                    self.logger().error(
                        f"First swap transaction confirmation failed for {first_swap_result['signature']}"
                    )
                    continue

                # Get quote_token balance after first swap
                updated_wallet_balances = await self._post_chain_balances(
                    buy_pool.get("chain"), buy_pool.get("network"), wallet_address, [quote_token]
                )

                if not updated_wallet_balances or "balances" not in updated_wallet_balances:
                    self.logger().error(f"Failed to get updated balance for wallet {wallet_address}")
                    continue

                quote_token_balance = Decimal(str(updated_wallet_balances["balances"].get(quote_token, 0)))

                # Execute second swap: quote_token -> base_token in sell_pool
                self.logger().info(
                    f"Step 2: Swap {quote_token_balance} {quote_token} back to {base_token} in pool {sell_pool['address']}"
                )

                second_swap_result = await self._post_execute_swap(
                    pool=sell_pool,
                    wallet_address=wallet_address,
                    base_token=quote_token,
                    quote_token=base_token,
                    amount=quote_token_balance,
                    side=TradeType.SELL,  # Selling quote_token to get back base_token
                    slippage_percentage=maximum_slippage_percentage,
                )

                if not second_swap_result or "signature" not in second_swap_result:
                    self.logger().error(
                        f"Second swap failed for wallet {wallet_address}: {quote_token} -> {base_token}"
                    )
                    continue

                self.logger().info(
                    f"Second swap completed with transaction signature: {second_swap_result['signature']}"
                )

                # Wait for second transaction to be confirmed using polling
                second_transaction_confirmation = await self._wait_for_transaction_confirmation(
                    sell_pool.get("chain"), sell_pool.get("network"), second_swap_result["signature"]
                )

                if not second_transaction_confirmation:
                    self.logger().error(
                        f"Second swap transaction confirmation failed for {second_swap_result['signature']}"
                    )
                    continue

                # Calculate actual profit
                final_wallet_balances = await self._post_chain_balances(
                    buy_pool.get("chain"), buy_pool.get("network"), wallet_address, [base_token]
                )

                if not final_wallet_balances or "balances" not in final_wallet_balances:
                    self.logger().error(f"Failed to get final balance for wallet {wallet_address}")
                    continue

                final_base_token_balance = Decimal(str(final_wallet_balances["balances"].get(base_token, 0)))
                actual_profit = final_base_token_balance - initial_base_token_balance
                actual_profit_percentage = (
                    (actual_profit / initial_base_token_balance) * 100 if initial_base_token_balance > 0 else 0
                )

                # Record trade result in execution history
                trade_result = {
                    "timestamp": time.time(),
                    "wallet_address": wallet_address,
                    "base_token": base_token,
                    "quote_token": quote_token,
                    "buy_pool": buy_pool["address"],
                    "sell_pool": sell_pool["address"],
                    "initial_amount": initial_base_token_balance,
                    "final_amount": final_base_token_balance,
                    "profit": actual_profit,
                    "profit_percentage": actual_profit_percentage,
                    "first_swap_tx": first_swap_result["signature"],
                    "second_swap_tx": second_swap_result["signature"],
                }

                database["execution_history"].append(trade_result)

                if actual_profit > 0:
                    self.logger().info(
                        f"Arbitrage trade successful for wallet {wallet_address}! Profit: {actual_profit} {base_token} ({actual_profit_percentage:.2f}%)"
                    )
                    overall_success = True
                else:
                    self.logger().warning(
                        f"Arbitrage trade completed with loss for wallet {wallet_address}: {actual_profit} {base_token} ({actual_profit_percentage:.2f}%)"
                    )

            except Exception as e:
                self.logger().error(f"Error executing arbitrage for wallet {wallet_address}: {str(e)}")
                continue

        return overall_success

    async def _wait_for_transaction_confirmation(
        self, chain: str, network: str, tx_hash: str, max_timeout: int = None
    ) -> bool:
        """
        Wait for a transaction to be confirmed using the poll mechanism.

        Args:
            chain: Chain identifier
            network: Network identifier
            tx_hash: Transaction hash to poll
            max_timeout: Maximum time to wait for confirmation in seconds (default: use class default)

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
                tx_status = await self._post_chain_poll(chain, network, tx_hash)

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

    async def _update_database(self):
        """Atualiza o database com as informações mais recentes de forma eficiente"""
        current_time = time.time()

        # Definir intervalos para cada tipo de atualização
        wallet_update_interval = 60  # 1 minuto
        token_update_interval = 300  # 5 minutos
        pool_update_interval = 120  # 2 minutos

        # Inicializar a estrutura principal se ainda não existir
        if "connections" not in database:
            database["connections"] = {}

        # Atualizar carteiras (menos frequente)
        if (
            not hasattr(self, "_last_wallet_update_time")
            or current_time - self._last_wallet_update_time >= wallet_update_interval
        ):
            self._last_wallet_update_time = current_time
            await self._update_wallet_structure()

        # Atualizar tokens (menos frequente)
        if (
            not hasattr(self, "_last_token_update_time")
            or current_time - self._last_token_update_time >= token_update_interval
        ):
            self._last_token_update_time = current_time
            await self._update_token_information()

        # Atualizar pools (mais frequente para preços)
        if (
            not hasattr(self, "_last_pool_update_time")
            or current_time - self._last_pool_update_time >= pool_update_interval
        ):
            self._last_pool_update_time = current_time
            await self._update_pool_information()

    async def _update_wallet_structure(self):
        """Garante que a estrutura básica do database esteja inicializada corretamente"""
        for chain in self._configuration["connections"].keys():
            # Garantir que a cadeia existe no database
            if chain not in database["connections"]:
                database["connections"][chain] = {}

            for network in self._configuration["connections"][chain].keys():
                # Garantir que a rede existe no database
                if network not in database["connections"][chain]:
                    database["connections"][chain][network] = {}

                for connector in self._configuration["connections"][chain][network].keys():
                    # Garantir que o connector existe no database
                    if connector not in database["connections"][chain][network]:
                        database["connections"][chain][network][connector] = {}

                    # Garantir que as estruturas de wallets, tokens e pools existam
                    for structure in ["wallets", "tokens", "pools"]:
                        if structure not in database["connections"][chain][network][connector]:
                            database["connections"][chain][network][connector][structure] = {}

                    # Agora podemos fazer a atualização usando o cache
                    await self._update_wallet_balances(chain, network, connector)

    async def _update_wallet_balances(self, chain, network, connector):
        """Atualiza os balances das carteiras usando o método de cache"""
        tokens = self._configuration["tokens"]
        wallets = self._configuration["connections"][chain][network][connector]["wallets"]

        for wallet_address in wallets:
            # Criar o internal_id da carteira
            wallet_internal_id = f"{chain}/{network}/{connector}/{wallet_address}"

            # Garantir que a carteira existe na estrutura
            if wallet_address not in database["connections"][chain][network][connector]["wallets"]:
                database["connections"][chain][network][connector]["wallets"][wallet_address] = {
                    "internal_id": wallet_internal_id,
                    "chain": chain,
                    "network": network,
                    "connector": connector,
                    "tokens": {},
                    "pools": {},
                }

            # Atualizar balances da carteira
            for token_symbol in tokens:
                balance = await self._get_token_balance_cached(chain, network, wallet_address, token_symbol)

                # Atualizar na estrutura do database
                if (
                    token_symbol
                    not in database["connections"][chain][network][connector]["wallets"][wallet_address]["tokens"]
                ):
                    database["connections"][chain][network][connector]["wallets"][wallet_address]["tokens"][
                        token_symbol
                    ] = {
                        "balances": {
                            "free": balance,
                            "locked": {"total": 0, "liquidity": {"total": 0, "pools": {}}},
                            "total": balance,
                        }
                    }
                else:
                    token_data = database["connections"][chain][network][connector]["wallets"][wallet_address][
                        "tokens"
                    ][token_symbol]
                    if "balances" not in token_data:
                        token_data["balances"] = {
                            "free": balance,
                            "locked": {"total": 0, "liquidity": {"total": 0, "pools": {}}},
                            "total": balance,
                        }
                    else:
                        token_data["balances"]["free"] = balance
                        token_data["balances"]["total"] = balance

    async def _update_token_information(self):
        """Update token information in the database"""

        for chain in self._configuration["connections"].keys():
            for network in self._configuration["connections"][chain].keys():
                for connector in self._configuration["connections"][chain][network].keys():
                    tokens = self._configuration["tokens"]

                    # Initialize connector tokens if not exists
                    if not database["connections"][chain][network][connector].get("tokens"):
                        database["connections"][chain][network][connector]["tokens"] = {}

                    # Get token information
                    token_info = await self._get_chain_tokens(chain, network, tokens)

                    if token_info and "tokens" in token_info:
                        for token in token_info["tokens"]:
                            symbol = token.get("symbol")
                            address = token.get("address")

                            if not symbol or not address:
                                continue

                            # Create internal_id for token
                            internal_id = f"{chain}/{network}/{connector}/{address}"

                            # Get token price if available
                            token_price = await self._get_token_price(symbol, chain, network)

                            database["connections"][chain][network][connector]["tokens"][symbol] = {
                                "internal_id": internal_id,
                                "address": address,
                                "chain": chain,
                                "network": network,
                                "connector": connector,
                                "symbol": symbol,
                                "name": token.get("name", symbol),
                                "decimals": token.get("decimals", 18),
                                "price": token_price,
                            }

        return database

    async def _update_pool_information(self):
        """Update pool information in the database"""

        for chain in self._configuration["connections"].keys():
            for network in self._configuration["connections"][chain].keys():
                for connector in self._configuration["connections"][chain][network].keys():
                    if "pools" not in database["connections"][chain][network][connector]:
                        database["connections"][chain][network][connector]["pools"] = {}

                    pools_info = await self._get_pools(chain, connector, network)

                    if not pools_info:
                        continue

                    _pools = pools_info.get("pools", [])

                    # Check if pools_info is a list
                    if not isinstance(_pools, list):
                        continue

                    # Update pool information in database for each pool
                    for pool in _pools:
                        if not isinstance(pool, dict):
                            continue

                        pool_address = pool.get("address")

                        # TODO remove!!!
                        if connector == "raydium" or connector == "raydium_amm":
                            pool_address = "7TbGqz32RsuwXbXY7EyBCiAnMbJq1gm1wKmfjQjuwoyF"
                            pool["address"] = pool_address

                        if not pool_address:
                            raise ValueError(f"Pool {pool} doesn't have an address")

                        should_ignore = False
                        for token in pool.get("tokens", []):
                            if token not in self._configuration["tokens"]:
                                if (
                                    pool.get("address")
                                    not in self._configuration["connections"][chain][network][connector]["pools"]
                                ):
                                    should_ignore = True
                                    break
                        if should_ignore:
                            continue

                        # Get detailed pool information
                        detailed_pool_info = await self._get_pool_information(
                            {"network": network, "connector": connector, "pool_address": pool_address}
                        )

                        if not detailed_pool_info:
                            continue

                        # Get pool tokens
                        pool_tokens = pool.get("tokens", [])
                        if not pool_tokens:
                            continue

                        # Create internal_id for pool
                        pool_internal_id = f"{chain}/{network}/{connector}/{pool_address}"

                        # Initialize the tokens structure in the database
                        database["connections"][chain][network][connector]["pools"][pool_address] = {
                            "internal_id": pool_internal_id,
                            "chain": chain,
                            "network": network,
                            "connector": connector,
                            "address": pool_address,
                            "type": pool.get("type", PoolType.UNKNOWN.value),
                            "tokens": {},
                            "tokens_list": pool_tokens,
                            "annual_percentage_rate": detailed_pool_info.get("apr"),
                            "total_value_locked": detailed_pool_info.get("tvl"),
                            "impermanent_loss": 0,
                            "volume": {"24h": detailed_pool_info.get("volume24h")},
                        }

                        # Generate all possible token permutations for this pool
                        token_permutations = self._generate_token_permutations(pool_tokens)

                        # Add pool to all token permutations in the map
                        for token_key in token_permutations:
                            if token_key not in database["maps"]["pools_by_tokens"]:
                                database["maps"]["pools_by_tokens"][token_key] = []
                            if pool_internal_id not in database["maps"]["pools_by_tokens"][token_key]:
                                database["maps"]["pools_by_tokens"][token_key].append(pool_internal_id)

                        # Handle specific pool types
                        pool_type = pool.get("type", PoolType.UNKNOWN.value)
                        if (
                            pool_type == PoolType.XYK.value
                            or pool_type == PoolType.STABLE.value
                            or pool_type == PoolType.AMM.value
                        ):
                            if len(pool_tokens) == 2:
                                base_token = pool_tokens[0]
                                quote_token = pool_tokens[1]
                                pool_price = detailed_pool_info.get("price", None)

                                if pool_price is None:
                                    self.logger().warning(f"Could not get price for pool {pool_address}")
                                    continue

                                # Update pool tokens information
                                for token_symbol in pool_tokens:
                                    database["connections"][chain][network][connector]["pools"][pool_address]["tokens"][
                                        token_symbol
                                    ] = {
                                        "price": {
                                            f"{pool_address}": detailed_pool_info.get(
                                                "price"
                                            ),  # TODO fix this on the gateway!!!
                                        },
                                    }

                                # Initialize base token
                                database["connections"][chain][network][connector]["pools"][pool_address]["tokens"][
                                    base_token
                                ] = {
                                    "balance": Decimal(str(detailed_pool_info.get("baseTokenAmount", "0"))),
                                    "prices": {
                                        quote_token: Decimal(str(pool_price)),  # Direct price
                                    },
                                }

                                # Initialize quote token
                                database["connections"][chain][network][connector]["pools"][pool_address]["tokens"][
                                    quote_token
                                ] = {
                                    "balance": Decimal(str(detailed_pool_info.get("quoteTokenAmount", "0"))),
                                    "prices": {
                                        base_token: Decimal("1") / Decimal(str(pool_price))
                                        if Decimal(str(pool_price)) != DECIMAL_ZERO
                                        else None,  # Inverse price
                                    },
                                }
                        elif pool_type == PoolType.OMNIPOOL.value:
                            # Handle Omni pool specific logic here
                            # For now, we'll just log that we found an Omni pool
                            self.logger().info(f"Found Omni pool: {pool_address}")
                        elif pool_type == PoolType.LBP.value:
                            # Handle LBP pool specific logic here
                            # For now, we'll just log that we found an LBP pool
                            self.logger().info(f"Found LBP pool: {pool_address}")
                        else:
                            self.logger().warning(f"Pool type {pool_type} not supported yet")
                            raise NotImplementedError(f"Pool type {pool_type} not supported")

                        # Update pool with additional information
                        database["connections"][chain][network][connector]["pools"][pool_address].update(
                            {
                                "address": pool_address,
                                "type": pool.get("type", PoolType.UNKNOWN.value),
                                "annual_percentage_rate": detailed_pool_info.get("apr", None),
                                "total_value_locked": detailed_pool_info.get("tvl", None),
                                "impermanent_loss": None,
                                "volume": {"24h": detailed_pool_info.get("volume24h", None)},
                            }
                        )

        return database

    # noinspection PyMethodMayBeStatic
    def _generate_token_permutations(self, tokens: List[str]) -> List[str]:
        """Generate all possible permutations of token pairs for a given list of tokens"""
        token_pairs = []

        for permutation in permutations(tokens, len(tokens)):
            # Join tokens with '/' to create the key
            token_key = "/".join(permutation)
            token_pairs.append(token_key)

        return token_pairs

    async def _calculate_price_difference_percentage(
        self, base_token: str, quote_token: str, pool_1: Dict[str, Any], pool_2: Dict[str, Any]
    ) -> Optional[Decimal]:
        """
        Calculate price difference between two pools for a token pair

        Returns:
            Decimal: Price difference as a decimal (0.01 = 1%)
        """
        # Get price of quote_token in terms of base_token in pool_1
        price_1 = await self._get_token_price_in_pool(base_token, quote_token, pool_1)

        # Get price of quote_token in terms of base_token in pool_2
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

        # Calculate price difference
        price_difference = DECIMAL_ONE_HUNDRED * ((price_2 - price_1) / price_1)

        return price_difference

    async def _get_token_price_in_pool(
        self, base_token: str, quote_token: str, pool: Dict[str, Any]
    ) -> Optional[Decimal]:
        """Get the price of quote_token in terms of base_token in the given pool"""
        # First try to get a quote for a small amount to determine price
        maximum_slippage_percentage = self._maximum_slippage_percentage

        quote = await self._get_quote_swap(
            pool, base_token, quote_token, DECIMAL_ONE, TradeType.SELL, maximum_slippage_percentage
        )

        if not quote or "estimatedAmountOut" not in quote:
            return None

        expected_out = Decimal(str(quote["estimatedAmountOut"]))

        price = expected_out

        return price

    async def _calculate_optimal_trade_amount(self, opportunity: Dict[str, Any], max_available: Decimal) -> Decimal:
        """Calculate the optimal amount to trade based on slippage considerations"""
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
        best_profit_percentage = DECIMAL_ZERO

        for amount in test_amounts:
            if amount > max_available:
                continue

            # Skip amounts that are too close to previously tested ones
            if best_amount > DECIMAL_ZERO and abs(amount - best_amount) / best_amount < DECIMAL_TEN_PERCENT:
                continue

            profit_percentage = await self._simulate_arbitrage_profit(opportunity, amount)

            if profit_percentage > best_profit_percentage:
                best_profit_percentage = profit_percentage
                best_amount = amount

        return best_amount

    async def _simulate_arbitrage_profit(self, opportunity: Dict[str, Any], trade_amount: Decimal) -> Decimal:
        """Simulate an arbitrage trade to calculate expected profit percentage"""
        base_token = opportunity["base_token"]
        quote_token = opportunity["quote_token"]
        buy_pool = opportunity["buy_pool"]
        sell_pool = opportunity["sell_pool"]

        maximum_slippage_percentage = self._maximum_slippage_percentage

        # Simulate first swap: base_token -> quote_token in buy_pool
        buy_quote = await self._get_quote_swap(
            buy_pool, base_token, quote_token, trade_amount, TradeType.SELL, maximum_slippage_percentage
        )

        if not buy_quote or "estimatedAmountOut" not in buy_quote:
            return DECIMAL_ZERO

        expected_quote_token = Decimal(str(buy_quote["estimatedAmountOut"]))

        # Simulate second swap: quote_token -> base_token in sell_pool
        sell_quote = await self._get_quote_swap(
            sell_pool, quote_token, base_token, expected_quote_token, TradeType.SELL, maximum_slippage_percentage
        )

        if not sell_quote or "estimatedAmountOut" not in sell_quote:
            return DECIMAL_ZERO

        expected_base_token_return = Decimal(str(sell_quote["estimatedAmountOut"]))

        # Calculate expected profit percentage
        expected_profit = expected_base_token_return - trade_amount
        expected_profit_percentage = (expected_profit / trade_amount) * DECIMAL_ONE_HUNDRED

        return expected_profit_percentage

    # noinspection PyMethodMayBeStatic
    def _get_all_tokens_from_database(self) -> List[str]:
        """Get all tokens from the database"""
        tokens = set()

        for chain in database["connections"]:
            for network in database["connections"][chain]:
                for connector in database["connections"][chain][network]:
                    if "tokens" in database["connections"][chain][network][connector]:
                        tokens.update(database["connections"][chain][network][connector]["tokens"].keys())

        return list(tokens)

    # noinspection PyMethodMayBeStatic
    def _get_all_pools_from_database(self) -> List[Dict[str, Any]]:
        """Get all pools from the database"""
        pools = []

        for chain in database["connections"]:
            for network in database["connections"][chain]:
                for connector in database["connections"][chain][network]:
                    if "pools" in database["connections"][chain][network][connector]:
                        for pool_address, pool_info in database["connections"][chain][network][connector][
                            "pools"
                        ].items():
                            pools.append(pool_info)

        return pools

    # noinspection PyMethodMayBeStatic
    def _find_pools_with_token_pair(self, base_token: str, quote_token: str) -> List[Dict[str, Any]]:
        """Find pools that contain both tokens"""
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
        """Get wallet addresses that can be used for a specific pool"""
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
        """Get token balance across all wallets"""
        total_balance = DECIMAL_ZERO

        for chain_name, chain in self._configuration["connections"].items():
            for network_name, network in chain.items():
                for _, connector in network.items():
                    for wallet_address in connector["wallets"].keys():
                        balances = await self._post_chain_balances(
                            chain_name, network_name, wallet_address, [token_symbol]
                        )

                        if balances and "balances" in balances:
                            balance = balances["balances"].get(token_symbol)
                            if balance is not None:
                                total_balance += Decimal(str(balance))

        return total_balance

    async def _check_gateway_status(self):
        """Check if Gateway server is online and verify wallet connections for multiple pools"""
        # Skip if gateway is already verified as not ready
        if self._gateway_is_ready:
            return

        self.logger().info("Checking Gateway server status...")
        try:
            if await self._gateway_http_client.ping_gateway():
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
        """Set gateway as not ready with appropriate error message"""
        self._gateway_is_ready = False
        self.logger().error(error_message)

    async def _verify_wallet_connections(self):
        """Verify wallet connections for all configured pools"""

        if not self._all_gateway_connections or len(self._all_gateway_connections) == 0:
            self.logger().error("No wallet connections found. Please connect a wallet using 'gateway connect'.")
            return

        pools = getattr(self._configuration, "pools", [])
        if not pools or len(pools) == 0:
            self.logger().error("No pools configured. Please add pool configurations.")
            return

        for pool in pools:
            await self._verify_pool_wallet(pool, self._all_gateway_connections)

    async def _verify_pool_wallet(self, pool: Dict[str, Any], all_gateway_connections: List[Dict[str, Any]]):
        """Verify wallet connection for a specific pool configuration"""
        chain = pool.get("chain")
        network = pool.get("network")
        connector = pool.get("connector")
        pool_address = pool.get("pool_address")
        base_tokens = pool.get("base_tokens")
        quote_tokens = pool.get("quote_tokens")

        required_fields = {
            "chain": chain,
            "network": network,
            "connector": connector,
            "pool_address": pool_address,
            "base_tokens": base_tokens,
            "quote_tokens": quote_tokens,
        }
        missing_fields = [field for field, value in required_fields.items() if not value]

        if missing_fields:
            self.logger().error(f"Invalid pool configuration. Missing required fields: {', '.join(missing_fields)}")
            return

        gateway_connection = [
            gateway_connection
            for gateway_connection in all_gateway_connections
            if gateway_connection["chain"] == chain
            and gateway_connection["connector"] == connector
            and gateway_connection["network"] == network
        ]

        if not gateway_connection:
            self.logger().error(
                f"""No gateway connection found for "{chain}/{connector}/{network}". Please connect using 'gateway connect'."""
            )
        else:
            wallet_address = gateway_connection[0]["wallet_address"]
            self.logger().info(f"""Found wallet connection for "{chain}/{connector}/{network}:{wallet_address}""")

            # Store wallet address in pool config for later use
            pool["wallet_address"] = wallet_address

            # Get pool info to get token information
            pool["information"] = await self._get_pool_information(pool)

    async def _get_pools(self, _chain: str, connector: str, network: str):
        """
        Fetch all available pools for a connector.

        Args:
            _chain: Chain identifier
            connector: Connector identifier
            network: Network identifier

        Returns:
            Dictionary containing pools information
        """
        return await self._gateway_http_client.amm_list_pools(connector, network)

    async def _get_pool_information(self, pool: Dict[str, Any]):
        """
        Get detailed information about a liquidity pool.

        Args:
            pool: Dictionary containing pool configuration parameters

        Returns:
            Dictionary containing pool information
        """
        network = pool.get("network")
        connector = pool.get("connector")
        pool_address = pool.get("pool_address")

        if not all([network, connector, pool_address]):
            return None

        return await self._gateway_http_client.amm_pool_info(connector, network, pool_address)

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
            slippage_percentage: Maximum acceptable slippage as a Decimal (e.g., Decimal("0.5") for 0.5%)

        Returns:
            Dictionary containing swap quote information
        """
        network = pool.get("network")
        connector = pool.get("connector")
        pool_address = pool.get("address")

        if not all([network, connector, pool_address]):
            return None

        return await self._gateway_http_client.amm_quote_swap(
            network=network,
            connector=connector,
            base_asset=base_token,
            quote_asset=quote_token,
            amount=amount,
            side=side,
            slippage_percentage=slippage_percentage,
            pool_address=pool_address,
        )

    async def _get_quote_liquidity(
        self,
        pool: Dict[str, Any],
        base_token_amount: Optional[Decimal] = None,
        quote_token_amount: Optional[Decimal] = None,
        slippage_percentage: Optional[Decimal] = None,
    ):
        """
        Get a quote for adding liquidity to a pool.

        Args:
            pool: Dictionary containing pool configuration parameters
            base_token_amount: Amount of base token to add
            quote_token_amount: Amount of quote token to add
            slippage_percentage: Maximum acceptable slippage as a percentage

        Returns:
            Dictionary containing liquidity quote information
        """
        network = pool.get("network")
        connector = pool.get("connector")
        pool_address = pool.get("pool_address")

        if not all([network, connector, pool_address]):
            return None

        return await self._gateway_http_client.amm_quote_liquidity(
            connector=connector,
            network=network,
            pool_address=pool_address,
            base_token_amount=base_token_amount,
            quote_token_amount=quote_token_amount,
            slippage_percentage=slippage_percentage,
        )

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
            base_token: Symbol of the base token
            quote_token: Symbol of the quote token
            amount: Amount to swap as Decimal
            side: Trade side (BUY or SELL)
            slippage_percentage: Maximum acceptable slippage as a Decimal (e.g., Decimal("0.5") for 0.5%)

        Returns:
            Dictionary containing swap execution result
        """
        network = pool.get("network")
        connector = pool.get("connector")
        pool_address = pool.get("pool_address")

        if not all([network, connector, pool_address, wallet_address]):
            return None

        return await self._gateway_http_client.amm_execute_swap(
            network=network,
            connector=connector,
            wallet_address=wallet_address,
            base_asset=base_token,
            quote_asset=quote_token,
            side=side,
            amount=amount,
            slippage_percentage=slippage_percentage,
            pool_address=pool_address,
        )

    async def _post_add_liquidity(
        self,
        pool: Dict[str, Any],
        base_token_amount: Optional[Decimal] = None,
        quote_token_amount: Optional[Decimal] = None,
        slippage_percentage: Optional[Decimal] = None,
    ):
        """
        Add liquidity to the specified pool.

        Args:
            pool: Dictionary containing pool configuration parameters
            base_token_amount: Amount of base token to add
            quote_token_amount: Amount of quote token to add
            slippage_percentage: Maximum acceptable slippage as a percentage

        Returns:
            Dictionary containing add liquidity operation result
        """
        network = pool.get("network")
        connector = pool.get("connector")
        pool_address = pool.get("pool_address")
        wallet_address = pool.get("wallet_address")

        if not all([network, connector, pool_address, wallet_address]):
            return None

        return await self._gateway_http_client.amm_add_liquidity(
            connector=connector,
            network=network,
            wallet_address=wallet_address,
            pool_address=pool_address,
            base_token_amount=base_token_amount,
            quote_token_amount=quote_token_amount,
            slippage_percentage=slippage_percentage,
        )

    async def _post_remove_liquidity(self, pool: Dict[str, Any], percentage_to_remove: Decimal):
        """
        Remove liquidity from the specified pool.

        Args:
            pool: Dictionary containing pool configuration parameters
            percentage_to_remove: Percentage of liquidity to remove as a Decimal

        Returns:
            Dictionary containing remove liquidity operation result
        """
        network = pool.get("network")
        connector = pool.get("connector")
        pool_address = pool.get("pool_address")
        wallet_address = pool.get("wallet_address")

        if not all([network, connector, pool_address, wallet_address]):
            return None

        return await self._gateway_http_client.amm_remove_liquidity(
            connector=connector,
            network=network,
            wallet_address=wallet_address,
            pool_address=pool_address,
            percentage_to_remove=percentage_to_remove,
        )

    async def _get_root_status(self):
        """
        Get the status of the Gateway server.

        Returns:
            Dictionary containing server status information
        """
        return await self._gateway_http_client.get_gateway_status()

    async def _get_config(self, chain_or_connector: Optional[str] = None):
        """
        Get Gateway configuration settings.

        Args:
            chain_or_connector: Optional chain or connector to filter configuration

        Returns:
            Dictionary containing Gateway configuration
        """
        return await self._gateway_http_client.getself._configuration(chain_or_connector)

    async def _post_config_update(self, config_path: str, config_value: Any):
        """
        Update Gateway configuration setting.

        Args:
            config_path: Path to the configuration setting
            config_value: New value for the configuration setting

        Returns:
            Dictionary containing operation result
        """
        return await self._gateway_http_client.update_config(config_path, config_value)

    async def _get_connectors(self):
        """
        Get all available connectors from Gateway.

        Returns:
            Dictionary containing available connectors information
        """
        return await self._gateway_http_client.get_connectors()

    async def _get_wallet(self):
        """
        Get wallet information for all connected chains.

        Returns:
            Dictionary containing wallet information
        """
        return await self._gateway_http_client.get_wallets()

    async def _get_chain_status(self, chain: str, network: str):
        """
        Get chain status.

        Args:
            chain: Chain identifier
            network: Network identifier

        Returns:
            Dictionary containing chain status information
        """
        return await self._gateway_http_client.get_network_status(chain, network)

    async def _post_chain_poll(self, chain: str, network: str, tx_hash: str):
        """
        Poll for transaction status.

        Args:
            chain: Chain identifier
            network: Network identifier
            tx_hash: Transaction hash to poll

        Returns:
            Dictionary containing transaction status
        """
        return await self._gateway_http_client.get_transaction_status(chain, network, tx_hash)

    async def _get_chain_tokens(self, chain: str, network: str, token_symbols: Optional[Union[str, List[str]]] = None):
        """
        Get token information.

        Args:
            chain: Chain identifier
            network: Network identifier
            token_symbols: Optional token symbols to filter

        Returns:
            Dictionary containing token information
        """
        return await self._gateway_http_client.get_tokens(chain, network, token_symbols)

    async def _post_chain_balances(
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
            Dictionary containing token balances
        """
        return await self._gateway_http_client.get_balances(chain, network, address, token_symbols)

    async def _get_token_price(self, token_symbol: str, chain: str, network: str) -> Optional[Decimal]:
        """
        Get the price of a token from a price feed or API.

        Args:
            token_symbol: Symbol of the token
            chain: Chain identifier
            network: Network identifier

        Returns:
            Decimal price of the token or None if not available
        """
        try:
            # This is a placeholder implementation
            # In a real implementation, you would fetch the price from a price feed or API
            # For example, you might use CoinGecko, CoinMarketCap, or a DEX price feed

            # For now, we'll return None to indicate that the price is not available
            # You should implement the actual price fetching logic here
            return None
        except Exception as e:
            self.logger().error(f"Error fetching price for {token_symbol}: {str(e)}")
            return None

    def format_status(self) -> str:
        """Format the strategy status for display"""
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

        # Format status message
        status = [
            "AMM Arbitrage Strategy Status:",
            f"Gateway Status: {'Ready' if self._gateway_is_ready else 'Not Ready'}",
            f"Opportunities Found (last hour): {len(recent_opportunities)}",
            f"Trades Executed (last hour): {len(recent_executions)}",
            f"Total Profit: {total_profit:.4f}",
        ]

        if recent_executions:
            status.append("\nRecent Trades:")
            for ex in recent_executions[-5:]:  # Show last 5 trades
                token = ex.get("base_token", "")
                profit = ex.get("profit")
                profit_percentage = ex.get("profit_percentage")
                if profit is not None and profit_percentage is not None:
                    profit_decimal = Decimal(str(profit))
                    profit_percentage_decimal = Decimal(str(profit_percentage))
                    status.append(f"  {token}: {profit_decimal:.4f} ({profit_percentage_decimal:.2f}%)")

        return "\n".join(status)

    async def _get_token_balance_cached(self, chain, network, wallet_address, token_symbol, max_age_seconds=30):
        cache_key = f"{chain}_{network}_{wallet_address}_{token_symbol}"
        current_time = time.time()

        if (
            cache_key in self._balance_cache
            and current_time - self._balance_cache[cache_key]["timestamp"] < max_age_seconds
        ):
            return self._balance_cache[cache_key]["balance"]

        # Se não estiver em cache ou estiver expirado, buscar do gateway
        balances = await self._post_chain_balances(chain, network, wallet_address, [token_symbol])

        if balances and "balances" in balances:
            balance = Decimal(str(balances["balances"].get(token_symbol, 0)))
            self._balance_cache[cache_key] = {"balance": balance, "timestamp": current_time}
            return balance

        return DECIMAL_ZERO

    def _find_most_promising_token_pairs(self):
        """Identificar pares de tokens com maior potencial de arbitragem"""
        tokens = self._configuration["tokens"]
        promising_pairs = []

        for i in range(len(tokens)):
            for j in range(i + 1, len(tokens)):
                token1, token2 = tokens[i], tokens[j]
                pools = self._find_pools_with_token_pair(token1, token2)

                if len(pools) >= 2:  # Precisamos de pelo menos 2 pools para arbitragem
                    # Verificar liquidez e volume para determinar potencial
                    total_volume = sum(Decimal(str(p.get("volume", {}).get("24h", 0) or 0)) for p in pools)
                    promising_pairs.append(
                        {"token1": token1, "token2": token2, "pools_count": len(pools), "total_volume": total_volume}
                    )

        # Ordenar por volume e quantidade de pools
        promising_pairs.sort(key=lambda x: (x["pools_count"], x["total_volume"]), reverse=True)
        return promising_pairs[:5]  # Retornar os 5 mais promissores
