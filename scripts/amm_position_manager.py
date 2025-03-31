import asyncio
import os
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple, Union

# noinspection PyUnresolvedReferences
from pydantic import Field

from hummingbot.client.config.config_data_types import BaseClientModel
from hummingbot.client.settings import GatewayConnectionSetting
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.event.events import TradeType
from hummingbot.core.gateway.gateway_http_client import GatewayHttpClient
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase

configuration: Dict[str, Any] = {
    "globals": {
        "maximum_slippage_percentage": "0.5",  # 1 means 1%, or 0.01, in the code
        "minimum_profit_percentage": "1",  # 1 means 1%, or 0.01, in the code
        "arbitrage_check_interval_seconds": "30",  # Time between arbitrage checks
        "minimum_trade_amount": "10",  # Minimum amount to consider for a trade
    },
    "connections": {
        "polkadot": {
            "mainnet": {
                "hydration": {
                    "wallets": [
                        "5HKTQCEWuuA9bJEqFbAEsbwFQfEe5tXrbZXWj7yQpuxVSHKt",
                    ],
                    "pools": [
                        "7JRrXBpB1K2JUapwojTYLZPoMvLPMQUDyiEyJb5hj7wad1of",  # XyK / Isolated pool
                        # "7L53bUTBbfuj14UpdCNPwmgzzHSsrsTWBHX5pys32mVWM3C1",  # Omni pool
                        # "7LVGEVLFXpsCCtnsvhzkSMQARU7gRVCtwMckG7u7d3V6FVvG",  # Stable pool
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
                        "G7mw1d83ismcQJKkzt62Ug4noXCjVhu3eV7U5EMgge6Z",  # XyK / Isolated pool
                        # "<pool_address>",  # Omni pool
                        # "<pool_address>",  # Stable pool
                        # "<pool_address>",  # LBP pool
                    ],
                }
            }
        },
    },
    "tokens": ["DOT", "SOL", "USDC", "USDT"],
}

## Example structure)
# database: Dict[str, Any] = {
#     "connections": {
#         "polkadot": {
#             "mainnet": {
#                 "hydration": {
#                     "wallets": {
#                         "<wallet_address>": {
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
#                             "address": "<token_address>",
#                             "symbol": "<token_symbol>",
#                             "name": "<token_name>",
#                             "decimals": "<token_decimals>",
#                             "price": "<token_price>"
#                         }
#                     },
#                     "pools": {
#                         "<pool_address>": {
#                             "address": "<pool_address>",
#                             "type": "<pool_type>",
#                             "tokens": {
#                                 "<token_symbol>": {
#                                     "price": "<token_price>",
#                                 },
#                                 "<token_symbol>": {
#                                     "price": "<token_price>",
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
#     }
# }
database: Dict[str, Any] = {
    "connections": {},
    "tokens": {},
    "pools": {},
    "arbitrage_opportunities": [],
    "execution_history": [],
}


class AMMRobustPositionManagerConfiguration(BaseClientModel):
    script_file_name: str = Field(default_factory=lambda: os.path.basename(__file__))
    pools: List[Dict[str, Any]] = Field(default=database["pools"])


# noinspection PyShadowingNames
class AMMRobustPositionManager(ScriptStrategyBase):
    markets: Dict[str, Any] = {}
    configuration = None
    gateway_is_ready = False
    gateway_http_client: Optional[GatewayHttpClient] = None
    last_arbitrage_check_time = 0

    def __init__(self, connectors: Dict[str, ConnectorBase]):
        super().__init__(connectors)

        AMMRobustPositionManager.gateway_http_client = GatewayHttpClient.get_instance()

        self._initialize()

    def _initialize(self, configuration: AMMRobustPositionManagerConfiguration = None):
        self.configuration = configuration

        self._log_initialization()

    def _log_initialization(self):
        self.logger().info(f"Starting {self.__class__.__name__} strategy")

    def on_tick(self):
        safe_ensure_future(self._async_on_tick())

    async def _async_on_tick(self):
        """Main strategy execution logic that runs on each tick"""
        # First check gateway status
        if not self.gateway_is_ready:
            await self._check_gateway_status()
            if not self.gateway_is_ready:
                return

        # Update database with latest information
        await self._update_database()

        # Check if it's time to run arbitrage check
        current_time = time.time()
        arbitrage_check_interval = float(configuration["globals"].get("arbitrage_check_interval_seconds", 30))

        if current_time - self.last_arbitrage_check_time >= arbitrage_check_interval:
            self.last_arbitrage_check_time = current_time
            await self._run_arbitrage_strategy()

    async def _run_arbitrage_strategy(self):
        """Execute the arbitrage strategy"""
        self.logger().info("Checking for arbitrage opportunities...")

        opportunities = await self._find_arbitrage_opportunities()

        for opportunity in opportunities:
            if await self._validate_opportunity(opportunity):
                await self._execute_arbitrage(opportunity)

                # Add a short delay between trades to prevent transaction collisions
                await asyncio.sleep(1)

    async def _find_arbitrage_opportunities(self) -> List[Dict[str, Any]]:
        """Find arbitrage opportunities across pools and tokens"""
        opportunities = []

        # Get all available tokens and pools from the database
        tokens = self._get_all_tokens_from_database()
        pools = self._get_all_pools_from_database()

        # Minimum profit percentage required for arbitrage
        min_profit_pct = float(configuration["globals"].get("minimum_profit_percentage", 1)) / 100

        # Iterate through all token pairs and pool combinations
        for token_a in tokens:
            for token_b in tokens:
                if token_a == token_b:
                    continue

                # Find pools that contain both tokens
                relevant_pools = self._find_pools_with_token_pair(pools, token_a, token_b)

                # Check for arbitrage opportunities between different pools
                for i, pool_1 in enumerate(relevant_pools):
                    for j, pool_2 in enumerate(relevant_pools):
                        if i == j:
                            continue

                        # Calculate price difference between the two pools
                        price_difference = await self._calculate_price_difference(token_a, token_b, pool_1, pool_2)

                        # If price difference exceeds minimum profit threshold
                        if price_difference > min_profit_pct:
                            # Determine which pool to buy from and which to sell to
                            buy_pool, sell_pool = (pool_1, pool_2) if price_difference > 0 else (pool_2, pool_1)

                            opportunity = {
                                "token_a": token_a,
                                "token_b": token_b,
                                "buy_pool": buy_pool,
                                "sell_pool": sell_pool,
                                "price_difference_pct": price_difference * 100,
                                "timestamp": time.time(),
                            }

                            opportunities.append(opportunity)

                            self.logger().info(
                                f"Found arbitrage opportunity: {token_a}/{token_b} with {price_difference*100:.2f}% difference "
                                f"between {buy_pool['address']} and {sell_pool['address']}"
                            )

        # Update the database with found opportunities
        database["arbitrage_opportunities"] = opportunities

        return opportunities

    async def _validate_opportunity(self, opportunity: Dict[str, Any]) -> bool:
        """Validate an arbitrage opportunity by checking slippage and available balances"""
        token_a = opportunity["token_a"]
        token_b = opportunity["token_b"]
        buy_pool = opportunity["buy_pool"]
        sell_pool = opportunity["sell_pool"]

        # Get token balances
        token_a_balance = await self._get_token_balance(token_a)

        # Check if we have enough balance for the trade
        min_trade_amount = float(configuration["globals"].get("minimum_trade_amount", 10))
        if token_a_balance < min_trade_amount:
            self.logger().info(f"Insufficient balance of {token_a} for arbitrage: {token_a_balance}")
            return False

        # Calculate optimal trade amount (considering slippage)
        trade_amount = await self._calculate_optimal_trade_amount(opportunity, token_a_balance)

        if trade_amount <= 0:
            self.logger().info(f"Optimal trade amount calculation resulted in zero or negative amount")
            return False

        # Check slippage for both trades
        max_slippage_pct = float(configuration["globals"].get("maximum_slippage_percentage", 0.5))

        # Get quote for buying token_b with token_a in buy_pool
        buy_quote = await self._get_quote_swap(
            buy_pool,
            token_a,
            token_b,
            str(trade_amount),
            "SELL",  # Selling token_a to buy token_b
            str(max_slippage_pct),
        )

        if not buy_quote or "expectedOut" not in buy_quote:
            self.logger().info(f"Failed to get buy quote for {token_a}/{token_b} in pool {buy_pool['address']}")
            return False

        expected_token_b = float(buy_quote["expectedOut"])

        # Get quote for selling token_b for token_a in sell_pool
        sell_quote = await self._get_quote_swap(
            sell_pool,
            token_b,
            token_a,
            str(expected_token_b),
            "SELL",  # Selling token_b to get back token_a
            str(max_slippage_pct),
        )

        if not sell_quote or "expectedOut" not in sell_quote:
            self.logger().info(f"Failed to get sell quote for {token_b}/{token_a} in pool {sell_pool['address']}")
            return False

        expected_token_a_return = float(sell_quote["expectedOut"])

        # Calculate expected profit
        expected_profit = expected_token_a_return - trade_amount
        expected_profit_pct = (expected_profit / trade_amount) * 100

        # Add profit details to the opportunity
        opportunity["trade_amount"] = trade_amount
        opportunity["expected_token_b"] = expected_token_b
        opportunity["expected_token_a_return"] = expected_token_a_return
        opportunity["expected_profit"] = expected_profit
        opportunity["expected_profit_pct"] = expected_profit_pct

        # Validate that the opportunity is still profitable after slippage
        min_profit_pct = float(configuration["globals"].get("minimum_profit_percentage", 1))
        if expected_profit_pct < min_profit_pct:
            self.logger().info(
                f"Arbitrage opportunity no longer profitable after slippage: {expected_profit_pct:.2f}% < {min_profit_pct}%"
            )
            return False

        self.logger().info(
            f"Validated arbitrage opportunity: {token_a}/{token_b} with expected profit of {expected_profit_pct:.2f}%"
        )
        return True

    async def _execute_arbitrage(self, opportunity: Dict[str, Any]) -> bool:
        """Execute an arbitrage trade"""
        token_a = opportunity["token_a"]
        token_b = opportunity["token_b"]
        buy_pool = opportunity["buy_pool"]
        sell_pool = opportunity["sell_pool"]
        trade_amount = opportunity["trade_amount"]

        self.logger().info(f"Executing arbitrage trade: {token_a}/{token_b}")
        self.logger().info(f"Step 1: Swap {trade_amount} {token_a} for {token_b} in pool {buy_pool['address']}")

        # Get wallet information
        wallet_address = self._get_wallet_address_for_pool(buy_pool)
        if not wallet_address:
            self.logger().error(f"No wallet address found for pool {buy_pool['address']}")
            return False

        # Record initial balance
        initial_token_a_balance = await self._get_token_balance(token_a)

        # Execute first swap: token_a -> token_b in buy_pool
        max_slippage_pct = float(configuration["globals"].get("maximum_slippage_percentage", 0.5))

        first_swap_result = await self._post_execute_swap(
            buy_pool,
            token_a,
            token_b,
            str(trade_amount),
            "SELL",  # Selling token_a to buy token_b
            str(max_slippage_pct),
        )

        if not first_swap_result or "signature" not in first_swap_result:
            self.logger().error(f"First swap failed: {token_a} -> {token_b}")
            return False

        self.logger().info(f"First swap completed with transaction signature: {first_swap_result['signature']}")

        # Wait for transaction to be confirmed
        await asyncio.sleep(2)

        # Get token_b balance after first swap
        token_b_balance = await self._get_token_balance(token_b)

        # Execute second swap: token_b -> token_a in sell_pool
        self.logger().info(f"Step 2: Swap {token_b_balance} {token_b} back to {token_a} in pool {sell_pool['address']}")

        second_swap_result = await self._post_execute_swap(
            sell_pool,
            token_b,
            token_a,
            str(token_b_balance),
            "SELL",  # Selling token_b to get back token_a
            str(max_slippage_pct),
        )

        if not second_swap_result or "signature" not in second_swap_result:
            self.logger().error(f"Second swap failed: {token_b} -> {token_a}")
            return False

        self.logger().info(f"Second swap completed with transaction signature: {second_swap_result['signature']}")

        # Wait for transaction to be confirmed
        await asyncio.sleep(2)

        # Calculate actual profit
        final_token_a_balance = await self._get_token_balance(token_a)
        actual_profit = final_token_a_balance - initial_token_a_balance
        actual_profit_pct = (actual_profit / initial_token_a_balance) * 100

        # Record trade result in execution history
        trade_result = {
            "timestamp": time.time(),
            "token_a": token_a,
            "token_b": token_b,
            "buy_pool": buy_pool["address"],
            "sell_pool": sell_pool["address"],
            "initial_amount": initial_token_a_balance,
            "final_amount": final_token_a_balance,
            "profit": actual_profit,
            "profit_percentage": actual_profit_pct,
            "first_swap_tx": first_swap_result["signature"],
            "second_swap_tx": second_swap_result["signature"],
        }

        database["execution_history"].append(trade_result)

        if actual_profit > 0:
            self.logger().info(
                f"Arbitrage trade successful! Profit: {actual_profit} {token_a} ({actual_profit_pct:.2f}%)"
            )
        else:
            self.logger().warning(
                f"Arbitrage trade completed with loss: {actual_profit} {token_a} ({actual_profit_pct:.2f}%)"
            )

        return actual_profit > 0

    async def _update_database(self):
        """Update the database with latest information"""
        # Update connected wallets and their tokens/pools
        await self._update_wallet_information()

        # Update token information
        await self._update_token_information()

        # Update pool information
        await self._update_pool_information()

    async def _update_wallet_information(self):
        """Update wallet information in the database"""
        all_gateway_connections = GatewayConnectionSetting.load()

        # Prepare database connections structure if not exists
        if "connections" not in database:
            database["connections"] = {}

        for connection in all_gateway_connections:
            chain = connection.get("chain")
            network = connection.get("network")
            connector = connection.get("connector")
            wallet_address = connection.get("wallet_address")
            tokens = connection.get("tokens", "").split(",")

            # Skip incomplete connections
            if not all([chain, network, connector, wallet_address]):
                continue

            # Initialize chain if not exists
            if chain not in database["connections"]:
                database["connections"][chain] = {}

            # Initialize network if not exists
            if network not in database["connections"][chain]:
                database["connections"][chain][network] = {}

            # Initialize connector if not exists
            if connector not in database["connections"][chain][network]:
                database["connections"][chain][network][connector] = {}

            # Initialize wallets if not exists
            if "wallets" not in database["connections"][chain][network][connector]:
                database["connections"][chain][network][connector]["wallets"] = {}

            # Initialize wallet if not exists
            if wallet_address not in database["connections"][chain][network][connector]["wallets"]:
                database["connections"][chain][network][connector]["wallets"][wallet_address] = {
                    "tokens": {},
                    "pools": {},
                }

            # Update wallet token balances
            wallet_token_balances = await self._post_chain_balances(chain, network, wallet_address, tokens)

            if wallet_token_balances and "balances" in wallet_token_balances:
                for token_symbol, balance in wallet_token_balances["balances"].items():
                    if (
                        token_symbol
                        not in database["connections"][chain][network][connector]["wallets"][wallet_address]["tokens"]
                    ):
                        database["connections"][chain][network][connector]["wallets"][wallet_address]["tokens"][
                            token_symbol
                        ] = {}

                    database["connections"][chain][network][connector]["wallets"][wallet_address]["tokens"][
                        token_symbol
                    ]["balances"] = {"free": balance, "total": balance}

    async def _update_token_information(self):
        """Update token information in the database"""
        all_gateway_connections = GatewayConnectionSetting.load()

        for connection in all_gateway_connections:
            chain = connection.get("chain")
            network = connection.get("network")
            connector = connection.get("connector")
            tokens = connection.get("tokens", "").split(",")

            # Skip incomplete connections
            if not all([chain, network, connector]):
                continue

            # Initialize connector tokens if not exists
            if not database["connections"][chain][network][connector].get("tokens"):
                database["connections"][chain][network][connector]["tokens"] = {}

            # Get token information
            token_info = await self._get_chain_tokens(chain, network, tokens)

            if token_info and "tokens" in token_info:
                for token in token_info["tokens"]:
                    symbol = token.get("symbol")

                    if not symbol:
                        continue

                    database["connections"][chain][network][connector]["tokens"][symbol] = {
                        "address": token.get("address"),
                        "symbol": symbol,
                        "name": token.get("name"),
                        "decimals": token.get("decimals"),
                    }

    async def _update_pool_information(self):
        """Update pool information in the database"""
        all_gateway_connections = GatewayConnectionSetting.load()

        for connection in all_gateway_connections:
            chain = connection.get("chain")
            network = connection.get("network")
            connector = connection.get("connector")

            if not all([chain, network, connector]):
                continue

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
                if (
                    not pool_address
                    or pool.get("address") not in configuration["connections"][chain][network][connector]["pools"]
                ):
                    continue

                # Get detailed pool information
                detailed_pool_info = await self._get_pool_information(
                    {"network": network, "connector": connector, "pool_address": pool_address}
                )

                if not detailed_pool_info:
                    continue

                database["connections"][chain][network][connector]["pools"][pool_address] = {
                    "address": pool_address,
                    "type": pool.get("type", "unknown"),
                    "tokens": {},  # TODO fix, it is needed to call the tokens before!!!
                    "annual_percentage_rate": detailed_pool_info.get("apr"),
                    "total_value_locked": detailed_pool_info.get("tvl"),
                    "volume": {"24h": detailed_pool_info.get("volume24h")},
                }

                # TODO Update token information in the pool!!!
                for token_symbol in detailed_pool_info.get("tokens", {}):
                    token_info = detailed_pool_info["tokens"].get(token_symbol, {})

                    database["connections"][chain][network][connector]["pools"][pool_address]["tokens"][
                        token_symbol
                    ] = {"price": token_info.get("price")}

    async def _calculate_price_difference(
        self, token_a: str, token_b: str, pool_1: Dict[str, Any], pool_2: Dict[str, Any]
    ) -> float:
        """
        Calculate price difference between two pools for a token pair

        Returns:
            float: Price difference as a decimal (0.01 = 1%)
        """
        # Get price of token_b in terms of token_a in pool_1
        price_1 = await self._get_token_price_in_pool(token_a, token_b, pool_1)

        # Get price of token_b in terms of token_a in pool_2
        price_2 = await self._get_token_price_in_pool(token_a, token_b, pool_2)

        if price_1 is None or price_2 is None or price_1 == 0:
            return 0

        # Calculate price difference
        price_difference = (price_2 - price_1) / price_1

        return price_difference

    async def _get_token_price_in_pool(self, token_a: str, token_b: str, pool: Dict[str, Any]) -> Optional[float]:
        """Get the price of token_b in terms of token_a in the given pool"""
        # First try to get a quote for a small amount to determine price
        min_trade_amount = float(configuration["globals"].get("minimum_trade_amount", 10))

        quote = await self._get_quote_swap(pool, token_a, token_b, str(min_trade_amount), "SELL", "0.5")

        if not quote or "expectedOut" not in quote:
            return None

        expected_out = float(quote["expectedOut"])

        # Calculate price: how much token_b you get for 1 token_a
        price = expected_out / min_trade_amount

        return price

    async def _calculate_optimal_trade_amount(self, opportunity: Dict[str, Any], max_available: float) -> float:
        """Calculate the optimal amount to trade based on slippage considerations"""
        token_a = opportunity["token_a"]
        token_b = opportunity["token_b"]
        buy_pool = opportunity["buy_pool"]

        # Start with a small test amount
        min_trade_amount = float(configuration["globals"].get("minimum_trade_amount", 10))

        # Try different trade amounts to find the optimal one
        test_amounts = [
            min_trade_amount,
            max_available * 0.1,
            max_available * 0.25,
            max_available * 0.5,
            max_available * 0.75,
            max_available,
        ]

        best_amount = 0
        best_profit_pct = 0

        for amount in test_amounts:
            if amount > max_available:
                continue

            # Skip amounts that are too close to previously tested ones
            if best_amount > 0 and abs(amount - best_amount) / best_amount < 0.1:
                continue

            profit_pct = await self._simulate_arbitrage_profit(opportunity, amount)

            if profit_pct > best_profit_pct:
                best_profit_pct = profit_pct
                best_amount = amount

        return best_amount

    async def _simulate_arbitrage_profit(self, opportunity: Dict[str, Any], trade_amount: float) -> float:
        """Simulate an arbitrage trade to calculate expected profit percentage"""
        token_a = opportunity["token_a"]
        token_b = opportunity["token_b"]
        buy_pool = opportunity["buy_pool"]
        sell_pool = opportunity["sell_pool"]

        max_slippage_pct = float(configuration["globals"].get("maximum_slippage_percentage", 0.5))

        # Simulate first swap: token_a -> token_b in buy_pool
        buy_quote = await self._get_quote_swap(
            buy_pool, token_a, token_b, str(trade_amount), "SELL", str(max_slippage_pct)
        )

        if not buy_quote or "expectedOut" not in buy_quote:
            return 0

        expected_token_b = float(buy_quote["expectedOut"])

        # Simulate second swap: token_b -> token_a in sell_pool
        sell_quote = await self._get_quote_swap(
            sell_pool, token_b, token_a, str(expected_token_b), "SELL", str(max_slippage_pct)
        )

        if not sell_quote or "expectedOut" not in sell_quote:
            return 0

        expected_token_a_return = float(sell_quote["expectedOut"])

        # Calculate expected profit percentage
        expected_profit = expected_token_a_return - trade_amount
        expected_profit_pct = (expected_profit / trade_amount) * 100

        return expected_profit_pct

    def _get_all_tokens_from_database(self) -> List[str]:
        """Get all tokens from the database"""
        tokens = set()

        for chain in database["connections"]:
            for network in database["connections"][chain]:
                for connector in database["connections"][chain][network]:
                    if "tokens" in database["connections"][chain][network][connector]:
                        tokens.update(database["connections"][chain][network][connector]["tokens"].keys())

        return list(tokens)

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
                            pool_info["chain"] = chain
                            pool_info["network"] = network
                            pool_info["connector"] = connector
                            pools.append(pool_info)

        return pools

    def _find_pools_with_token_pair(
        self, pools: List[Dict[str, Any]], token_a: str, token_b: str
    ) -> List[Dict[str, Any]]:
        """Find pools that contain both tokens"""
        matching_pools = []

        for pool in pools:
            if "tokens" in pool and token_a in pool["tokens"] and token_b in pool["tokens"]:
                matching_pools.append(pool)

        return matching_pools

    def _get_wallet_address_for_pool(self, pool: Dict[str, Any]) -> Optional[str]:
        """Get a wallet address that can be used for a specific pool"""
        chain = pool.get("chain")
        network = pool.get("network")
        connector = pool.get("connector")

        if not all([chain, network, connector]):
            return None

        all_gateway_connections = GatewayConnectionSetting.load()

        for connection in all_gateway_connections:
            if (
                connection.get("chain") == chain
                and connection.get("network") == network
                and connection.get("connector") == connector
            ):
                return connection.get("wallet_address")

        return None

    async def _get_token_balance(self, token_symbol: str) -> float:
        """Get token balance across all wallets"""
        total_balance = 0

        all_gateway_connections = GatewayConnectionSetting.load()

        for connection in all_gateway_connections:
            chain = connection.get("chain")
            network = connection.get("network")
            wallet_address = connection.get("wallet_address")

            if not all([chain, network, wallet_address]):
                continue

            token_balances = await self._post_chain_balances(chain, network, wallet_address, [token_symbol])

            if token_balances and "balances" in token_balances:
                total_balance += float(token_balances["balances"].get(token_symbol, 0))

        return total_balance

    async def _check_gateway_status(self):
        """Check if Gateway server is online and verify wallet connections for multiple pools"""
        # Skip if gateway is already verified as not ready
        if self.gateway_is_ready:
            return

        self.logger().info("Checking Gateway server status...")
        try:
            if await self.gateway_http_client.ping_gateway():
                self.gateway_is_ready = True

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
        self.gateway_is_ready = False
        self.logger().error(error_message)

    async def _verify_wallet_connections(self):
        """Verify wallet connections for all configured pools"""
        all_gateway_connections = GatewayConnectionSetting.load()

        if not all_gateway_connections or len(all_gateway_connections) == 0:
            self.logger().error("No wallet connections found. Please connect a wallet using 'gateway connect'.")
            return

        # Get pools configuration
        pools = getattr(self.configuration, "pools", [])
        if not pools or len(pools) == 0:
            self.logger().error("No pools configured. Please add pool configurations.")
            return

        for pool in pools:
            await self._verify_pool_wallet(pool, all_gateway_connections)

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
        return await self.gateway_http_client.amm_pools(connector, network)

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

        return await self.gateway_http_client.amm_pool_info(connector, network, pool_address)

    async def _get_quote_swap(
        self,
        pool: Dict[str, Any],
        base_token: str,
        quote_token: str,
        amount: str,
        side: str,
        slippage_percentage: str = "0.5",
    ):
        """
        Get a quote for swapping tokens in a pool.

        Args:
            pool: Dictionary containing pool configuration parameters
            base_token: Symbol of the base token
            quote_token: Symbol of the quote token
            amount: Amount to swap
            side: Trade side (BUY or SELL)
            slippage_percentage: Maximum acceptable slippage as a percentage

        Returns:
            Dictionary containing swap quote information
        """
        network = pool.get("network")
        connector = pool.get("connector")
        pool_address = pool.get("pool_address")

        if not all([network, connector, pool_address]):
            return None

        trade_type = TradeType.BUY if side.upper() == "BUY" else TradeType.SELL

        return await self.gateway_http_client.amm_quote_swap(
            network=network,
            connector=connector,
            base_asset=base_token,
            quote_asset=quote_token,
            amount=Decimal(amount),
            side=trade_type,
            slippage_pct=Decimal(slippage_percentage) if slippage_percentage else None,
            pool_address=pool_address,
        )

    async def _get_quote_liquidity(
        self, pool: Dict[str, Any], base_token_amount: str, quote_token_amount: str, slippage_percentage: str = "0.5"
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

        return await self.gateway_http_client.amm_quote_liquidity(
            connector=connector,
            network=network,
            pool_address=pool_address,
            base_token_amount=float(base_token_amount) if base_token_amount else None,
            quote_token_amount=float(quote_token_amount) if quote_token_amount else None,
            slippage_pct=float(slippage_percentage) if slippage_percentage else None,
        )

    async def _post_execute_swap(
        self,
        pool: Dict[str, Any],
        base_token: str,
        quote_token: str,
        amount: str,
        side: str,
        slippage_percentage: str = "0.5",
    ):
        """
        Execute a token swap in the specified pool.

        Args:
            pool: Dictionary containing pool configuration parameters
            base_token: Symbol of the base token
            quote_token: Symbol of the quote token
            amount: Amount to swap
            side: Trade side (BUY or SELL)
            slippage_percentage: Maximum acceptable slippage as a percentage

        Returns:
            Dictionary containing swap execution result
        """
        network = pool.get("network")
        connector = pool.get("connector")
        pool_address = pool.get("pool_address")
        wallet_address = pool.get("wallet_address")

        if not all([network, connector, pool_address, wallet_address]):
            return None

        trade_type = TradeType.BUY if side.upper() == "BUY" else TradeType.SELL

        return await self.gateway_http_client.amm_execute_swap(
            network=network,
            connector=connector,
            address=wallet_address,
            base_asset=base_token,
            quote_asset=quote_token,
            side=trade_type,
            amount=Decimal(amount),
            slippage_pct=Decimal(slippage_percentage) if slippage_percentage else None,
            pool_address=pool_address,
        )

    async def _post_add_liquidity(
        self, pool: Dict[str, Any], base_token_amount: str, quote_token_amount: str, slippage_percentage: str = "0.5"
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

        return await self.gateway_http_client.amm_add_liquidity(
            connector=connector,
            network=network,
            wallet_address=wallet_address,
            pool_address=pool_address,
            base_token_amount=float(base_token_amount) if base_token_amount else None,
            quote_token_amount=float(quote_token_amount) if quote_token_amount else None,
            slippage_pct=float(slippage_percentage) if slippage_percentage else None,
        )

    async def _post_remove_liquidity(self, pool: Dict[str, Any], percentage_to_remove: str):
        """
        Remove liquidity from the specified pool.

        Args:
            pool: Dictionary containing pool configuration parameters
            percentage_to_remove: Percentage of liquidity to remove

        Returns:
            Dictionary containing remove liquidity operation result
        """
        network = pool.get("network")
        connector = pool.get("connector")
        pool_address = pool.get("pool_address")
        wallet_address = pool.get("wallet_address")

        if not all([network, connector, pool_address, wallet_address]):
            return None

        return await self.gateway_http_client.amm_remove_liquidity(
            connector=connector,
            network=network,
            wallet_address=wallet_address,
            pool_address=pool_address,
            percentage_to_remove=float(percentage_to_remove),
        )

    async def _get_root_status(self):
        """
        Get the status of the Gateway server.

        Returns:
            Dictionary containing server status information
        """
        return await self.gateway_http_client.get_gateway_status()

    async def _get_config(self, chain_or_connector: Optional[str] = None):
        """
        Get Gateway configuration settings.

        Args:
            chain_or_connector: Optional chain or connector to filter configuration

        Returns:
            Dictionary containing Gateway configuration
        """
        return await self.gateway_http_client.get_configuration(chain_or_connector)

    async def _post_config_update(self, config_path: str, config_value: Any):
        """
        Update Gateway configuration setting.

        Args:
            config_path: Path to the configuration setting
            config_value: New value for the configuration setting

        Returns:
            Dictionary containing operation result
        """
        return await self.gateway_http_client.update_config(config_path, config_value)

    async def _get_connectors(self):
        """
        Get all available connectors from Gateway.

        Returns:
            Dictionary containing available connectors information
        """
        return await self.gateway_http_client.get_connectors()

    async def _get_wallet(self):
        """
        Get wallet information for all connected chains.

        Returns:
            Dictionary containing wallet information
        """
        return await self.gateway_http_client.get_wallets()

    async def _get_chain_status(self, chain: str, network: str):
        """
        Get chain status.

        Args:
            chain: Chain identifier
            network: Network identifier

        Returns:
            Dictionary containing chain status information
        """
        return await self.gateway_http_client.get_network_status(chain, network)

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
        return await self.gateway_http_client.get_transaction_status(chain, network, tx_hash)

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
        return await self.gateway_http_client.get_tokens(chain, network, token_symbols)

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
        return await self.gateway_http_client.get_balances(chain, network, address, token_symbols)

    def format_status(self) -> str:
        """Format the strategy status for display"""
        if not self.gateway_is_ready:
            return "Gateway is not ready. Please check connection."

        # Get recent arbitrage opportunities
        opportunities = database.get("arbitrage_opportunities", [])
        recent_opportunities = [op for op in opportunities if time.time() - op.get("timestamp", 0) < 3600]  # Last hour

        # Get recent executions
        executions = database.get("execution_history", [])
        recent_executions = [ex for ex in executions if time.time() - ex.get("timestamp", 0) < 3600]  # Last hour

        # Calculate profit statistics
        total_profit = sum(ex.get("profit", 0) for ex in executions)

        # Format status message
        status = []
        status.append(f"AMM Arbitrage Strategy Status:")
        status.append(f"Gateway Status: {'Ready' if self.gateway_is_ready else 'Not Ready'}")
        status.append(f"Opportunities Found (last hour): {len(recent_opportunities)}")
        status.append(f"Trades Executed (last hour): {len(recent_executions)}")
        status.append(f"Total Profit: {total_profit:.4f}")

        if recent_executions:
            status.append("\nRecent Trades:")
            for ex in recent_executions[-5:]:  # Show last 5 trades
                token = ex.get("token_a", "")
                profit = ex.get("profit", 0)
                profit_pct = ex.get("profit_percentage", 0)
                status.append(f"  {token}: {profit:.4f} ({profit_pct:.2f}%)")

        return "\n".join(status)
