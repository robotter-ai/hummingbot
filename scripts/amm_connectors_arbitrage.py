"""
AMM Connectors Arbitrage Strategy

This module implements a strategy for executing arbitrage trades between different AMM connectors
using token pairs. The strategy identifies price discrepancies between the same token pairs
across different connectors and executes trades to profit from these differences.

Key Features:
    - Cross-connector arbitrage execution
    - Token pair price monitoring
    - Portfolio-based profit calculation
    - Automated trade execution with slippage protection
    - Transaction confirmation and balance tracking
    - Comprehensive trade history and performance metrics

Strategy Flow:
    1. Monitor token pair prices across different connectors
    2. Identify price discrepancies that exceed minimum profitability threshold
    3. Validate opportunities by simulating trades and checking balances
    4. Execute trades when profitable opportunities are found
    5. Track and record trade performance

Configuration:
    - Minimum profitability threshold
    - Maximum slippage tolerance
    - Trade amount limits
    - Token pairs to monitor
    - Connector-specific settings

Dependencies:
    - AMMPortfolioManagerBase: Base class providing common functionality
    - GatewayHttpClient: For interacting with blockchain networks
    - Various utility functions for logging, caching, and error handling
"""

import asyncio
import logging
import os
import time
from decimal import Decimal
from typing import Any, Dict, List

from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import OrderState
from scripts.community.amm_portfolio_base import (
    DECIMAL_FIFTY_PERCENT,
    DECIMAL_NEGATIVE_INFINITY,
    DECIMAL_ONE_HUNDRED,
    DECIMAL_SEVENTY_FIVE_PERCENT,
    DECIMAL_TEN_PERCENT,
    DECIMAL_TWENTY_FIVE_PERCENT,
    DECIMAL_ZERO,
    AMMPortfolioManagerBase,
    Logger,
    StrategyType,
    logged_class,
)

# ==============================================================================
# Global Configurations for the Strategy
# ==============================================================================
configuration: Dict[str, Any] = {
    "globals": {
        "maximum_slippage_percentage": "0.5",  # Maximum allowed price slippage when executing trades (Ex.: 0.5 for 0.5%)
        "minimum_profitability_percentage": "-30",  # Minimum profit threshold required to execute an arbitrage (Ex.: 0.1 for 0.1%)
        "arbitrage_check_interval_seconds": "60",  # Time interval between subsequent arbitrage opportunity checks
        "minimum_trade_amount": "0.1",  # Minimum trade amount
        "maximum_trade_amount": "0.1",  # Maximum trade amount
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
        "strategy_type": StrategyType.CONNECTORS_ARBITRAGE,  # Strategy type identifier for connectors arbitrage
        "rate_source": "custom",  # Use our custom rate source for HDX price
    },
    "connections": {
        "polkadot": {
            "mainnet": {
                "hydration": {
                    "native_token_symbol": "HDX",
                    "fee_payment_token_symbol": "HDX",
                    "wallets": [
                        os.environ["POLKADOT_MAINNET_HYDRATION_WALLET_ADDRESS"]
                    ],
                    "pools": [],
                }
            },
        },
        "solana": {
            "mainnet-beta": {
                "raydium": {
                    "native_token_symbol": "SOL",
                    "fee_payment_token_symbol": "SOL",
                    "wallets": [
                        os.environ["SOLANA_MAINNET_BETA_RAYDIUM_WALLET_ADDRESS"]
                    ],
                    "pools": [],
                }
            },
        }
    },
    "token_pairs": ["USDC/USDT"],
    "token_triads": [],
}

# ==============================================================================
# Logger Initialization
# ==============================================================================
logger = Logger(path="logs/logs_amm_connectors_arbitrage.log", level=logging.DEBUG)


# ==============================================================================
# AMMConnectorsArbitrage Strategy Class
# ==============================================================================
@logged_class(logger=logger, disallowed_methods=["on_tick"])
class AMMConnectorsArbitrage(AMMPortfolioManagerBase):
    """
    AMM Connectors Arbitrage Strategy

    This strategy implements cross-connector arbitrage by monitoring token pair prices
    across different AMM connectors and executing trades when profitable opportunities
    are identified.

    The strategy extends AMMPortfolioManagerBase and implements the specific logic
    for arbitrage between different connectors.

    Key Methods:
        _run_connectors_arbitrage: Main execution method for connectors arbitrage
        _find_arbitrage_opportunities: Identifies profitable arbitrage opportunities
        _validate_opportunity: Validates potential arbitrage opportunities
        _calculate_optimal_trade_amount: Determines optimal trade size
        _execute_arbitrage: Executes validated arbitrage trades

    Configuration Parameters:
        - minimum_profitability_percentage: Minimum profit threshold for trades
        - maximum_slippage_percentage: Maximum allowed slippage
        - minimum_trade_amount: Minimum trade size
        - maximum_trade_amount: Maximum trade size
        - token_pairs: List of token pairs to monitor
    """

    @classmethod
    @property
    def markets(cls):
        output = {}

        for (chain_name, chain_configuration) in configuration["connections"].items():
            for (network_name, network_configuration) in chain_configuration.items():
                for connector_name, connector_configuration in network_configuration.items():
                    output[f"{connector_name}/amm_{chain_name}_{network_name}"] = configuration["token_pairs"]

        return output

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs, configuration=configuration, logger=logger)

        self._strategy_type = StrategyType.CONNECTORS_ARBITRAGE

    async def _run_triangular_arbitrage(self):
        pass

    async def _run_connectors_arbitrage(self):
        """
        Executes the connectors arbitrage strategy:
         - Discovers promising token pairs
         - Validates each opportunity
         - Executes arbitrage trades for opportunities that pass validation
        """
        # Finds promising token pairs
        promising_pairs = self._find_most_promising_token_pairs()
        if not promising_pairs:
            logger.info("No promising token pair found.")
            return

        # Find arbitrage opportunities
        opportunities = await self._find_arbitrage_opportunities(promising_pairs)
        logger.info(f"Found {len(opportunities)} arbitrage opportunities.")

        # Validates and executes each opportunity
        for opportunity in opportunities:
            # Registers the opportunity in the database
            if "arbitrage_opportunities" not in self._database:
                self._database["arbitrage_opportunities"] = []
            self._database["arbitrage_opportunities"].append(opportunity)

            # Validates the opportunity
            logger.info(
                f"Validating opportunity: {opportunity['base_token']}/{opportunity['quote_token']} with difference of {opportunity['price_diff']:.2f}%"
            )
            valid = await self._validate_opportunity(opportunity)

            if valid:
                # Executes arbitrage if valid
                logger.info(f"Executing arbitrage: {opportunity['base_token']}/{opportunity['quote_token']}")
                success = await self._execute_arbitrage(opportunity)

                if success:
                    logger.info("Arbitrage successfully collected profit.")
                else:
                    logger.warning("Arbitrage incurred in loss or no profit.")

                # Waits the configured delay between arbitrages
                if self._time_delay_between_arbitrages > 0:
                    await asyncio.sleep(self._time_delay_between_arbitrages)
            else:
                logger.info("Opportunity invalidated after detailed validation.")

    def _find_most_promising_token_pairs(self) -> List[Dict[str, Any]]:
        """
        Identifies promising token pairs based on current pool data.

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

    async def _find_arbitrage_opportunities(self, promising_pairs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Finds arbitrage opportunities based on promising token pairs.

        Args:
            promising_pairs: List of dictionaries of token pairs.
        Returns:
            List of dictionaries of arbitrage opportunities.
        """
        opportunities = []

        # Extracts token pairs from dictionaries
        token_pairs = [(pair["base_token"], pair["quote_token"]) for pair in promising_pairs]

        for base_token, quote_token in token_pairs:
            # Finds all pools containing both tokens
            pools = self._find_pools_with_token_pair(base_token, quote_token)

            # Needs at least 2 pools for arbitrage
            if len(pools) < 2:
                continue

            # Compares each pair of pools for potential arbitrage
            for pool_1_index in range(len(pools)):
                for pool_2_index in range(pool_1_index + 1, len(pools)):
                    pool_1, pool_2 = pools[pool_1_index], pools[pool_2_index]

                    # Quick check using price difference as initial filter
                    price_diff = await self._calculate_price_difference_percentage(
                        base_token, quote_token, pool_1, pool_2
                    )

                    # Skip if price difference is below threshold or can't be calculated
                    if price_diff is None or abs(price_diff) <= self._minimum_profitability_percentage:
                        continue

                    # Determine buy and sell pools based on price difference
                    if price_diff > 0:
                        buy_pool, sell_pool = pool_1, pool_2
                    else:
                        buy_pool, sell_pool = pool_2, pool_1

                    # Initial opportunity record with basic information
                    opportunity = {
                        "base_token": base_token,
                        "quote_token": quote_token,
                        "buy_pool": buy_pool,
                        "sell_pool": sell_pool,
                        "price_diff": abs(price_diff),
                        "timestamp": time.time(),
                    }

                    opportunities.append(opportunity)
                    logger.info(
                        f"Arbitrage opportunity found: {base_token}/{quote_token} "
                        f"difference {abs(price_diff):.2f}% between {buy_pool.get('internal_id')} and {sell_pool.get('internal_id')}"
                    )

        return opportunities

    def _find_pool_by_internal_id(self, pool_id):
        """Finds a pool by its internal ID in the database."""
        for chain in self._database["connections"].values():
            for network in chain.values():
                for connector in network.values():
                    if pool_id in connector["pools"]:
                        return connector["pools"][pool_id]
        return None

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
            logger.info(f"Insufficient balance of {base_token}: {available_balance}")
            return False

        # Calculates ideal trade amount
        trade_amount = await self._calculate_optimal_trade_amount(opportunity, available_balance)
        if not trade_amount or trade_amount <= DECIMAL_ZERO:
            logger.info(f"Invalid optimal trade amount: {trade_amount}")
            return False

        try:
            # Get wallet addresses for each pool
            buy_wallets = await self._get_wallets_for_pool(buy_pool)
            sell_wallets = await self._get_wallets_for_pool(sell_pool)
            if not buy_wallets or not sell_wallets:
                logger.info("No wallets found for pools")
                return False

            # For simplicity, use first wallet for each pool
            buy_wallet = buy_wallets[0]
            sell_wallet = sell_wallets[0]

            # Get initial balances
            buy_pool_initial_balances = await self._gateway_get_balances(
                buy_pool.get("chain"), buy_pool.get("network"), buy_wallet.get("address"), [base_token, quote_token]
            )
            if not buy_pool_initial_balances or "balances" not in buy_pool_initial_balances:
                raise Exception("Failed to get initial balances for wallet")

            sell_pool_initial_balances = await self._gateway_get_balances(
                sell_pool.get("chain"), sell_pool.get("network"), sell_wallet.get("address"), [base_token, quote_token]
            )
            if not sell_pool_initial_balances or "balances" not in sell_pool_initial_balances:
                raise Exception("Failed to get initial sell wallet balances")

            # Simulates buy: base_token -> quote_token
            buy_quote = await self._gateway_quote_swap(
                buy_pool.get("network"),
                buy_pool.get("connector"),
                base_token,
                quote_token,
                trade_amount,
                TradeType.SELL,
                self._maximum_slippage_percentage,
                buy_pool.get("address"),
            )
            if not buy_quote or "estimatedAmountOut" not in buy_quote:
                logger.info(f"Buy quote unavailable for pool {buy_pool.get('internal_id')}")
                return False

            expected_quote = Decimal(str(buy_quote["estimatedAmountOut"]))

            # Simulates sell: quote_token -> base_token
            sell_quote = await self._gateway_quote_swap(
                sell_pool.get("network"),
                sell_pool.get("connector"),
                quote_token,
                base_token,
                expected_quote,
                TradeType.SELL,
                self._maximum_slippage_percentage,
                sell_pool.get("address"),
            )
            if not sell_quote or "estimatedAmountOut" not in sell_quote:
                logger.info(f"Sell quote unavailable for pool {sell_pool.get('internal_id')}")
                return False

            # Make sure we have the most recent token prices
            await self._update_token_information()

            # Get token prices
            base_token_price_buy = self._database["connections"][buy_pool.get("chain")][buy_pool.get("network")][
                buy_pool.get("connector")
            ]["tokens"][base_token]["price"]

            quote_token_price_buy = self._database["connections"][buy_pool.get("chain")][buy_pool.get("network")][
                buy_pool.get("connector")
            ]["tokens"][quote_token]["price"]

            base_token_price_sell = self._database["connections"][sell_pool.get("chain")][sell_pool.get("network")][
                sell_pool.get("connector")
            ]["tokens"][base_token]["price"]

            quote_token_price_sell = self._database["connections"][sell_pool.get("chain")][sell_pool.get("network")][
                sell_pool.get("connector")
            ]["tokens"][quote_token]["price"]

            # Simulate final balances after transactions
            buy_pool_initial_base_balance = Decimal(str(buy_pool_initial_balances["balances"].get(base_token, 0)))
            buy_pool_initial_quote_balance = Decimal(str(buy_pool_initial_balances["balances"].get(quote_token, 0)))
            sell_pool_initial_base_balance = Decimal(str(sell_pool_initial_balances["balances"].get(base_token, 0)))
            sell_pool_initial_quote_balance = Decimal(str(sell_pool_initial_balances["balances"].get(quote_token, 0)))

            # Simulate buy transaction effect
            buy_pool_final_base_balance = buy_pool_initial_base_balance - trade_amount
            buy_pool_final_quote_balance = buy_pool_initial_quote_balance + expected_quote

            # Simulate sell transaction effect
            expected_return = Decimal(str(sell_quote["estimatedAmountOut"]))
            sell_pool_final_quote_balance = sell_pool_initial_quote_balance - expected_quote
            sell_pool_final_base_balance = sell_pool_initial_base_balance + expected_return

            # Calculate profit using portfolio approach
            profit_information = self.calculate_portfolio_profit(
                {
                    "buy": {
                        "base": {
                            "balance": {
                                "initial": buy_pool_initial_base_balance,
                                "final": buy_pool_final_base_balance,
                            },
                            "price": base_token_price_buy,
                        },
                        "quote": {
                            "balance": {
                                "initial": buy_pool_initial_quote_balance,
                                "final": buy_pool_final_quote_balance,
                            },
                            "price": quote_token_price_buy,
                        },
                    },
                    "sell": {
                        "base": {
                            "balance": {
                                "initial": sell_pool_initial_base_balance,
                                "final": sell_pool_final_base_balance,
                            },
                            "price": base_token_price_sell,
                        },
                        "quote": {
                            "balance": {
                                "initial": sell_pool_initial_quote_balance,
                                "final": sell_pool_final_quote_balance,
                            },
                            "price": quote_token_price_sell,
                        },
                    },
                }
            )

            # Updates opportunity with calculated values
            opportunity.update(
                {
                    "trade_amount": trade_amount,
                    "expected_quote_token": expected_quote,
                    "expected_base_token_return": expected_return,
                    "expected_profit": profit_information["profit"]["absolute"],
                    "expected_profit_percentage": profit_information["profit"]["percentage"],
                }
            )

            # Checks if opportunity meets minimum profitability
            if profit_information["profit"]["percentage"] < self._minimum_profitability_percentage:
                logger.info(
                    f"Opportunity not profitable after slippage: "
                    f"{profit_information['profit']['percentage']:.2f}% < {self._minimum_profitability_percentage}%"
                )
                return False

            logger.info(
                f"Opportunity validated: {base_token}/{quote_token} expected profit {profit_information['profit']['percentage']:.2f}%"
            )
            return True
        except Exception as exception:
            logger.ignore_exception(exception, "Error during opportunity validation")
            return False

    async def _calculate_optimal_trade_amount(self, opportunity: Dict[str, Any], maximum_available: Decimal) -> Decimal:
        """
        Determines the optimal trade amount by simulating profits at different sizes.

        Args:
            opportunity: Arbitrage opportunity dictionary.
            maximum_available: Maximum available balance of the base token.
        Returns:
            Optimal trade amount as Decimal.
        """
        # Cap maximum available to configured maximum trade amount
        maximum_available = min(maximum_available, self._maximum_trade_amount)

        if maximum_available <= self._minimum_trade_amount:
            return maximum_available if maximum_available > DECIMAL_ZERO else DECIMAL_ZERO

        test_amounts = [
            self._minimum_trade_amount,
            maximum_available * DECIMAL_TEN_PERCENT,
            maximum_available * DECIMAL_TWENTY_FIVE_PERCENT,
            maximum_available * DECIMAL_FIFTY_PERCENT,
            maximum_available * DECIMAL_SEVENTY_FIVE_PERCENT,
            maximum_available,
        ]

        best_amount = DECIMAL_ZERO
        best_profit_percentage = DECIMAL_NEGATIVE_INFINITY

        for amount in sorted(test_amounts):
            if amount < self._minimum_trade_amount or amount > maximum_available:
                continue

            profit_percentage = await self._simulate_arbitrage_profit(opportunity, amount)

            if profit_percentage > best_profit_percentage:
                best_profit_percentage = profit_percentage
                best_amount = amount

        return best_amount if best_amount > DECIMAL_ZERO else self._minimum_trade_amount

    async def _simulate_arbitrage_profit(self, opportunity: Dict[str, Any], amount: Decimal) -> Decimal:
        """
        Simulates expected profit percentage for a given trade amount using portfolio approach.

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

        try:
            # Get wallet addresses for each pool
            buy_wallets = await self._get_wallets_for_pool(buy_pool)
            sell_wallets = await self._get_wallets_for_pool(sell_pool)

            if not buy_wallets or not sell_wallets:
                return DECIMAL_NEGATIVE_INFINITY

            # For simplicity, use first wallet for each pool
            buy_wallet = buy_wallets[0]
            sell_wallet = sell_wallets[0]

            # Get initial balances
            buy_pool_initial_balances = await self._gateway_get_balances(
                buy_pool.get("chain"), buy_pool.get("network"), buy_wallet.get("address"), [base_token, quote_token]
            )
            if not buy_pool_initial_balances or "balances" not in buy_pool_initial_balances:
                return DECIMAL_NEGATIVE_INFINITY

            sell_pool_initial_balances = await self._gateway_get_balances(
                sell_pool.get("chain"), sell_pool.get("network"), sell_wallet.get("address"), [base_token, quote_token]
            )
            if not sell_pool_initial_balances or "balances" not in sell_pool_initial_balances:
                return DECIMAL_NEGATIVE_INFINITY

            # Simulate buy trade
            buy_quote = await self._gateway_quote_swap(
                buy_pool.get("network"),
                buy_pool.get("connector"),
                base_token,
                quote_token,
                amount,
                TradeType.SELL,
                self._maximum_slippage_percentage,
                buy_pool.get("address"),
            )

            if not buy_quote or "estimatedAmountOut" not in buy_quote:
                return DECIMAL_NEGATIVE_INFINITY

            expected_quote = Decimal(str(buy_quote["estimatedAmountOut"]))

            # Simulate sell trade
            sell_quote = await self._gateway_quote_swap(
                sell_pool.get("network"),
                sell_pool.get("connector"),
                quote_token,
                base_token,
                expected_quote,
                TradeType.SELL,
                self._maximum_slippage_percentage,
                sell_pool.get("address"),
            )

            if not sell_quote or "estimatedAmountOut" not in sell_quote:
                return DECIMAL_NEGATIVE_INFINITY

            # Update token prices
            await self._update_token_information()

            # Get token prices
            base_token_price_buy = self._database["connections"][buy_pool.get("chain")][buy_pool.get("network")][
                buy_pool.get("connector")
            ]["tokens"][base_token]["price"]

            quote_token_price_buy = self._database["connections"][buy_pool.get("chain")][buy_pool.get("network")][
                buy_pool.get("connector")
            ]["tokens"][quote_token]["price"]

            base_token_price_sell = self._database["connections"][sell_pool.get("chain")][sell_pool.get("network")][
                sell_pool.get("connector")
            ]["tokens"][base_token]["price"]

            quote_token_price_sell = self._database["connections"][sell_pool.get("chain")][sell_pool.get("network")][
                sell_pool.get("connector")
            ]["tokens"][quote_token]["price"]

            # Process balances
            buy_pool_initial_base_balance = Decimal(str(buy_pool_initial_balances["balances"].get(base_token, 0)))
            buy_pool_initial_quote_balance = Decimal(str(buy_pool_initial_balances["balances"].get(quote_token, 0)))
            sell_pool_initial_base_balance = Decimal(str(sell_pool_initial_balances["balances"].get(base_token, 0)))
            sell_pool_initial_quote_balance = Decimal(str(sell_pool_initial_balances["balances"].get(quote_token, 0)))

            # Simulate final balances after trades
            buy_pool_final_base_balance = buy_pool_initial_base_balance - amount
            buy_pool_final_quote_balance = buy_pool_initial_quote_balance + expected_quote

            expected_return = Decimal(str(sell_quote["estimatedAmountOut"]))
            sell_pool_final_quote_balance = sell_pool_initial_quote_balance - expected_quote
            sell_pool_final_base_balance = sell_pool_initial_base_balance + expected_return

            # Calculate profit using portfolio approach
            profit_information = self.calculate_portfolio_profit(
                {
                    "buy": {
                        "base": {
                            "balance": {
                                "initial": buy_pool_initial_base_balance,
                                "final": buy_pool_final_base_balance,
                            },
                            "price": base_token_price_buy,
                        },
                        "quote": {
                            "balance": {
                                "initial": buy_pool_initial_quote_balance,
                                "final": buy_pool_final_quote_balance,
                            },
                            "price": quote_token_price_buy,
                        },
                    },
                    "sell": {
                        "base": {
                            "balance": {
                                "initial": sell_pool_initial_base_balance,
                                "final": sell_pool_final_base_balance,
                            },
                            "price": base_token_price_sell,
                        },
                        "quote": {
                            "balance": {
                                "initial": sell_pool_initial_quote_balance,
                                "final": sell_pool_final_quote_balance,
                            },
                            "price": quote_token_price_sell,
                        },
                    },
                }
            )

            return profit_information["profit"]["percentage"]
        except Exception as exception:
            logger.ignore_exception(exception, "Error simulating arbitrage profit")
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
        buy_pool_swap_amount = opportunity["trade_amount"]
        expected_quote = opportunity["expected_quote_token"]

        logger.info(f"Executing arbitrage for {base_token}/{quote_token}")

        # Gets wallet addresses for each pool.
        buy_wallets = await self._get_wallets_for_pool(buy_pool)
        sell_wallets = await self._get_wallets_for_pool(sell_pool)
        if not buy_wallets:
            raise Exception("No wallet found for buy pool")
        if not sell_wallets:
            raise Exception("No wallet found for sell pool")

        # For now, we only support one wallet per pool.
        buy_wallet = buy_wallets[0]
        sell_wallet = sell_wallets[0]

        try:
            # Get initial balances
            buy_pool_initial_balances = await self._gateway_get_balances(
                buy_pool.get("chain"), buy_pool.get("network"), buy_wallet.get("address"), [base_token, quote_token]
            )
            if not buy_pool_initial_balances or "balances" not in buy_pool_initial_balances:
                raise Exception("Failed to get initial balances for wallet")

            buy_pool_initial_base_balance = Decimal(str(buy_pool_initial_balances["balances"].get(base_token, 0)))
            buy_pool_initial_quote_balance = Decimal(str(buy_pool_initial_balances["balances"].get(quote_token, 0)))
            if buy_pool_initial_base_balance < buy_pool_swap_amount:
                logger.info(
                    f"Insufficient balance in {buy_wallet.get('internal_id')}: {buy_pool_initial_base_balance} {base_token}"
                )
                return False

            # First swap (buy pool): base_token -> quote_token
            logger.info(
                f"Step 1: Swapping {buy_pool_swap_amount} {base_token} for {quote_token} in pool {buy_pool.get('address')}"
            )

            # Get the price for the first swap
            price_1 = self._get_token_pair_relative_price_in_pool(buy_pool, base_token, quote_token)
            if price_1 is None or price_1 == DECIMAL_ZERO:
                raise Exception("Could not get price for")

            # Execute buy using the connector's sell method
            buy_pool_connector = self.connectors[f"{buy_pool.get('connector')}/amm_{buy_pool.get('chain')}_{buy_pool.get('network')}"]
            buy_order_id = buy_pool_connector.sell(
                f"{base_token}-{quote_token}",
                buy_pool_swap_amount,
                OrderType.MARKET,
                price_1,
                **{
                    "slippage_pct": self._maximum_slippage_percentage,
                    "pool_address": buy_pool.get("address"),
                }
            )

            # Use _order_tracker_fetch_order with built-in retries
            buy_order = self._order_tracker_fetch_order(buy_pool_connector, buy_order_id)
            if buy_order.current_state != OrderState.FILLED:
                raise Exception("First swap failed - order state")

            # Get transaction hash from the order
            buy_tx_hash = buy_order.exchange_order_id
            logger.info(f"First swap signature: {buy_tx_hash}")

            # Wait for transaction confirmation
            buy_pool_swap_confirmation = await self._wait_for_transaction_confirmation(
                buy_pool.get("chain"), buy_pool.get("network"), buy_tx_hash
            )
            if not buy_pool_swap_confirmation:
                raise Exception("First swap transaction not confirmed")

            await asyncio.sleep(self._balance_update_delay)  # Wait for balances to update
            buy_pool_final_balances = await self._gateway_get_balances(
                buy_pool.get("chain"), buy_pool.get("network"), buy_wallet.get("address"), [base_token, quote_token]
            )
            if not buy_pool_final_balances or "balances" not in buy_pool_final_balances:
                raise Exception("Failed to get updated balances for wallet")

            buy_pool_final_base_balance = Decimal(str(buy_pool_final_balances["balances"].get(base_token, 0)))
            buy_pool_final_quote_balance = Decimal(str(buy_pool_final_balances["balances"].get(quote_token, 0)))
            buy_pool_quote_balance_received = buy_pool_final_quote_balance - buy_pool_initial_quote_balance
            sell_pool_swap_amount = (
                buy_pool_quote_balance_received if buy_pool_quote_balance_received > DECIMAL_ZERO else expected_quote
            )

            # Second swap (sell pool): quote_token -> base_token
            logger.info(
                f"Step 2: Swapping {sell_pool_swap_amount} {quote_token} to {base_token} in pool {sell_pool.get('address')}"
            )

            sell_pool_initial_balances = await self._gateway_get_balances(
                sell_pool.get("chain"), sell_pool.get("network"), sell_wallet.get("address"), [base_token, quote_token]
            )
            if not sell_pool_initial_balances or "balances" not in sell_pool_initial_balances:
                raise Exception("Failed to get initial sell wallet balances")

            sell_pool_initial_base_balance = Decimal(str(sell_pool_initial_balances["balances"].get(base_token, 0)))
            sell_pool_initial_quote_balance = Decimal(str(sell_pool_initial_balances["balances"].get(quote_token, 0)))

            # Get the price for the second swap
            price_2 = self._get_token_pair_relative_price_in_pool(sell_pool, quote_token, base_token)
            if price_2 is None or price_2 == DECIMAL_ZERO:
                raise Exception("Could not get price for")

            # Execute sell using the connector's sell method
            sell_pool_connector = self.connectors[f"{sell_pool.get('connector')}/amm_{sell_pool.get('chain')}_{sell_pool.get('network')}"]
            sell_order_id = sell_pool_connector.sell(
                f"{quote_token}-{base_token}",
                sell_pool_swap_amount,
                OrderType.MARKET,
                price_2,
                **{
                    "slippage_pct": self._maximum_slippage_percentage,
                    "pool_address": sell_pool.get("address"),
                }
            )

            # Use _order_tracker_fetch_order with built-in retries
            sell_order = await self._order_tracker_fetch_order(sell_pool_connector, sell_order_id)
            if sell_order.current_state != OrderState.FILLED:
                raise Exception("Second swap failed - order state")

            # Get transaction hash from the order
            sell_tx_hash = sell_order.exchange_order_id
            logger.info(f"Second swap signature: {sell_tx_hash}")

            # Wait for transaction confirmation
            sell_pool_swap_confirmation = await self._wait_for_transaction_confirmation(
                sell_pool.get("chain"), sell_pool.get("network"), sell_tx_hash
            )
            if not sell_pool_swap_confirmation:
                raise Exception("Second swap transaction not confirmed")

            await asyncio.sleep(self._balance_update_delay)  # Wait for balances to update
            sell_pool_final_balances = await self._gateway_get_balances(
                sell_pool.get("chain"), sell_pool.get("network"), sell_wallet.get("address"), [base_token, quote_token]
            )
            if not sell_pool_final_balances or "balances" not in sell_pool_final_balances:
                raise Exception("Failed to get updated sell wallet balance")

            sell_pool_final_base_balance = Decimal(str(sell_pool_final_balances["balances"].get(base_token, 0)))
            sell_pool_final_quote_balance = Decimal(str(sell_pool_final_balances["balances"].get(quote_token, 0)))

            await self._update_token_information()

            profit_information = self.calculate_portfolio_profit(
                {
                    "buy": {
                        "base": {
                            "balance": {
                                "initial": buy_pool_initial_base_balance,
                                "final": buy_pool_final_base_balance,
                            },
                            "price": self._database["connections"][buy_pool.get("chain")][buy_pool.get("network")][
                                buy_pool.get("connector")
                            ]["tokens"][base_token]["price"],
                        },
                        "quote": {
                            "balance": {
                                "initial": buy_pool_initial_quote_balance,
                                "final": buy_pool_final_quote_balance,
                            },
                            "price": self._database["connections"][buy_pool.get("chain")][buy_pool.get("network")][
                                buy_pool.get("connector")
                            ]["tokens"][quote_token]["price"],
                        },
                    },
                    "sell": {
                        "base": {
                            "balance": {
                                "initial": sell_pool_initial_base_balance,
                                "final": sell_pool_final_base_balance,
                            },
                            "price": self._database["connections"][sell_pool.get("chain")][sell_pool.get("network")][
                                sell_pool.get("connector")
                            ]["tokens"][base_token]["price"],
                        },
                        "quote": {
                            "balance": {
                                "initial": sell_pool_initial_quote_balance,
                                "final": sell_pool_final_quote_balance,
                            },
                            "price": self._database["connections"][sell_pool.get("chain")][sell_pool.get("network")][
                                sell_pool.get("connector")
                            ]["tokens"][quote_token]["price"],
                        },
                    },
                }
            )

            trade_record = {
                "timestamp": time.time(),
                "buy_wallet": buy_wallet.get("internal_id"),
                "sell_wallet": sell_wallet.get("internal_id"),
                "base_token": base_token,
                "quote_token": quote_token,
                "buy_pool": buy_pool.get("internal_id"),
                "sell_pool": sell_pool.get("internal_id"),
                "buy_pool_swap_amount": buy_pool_swap_amount,
                "sell_pool_swap_amount": sell_pool_swap_amount,
                "profit": profit_information,
                "buy_pool_swap_transaction_hash": buy_tx_hash,
                "sell_pool_swap_transaction_hash": sell_tx_hash,
            }
            self._database["execution_history"].append(trade_record)

            logger.info("Trade record:", trade_record)

            if profit_information["profit"]["percentage"] > 0:
                logger.info(
                    f"Arbitrage trade successful! Profit: {profit_information['profit']['absolute']} {base_token} ({profit_information['profit']['percentage']:.2f}%)"
                )
                result = True
            else:
                logger.warning(
                    f"Arbitrage trade executed with loss or no profit: {profit_information['profit']['absolute']} {base_token} ({profit_information['profit']['percentage']:.2f}%)"
                )
                result = False

            return result
        except Exception as exception:
            logger.ignore_exception(exception, "Error during arbitrage execution")
            return False
