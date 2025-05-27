"""
AMM Triangular Arbitrage Strategy

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
  - Supports both token pair arbitrage and triangular (token triad) arbitrage.
"""

import asyncio
import logging
import os
import time
from decimal import Decimal
from typing import Any, Dict, List

from hummingbot.core.data_type.common import TradeType
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
)

# ==============================================================================
# Global Configurations for the Strategy
# ==============================================================================
configuration: Dict[str, Any] = {
    "globals": {
        "maximum_slippage_percentage": "0.5",  # 0.5% allowed slippage
        "minimum_profitability_percentage": "1",  # Profit threshold (e.g., "1" for 1%)
        "arbitrage_check_interval_seconds": "60",  # Time between arbitrage checks
        "minimum_trade_amount": "0.1",  # Minimum trade amount
        "maximum_trade_amount": "0.1",  # Maximum trade amount
        "time_delay_between_arbitrages": "1",  # Delay between arbitrage trades
        "transaction_confirmation_delay": "2",  # Delay for transaction confirmation polling
        "balance_update_delay": "3",  # Delay for wallet balance_update_delay"
        "transaction_polling_interval": "2",  # Polling interval for transaction confirmation
        "data_update_intervals": {
            "wallet": "60",  # Update wallet data every x seconds
            "token": "60",  # Update token data every x seconds
            "pool": "60",  # Update pool data every x seconds
        },
        "use_async_data_updates": False,
        "main_quote_token": "USDT",
        "strategy_type": "TOKEN_PAIRS_ARBITRAGE",
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
        }
    },
    "token_pairs": ["USDC/USDT"],
    "token_triads": [],
}

# ==============================================================================
# Logger Initialization
# ==============================================================================
logger = Logger(path="logs/logs_amm_token_pairs_arbitrage.py", level=logging.DEBUG)


# ==============================================================================
# AMMTokenPairsArbitrage Strategy Class
# ==============================================================================
# @logged_class(logger=logger, disallowed_methods=["on_tick"])
class AMMTokenPairsArbitrage(AMMPortfolioManagerBase):
    """
    AMM Token Pairs Arbitrage Strategy

    This strategy monitors AMM pools, discovers arbitrage opportunities
    and executes paired trades between pools with differing prices.
    Uses asynchronous tasks for dynamic data updates.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._strategy_type = StrategyType.TOKEN_PAIRS_ARBITRAGE

    async def _run_token_pairs_arbitrage(self):
        """
        Executes the token pairs arbitrage strategy:
         - Discovers promising token pairs
         - Validates each opportunity
         - Executes arbitrage trades for opportunities that pass validation
        """
        # Finds promising token pairs
        promising_pairs = self._find_most_promising_token_pairs()
        if not promising_pairs:
            logger.info("No promising token pair found.")

            return

        # Searches for arbitrage opportunities in promising pairs
        opportunities = await self._find_arbitrage_opportunities(promising_pairs)
        if not opportunities:
            logger.info("No arbitrage opportunity found.")

            return

        logger.info(f"Found {len(opportunities)} arbitrage opportunities.")

        # Validates and executes each opportunity
        for opportunity in opportunities:
            # Registers the opportunity in the database
            if "arbitrage_opportunities" not in self._database:
                self._database["arbitrage_opportunities"] = []
            self._database["arbitrage_opportunities"].append(opportunity)

            # Validates the opportunity
            logger.info(
                f"Validating opportunity: {opportunity['base_token']}/{opportunity['quote_token']} with difference of {opportunity['price_difference_percentage']:.2f}%"
            )
            valid = await self._validate_opportunity(opportunity)

            if valid:
                # Executes arbitrage if valid
                logger.info(f"Executing arbitrage: {opportunity['base_token']}/{opportunity['quote_token']}")
                success = await self._execute_arbitrage(opportunity)

                if success:
                    logger.info("Arbitrage executed successfully.")
                else:
                    logger.warning("Arbitrage execution failed.")

                # Waits the configured delay between arbitrages
                if self._time_delay_between_arbitrages > 0:
                    await asyncio.sleep(self._time_delay_between_arbitrages)
            else:
                logger.info("Opportunity invalidated after detailed validation.")

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

    async def _find_arbitrage_opportunities(self, promising_pairs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Finds arbitrage opportunities based on promising token pairs.
        Uses portfolio approach to calculate expected profitability.

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
                    price_difference_percentage = await self._calculate_price_difference_percentage(
                        base_token, quote_token, pool_1, pool_2
                    )

                    # Skip if price difference is below threshold or can't be calculated
                    if (
                        price_difference_percentage is None
                        or abs(price_difference_percentage) <= self._minimum_profitability_percentage
                    ):
                        continue

                    # Determine buy and sell pools based on price difference
                    if price_difference_percentage > 0:
                        buy_pool, sell_pool = pool_1, pool_2
                    else:
                        buy_pool, sell_pool = pool_2, pool_1

                    # Initial opportunity record with basic information
                    opportunity = {
                        "base_token": base_token,
                        "quote_token": quote_token,
                        "buy_pool": buy_pool,
                        "sell_pool": sell_pool,
                        "price_difference_percentage": abs(price_difference_percentage),
                        "timestamp": time.time(),
                    }

                    # Run a simulated validation to get a more accurate profitability assessment
                    # This is computationally more expensive but gives more accurate results
                    test_amount = self._minimum_trade_amount

                    # Get wallet addresses for each pool to simulate portfolio value
                    buy_wallets = await self._get_wallets_for_pool(buy_pool)
                    sell_wallets = await self._get_wallets_for_pool(sell_pool)

                    if not buy_wallets or not sell_wallets:
                        logger.info(f"No wallets found for pools - skipping {base_token}/{quote_token}")
                        continue

                    # For simplicity, use first wallet for each pool
                    buy_wallet = buy_wallets[0]
                    sell_wallet = sell_wallets[0]

                    try:
                        # Get initial balances to simulate portfolio value
                        buy_pool_initial_balances = await self._gateway_get_balances(
                            buy_pool.get("chain"),
                            buy_pool.get("network"),
                            buy_wallet.get("address"),
                            [base_token, quote_token],
                        )
                        if not buy_pool_initial_balances or "balances" not in buy_pool_initial_balances:
                            continue

                        sell_pool_initial_balances = await self._gateway_get_balances(
                            sell_pool.get("chain"),
                            sell_pool.get("network"),
                            sell_wallet.get("address"),
                            [base_token, quote_token],
                        )
                        if not sell_pool_initial_balances or "balances" not in sell_pool_initial_balances:
                            continue

                        # Simulates buy: base_token -> quote_token
                        buy_quote = await self._gateway_quote_swap(
                            buy_pool.get("network"),
                            buy_pool.get("connector"),
                            base_token,
                            quote_token,
                            test_amount,
                            TradeType.SELL,
                            self._maximum_slippage_percentage,
                            buy_pool.get("address"),
                        )
                        if not buy_quote or "estimatedAmountOut" not in buy_quote:
                            continue

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
                            continue

                        # Make sure we have the most recent token prices
                        await self._update_token_information()

                        # Get token prices
                        base_token_price_buy = self._database["connections"][buy_pool.get("chain")][
                            buy_pool.get("network")
                        ][buy_pool.get("connector")]["tokens"][base_token]["price"]

                        quote_token_price_buy = self._database["connections"][buy_pool.get("chain")][
                            buy_pool.get("network")
                        ][buy_pool.get("connector")]["tokens"][quote_token]["price"]

                        base_token_price_sell = self._database["connections"][sell_pool.get("chain")][
                            sell_pool.get("network")
                        ][sell_pool.get("connector")]["tokens"][base_token]["price"]

                        quote_token_price_sell = self._database["connections"][sell_pool.get("chain")][
                            sell_pool.get("network")
                        ][sell_pool.get("connector")]["tokens"][quote_token]["price"]

                        # Simulate final balances after transactions
                        buy_pool_initial_base_balance = Decimal(
                            str(buy_pool_initial_balances["balances"].get(base_token, 0))
                        )
                        buy_pool_initial_quote_balance = Decimal(
                            str(buy_pool_initial_balances["balances"].get(quote_token, 0))
                        )
                        sell_pool_initial_base_balance = Decimal(
                            str(sell_pool_initial_balances["balances"].get(base_token, 0))
                        )
                        sell_pool_initial_quote_balance = Decimal(
                            str(sell_pool_initial_balances["balances"].get(quote_token, 0))
                        )

                        # Simulate buy transaction effect
                        buy_pool_final_base_balance = buy_pool_initial_base_balance - test_amount
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

                        # Add simulated profitability to opportunity record
                        opportunity.update(
                            {
                                "simulated_profit_percentage": profit_information["profit"]["percentage"],
                                "simulated_profit_absolute": profit_information["profit"]["absolute"],
                            }
                        )

                        # Only add if expected to be profitable
                        if profit_information["profit"]["percentage"] > self._minimum_profitability_percentage:
                            opportunities.append(opportunity)
                            logger.info(
                                f"Arbitrage opportunity found: {base_token}/{quote_token} "
                                f"difference {abs(price_difference_percentage):.2f}% between {buy_pool.get('internal_id')} and {sell_pool.get('internal_id')} "
                                f"with expected profit {profit_information['profit']['percentage']:.2f}%"
                            )
                    except Exception as exception:
                        logger.ignore_exception(exception, f"Error simulating arbitrage for {base_token}/{quote_token}")
                        continue

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
            logger.info(f"Insufficient balance of {base_token}: {available_balance}")

            return False

        # Calculates ideal trade amount
        trade_amount = await self._calculate_optimal_trade_amount(opportunity, available_balance)
        if not trade_amount or trade_amount <= DECIMAL_ZERO:
            logger.info(f"Invalid optimal trade amount: {trade_amount}")

            return False

        try:
            # Get wallet addresses for each pool to simulate portfolio value
            buy_wallets = await self._get_wallets_for_pool(buy_pool)
            sell_wallets = await self._get_wallets_for_pool(sell_pool)
            if not buy_wallets or not sell_wallets:
                logger.info("No wallets found for pools")
                return False

            # For simplicity, use first wallet for each pool
            buy_wallet = buy_wallets[0]
            sell_wallet = sell_wallets[0]

            # Get initial balances to calculate initial portfolio value
            buy_pool_initial_balances = await self._gateway_get_balances(
                buy_pool.get("chain"), buy_pool.get("network"), buy_wallet.get("address"), [base_token, quote_token]
            )
            if not buy_pool_initial_balances or "balances" not in buy_pool_initial_balances:
                logger.error(f"Failed to get initial balances for wallet {buy_wallet.get('internal_id')}")
                return False

            sell_pool_initial_balances = await self._gateway_get_balances(
                sell_pool.get("chain"), sell_pool.get("network"), sell_wallet.get("address"), [base_token, quote_token]
            )
            if not sell_pool_initial_balances or "balances" not in sell_pool_initial_balances:
                logger.error(f"Failed to get initial sell wallet balances for {sell_wallet.get('internal_id')}")
                return False

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
            logger.error(f"No wallet found for buy pool {buy_pool.get('address')}")

            return False
        if not sell_wallets:
            logger.error(f"No wallet found for sell pool {sell_pool.get('address')}")

            return False

        # TODO: Support multiple wallets per pool.
        # For now, we only support one wallet per pool.
        buy_wallet = buy_wallets[0]
        sell_wallet = sell_wallets[0]

        try:
            # First swap (buy pool): base_token -> quote_token.
            logger.info(
                f"Step 1: Swapping {buy_pool_swap_amount} {base_token} for {quote_token} in pool {buy_pool.get('address')}"
            )
            buy_pool_initial_balances = await self._gateway_get_balances(
                buy_pool.get("chain"), buy_pool.get("network"), buy_wallet.get("address"), [base_token, quote_token]
            )
            if not buy_pool_initial_balances or "balances" not in buy_pool_initial_balances:
                logger.error(f"Failed to get initial balances for wallet {buy_wallet.get('internal_id')}")

                return False

            buy_pool_initial_base_balance = Decimal(str(buy_pool_initial_balances["balances"].get(base_token, 0)))
            buy_pool_initial_quote_balance = Decimal(str(buy_pool_initial_balances["balances"].get(quote_token, 0)))
            if buy_pool_initial_base_balance < buy_pool_swap_amount:
                logger.info(
                    f"Insufficient balance in {buy_wallet.get('internal_id')}: {buy_pool_initial_base_balance} {base_token}"
                )

                return False

            buy_pool_swap = await self._gateway_execute_swap(
                buy_pool.get("network"),
                buy_pool.get("connector"),
                buy_wallet.get("address"),
                base_token,
                quote_token,
                TradeType.SELL,
                buy_pool_swap_amount,
                self._maximum_slippage_percentage,
                buy_pool.get("address"),
            )
            if not buy_pool_swap or "signature" not in buy_pool_swap:
                logger.error(f"First swap failed in wallet {buy_wallet.get('internal_id')}")

                return False

            logger.info(f"First swap signature: {buy_pool_swap['signature']}")

            buy_pool_swap_confirmation = await self._wait_for_transaction_confirmation(
                buy_pool.get("chain"), buy_pool.get("network"), buy_pool_swap["signature"]
            )
            if not buy_pool_swap_confirmation:
                logger.error("First swap transaction not confirmed")

                return False

            await asyncio.sleep(3)  # Wait some seconds to try to retrieve the updated balance
            buy_pool_final_balances = await self._gateway_get_balances(
                buy_pool.get("chain"), buy_pool.get("network"), buy_wallet.get("address"), [base_token, quote_token]
            )
            if not buy_pool_final_balances or "balances" not in buy_pool_final_balances:
                logger.error(f"Failed to get updated balances for wallet {buy_wallet.get('internal_id')}")

                return False

            buy_pool_final_base_balance = Decimal(str(buy_pool_final_balances["balances"].get(base_token, 0)))
            buy_pool_final_quote_balance = Decimal(str(buy_pool_final_balances["balances"].get(quote_token, 0)))
            buy_pool_quote_balance_received = buy_pool_final_quote_balance - buy_pool_initial_quote_balance
            sell_pool_swap_amount = (
                buy_pool_quote_balance_received if buy_pool_quote_balance_received > DECIMAL_ZERO else expected_quote
            )
            if buy_pool_quote_balance_received <= 0:
                logger.warning(f"Actual quote received undetermined; using expected: {expected_quote} {quote_token}")

            # Second swap (sell pool): quote_token -> base_token.
            logger.info(
                f"Step 2: Swapping {sell_pool_swap_amount} {quote_token} to {base_token} in pool {sell_pool.get('address')}"
            )

            await asyncio.sleep(3)  # Wait some seconds to try to retrieve the updated balance
            sell_pool_initial_balances = await self._gateway_get_balances(
                sell_pool.get("chain"), sell_pool.get("network"), sell_wallet.get("address"), [base_token, quote_token]
            )
            if not sell_pool_initial_balances or "balances" not in sell_pool_initial_balances:
                logger.error(f"Failed to get initial sell wallet balances for {sell_wallet.get('internal_id')}")

                return False

            sell_pool_initial_base_balance = Decimal(str(sell_pool_initial_balances["balances"].get(base_token, 0)))
            sell_pool_initial_quote_balance = Decimal(str(sell_pool_initial_balances["balances"].get(quote_token, 0)))
            sell_pool_swap = await self._gateway_execute_swap(
                sell_pool.get("network"),
                sell_pool.get("connector"),
                sell_wallet.get("address"),
                quote_token,
                base_token,
                TradeType.SELL,
                sell_pool_swap_amount,
                self._maximum_slippage_percentage,
                sell_pool.get("address"),
            )
            if not sell_pool_swap or "signature" not in sell_pool_swap:
                logger.error(f"Second swap failed in wallet {sell_wallet.get('address')}")

                return False

            logger.info(f"Second swap signature: {sell_pool_swap['signature']}")
            sell_pool_swap_confirmation = await self._wait_for_transaction_confirmation(
                sell_pool.get("chain"), sell_pool.get("network"), sell_pool_swap["signature"]
            )
            if not sell_pool_swap_confirmation:
                logger.error("Second swap transaction not confirmed")

                return False

            await asyncio.sleep(3)  # Wait some seconds to try to retrieve the updated balance
            sell_pool_final_balances = await self._gateway_get_balances(
                sell_pool.get("chain"), sell_pool.get("network"), sell_wallet.get("address"), [base_token, quote_token]
            )
            if not sell_pool_final_balances or "balances" not in sell_pool_final_balances:
                logger.error(f"Failed to get updated sell wallet balance for {sell_wallet.get('address')}")

                return False

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
                "buy_pool_swap_transaction_hash": buy_pool_swap["signature"],
                "sell_pool_swap_transaction_hash": sell_pool_swap["signature"],
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
