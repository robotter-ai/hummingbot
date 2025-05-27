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
        "strategy_type": "TOKEN_TRIADS_ARBITRAGE",
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
    "token_pairs": [],
    "token_triads": ["USDC/HDX/USDT"],
}

# ==============================================================================
# Logger Initialization
# ==============================================================================
logger = Logger(path="logs/logs_amm_triangular_arbitrage.py", level=logging.DEBUG)


# ==============================================================================
# AMMTriangularArbitrage Strategy Class
# ==============================================================================
# @logged_class(logger=logger, disallowed_methods=["on_tick"])
class AMMTriangularArbitrage(AMMPortfolioManagerBase):
    """
    AMM Triangular Arbitrage Strategy

    This strategy monitors AMM pools, discovers arbitrage opportunities
    and executes paired trades between pools with differing prices.
    Uses asynchronous tasks for dynamic data updates.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._strategy_type = StrategyType.TOKEN_TRIADS_ARBITRAGE

    async def _run_token_triads_arbitrage(self):
        """
        Executes the token triads (triangular) arbitrage strategy:
         - Discovers profitable triangular arbitrage opportunities
         - Validates each opportunity
         - Executes arbitrage trades for opportunities that pass validation
        """
        # Find profitable token triads
        opportunities = await self._find_triangular_arbitrage_opportunities()
        if not opportunities:
            logger.info("No triangular arbitrage opportunity found.")

            return

        logger.info(f"Found {len(opportunities)} triangular arbitrage opportunities.")

        # Validates and executes each opportunity
        for opportunity in opportunities:
            # Registers the opportunity in the database
            if "arbitrage_opportunities" not in self._database:
                self._database["arbitrage_opportunities"] = []
            self._database["arbitrage_opportunities"].append(opportunity)

            # Validates the opportunity
            logger.info(
                f"Validating triangular opportunity: {opportunity['token1']}->{opportunity['token2']}->{opportunity['token3']}->{opportunity['token1']} "
                f"with expected profit of {opportunity['expected_profit_percentage']:.2f}%"
            )
            valid = await self._validate_triangular_opportunity(opportunity)

            if valid:
                # Executes arbitrage if valid
                logger.info(f"Executing triangular arbitrage: {opportunity['token1']}->{opportunity['token2']}->{opportunity['token3']}->{opportunity['token1']}")
                success = await self._execute_triangular_arbitrage(opportunity)

                if success:
                    logger.info("Triangular arbitrage executed successfully.")
                else:
                    logger.warning("Triangular arbitrage execution failed.")

                # Waits the configured delay between arbitrages
                if self._time_delay_between_arbitrages > 0:
                    await asyncio.sleep(self._time_delay_between_arbitrages)
            else:
                logger.info("Triangular opportunity invalidated after detailed validation.")

    async def _find_triangular_arbitrage_opportunities(self) -> List[Dict[str, Any]]:
        """
        Finds triangular arbitrage opportunities based on token triads.
        Uses direct quotes instead of relying on pool information.

        Returns:
            List of dictionaries of triangular arbitrage opportunities.
        """
        opportunities = []

        for triad in self._token_triads:
            token1, token2, token3 = self._extract_token_symbols_from_token_triad(triad)

            try:
                # For each triad, we need to check if there's a profitable circular path
                # Get quotes for all three legs of the triangle
                for chain_name, chain_configuration in self._database["connections"].items():
                    for network_name, network_configuration in chain_configuration.items():
                        for connector_name in network_configuration.keys():
                            # Simulate the triangular trade with minimum amount
                            expected_profit = await self._simulate_triangular_trade(
                                chain_name, network_name, connector_name,
                                token1, token2, token3,
                                self._minimum_trade_amount
                            )

                            if expected_profit["profit_percentage"] <= self._minimum_profitability_percentage:
                                continue

                            # Create opportunity record
                            opportunity = {
                                "type": "triangular",
                                "token1": token1,
                                "token2": token2,
                                "token3": token3,
                                "chain": chain_name,
                                "network": network_name,
                                "connector": connector_name,
                                "expected_profit_amount": expected_profit["profit_amount"],
                                "expected_profit_percentage": expected_profit["profit_percentage"],
                                "expected_fees_cost": expected_profit["fees_cost"],
                                "expected_fees_cost_in_token1": expected_profit["fees_cost_in_token1"],
                                "timestamp": time.time(),
                            }

                            opportunities.append(opportunity)

                            logger.info(
                                f"Triangular arbitrage opportunity found: {token1}->{token2}->{token3}->{token1} "
                                f"on {chain_name}/{network_name}/{connector_name} with expected profit {expected_profit['profit_percentage']:.2f}% after fees"
                            )
            except Exception as exception:
                logger.ignore_exception(
                    exception,
                    f"Error evaluating triangular arbitrage for {token1}/{token2}/{token3}"
                )

                continue

        # Sort opportunities by expected profit (descending)
        opportunities.sort(key=lambda opportunity: opportunity["expected_profit_percentage"], reverse=True)

        return opportunities

    async def _simulate_triangular_trade(
        self,
        _chain: str,
        network: str,
        connector: str,
        token1: str,
        token2: str,
        token3: str,
        amount: Decimal
    ) -> Dict[str, Any]:
        """
        Simulates a triangular trade using direct swaps without requiring pool information.

        Args:
            _chain: Blockchain chain
            network: Network name
            connector: Connector name
            token1: First token in the triangle
            token2: Second token in the triangle
            token3: Third token in the triangle
            amount: Initial amount of token1 to trade

        Returns:
            Dictionary with profit information
        """
        try:
            connector_configuration = self._database["connections"].get(_chain, {}).get(network, {}).get(connector, {})
            fee_payment_token_symbol = connector_configuration.get("fee_payment_token_symbol")

            fees_cost = DECIMAL_ZERO

            # Simulate first swap: token1 -> token2
            swap1_quote = await self._gateway_quote_swap(
                network,
                connector,
                token1,
                token2,
                amount,
                TradeType.SELL,
                self._maximum_slippage_percentage
            )

            if not swap1_quote or "estimatedAmountOut" not in swap1_quote:
                return {"profit_amount": DECIMAL_ZERO, "profit_percentage": DECIMAL_ZERO}

            token2_amount = Decimal(str(swap1_quote["estimatedAmountOut"]))
            if "gasCost" in swap1_quote:
                fees_cost += Decimal(str(swap1_quote["gasCost"]))

            # Simulate second swap: token2 -> token3
            swap2_quote = await self._gateway_quote_swap(
                network,
                connector,
                token2,
                token3,
                token2_amount,
                TradeType.SELL,
                self._maximum_slippage_percentage
            )

            if not swap2_quote or "estimatedAmountOut" not in swap2_quote:
                return {"profit_amount": DECIMAL_ZERO, "profit_percentage": DECIMAL_ZERO}

            token3_amount = Decimal(str(swap2_quote["estimatedAmountOut"]))
            if "gasCost" in swap2_quote:
                fees_cost += Decimal(str(swap2_quote["gasCost"]))

            # Simulate third swap: token3 -> token1
            swap3_quote = await self._gateway_quote_swap(
                network,
                connector,
                token3,
                token1,
                token3_amount,
                TradeType.SELL,
                self._maximum_slippage_percentage
            )

            if not swap3_quote or "estimatedAmountOut" not in swap3_quote:
                return {"profit_amount": DECIMAL_ZERO, "profit_percentage": DECIMAL_ZERO}

            final_token1_amount = Decimal(str(swap3_quote["estimatedAmountOut"]))
            if "gasCost" in swap3_quote:
                fees_cost += Decimal(str(swap3_quote["gasCost"]))

            # Convert fees to token1 value if fee payment token is different from token1
            fees_cost_in_token1 = DECIMAL_ZERO
            if fees_cost > DECIMAL_ZERO:
                if fee_payment_token_symbol and fee_payment_token_symbol != token1:
                    fees_quote_swap = await self._gateway_quote_swap(
                        network,
                        connector,
                        fee_payment_token_symbol,
                        token1,
                        fees_cost,
                        TradeType.SELL,
                        self._maximum_slippage_percentage,
                        None
                    )

                    if fees_quote_swap and "estimatedAmountOut" in fees_quote_swap:
                        fees_cost_in_token1 = Decimal(str(fees_quote_swap["estimatedAmountOut"]))
                    else:
                        raise Exception(f"Failed to get quote for {fee_payment_token_symbol} to {token1}")
                else:
                    fees_cost_in_token1 = fees_cost  # Fee token is already token1

            # Calculate profit accounting for fees
            profit_amount = final_token1_amount - amount - fees_cost_in_token1
            profit_percentage = (profit_amount / amount) * DECIMAL_ONE_HUNDRED

            return {
                "profit_amount": profit_amount,
                "profit_percentage": profit_percentage,
                "initial_amount": amount,
                "token2_amount": token2_amount,
                "token3_amount": token3_amount,
                "final_amount": final_token1_amount,
                "fees_cost": fees_cost,
                "fees_cost_in_token1": fees_cost_in_token1
            }

        except Exception as exception:
            logger.ignore_exception(exception, "Error in triangular trade simulation")

            return {"profit_amount": DECIMAL_ZERO, "profit_percentage": DECIMAL_ZERO}

    async def _validate_triangular_opportunity(self, opportunity: Dict[str, Any]) -> bool:
        """
        Validates a triangular arbitrage opportunity by checking available balances and
        simulating trades with optimal amounts.

        Args:
            opportunity: Dictionary of triangular arbitrage opportunity.
        Returns:
            True if opportunity is valid and expected profit meets threshold;
            otherwise, False.
        """
        chain = opportunity["chain"]
        network = opportunity["network"]
        connector = opportunity["connector"]

        # Checks if there is available balance for token1
        available_balance = await self._get_total_token_balance_from_all_wallets(opportunity["token1"])
        if available_balance < self._minimum_trade_amount:
            logger.info(f"Insufficient balance of {opportunity["token1"]}: {available_balance}")

            return False

        # Calculates ideal trade amount
        trade_amount = await self._calculate_optimal_triangular_trade_amount(
            opportunity, available_balance
        )

        if not trade_amount or trade_amount <= DECIMAL_ZERO:
            logger.info(f"Invalid optimal trade amount: {trade_amount}")

            return False

        # Simulate the triangular trade with the optimal amount
        expected_profit = await self._simulate_triangular_trade(
            chain,
            network,
            connector,
            opportunity["token1"],
            opportunity["token2"],
            opportunity["token3"],
            trade_amount
        )

        # Checks if opportunity meets minimum profitability
        if expected_profit["profit_percentage"] < self._minimum_profitability_percentage:
            logger.info(
                f"Triangular opportunity not profitable: "
                f"{expected_profit['profit_percentage']:.2f}% < {self._minimum_profitability_percentage}%"
            )

            return False

        # Update opportunity with calculated values
        opportunity.update({
            "trade_amount": trade_amount,
            "expected_token2_amount": expected_profit["token2_amount"],
            "expected_token3_amount": expected_profit["token3_amount"],
            "expected_final_amount": expected_profit["final_amount"],
            "expected_profit_amount": expected_profit["profit_amount"],
            "expected_profit_percentage": expected_profit["profit_percentage"],
            "expected_fees_cost": expected_profit["fees_cost"],
            "expected_fees_cost_in_token1": expected_profit["fees_cost_in_token1"]
        })

        logger.info(
            f"Triangular opportunity validated: {opportunity["token1"]}->{opportunity["token2"]}->{opportunity["token3"]}->{opportunity["token1"]} "
            f"with expected profit {expected_profit['profit_percentage']:.2f}% after fees"
        )

        return True

    async def _calculate_optimal_triangular_trade_amount(self, opportunity: Dict[str, Any], maximum_available: Decimal) -> Decimal:
        """
        Determines the optimal trade amount for triangular arbitrage by simulating profits at different sizes.

        Args:
            opportunity: Triangular arbitrage opportunity dictionary.
            maximum_available: Maximum available balance of token1.
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

            # Simulate triangular trade with different amounts
            profit_info = await self._simulate_triangular_trade(
                opportunity["chain"],
                opportunity["network"],
                opportunity["connector"],
                opportunity["token1"],
                opportunity["token2"],
                opportunity["token3"],
                amount
            )

            if profit_info["profit_percentage"] > best_profit_percentage:
                best_profit_percentage = profit_info["profit_percentage"]
                best_amount = amount

        return best_amount if best_amount > DECIMAL_ZERO else self._minimum_trade_amount

    async def _execute_triangular_arbitrage(self, opportunity: Dict[str, Any]) -> bool:
        """
        Executes a triangular arbitrage trade by performing three sequential swaps.

        Args:
            opportunity: Triangular arbitrage opportunity dictionary.
        Returns:
            True if the trade is executed successfully with profit; otherwise, False.
        """
        token1 = opportunity["token1"]
        token2 = opportunity["token2"]
        token3 = opportunity["token3"]
        chain = opportunity["chain"]
        network = opportunity["network"]
        connector = opportunity["connector"]
        token1_amount = opportunity["trade_amount"]
        connector_configuration = self._database["connections"][chain][network][connector]
        fee_payment_token_symbol = connector_configuration.get("fee_payment_token_symbol")

        logger.info(f"Executing triangular arbitrage: {token1}->{token2}->{token3}->{token1}")

        # Find wallet for the chain/network/connector
        wallet = await self._get_wallet_for_chain_network_connector(chain, network, connector)
        if not wallet:
            logger.error(f"No wallet found for {chain}/{network}/{connector}")

            return False

        wallet_address = wallet.get("address")

        try:
            # Get initial balance to calculate profit later
            initial_balances = await self._gateway_get_balances(
                chain, network, wallet_address, [token1, token2, token3]
            )
            if not initial_balances or "balances" not in initial_balances:
                logger.error(f"Failed to get initial balances for wallet {wallet_address}")

                return False

            initial_token1_balance = Decimal(str(initial_balances["balances"].get(token1, 0)))
            initial_token2_balance = Decimal(str(initial_balances["balances"].get(token2, 0)))
            initial_token3_balance = Decimal(str(initial_balances["balances"].get(token3, 0)))

            if initial_token1_balance < token1_amount:
                logger.info(f"Insufficient balance in wallet {wallet_address}: {initial_token1_balance} {token1}")

                return False

            fees_cost = DECIMAL_ZERO

            # Step 1: Swap token1 -> token2
            logger.info(f"Step 1: Swapping {token1_amount} {token1} for {token2}")
            swap1 = await self._gateway_execute_swap(
                network,
                connector,
                wallet_address,
                token1,
                token2,
                TradeType.SELL,
                token1_amount,
                self._maximum_slippage_percentage,
                None  # No specific pool address needed
            )

            if not swap1 or "signature" not in swap1:
                logger.error(f"First swap failed for wallet {wallet_address}")

                return False

            swap1_confirmation = await self._wait_for_transaction_confirmation(
                chain, network, swap1["signature"]
            )
            if not swap1_confirmation:
                logger.error("First swap transaction not confirmed")

                return False

            # await asyncio.sleep(self._balance_update_delay)  # Wait for balance update
            #
            # # Get updated balances to determine the amount received
            # intermediate_balances = await self._gateway_get_balances(
            #     chain, network, wallet_address, [token1, token2, token3]
            # )
            # if not intermediate_balances or "balances" not in intermediate_balances:
            #     logger.error(f"Failed to get updated balances for wallet {wallet_address}")
            #
            #     return False
            #
            # received_token2_amount = Decimal(str(intermediate_balances["balances"].get(token2, 0)))
            # token2_amount = received_token2_amount if received_token2_amount > DECIMAL_ZERO else opportunity.get("expected_token2_amount")

            token2_amount = Decimal(swap1.get("totalOutputSwapped"))
            fees_cost += Decimal(swap1.get("fee", 0))

            # Step 2: Swap token2 -> token3
            logger.info(f"Step 2: Swapping {token2_amount} {token2} for {token3}")
            swap2 = await self._gateway_execute_swap(
                network,
                connector,
                wallet_address,
                token2,
                token3,
                TradeType.SELL,
                token2_amount,
                self._maximum_slippage_percentage,
                None  # No specific pool address needed
            )

            if not swap2 or "signature" not in swap2:
                logger.error(f"Second swap failed for wallet {wallet_address}")

                return False

            swap2_confirmation = await self._wait_for_transaction_confirmation(
                chain, network, swap2["signature"]
            )
            if not swap2_confirmation:
                logger.error("Second swap transaction not confirmed")

                return False

            # await asyncio.sleep(self._balance_update_delay)  # Wait for balance update
            #
            # # Get updated balances to determine the amount received
            # intermediate_balances2 = await self._gateway_get_balances(
            #     chain, network, wallet_address, [token1, token2, token3]
            # )
            # if not intermediate_balances2 or "balances" not in intermediate_balances2:
            #     logger.error(f"Failed to get updated balances for wallet {wallet_address}")
            #
            #     return False
            #
            # received_token3_amount = Decimal(str(intermediate_balances2["balances"].get(token3, 0)))
            # token3_amount = received_token3_amount if received_token3_amount > DECIMAL_ZERO else opportunity.get("expected_token3_amount")

            token3_amount = Decimal(swap2.get("totalOutputSwapped"))
            fees_cost += Decimal(swap2.get("fee", 0))

            # Step 3: Swap token3 -> token1
            logger.info(f"Step 3: Swapping {token3_amount} {token3} for {token1}")
            swap3 = await self._gateway_execute_swap(
                network,
                connector,
                wallet_address,
                token3,
                token1,
                TradeType.SELL,
                token3_amount,
                self._maximum_slippage_percentage,
                None  # No specific pool address needed
            )

            if not swap3 or "signature" not in swap3:
                logger.error(f"Third swap failed for wallet {wallet_address}")

                return False

            swap3_confirmation = await self._wait_for_transaction_confirmation(
                chain, network, swap3["signature"]
            )
            if not swap3_confirmation:
                logger.error("Third swap transaction not confirmed")

                return False

            fees_cost += Decimal(swap3.get("fee", 0))

            fees_cost_in_token1 = DECIMAL_ZERO
            if fees_cost > DECIMAL_ZERO:
                if fee_payment_token_symbol and fee_payment_token_symbol != token1:
                    fees_quote_swap = await self._gateway_quote_swap(
                        network,
                        connector,
                        fee_payment_token_symbol,
                        token1,
                        fees_cost,
                        TradeType.SELL,
                        self._maximum_slippage_percentage,
                        None
                    )

                    if fees_quote_swap and "estimatedAmountOut" in fees_quote_swap:
                        fees_cost_in_token1 = Decimal(str(fees_quote_swap["estimatedAmountOut"]))
                    else:
                        raise Exception(f"Failed to get quote for {fee_payment_token_symbol} to {token1}")
                else:
                    fees_cost_in_token1 = fees_cost  # Fee token is already token1

            await asyncio.sleep(self._balance_update_delay)  # Wait for balance update

            # Get final balances to determine profit
            final_balances = await self._gateway_get_balances(
                chain, network, wallet_address, [token1, token2, token3]
            )
            if not final_balances or "balances" not in final_balances:
                logger.error(f"Failed to get updated balances for wallet {wallet_address}")

                return False

            final_token1_balance = Decimal(str(final_balances["balances"].get(token1, 0)))
            final_token2_balance = Decimal(str(final_balances["balances"].get(token2, 0)))
            final_token3_balance = Decimal(str(final_balances["balances"].get(token3, 0)))

            # Calculate actual profit
            actual_profit = final_token1_balance - initial_token1_balance - fees_cost_in_token1
            actual_profit_percentage = (actual_profit / token1_amount) * DECIMAL_ONE_HUNDRED

            # Record trade execution
            trade_record = {
                "type": "triangular",
                "timestamp": time.time(),
                "token1": token1,
                "token2": token2,
                "token3": token3,
                "wallet_address": wallet_address,
                "chain": chain,
                "network": network,
                "connector": connector,
                "token1_amount": token1_amount,
                "token2_amount": token2_amount,
                "token3_amount": token3_amount,
                "initial_token1_balance": initial_token1_balance,
                "initial_token2_balance": initial_token2_balance,
                "initial_token3_balance": initial_token3_balance,
                "final_token1_balance": final_token1_balance,
                "final_token2_balance": final_token2_balance,
                "final_token3_balance": final_token3_balance,
                "token_1_balance_change": final_token1_balance - initial_token1_balance,
                "token_2_balance_change": final_token2_balance - initial_token2_balance,
                "token_3_balance_change": final_token3_balance - initial_token3_balance,
                "profit_amount": actual_profit,
                "profit_percentage": actual_profit_percentage,
                "swap1_transaction_hash": swap1["signature"],
                "swap2_transaction_hash": swap2["signature"],
                "swap3_transaction_hash": swap3["signature"],
                "total_fees_cost": fees_cost,
                "fees_cost_in_token1": fees_cost_in_token1,
            }

            # Store trade record in database
            if "execution_history" not in self._database:
                self._database["execution_history"] = []
            self._database["execution_history"].append(trade_record)

            logger.info("Trade record:", trade_record)

            if actual_profit_percentage > 0:
                logger.info(
                    f"Triangular arbitrage successful! Profit: {actual_profit} {token1} ({actual_profit_percentage:.2f}%)"
                )

                result = True
            else:
                logger.warning(
                    f"Triangular arbitrage executed with loss or no profit: {actual_profit} {token1} ({actual_profit_percentage:.2f}%)"
                )

                result = False

            return result
        except Exception as exception:
            logger.ignore_exception(exception, "Error during triangular arbitrage execution")

            return False
