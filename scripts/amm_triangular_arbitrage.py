"""
AMM Triangular Arbitrage Strategy

This module implements a triangular arbitrage strategy that monitors liquidity pools
within a single connector (DEX/AMM), identifies price discrepancies between three related
tokens, and executes a series of trades to profit from these discrepancies.

Key Features:
  - Identifies and executes profitable triangular trading paths (token1->token2->token3->token1)
  - Calculates optimal trade amounts to maximize arbitrage profits while managing risk
  - Simulates trades with slippage consideration before execution
  - Tracks and validates arbitrage opportunities before execution
  - Handles transaction confirmation and profit calculation
  - Asynchronous periodic updates for wallet balances, token prices, and pool statistics
  - Comprehensive error handling and trade validation
  - Portfolio-based profit calculation for accurate profitability assessment
  - Configurable parameters for slippage, minimum profitability, trade amounts, and delays
  - Detailed logging and monitoring of trade execution and performance
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
# Global Configurations for the Triangular Arbitrage Strategy
# ==============================================================================
configuration: Dict[str, Any] = {
    "globals": {
        "maximum_slippage_percentage": "0.5",  # Maximum allowed price slippage when executing trades (Ex.: 0.5 for 0.5%)
        "minimum_profitability_percentage": "-30",  # Minimum profit threshold required to execute an arbitrage (Ex.: 1 for 1%)
        "arbitrage_check_interval_seconds": "60",  # Time interval between subsequent arbitrage opportunity checks
        "minimum_trade_amount": "0.1",  # Minimum amount to trade in the first token of the triangle
        "maximum_trade_amount": "0.2",  # Maximum amount to trade in the first token of the triangle
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
        "strategy_type": StrategyType.TRIANGULAR_ARBITRAGE,  # Strategy type identifier for triangular arbitrage
        "rate_source": "custom",  # Use our custom rate source for HDX price
    },
    "connections": {
        "polkadot": {
            "mainnet": {
                "hydration": {
                    "native_token_symbol": "HDX",
                    "fee_payment_token_symbol": "HDX",
                    "fee_payment_token_amount": "0.5",
                    "wallets": [
                        os.environ["POLKADOT_MAINNET_HYDRATION_WALLET_ADDRESS"]
                    ],
                    "pools": [],
                }
            },
        }
    },
    "token_pairs": [],
    "token_triads": [
        "USDC/HDX/USDT",
    ],
}

# ==============================================================================
# Logger Initialization
# ==============================================================================
logger = Logger(path="logs/logs_amm_triangular_arbitrage.log", level=logging.DEBUG)


# ==============================================================================
# AMMTriangularArbitrage Strategy Class
# ==============================================================================
@logged_class(logger=logger, disallowed_methods=["on_tick"])
class AMMTriangularArbitrage(AMMPortfolioManagerBase):
    """
    AMM Triangular Arbitrage Strategy

    This strategy monitors AMM pools within a single connector to identify and exploit
    price discrepancies between three related tokens (token triads). It executes a
    sequence of three trades: token1 → token2 → token3 → token1, to profit from
    these discrepancies.

    The strategy works by:
    1. Monitoring configured token triads (e.g., USDC/HDX/USDT) within a connector
    2. Identifying price discrepancies that create profitable trading cycles
    3. Simulating trades to calculate expected profits and fees
    4. Validating opportunities by checking balances and simulating trades
    5. Executing trades when opportunities are validated and profitable
    6. Managing the portfolio to optimize returns

    The strategy uses a portfolio-based approach to calculate profitability, taking into
    account:
    - Token balances and prices
    - Expected trade amounts and slippage
    - Transaction costs and fees
    - Market impact and liquidity constraints
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

        self._strategy_type = StrategyType.TRIANGULAR_ARBITRAGE

    async def _run_triangular_arbitrage(self):
        """
        Executes the triangular arbitrage strategy by following these steps:
        1. Discovers profitable triangular arbitrage opportunities (token1->token2->token3->token1)
        2. Registers found opportunities in the database for tracking
        3. Validates each opportunity by checking balances and simulating trades
        4. Executes trades for validated opportunities using the optimal amount
        5. Implements configurable delays between arbitrage executions

        The strategy targets opportunities where the final amount exceeds the initial amount plus fees,
        resulting in a profitable cycle of trades.
        """
        # Find profitable token triads
        opportunities = await self._find_arbitrage_opportunities()
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
            valid = await self._validate_opportunity(opportunity)

            if valid:
                # Executes arbitrage if valid
                logger.info(f"Executing triangular arbitrage: {opportunity['token1']}->{opportunity['token2']}->{opportunity['token3']}->{opportunity['token1']}")
                success = await self._execute_arbitrage(opportunity)

                if success:
                    logger.info("Arbitrage successfully collected profit.")
                else:
                    logger.warning("Arbitrage incurred in loss or no profit.")

                # Waits the configured delay between arbitrages
                if self._time_delay_between_arbitrages > 0:
                    await asyncio.sleep(self._time_delay_between_arbitrages)
            else:
                logger.info("Triangular opportunity invalidated after detailed validation.")

    async def _run_connectors_arbitrage(self):
        pass

    async def _find_arbitrage_opportunities(self) -> List[Dict[str, Any]]:
        """
        Discovers triangular arbitrage opportunities by analyzing token triads.

        For each configured triad (e.g., USDC/HDX/USDT), the method:
        1. Extracts the three token symbols in the triad
        2. Simulates the three sequential trades (token1->token2->token3->token1)
        3. Calculates expected profit after accounting for fees
        4. Records opportunities exceeding the minimum profitability threshold

        Returns:
            List of dictionaries containing details of triangular arbitrage opportunities,
            sorted by expected profit percentage in descending order.
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
                            expected_profit = await self._simulate_arbitrage_trade(
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
                                "price_token1_to_token2": expected_profit["price_token1_to_token2"],
                                "price_token2_to_token3": expected_profit["price_token2_to_token3"],
                                "price_token3_to_token1": expected_profit["price_token3_to_token1"],
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

    async def _simulate_arbitrage_trade(
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
        Simulates a complete triangular trade cycle to estimate profitability.

        This method performs these steps:
        1. Simulates swap from token1 to token2 with the input amount
        2. Simulates swap from token2 to token3 with the result of the first swap
        3. Simulates swap from token3 back to token1 with the result of the second swap
        4. Calculates fees for all three swaps and converts them to token1 value
        5. Computes the final profit amount and percentage after accounting for all fees

        Args:
            _chain: Blockchain chain identifier
            network: Network name (e.g., "mainnet")
            connector: Protocol connector name (e.g., "hydration")
            token1: First token in the triangular path
            token2: Second token in the triangular path
            token3: Third token in the triangular path
            amount: Initial amount of token1 to trade

        Returns:
            Dictionary containing profit information including:
            - profit_amount: Absolute profit amount in token1
            - profit_percentage: Percentage profit relative to initial amount
            - intermediate amounts and fee details
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
                "fees_cost_in_token1": fees_cost_in_token1,
                "price_token1_to_token2": Decimal(str(swap1_quote["price"])) if "price" in swap1_quote else DECIMAL_ZERO,
                "price_token2_to_token3": Decimal(str(swap2_quote["price"])) if "price" in swap2_quote else DECIMAL_ZERO,
                "price_token3_to_token1": Decimal(str(swap3_quote["price"])) if "price" in swap3_quote else DECIMAL_ZERO,
            }

        except Exception as exception:
            logger.ignore_exception(exception, "Error in triangular trade simulation")

            return {"profit_amount": DECIMAL_ZERO, "profit_percentage": DECIMAL_ZERO}

    async def _validate_opportunity(self, opportunity: Dict[str, Any]) -> bool:
        """
        Validates a triangular arbitrage opportunity before execution.

        This method performs comprehensive validation including:
        1. Checking if there is sufficient token1 balance across all wallets
        2. Calculating the optimal trade amount based on available balance
        3. Simulating the triangular trade with the optimal amount
        4. Verifying that the expected profit meets the minimum threshold
        5. Updating the opportunity record with calculated values

        Args:
            opportunity: Dictionary containing triangular arbitrage opportunity details

        Returns:
            True if the opportunity is valid and expected to be profitable; False otherwise
        """
        chain = opportunity["chain"]
        network = opportunity["network"]
        connector = opportunity["connector"]

        # Checks if there is available balance for token1
        available_balance = await self._get_total_token_balance_from_all_wallets(opportunity["token1"])
        if available_balance < self._minimum_trade_amount:
            raise Exception(f"Insufficient balance of {opportunity['token1']}: {available_balance}")

        # Calculates ideal trade amount
        trade_amount = await self._calculate_optimal_trade_amount(
            opportunity, available_balance
        )

        if not trade_amount or trade_amount <= DECIMAL_ZERO:
            raise Exception(f"Invalid optimal trade amount: {trade_amount}")

        # Simulate the triangular trade with the optimal amount
        expected_profit = await self._simulate_arbitrage_trade(
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
            raise Exception(
                f"Triangular opportunity not profitable: "
                f"{expected_profit['profit_percentage']:.2f}% < {self._minimum_profitability_percentage}%"
            )

        # Update opportunity with calculated values
        opportunity.update({
            "trade_amount": trade_amount,
            "expected_token2_amount": expected_profit["token2_amount"],
            "expected_token3_amount": expected_profit["token3_amount"],
            "expected_final_amount": expected_profit["final_amount"],
            "expected_profit_amount": expected_profit["profit_amount"],
            "expected_profit_percentage": expected_profit["profit_percentage"],
            "expected_fees_cost": expected_profit["fees_cost"],
            "expected_fees_cost_in_token1": expected_profit["fees_cost_in_token1"],
            "price_token1_to_token2": expected_profit["price_token1_to_token2"],
            "price_token2_to_token3": expected_profit["price_token2_to_token3"],
            "price_token3_to_token1": expected_profit["price_token3_to_token1"],
        })

        logger.info(
            f"Triangular opportunity validated: {opportunity["token1"]}->{opportunity["token2"]}->{opportunity["token3"]}->{opportunity["token1"]} "
            f"with expected profit {expected_profit['profit_percentage']:.2f}% after fees"
        )

        return True

    async def _calculate_optimal_trade_amount(self, opportunity: Dict[str, Any], maximum_available: Decimal) -> Decimal:
        """
        Determines the optimal trade amount for triangular arbitrage to maximize profit.

        This method:
        1. Tests multiple trade amounts within the available balance range
        2. Simulates the complete triangular trade at each test amount
        3. Identifies the amount that yields the highest profit percentage
        4. Respects configured minimum and maximum trade amount limits

        The test amounts include various percentages of the maximum available balance
        to find the sweet spot where profitability is highest, accounting for trade size
        impact on slippage and fees.

        Args:
            opportunity: Triangular arbitrage opportunity dictionary
            maximum_available: Maximum available balance of token1

        Returns:
            Optimal trade amount as a Decimal value
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
            profit_info = await self._simulate_arbitrage_trade(
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

    async def _execute_arbitrage(self, opportunity: Dict[str, Any]) -> bool:
        """
        Executes a triangular arbitrage trade by performing three sequential swaps.

        This method:
        1. Retrieves initial balances for all tokens in the triangular path
        2. Executes the first swap: token1 -> token2
        3. Waits for transaction confirmation before proceeding
        4. Executes the second swap: token2 -> token3
        5. Waits for transaction confirmation before proceeding
        6. Executes the third swap: token3 -> token1
        7. Calculates actual profit after all trades complete
        8. Records the complete trade execution in the database

        The method implements proper error handling and transaction confirmation
        at each step to ensure the integrity of the arbitrage execution.

        Args:
            opportunity: Dictionary containing validated triangular arbitrage opportunity details

        Returns:
            True if the arbitrage was executed successfully with a profit; False otherwise
        """
        token1 = opportunity["token1"]
        token2 = opportunity["token2"]
        token3 = opportunity["token3"]
        price_token_1_to_2 = opportunity["price_token1_to_token2"]
        price_token_2_to_3 = opportunity["price_token2_to_token3"]
        price_token_3_to_1 = opportunity["price_token3_to_token1"]
        chain_name = opportunity["chain"]
        network_name = opportunity["network"]
        connector_name = opportunity["connector"]
        token1_amount = opportunity["trade_amount"]
        connector_configuration = self._database["connections"][chain_name][network_name][connector_name]
        fee_payment_token_symbol = connector_configuration.get("fee_payment_token_symbol")
        fee_payment_token_amount = Decimal(connector_configuration.get("fee_payment_token_amount"))

        logger.info(f"Executing triangular arbitrage: {token1}->{token2}->{token3}->{token1}")

        # Find wallet for the chain/network/connector
        wallet = await self._get_wallet_for_chain_network_connector(chain_name, network_name, connector_name)
        if not wallet:
            raise Exception(f"No wallet found for {chain_name}/{network_name}/{connector_name}")

        wallet_address = wallet.get("address")

        try:
            # Get initial balance to calculate profit later
            initial_balances = await self._gateway_get_balances(
                chain_name, network_name, wallet_address, [token1, token2, token3]
            )
            if not initial_balances or "balances" not in initial_balances:
                raise Exception(f"Failed to get initial balances for wallet {wallet_address}")

            initial_token1_balance = Decimal(str(initial_balances["balances"].get(token1, 0)))
            initial_token2_balance = Decimal(str(initial_balances["balances"].get(token2, 0)))
            initial_token3_balance = Decimal(str(initial_balances["balances"].get(token3, 0)))

            if initial_token1_balance < token1_amount:
                raise Exception(f"Insufficient balance in wallet {wallet_address}: {initial_token1_balance} {token1}")

            fees_cost = DECIMAL_ZERO

            # Step 1: Swap token1 -> token2
            logger.info(f"Step 1: Swapping {token1_amount} {token1} for {token2}")
            connector = self.connectors[f"{connector_name}/amm_{chain_name}_{network_name}"]
            swap1_order_id = connector.sell(
                f"{token1}-{token2}",
                token1_amount,
                OrderType.MARKET,
                price_token_1_to_2,
                **{
                    "slippage_pct": self._maximum_slippage_percentage,
                    "pool_address": None,
                }
            )

            swap1_order = await self._order_tracker_fetch_order(connector, swap1_order_id)
            if swap1_order.current_state not in [OrderState.FILLED]:
                raise Exception(f"First swap failed - order state: {swap1_order.current_state}")

            # Get transaction hash from the order
            swap1_tx_hash = swap1_order.exchange_order_id
            logger.info(f"First swap signature: {swap1_tx_hash}")

            swap1_confirmation = await self._wait_for_transaction_confirmation(
                chain_name, network_name, swap1_tx_hash
            )
            if not swap1_confirmation:
                raise Exception("First swap transaction not confirmed")

            token2_amount = Decimal(swap1_order.executed_amount_quote)
            fees_cost += fee_payment_token_amount if fee_payment_token_amount > DECIMAL_ZERO else DECIMAL_ZERO

            # await asyncio.sleep(self._balantrade_feece_update_delay)  # Wait for balance update
            #
            # # Get updated balances to determine the amount received
            # intermediate_balances = await self._gateway_get_balances(
            #     chain, network, wallet_address, [token1, token2, token3]
            # )
            # if not intermediate_balances or "balances" not in intermediate_balances:
            #     raise Exception(f"Failed to get updated balances for wallet {wallet_address}")
            #
            # received_token2_amount = Decimal(str(intermediate_balances["balances"].get(token2, 0)))
            # token2_amount = received_token2_amount if received_token2_amount > DECIMAL_ZERO else opportunity.get("expected_token2_amount")

            # Step 2: Swap token2 -> token3
            logger.info(f"Step 2: Swapping {token2_amount} {token2} for {token3}")
            connector = self.connectors[f"{connector_name}/amm_{chain_name}_{network_name}"]
            swap2_order_id = connector.sell(
                f"{token2}-{token3}",
                token2_amount,
                OrderType.MARKET,
                price_token_2_to_3,
                **{
                    "slippage_pct": self._maximum_slippage_percentage,
                    "pool_address": None,
                }
            )

            swap2_order = await self._order_tracker_fetch_order(connector, swap2_order_id)
            if swap2_order.current_state not in [OrderState.FILLED]:
                raise Exception(f"Second swap failed - order state: {swap2_order.current_state}")

            # Get transaction hash from the order
            swap2_tx_hash = swap2_order.exchange_order_id
            logger.info(f"Second swap signature: {swap2_tx_hash}")

            swap2_confirmation = await self._wait_for_transaction_confirmation(
                chain_name, network_name, swap2_tx_hash
            )
            if not swap2_confirmation:
                raise Exception("Second swap transaction not confirmed")

            token3_amount = Decimal(swap2_order.executed_amount_quote)
            fees_cost += fee_payment_token_amount if fee_payment_token_amount > DECIMAL_ZERO else DECIMAL_ZERO

            # await asyncio.sleep(self._balance_update_delay)  # Wait for balance update
            #
            # # Get updated balances to determine the amount received
            # intermediate_balances2 = await self._gateway_get_balances(
            #     chain, network, wallet_address, [token1, token2, token3]
            # )
            # if not intermediate_balances2 or "balances" not in intermediate_balances2:
            #     raise Exception(f"Failed to get updated balances for wallet {wallet_address}")
            #
            # received_token3_amount = Decimal(str(intermediate_balances2["balances"].get(token3, 0)))
            # token3_amount = received_token3_amount if received_token3_amount > DECIMAL_ZERO else opportunity.get("expected_token3_amount")

            # Step 3: Swap token3 -> token1
            logger.info(f"Step 3: Swapping {token3_amount} {token3} for {token1}")
            connector = self.connectors[f"{connector_name}/amm_{chain_name}_{network_name}"]
            swap3_order_id = connector.sell(
                f"{token3}-{token1}",
                token3_amount,
                OrderType.MARKET,
                price_token_3_to_1,
                **{
                    "slippage_pct": self._maximum_slippage_percentage,
                    "pool_address": None,
                }
            )

            swap3_order = await self._order_tracker_fetch_order(connector, swap3_order_id)
            if swap3_order.current_state not in [OrderState.FILLED]:
                raise Exception(f"Third swap failed - order state: {swap3_order.current_state}")

            # Get transaction hash from the order
            swap3_tx_hash = swap3_order.exchange_order_id
            logger.info(f"Third swap signature: {swap3_tx_hash}")

            swap3_confirmation = await self._wait_for_transaction_confirmation(
                chain_name, network_name, swap3_tx_hash
            )
            if not swap3_confirmation:
                raise Exception("Third swap transaction not confirmed")

            token1_amount = Decimal(swap3_order.executed_amount_quote)
            fees_cost += fee_payment_token_amount if fee_payment_token_amount > DECIMAL_ZERO else DECIMAL_ZERO

            fees_cost_in_token1 = DECIMAL_ZERO
            if fees_cost > DECIMAL_ZERO:
                if fee_payment_token_symbol and fee_payment_token_symbol != token1:
                    fees_quote_swap = await self._gateway_quote_swap(
                        network_name,
                        connector_name,
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
                chain_name, network_name, wallet_address, [token1, token2, token3]
            )
            if not final_balances or "balances" not in final_balances:
                raise Exception(f"Failed to get updated balances for wallet {wallet_address}")

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
                "chain": chain_name,
                "network": network_name,
                "connector": connector_name,
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
                "swap1_transaction_hash": swap1_tx_hash,
                "swap2_transaction_hash": swap2_tx_hash,
                "swap3_transaction_hash": swap3_tx_hash,
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
