import logging
import os
from decimal import Decimal
from typing import Dict

from pydantic import Field

from hummingbot.client.config.config_data_types import BaseClientModel
from hummingbot.client.settings import GatewayConnectionSetting
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.event.events import TradeType
from hummingbot.core.gateway.gateway_http_client import GatewayHttpClient
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase


class DEXTradeConfig(BaseClientModel):
    script_file_name: str = Field(default_factory=lambda: os.path.basename(__file__))
    connector: str = Field("hydration", json_schema_extra={
        "prompt": "Connector name (e.g. hydration, uniswap)", "prompt_on_new": True})
    chain: str = Field("hydration", json_schema_extra={
        "prompt": "Chain (e.g. hydration, ethereum)", "prompt_on_new": True})
    network: str = Field("mainnet", json_schema_extra={
        "prompt": "Network (e.g. mainnet (hydration), base (ethereum))", "prompt_on_new": True})
    trading_pair: str = Field("HDX-DOT", json_schema_extra={
        "prompt": "Trading pair (e.g. HDX-DOT)", "prompt_on_new": True})
    target_price: Decimal = Field(Decimal("1"), json_schema_extra={
        "prompt": "Target price to trigger trade", "prompt_on_new": True})
    trigger_above: bool = Field(False, json_schema_extra={
        "prompt": "Trigger when price rises above target? (True for above/False for below)", "prompt_on_new": True})
    is_buy: bool = Field(True, json_schema_extra={
        "prompt": "Buying or selling the base asset? (True for buy, False for sell)", "prompt_on_new": True})
    amount: Decimal = Field(Decimal("1"), json_schema_extra={
        "prompt": "Order amount (in base token)", "prompt_on_new": True})
    pool_address: str = Field("7HkYL6b2p4tuppGaKpugS2WaAjngiCXDXDKrEeyYgTaFP8KA", json_schema_extra={
        "prompt": "Pool address for trading (HDX-DOT pool)", "prompt_on_new": True})
    slippage_pct: Decimal = Field(Decimal("1.0"), json_schema_extra={
        "prompt": "Slippage percentage (e.g. 1.0 for 1%)", "prompt_on_new": True})


class DEXTrade(ScriptStrategyBase):
    """
    This strategy monitors DEX prices and executes a swap when a price threshold is reached.
    """

    markets = {}  # Initialize markets as class attribute

    @classmethod
    def init_markets(cls, config: DEXTradeConfig):
        # AMM strategies expect "/amm" in the gateway market key
        connector_chain_network = f"{config.connector}/amm_{config.chain}_{config.network}"
        cls.markets = {connector_chain_network: {config.trading_pair}}

    def __init__(self, connectors: Dict[str, ConnectorBase]):
        super().__init__(connectors)
        # Create config from class attributes
        self.config = DEXTradeConfig()
        self.gateway_client = GatewayHttpClient.get_instance()
        self.base, self.quote = self.config.trading_pair.split("-")

        # State tracking
        self.trade_executed = False
        self.trade_in_progress = False
        self.wallet_address = None

        # Get wallet address from gateway connections
        safe_ensure_future(self.get_wallet_address())

        # Log trade information
        condition = "rises above" if self.config.trigger_above else "falls below"
        side = "BUY" if self.config.is_buy else "SELL"
        self.log_with_clock(logging.INFO, f"Will {side} {self.config.amount} {self.base} for {self.quote} on {self.config.connector} when price {condition} {self.config.target_price}")

    async def get_wallet_address(self):
        """Get wallet address from gateway connections"""
        try:
            gateway_connections_conf = GatewayConnectionSetting.load()

            if len(gateway_connections_conf) < 1:
                self.log_with_clock(logging.ERROR, "No wallet connections found. Please connect a wallet using 'gateway connect'.")
                return

            # Find wallet for this connector/chain/network
            # Handle different connector naming patterns
            wallet = [
                w for w in gateway_connections_conf
                if w["network"] == self.config.network and (
                    (w["chain"] == self.config.chain and w["connector"] == self.config.connector) or
                    (w["chain"] == "hydration" and w["connector"] == f"{self.config.connector}/amm") or
                    (w["chain"] == "hydration" and self.config.connector == "hydration" and self.config.chain == "hydration")
                )
            ]

            if not wallet:
                self.log_with_clock(logging.ERROR, f"No wallet found for {self.config.chain}/{self.config.connector}/{self.config.network}. Please connect using 'gateway connect'.")
            else:
                self.wallet_address = wallet[0]["wallet_address"]
                self.log_with_clock(logging.INFO, f"Found wallet connection: {self.wallet_address}")

        except Exception as e:
            self.log_with_clock(logging.ERROR, f"Error getting wallet address: {e}")

    def on_tick(self):
        # Don't check price if trade already executed or in progress
        if self.trade_executed or self.trade_in_progress:
            return

        # Check price on each tick
        safe_ensure_future(self.check_price_and_trade())

    async def check_price_and_trade(self):
        """Check current price and trigger trade if condition is met"""
        if self.trade_in_progress or self.trade_executed:
            return

        self.trade_in_progress = True
        current_price = None  # Initialize current_price

        side = "buy" if self.config.is_buy else "sell"
        msg = (f"Getting quote on {self.config.connector} "
               f"({self.config.chain}/{self.config.network}) "
               f"to {side} {self.config.amount} {self.base} "
               f"for {self.quote}")

        try:
            self.log_with_clock(logging.INFO, msg)
            price_data = await self.gateway_client.amm_quote_swap(
                network=self.config.network,
                connector=self.config.connector,
                base_asset=self.base,
                quote_asset=self.quote,
                amount=self.config.amount,
                side=TradeType.BUY if self.config.is_buy else TradeType.SELL,
                pool_address=self.config.pool_address,
            )
            current_price = price_data.get("price", None)
            if current_price is not None:
                current_price = float(current_price)
            self.log_with_clock(logging.INFO, f"Price: {current_price}")
        except Exception as e:
            self.log_with_clock(logging.ERROR, f"Error getting quote: {e}")
            self.trade_in_progress = False
            return  # Exit if we couldn't get the price

        # Continue with rest of the function only if we have a valid price
        if current_price is not None:
            # Check if price condition is met
            condition_met = False
            if self.config.trigger_above and current_price > self.config.target_price:
                condition_met = True
                self.log_with_clock(logging.INFO, f"Price rose above target: {current_price} > {self.config.target_price}")
            elif not self.config.trigger_above and current_price < self.config.target_price:
                condition_met = True
                self.log_with_clock(logging.INFO, f"Price fell below target: {current_price} < {self.config.target_price}")

            if condition_met:
                try:
                    self.log_with_clock(logging.INFO, "Price condition met! Executing trade...")

                    # Execute trade using gateway client
                    if not self.wallet_address:
                        self.log_with_clock(logging.ERROR, "Wallet address not found! Please connect a wallet using 'gateway connect'.")
                        return

                    trade_result = await self.gateway_client.amm_execute_swap(
                        network=self.config.network,
                        connector=self.config.connector,
                        wallet_address=self.wallet_address,
                        base_asset=self.base,
                        quote_asset=self.quote,
                        side=TradeType.BUY if self.config.is_buy else TradeType.SELL,
                        amount=self.config.amount,
                        slippage_percentage=self.config.slippage_pct,
                        pool_address=self.config.pool_address,
                    )
                    self.log_with_clock(logging.INFO, f"Trade executed: {trade_result}")
                    self.trade_executed = True
                except Exception as e:
                    self.log_with_clock(logging.ERROR, f"Error executing trade: {str(e)}")
                finally:
                    if not self.trade_executed:
                        self.trade_in_progress = False

    def format_status(self) -> str:
        """Format status message for display in Hummingbot"""
        if self.trade_executed:
            return "Trade has been executed successfully!"

        if self.trade_in_progress:
            return "Currently checking price or executing trade..."

        condition = "rises above" if self.config.trigger_above else "falls below"

        lines = []
        side = "buy" if self.config.is_buy else "sell"
        connector_chain_network = f"{self.config.connector}/amm_{self.config.chain}_{self.config.network}"
        lines.append(f"Monitoring {self.base}-{self.quote} price on {connector_chain_network}")
        lines.append(f"Will execute {side} trade when price {condition} {self.config.target_price}")
        lines.append(f"Trade amount: {self.config.amount} {self.base}")
        lines.append("Checking price on every tick")

        return "\n".join(lines)
