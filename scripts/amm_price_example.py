import logging
from decimal import Decimal
from typing import Dict

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.event.events import TradeType
from hummingbot.core.gateway.gateway_http_client import GatewayHttpClient
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase


class DEXPrice(ScriptStrategyBase):
    """
    This example shows how to use the GatewaySwap connector to fetch price for a swap
    """

    # Configuration as class attributes
    connector = "hydration/amm"
    chain = "hydration"
    network = "mainnet"
    trading_pair = "HDX-DOT"
    is_buy = True
    amount = Decimal("1")

    # Markets definition - empty since we're using gateway client directly
    markets = {}

    def __init__(self, connectors: Dict[str, ConnectorBase]):
        super().__init__(connectors)
        self.gateway_client = GatewayHttpClient.get_instance()
        self.base, self.quote = self.trading_pair.split("-")

    def on_tick(self):
        # wrap async task in safe_ensure_future
        safe_ensure_future(self.async_task())

    # async task since we are using Gateway
    async def async_task(self):
        # Check if gateway is accessible first
        try:
            # Test gateway connection
            await self.gateway_client.ping_gateway()
        except Exception as e:
            self.log_with_clock(logging.ERROR, f"Gateway not accessible: {e}")
            return

        # fetch price using Gateway client directly
        side = "buy" if self.is_buy else "sell"
        msg = (f"Getting quote on {self.connector} "
               f"({self.chain}/{self.network}) "
               f"to {side} {self.amount} {self.base} "
               f"for {self.quote}")
        try:
            self.log_with_clock(logging.INFO, msg)
            price_data = await self.gateway_client.get_price(
                chain=self.chain,
                network=self.network,
                connector=self.connector,
                base_asset=self.base,
                quote_asset=self.quote,
                amount=self.amount,
                side=TradeType.BUY if self.is_buy else TradeType.SELL,
            )
            if price_data and "price" in price_data:
                price = price_data.get("price", "N/A")
                self.log_with_clock(logging.INFO, f"Price: {price}")
            else:
                self.log_with_clock(logging.WARNING, f"No price data received: {price_data}")
        except Exception as e:
            self.log_with_clock(logging.ERROR, f"Error getting quote: {e}")
