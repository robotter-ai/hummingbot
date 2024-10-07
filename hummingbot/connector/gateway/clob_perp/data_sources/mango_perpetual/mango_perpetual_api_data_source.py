import asyncio
import time
from collections import defaultdict
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from bidict import bidict

from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.connector.derivative.position import Position
from hummingbot.connector.gateway.clob_perp.data_sources.clob_perp_api_data_source_base import CLOBPerpAPIDataSourceBase
from hummingbot.connector.gateway.clob_perp.data_sources.mango_perpetual import mango_perpetual_constants as CONSTANTS
from hummingbot.connector.gateway.clob_perp.data_sources.mango_perpetual.mango_perpetual_constants import (
    CONNECTOR_NAME,
    MANGO_DERIVATIVE_ORDER_STATES,
    ORDER_SIDE_MAP,
)
from hummingbot.connector.gateway.common_types import CancelOrderResult, PlaceOrderResult
from hummingbot.connector.gateway.gateway_in_flight_order import GatewayInFlightOrder
from hummingbot.connector.trading_rule import TradingRule, split_hb_trading_pair
from hummingbot.connector.utils import combine_to_hb_trading_pair, get_new_numeric_client_order_id
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.data_type.common import OrderType, PositionMode, PositionSide
from hummingbot.core.data_type.funding_info import FundingInfo
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.trade_fee import MakerTakerExchangeFeeRates, TokenAmount, TradeFeeBase, TradeFeeSchema
from hummingbot.core.event.events import (  # ,; BalanceUpdateEvent,; OrderBookDataSourceEvent,; PositionUpdateEvent,
    AccountEvent,
    OrderBookDataSourceEvent,
    PositionUpdateEvent,
)
from hummingbot.core.gateway.gateway_http_client import GatewayHttpClient
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.core.utils.async_utils import safe_gather
from hummingbot.core.utils.tracking_nonce import NonceCreator
from hummingbot.logger import HummingbotLogger


class MangoPerpetualAPIDataSource(CLOBPerpAPIDataSourceBase):
    """An interface class to the Mango markets."""

    _logger: Optional[HummingbotLogger] = None

    def __init__(
        self,
        trading_pairs: List[str],
        connector_spec: Dict[str, Any],
        client_config_map: ClientConfigAdapter,
    ):
        super().__init__(
            trading_pairs=trading_pairs, connector_spec=connector_spec, client_config_map=client_config_map
        )
        self._connector_name = CONNECTOR_NAME
        self._chain = connector_spec["chain"]
        self._network = connector_spec["network"]
        self._account_id = connector_spec["wallet_address"]
        self._throttler = AsyncThrottler(rate_limits=CONSTANTS.RATE_LIMITS)
        self._markets_info_lock = asyncio.Lock()
        self._hb_to_exchange_tokens_map: bidict[str, str] = bidict()
        self._last_traded_prices: Dict[str, Decimal] = {}

        self._client_order_id_nonce_provider = NonceCreator.for_microseconds()

    @property
    def connector_name(self) -> str:
        return CONNECTOR_NAME

    @property
    def real_time_balance_update(self) -> bool:
        return False

    @property
    def events_are_streamed(self) -> bool:
        return False

    async def start(self):
        await super().start()

    async def stop(self):
        await super().stop()

    def get_supported_order_types(self) -> List[OrderType]:
        return [OrderType.LIMIT, OrderType.MARKET, OrderType.LIMIT_MAKER]

    def supported_position_modes(self) -> List[PositionMode]:
        return [PositionMode.ONEWAY]

    def supported_stream_events(self) -> List[Enum]:
        return [
            AccountEvent.PositionUpdate,
            OrderBookDataSourceEvent.SNAPSHOT_EVENT,
        ]

    async def check_network_status(self) -> NetworkStatus:
        # self.logger().debug("check_network_status: start")

        try:
            status = await self._gateway_ping_gateway()

            if status:
                return NetworkStatus.CONNECTED
            else:
                return NetworkStatus.NOT_CONNECTED
        except asyncio.CancelledError:
            raise
        except Exception as exception:
            self.logger().error(exception)

            return NetworkStatus.NOT_CONNECTED

    async def fetch_last_fee_payment(self, trading_pair: str) -> Tuple[float, Decimal, Decimal]:
        timestamp, funding_rate, payment = 0, Decimal("-1"), Decimal("-1")

        return timestamp, funding_rate, payment

    async def fetch_positions(self) -> List[Position]:
        resp = await self._get_gateway_instance().clob_perp_positions(
            connector=self.connector_name,
            chain=self._chain,
            network=self._network,
            trading_pairs=[],
            address=self._account_id,
        )

        positions = resp.get("positions")
        timestamp = resp.get("timestamp")

        if len(positions) == 0:
            return []

        # Map positions into events
        position_events = [
            PositionUpdateEvent(
                timestamp=timestamp,
                trading_pair=position["market"].replace("PERP", "USDC"),
                position_side=PositionSide.LONG if position["side"] == "LONG" else PositionSide.SHORT,
                unrealized_pnl=Decimal(position["unrealizedPnl"]),
                entry_price=Decimal(position["averageEntryPrice"]),
                amount=Decimal(position["amount"]),
                leverage=Decimal(position["leverage"]),
            )
            for position in positions
        ]

        # Map position into array of Position objects
        positions_parsed = [
            Position(
                trading_pair=position_event.trading_pair,
                position_side=position_event.position_side,
                unrealized_pnl=position_event.unrealized_pnl,
                entry_price=position_event.entry_price,
                amount=position_event.amount,
                leverage=position_event.leverage,
            )
            for position_event in position_events
        ]

        # For each position, trigger an event
        for position_event in position_events:
            self._publisher.trigger_event(event_tag=AccountEvent.PositionUpdate, message=position_event)
        return positions_parsed

    async def set_trading_pair_leverage(self, trading_pair: str, leverage: int) -> Tuple[bool, str]:
        """
        Leverage is set on a per order basis. See place_order()
        """
        # TODO: Implement this as default leverage
        return True, ""

    def _check_markets_initialized(self) -> bool:
        return len(self._markets_info) != 0

    async def _gateway_ping_gateway(self, _request=None):
        return await self._get_gateway_instance().ping_gateway()

    def _get_exchange_trading_pair_from_market_info(self, market_info: Any) -> str:
        return market_info.get("name")

    async def get_funding_info(self, trading_pair: str) -> FundingInfo:
        # TODO: This is a temporary implementation. We need to get the funding info from the gateway.
        last_trade_price = await self.get_last_traded_price(trading_pair)

        funding_info = FundingInfo(
            trading_pair=trading_pair,
            index_price=last_trade_price,  # Default to using last trade price
            mark_price=last_trade_price,
            next_funding_utc_timestamp=(int(self._time() * 1e-3) * 2),
            rate=Decimal(0),
        )
        return funding_info

    async def trading_pair_position_mode_set(self, mode: PositionMode, trading_pair: str) -> Tuple[bool, str]:
        """
        Leverage is set on a per order basis. See place_order()
        """
        return True, ""

    async def place_order(
        self, order: GatewayInFlightOrder, **kwargs
    ) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
        order_result = await self._get_gateway_instance().clob_perp_place_identifiable_order(
            connector=self._connector_name,
            chain=self._chain,
            network=self._network,
            trading_pair=self._convert_trading_pair(order.trading_pair),
            address=self._account_id,
            trade_type=order.trade_type,
            order_type=order.order_type,
            price=order.price,
            size=order.amount,
            leverage=order.leverage,
            client_order_id=order.client_order_id,
        )

        transaction_hash: Optional[str] = order_result.get("txHash")
        identified_orders: Optional[List[str]] = order_result.get("exchangeOrderId")

        if transaction_hash is None:
            await self._on_create_order_transaction_failure(order=order, order_result=order_result)

        transaction_hash = transaction_hash.lower()
        exchange_order_id = identified_orders[0]

        misc_updates = {
            "creation_transaction_hash": transaction_hash,
        }

        return exchange_order_id, misc_updates

    async def batch_order_create(self, orders_to_create: List[GatewayInFlightOrder]) -> List[PlaceOrderResult]:
        place_order_results = []

        for order in orders_to_create:
            exchange_order_id, misc_updates = await self.place_order(order)

            exception = None
            if misc_updates is None:
                self.logger().error("The batch order create transaction failed.")
                exception = ValueError(f"The creation transaction has failed for order: {order.client_order_id}.")

            place_order_results.append(
                PlaceOrderResult(
                    update_timestamp=self._time(),
                    client_order_id=order.client_order_id,
                    exchange_order_id=exchange_order_id,
                    trading_pair=order.trading_pair,
                    misc_updates={
                        "creation_transaction_hash": misc_updates["creation_transaction_hash"],
                    },
                    exception=exception,
                )
            )

        return place_order_results

    async def cancel_order(self, order: GatewayInFlightOrder) -> Tuple[bool, Optional[Dict[str, Any]]]:
        cancellation_result = await self._get_gateway_instance().clob_perp_cancel_order(
            chain=self._chain,
            network=self._network,
            connector=self._connector_name,
            address=self._account_id,
            trading_pair=self._convert_trading_pair(order.trading_pair),
            exchange_order_id=order.exchange_order_id,
        )
        transaction_hash: Optional[str] = cancellation_result.get("txHash")

        if transaction_hash in [None, ""]:
            raise ValueError(
                f"The cancellation transaction for {order.client_order_id} failed. Please ensure there is sufficient"
                f" SOL in the bank to cover transaction fees."
            )

        self.logger().debug(
            f"Canceling order {order.client_order_id}"
            f" with order hash {order.exchange_order_id} and tx hash {transaction_hash}."
        )

        transaction_hash = transaction_hash.lower()

        misc_updates = {"cancellation_transaction_hash": transaction_hash}

        return True, misc_updates

    async def batch_order_cancel(self, orders_to_cancel: List[GatewayInFlightOrder]) -> List[CancelOrderResult]:
        in_flight_orders_to_cancel = [
            self._gateway_order_tracker.fetch_tracked_order(client_order_id=order.client_order_id)
            for order in orders_to_cancel
        ]
        cancel_order_results = []
        if len(in_flight_orders_to_cancel) != 0:
            exchange_order_ids_to_cancel = await safe_gather(
                *[order.get_exchange_order_id() for order in in_flight_orders_to_cancel],
                return_exceptions=True,
            )
            found_orders_to_cancel = [
                order
                for order, result in zip(orders_to_cancel, exchange_order_ids_to_cancel)
                if not isinstance(result, asyncio.TimeoutError)
            ]

            for order in found_orders_to_cancel:
                _, misc_updates = await self.cancel_order(order)

                exception = None
                if misc_updates is None:
                    self.logger().error("The batch order cancel transaction failed.")
                    exception = ValueError(
                        f"The cancellation transaction has failed for order: {order.client_order_id}"
                    )

                cancel_order_results.append(
                    CancelOrderResult(
                        client_order_id=order.client_order_id,
                        trading_pair=order.trading_pair,
                        misc_updates={
                            "cancelation_transaction_hash": misc_updates["cancellation_transaction_hash"],
                        },
                        exception=exception,
                    )
                )

        return cancel_order_results

    async def get_order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        # async with self._throttler.execute_task(limit_id=CONSTANTS.CHAIN_RPC_LIMIT_ID):
        data = await self._get_gateway_instance().get_clob_orderbook_snapshot(
            trading_pair=self._convert_trading_pair(trading_pair),
            connector=self.connector_name,
            chain=self._chain,
            network=self._network,
        )
        bids = [
            (Decimal(bid["price"]), Decimal(bid["quantity"])) for bid in data["buys"] if Decimal(bid["quantity"]) != 0
        ]
        asks = [
            (Decimal(ask["price"]), Decimal(ask["quantity"])) for ask in data["sells"] if Decimal(ask["quantity"]) != 0
        ]
        snapshot_msg = OrderBookMessage(
            message_type=OrderBookMessageType.SNAPSHOT,
            content={
                "trading_pair": trading_pair,
                "update_id": self._time() * 1e3,
                "bids": bids,
                "asks": asks,
            },
            timestamp=data["timestamp"],
        )

        self._publisher.trigger_event(event_tag=OrderBookDataSourceEvent.SNAPSHOT_EVENT, message=snapshot_msg)
        return snapshot_msg

    def get_client_order_id(self, trading_pair: str, is_buy: bool, hbot_order_id_prefix: str, max_id_len: int) -> str:
        decimal_id = get_new_numeric_client_order_id(
            nonce_creator=self._client_order_id_nonce_provider,
            max_id_bit_count=32,
        )
        return f"{decimal_id}"

    async def get_account_balances(self) -> Dict[str, Dict[str, Decimal]]:
        self._check_markets_initialized() or await self._update_markets()

        result = await self._get_gateway_instance().get_balances(
            chain=self._chain,
            network=self._network,
            address=self._account_id,
            token_symbols=list(self._hb_to_exchange_tokens_map.values()),
            connector=self.connector_name,
        )
        balances = defaultdict(dict)
        for token, value in result["balances"].items():
            client_token = self._hb_to_exchange_tokens_map.inverse[token]
            balance_value = Decimal(value)
            if balance_value != 0:
                balances[client_token]["total_balance"] = balance_value
                balances[client_token]["available_balance"] = balance_value

        return balances

    async def get_order_status_update(self, in_flight_order: GatewayInFlightOrder) -> OrderUpdate:
        # print("request update for in_flight_order: ", in_flight_order.client_order_id)

        status_update = await self._get_order_status_update_with_order_id(in_flight_order=in_flight_order)

        # print("status_update: ", status_update)

        # self._publisher.trigger_event(event_tag=MarketEvent.OrderUpdate, message=status_update)

        if status_update is None:
            raise ValueError(f"No update found for order {in_flight_order.exchange_order_id}.")

        return status_update

    async def get_last_traded_price(self, trading_pair: str) -> Decimal:
        # async with self._throttler.execute_task(limit_id=CONSTANTS.CHAIN_RPC_LIMIT_ID):
        await self.get_order_book_snapshot(trading_pair=trading_pair)
        try:
            resp = await self._get_gateway_instance().clob_perp_last_trade_price(
                chain=self._chain,
                connector=self.connector_name,
                network=self._network,
                trading_pair=self._convert_trading_pair(trading_pair),
            )
        except Exception as exception:
            self.logger().error(exception)
            last_traded_price = self._last_traded_prices[trading_pair]
        else:
            last_traded_price = Decimal(resp.get("lastTradePrice"))
            self._last_traded_prices[trading_pair] = last_traded_price

        return last_traded_price

    async def get_all_order_fills(self, in_flight_order: GatewayInFlightOrder) -> List[TradeUpdate]:
        self._check_markets_initialized() or await self._update_markets()

        # async with self._throttler.execute_task(limit_id=CONSTANTS.CHAIN_RPC_LIMIT_ID):
        resp = await self._get_gateway_instance().clob_perp_get_orders(
            market=self._convert_trading_pair(in_flight_order.trading_pair),
            chain=self._chain,
            network=self._network,
            connector=self.connector_name,
            address=self._account_id,
            order_id=in_flight_order.exchange_order_id,
            client_order_id=in_flight_order.client_order_id,
        )

        orders = resp.get("orders")

        if len(orders) == 0:
            return []

        order = orders[0]
        exchange_order_id = order.get("exchangeOrderId")
        trade_updates = order.get("fills")
        mapped_trade_updates = [
            TradeUpdate(
                trade_id=trade_update.get("id"),
                client_order_id=in_flight_order.client_order_id,
                exchange_order_id=exchange_order_id,
                trading_pair=in_flight_order.trading_pair,
                fill_timestamp=int(trade_update.get("timestamp")),
                fill_price=Decimal(trade_update.get("price")),
                fill_base_amount=Decimal(trade_update.get("quantity")),
                fill_quote_amount=Decimal(trade_update.get("price")) * Decimal(trade_update.get("quantity")),
                fee=TradeFeeBase.new_spot_fee(
                    fee_schema=TradeFeeSchema(),
                    trade_type=ORDER_SIDE_MAP[order.get("side")],
                    flat_fees=[TokenAmount(token="USDC", amount=Decimal(trade_update.get("fee")))],
                ),
                is_taker=True,
            )
            for trade_update in trade_updates
        ]

        return mapped_trade_updates

    def _get_exchange_base_quote_tokens_from_market_info(self, market_info: str) -> Tuple[str, str]:
        split_name = str(market_info).split("-")
        base = split_name[0].upper()
        quote = "USDC"
        return base, quote

    async def _update_markets(self):
        async with self._markets_info_lock:
            markets = await self._get_markets_info()
            for market_info in markets.items():
                trading_pair = self._get_trading_pair_from_market_info(market_info=market_info[0])
                self._markets_info[trading_pair] = market_info[1]
                base, quote = split_hb_trading_pair(trading_pair=trading_pair)
                base_exchange, quote_exchange = self._get_exchange_base_quote_tokens_from_market_info(
                    market_info=market_info[0]
                )
                self._hb_to_exchange_tokens_map[base] = base_exchange
                self._hb_to_exchange_tokens_map[quote] = quote_exchange

    async def _get_markets_info(self) -> Dict[str, Any]:
        resp = await self._get_gateway_instance().get_clob_markets(
            connector=self.connector_name, chain=self._chain, network=self._network
        )

        markets = resp.get("markets")

        # Change the key from BTC-PERP to BTC-USDC
        for key in list(markets.keys()):
            markets[key.replace("PERP", "USDC")] = markets.pop(key)

        return markets

    def _get_trading_pair_from_market_info(self, market_info: str) -> str:
        split_name = str(market_info).split("-")
        base = split_name[0].upper()
        quote = "USDC"
        trading_pair = combine_to_hb_trading_pair(base=base, quote=quote)
        return trading_pair

    def _get_maker_taker_exchange_fee_rates_from_market_info(
        self, market_info: Dict[str, Any]
    ) -> MakerTakerExchangeFeeRates:
        # Currently, trading fees on XRPL dex are not following maker/taker model, instead they based on transfer fees
        # https://xrpl.org/transfer-fees.html
        maker_taker_exchange_fee_rates = MakerTakerExchangeFeeRates(
            maker=Decimal(market_info["makerFee"]),
            taker=Decimal(market_info["takerFee"]),
            maker_flat_fees=[],  # TODO: Add solana flat fees
            taker_flat_fees=[],
        )
        return maker_taker_exchange_fee_rates

    def _parse_trading_rule(self, trading_pair: str, market_info: Dict[str, Any]) -> TradingRule:
        split_name = market_info["name"].split("-")
        base = split_name[0].upper()
        quote = "USDC"
        return TradingRule(
            trading_pair=combine_to_hb_trading_pair(base=base, quote=quote),
            min_order_size=Decimal(market_info["miniumOrderSize"]),
            min_price_increment=Decimal(market_info["tickSize"]),
            min_quote_amount_increment=Decimal(market_info["minQuoteAmountIncrement"]),
            min_base_amount_increment=Decimal(market_info["minBaseAmountIncrement"]),
        )

    def is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        return str(status_update_exception).startswith("No update found for order")

    def is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        return False

    async def _get_order_status_update_with_order_id(self, in_flight_order: InFlightOrder) -> Optional[OrderUpdate]:
        # TODO: on order creation, exchange_order_id will be None, we will send client_order_id to gateway to track
        #       order state. Once we get exchange_order_id, we will use it to track order state.
        #       Implement a quick way to manage order state on gateway side.
        try:
            # async with self._throttler.execute_task(limit_id=CONSTANTS.CHAIN_RPC_LIMIT_ID):
            resp = await self._get_gateway_instance().clob_perp_get_orders(
                chain=self._chain,
                network=self._network,
                connector=self.connector_name,
                address=self._account_id,
                order_id=in_flight_order.exchange_order_id,
                client_order_id=in_flight_order.client_order_id,
                market=self._convert_trading_pair(in_flight_order.trading_pair),
            )

        except OSError as e:
            if "HTTP status is 404" in str(e):
                raise ValueError(f"No update found for order {in_flight_order.client_order_id}.")
            raise e

        if resp.get("orders") == "":
            raise ValueError(f"No update found for order {in_flight_order.client_order_id}.")
        else:
            orders = resp.get("orders")

            if len(orders) == 0:
                raise ValueError(f"No update found for order {in_flight_order.client_order_id}.")

            status_update = OrderUpdate(
                trading_pair=in_flight_order.trading_pair,
                update_timestamp=pd.Timestamp(resp["timestamp"]).timestamp(),
                new_state=MANGO_DERIVATIVE_ORDER_STATES[orders[0]["status"]],
                client_order_id=in_flight_order.client_order_id,
                exchange_order_id=orders[0].get("exchangeOrderId"),
            )

        return status_update

    async def _on_create_order_transaction_failure(self, order: GatewayInFlightOrder, order_result: Dict[str, Any]):
        raise ValueError(
            f"The creation transaction for {order.client_order_id} failed. Please ensure you have sufficient"
            f" funds to cover the transaction gas costs."
        )

    async def _on_cancel_order_transaction_failure(
        self, order: GatewayInFlightOrder, cancelation_result: Dict[str, Any]
    ):
        raise ValueError(
            f"The cancelation transaction for {order.client_order_id} failed. Please ensure you have sufficient"
            f" funds to cover the transaction gas costs."
        )

    @staticmethod
    async def _sleep(delay: float):
        await asyncio.sleep(delay)

    @staticmethod
    def _time() -> float:
        return time.time()

    def _get_gateway_instance(self) -> GatewayHttpClient:
        gateway_instance = GatewayHttpClient.get_instance(self._client_config)
        return gateway_instance

    # Convert trading pair from BTC-USDC to BTC-PERP
    def _convert_trading_pair(self, trading_pair: str) -> str:
        return trading_pair.replace("USDC", "PERP")
