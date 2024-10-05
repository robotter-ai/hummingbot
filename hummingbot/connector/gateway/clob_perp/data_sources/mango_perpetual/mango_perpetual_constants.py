from hummingbot.connector.constants import MINUTE
from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.data_type.common import OrderType, PositionMode, TradeType
from hummingbot.core.data_type.in_flight_order import OrderState

CONNECTOR_NAME = "mango_perpetual"
LOST_ORDER_COUNT_LIMIT = 3
ORDER_CHAIN_PROCESSING_TIMEOUT = 5

MARKETS_UPDATE_INTERVAL = 8 * 60 * 60

SUPPORTED_ORDER_TYPES = [OrderType.LIMIT, OrderType.LIMIT_MAKER, OrderType.MARKET]
SUPPORTED_POSITION_MODES = [PositionMode.ONEWAY]

MANGO_DERIVATIVE_ORDER_STATES = {
    "CREATED": OrderState.PENDING_CREATE,
    "OPEN": OrderState.OPEN,
    "PARTIALLY_FILLED": OrderState.PARTIALLY_FILLED,
    "FILLED": OrderState.FILLED,
    "CANCELLED": OrderState.CANCELED,
    "PENDING_CANCEL": OrderState.PENDING_CANCEL,
    "EXPIRED": OrderState.CANCELED,
}

ORDER_SIDE_MAP = {
    "BUY": TradeType.BUY,
    "SELL": TradeType.SELL,
}

CHAIN_RPC_LIMIT_ID = "ChainRPCLimitID"
CHAIN_RPC_LIMIT = 60

RATE_LIMITS = [RateLimit(limit_id=CHAIN_RPC_LIMIT_ID, limit=CHAIN_RPC_LIMIT, time_interval=MINUTE)]
