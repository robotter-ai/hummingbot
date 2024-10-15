import time
from decimal import Decimal
from typing import Dict, List, Set

import pandas as pd
from pydantic import Field

from hummingbot.client.config.config_data_types import ClientFieldData
from hummingbot.client.ui.interface_utils import format_df_for_printout
from hummingbot.core.data_type.common import PriceType, TradeType
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase, ControllerConfigBase
from hummingbot.strategy_v2.executors.data_types import ConnectorPair
from hummingbot.strategy_v2.executors.xemm_executor.data_types import XEMMExecutorConfig
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, ExecutorAction


class XEMMCopycatConfig(ControllerConfigBase):
    controller_name: str = "xemm_copycat"
    candles_config: List[CandlesConfig] = []
    maker_connector: str = Field(
        default="cube",
        client_data=ClientFieldData(
            prompt=lambda e: "Enter the maker connector: ",
            prompt_on_new=True
        ))
    maker_trading_pair: str = Field(
        default="$DOG-USDC",
        client_data=ClientFieldData(
            prompt=lambda e: "Enter the maker trading pair: ",
            prompt_on_new=True
        ))
    taker_connector: str = Field(
        default="gate_io",
        client_data=ClientFieldData(
            prompt=lambda e: "Enter the taker connector: ",
            prompt_on_new=True
        ))
    taker_trading_pair: str = Field(
        default="DOG-USDT",
        client_data=ClientFieldData(
            prompt=lambda e: "Enter the taker trading pair: ",
            prompt_on_new=True
        ))
    target_bid_liquidity: Decimal = Field(
        default=100,
        client_data=ClientFieldData(
            prompt=lambda e: "Enter the target bid liquidity: ",
            prompt_on_new=True
        ))
    target_ask_liquidity: Decimal = Field(
        default=100,
        client_data=ClientFieldData(
            prompt=lambda e: "Enter the target ask liquidity: ",
            prompt_on_new=True
        ))
    min_profitability: Decimal = Field(
        default=0.2,
        client_data=ClientFieldData(
            prompt=lambda e: "Enter the minimum profitability (in %): ",
            prompt_on_new=True
        ))
    max_spread: Decimal = Field(
        default=1,
        client_data=ClientFieldData(
            prompt=lambda e: "Enter the maximum spread (in %): ",
            prompt_on_new=True
        ))
    max_order_levels: int = Field(
        default=5,
        client_data=ClientFieldData(
            prompt=lambda e: "Enter the maximum number of order levels per side: ",
            prompt_on_new=True
        ))

    def update_markets(self, markets: Dict[str, Set[str]]) -> Dict[str, Set[str]]:
        if self.maker_connector not in markets:
            markets[self.maker_connector] = set()
        markets[self.maker_connector].add(self.maker_trading_pair)
        if self.taker_connector not in markets:
            markets[self.taker_connector] = set()
        markets[self.taker_connector].add(self.taker_trading_pair)
        return markets


class XEMMCopycat(ControllerBase):

    def __init__(self, config: XEMMCopycatConfig, *args, **kwargs):
        self.config = config
        super().__init__(config, *args, **kwargs)

    async def update_processed_data(self):
        pass

    def get_targets(self):
        """
        Returns the target profitability and amount for each executor.

        It is based on the hedge exchange order book.
        We take the best bids and asks and add levels of liquidity, based on the amount of liquidity posted on
        the hedge exchange. We stop adding levels when the target liquidity is reached. Target profitability is
        based on the price levels we added.
        """

    def determine_executor_actions(self) -> List[ExecutorAction]:
        executor_actions = []
        mid_price = self.market_data_provider.get_price_by_type(self.config.maker_connector, self.config.maker_trading_pair, PriceType.MidPrice)
        active_buy_executors = self.filter_executors(
            executors=self.executors_info,
            filter_func=lambda e: not e.is_done and e.config.maker_side == TradeType.BUY
        )
        active_sell_executors = self.filter_executors(
            executors=self.executors_info,
            filter_func=lambda e: not e.is_done and e.config.maker_side == TradeType.SELL
        )
        stopped_buy_executors = self.filter_executors(
            executors=self.executors_info,
            filter_func=lambda e: e.is_done and e.config.maker_side == TradeType.BUY and e.filled_amount_quote != 0
        )
        stopped_sell_executors = self.filter_executors(
            executors=self.executors_info,
            filter_func=lambda e: e.is_done and e.config.maker_side == TradeType.SELL and e.filled_amount_quote != 0
        )
        imbalance = len(stopped_buy_executors) - len(stopped_sell_executors)
        for target_profitability, amount in self.buy_levels_targets_amount:
            active_buy_executors_target = [e.config.target_profitability == target_profitability for e in active_buy_executors]

            if len(active_buy_executors_target) == 0 and imbalance < self.config.max_executors_imbalance:
                config = XEMMExecutorConfig(
                    controller_id=self.config.id,
                    timestamp=self.market_data_provider.time(),
                    buying_market=ConnectorPair(connector_name=self.config.maker_connector,
                                                trading_pair=self.config.maker_trading_pair),
                    selling_market=ConnectorPair(connector_name=self.config.taker_connector,
                                                 trading_pair=self.config.taker_trading_pair),
                    maker_side=TradeType.BUY,
                    order_amount=amount / mid_price,
                    min_profitability=self.config.min_profitability,
                    target_profitability=target_profitability,
                    max_profitability=self.config.max_profitability
                )
                executor_actions.append(CreateExecutorAction(executor_config=config, controller_id=self.config.id))
        for target_profitability, amount in self.sell_levels_targets_amount:
            active_sell_executors_target = [e.config.target_profitability == target_profitability for e in active_sell_executors]
            if len(active_sell_executors_target) == 0 and imbalance > -self.config.max_executors_imbalance:
                config = XEMMExecutorConfig(
                    controller_id=self.config.id,
                    timestamp=time.time(),
                    buying_market=ConnectorPair(connector_name=self.config.taker_connector,
                                                trading_pair=self.config.taker_trading_pair),
                    selling_market=ConnectorPair(connector_name=self.config.maker_connector,
                                                 trading_pair=self.config.maker_trading_pair),
                    maker_side=TradeType.SELL,
                    order_amount=amount / mid_price,
                    min_profitability=self.config.min_profitability,
                    target_profitability=target_profitability,
                    max_profitability=self.config.max_profitability
                )
                executor_actions.append(CreateExecutorAction(executor_config=config, controller_id=self.config.id))
        return executor_actions

    def to_format_status(self) -> List[str]:
        all_executors_custom_info = pd.DataFrame(e.custom_info for e in self.executors_info)
        return [format_df_for_printout(all_executors_custom_info, table_format="psql", )]
