import logging
from decimal import Decimal
from typing import Dict

import pandas as pd

from hummingbot.client.ui.interface_utils import format_df_for_printout
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.data_feed.amm_gateway_data_feed import AmmGatewayDataFeed
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase


class AMMDataFeedExample(ScriptStrategyBase):
    amm_data_feed_hydration = AmmGatewayDataFeed(
        connector_chain_network="hydration/amm_hydration_mainnet",
        trading_pairs={"HDX-DOT", "HDX-USDT", "DOT-USDT"},
        order_amount_in_base=Decimal("1"),
    )
    markets = {}

    def __init__(self, connectors: Dict[str, ConnectorBase]):
        super().__init__(connectors)
        try:
            self.amm_data_feed_hydration.start()
            self.log_with_clock(logging.INFO, "AMM Data Feed started successfully")
        except Exception as e:
            self.log_with_clock(logging.ERROR, f"Failed to start AMM Data Feed: {e}")

    async def on_stop(self):
        self.amm_data_feed_hydration.stop()

    def on_tick(self):
        pass

    def format_status(self) -> str:
        if self.amm_data_feed_hydration.is_ready():
            try:
                price_dict = self.amm_data_feed_hydration.price_dict or {}
                if not price_dict:
                    return "AMM Data Feed is ready. No prices yet."

                rows = []
                for token, price in price_dict.items():
                    # Normalize each row to a dict with a 'pair' column
                    row: Dict[str, object] = {"pair": token}
                    if isinstance(price, dict):
                        row.update(price)
                    else:
                        # Fallback: represent non-dict price as a value column
                        row.update({"price": price})
                    rows.append(row)

                if not rows:
                    return "AMM Data Feed is ready. No price rows to display."

                df = pd.DataFrame.from_records(rows)
                # Limit number of rows/cols for CLI stability
                max_rows = 50
                max_cols = 8
                if len(df) > max_rows:
                    df = df.head(max_rows)
                if df.shape[1] > max_cols:
                    keep_cols = ["pair"] + [c for c in df.columns if c != "pair"][: max_cols - 1]
                    df = df[keep_cols]

                prices_str = format_df_for_printout(df, table_format="psql")
                return f"AMM Data Feed is ready.\n{prices_str}"
            except Exception as e:
                return f"AMM Data Feed is ready. Failed to render prices: {e}"
        else:
            return "AMM Data Feed is not ready."
