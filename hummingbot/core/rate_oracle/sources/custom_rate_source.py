from decimal import Decimal
from typing import Dict, Optional

from hummingbot.core.rate_oracle.sources.rate_source_base import RateSourceBase


class CustomRateSource(RateSourceBase):
    def __init__(self):
        super().__init__()
        self._prices = {
            "HDX-USDT": Decimal("0.0095"),
            "HDX-USDC": Decimal("0.0095"),
            "SOL-USDT": Decimal("239.60"),
            "SOL-USDC": Decimal("239.60"),
        }

    @property
    def name(self) -> str:
        return "custom"

    async def start(self):
        pass

    async def stop(self):
        pass

    async def get_prices(self, quote_token: Optional[str] = None) -> Dict[str, Decimal]:
        return self._prices
