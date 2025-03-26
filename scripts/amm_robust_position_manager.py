import os
from typing import Any, Dict, List, Tuple

from pydantic import Field

from hummingbot.client.config.config_data_types import BaseClientModel
from hummingbot.client.settings import GatewayConnectionSetting
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.gateway.gateway_http_client import GatewayHttpClient
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase

configuration: Dict[str, Any] = {
    "pools": [
        {
            "chain": "solana",
            "network": "mainnet-beta",
            "connector": "raydium",
            "pool_address": "0x1234567890123456789012345678901234567890",
            "base_tokens": ["ETH", "SOL"],
            "quote_tokens": ["USDC", "USDT"]
        },
        {
            "chain": "polkadot",
            "network": "mainnet",
            "connector": "hydration",
            "pool": "0x1234567890123456789012345678901234567890",
            "base_tokens": ["DOT", "HDX"],
            "quote_tokens": ["USDC", "USDT"]
        }
    ]
}

class AMMRobustPositionManagerConfiguration(BaseClientModel):

    script_file_name: str = Field(default_factory=lambda: os.path.basename(__file__))


class AMMRobustPositionManager(ScriptStrategyBase):

    def __init__(self, connectors: Dict[str, ConnectorBase], configuration: AMMRobustPositionManagerConfiguration):
        super().__init__(connectors)

        self.initialize(configuration)

    def initialize(self):
        self.initialize_configuration()

        self.gateway_is_ready = False

        self.log_initialization()

    def initialize_configuration(self, configuration: AMMRobustPositionManagerConfiguration):
        self.configuration = configuration

        for key, value in self.configuration.items():
            setattr(self.configuration, key, value)

    def log_initialization(self):
        self.logger().info(f"Starting {self.__class__.__name__} strategy")

    def on_tick(self):
        safe_ensure_future(self.async_on_tick())

    async def async_on_tick(self):
        await self._check_gateway_status()

    async def _check_gateway_status(self):
        """Check if Gateway server is online and verify wallet connections for multiple pools"""
        # Skip if gateway is already verified as ready
        if not self.gateway_is_ready:
            return
            
        self.logger().info("Checking Gateway server status...")
        try:
            if await GatewayHttpClient.get_instance().ping_gateway():
                self.gateway_is_ready = True

                self.logger().info("Gateway server is online!")

                await self._verify_wallet_connections()
            else:
                self._set_gateway_as_not_ready("Gateway server is offline! Make sure Gateway is running before using this strategy.")
        except Exception as exception:
            self._set_gateway_as_not_ready(f"Error connecting to Gateway server: {str(exception)}")
    
    def _set_gateway_as_not_ready(self, error_message: str):
        """Set gateway as not ready with appropriate error message"""
        self.gateway_is_ready = False
        self.logger().error(error_message)
    
    async def _verify_wallet_connections(self):
        """Verify wallet connections for all configured pools"""
        all_gateway_connections = GatewayConnectionSetting.load()

        if not all_gateway_connections or len(all_gateway_connections) == 0:
            self.logger().error("No wallet connections found. Please connect a wallet using 'gateway connect'.")

            return

        # Get pools configuration
        pools = getattr(self.configuration, "pools", [])
        if not pools or len(pools) == 0:
            self.logger().error("No pools configured. Please add pool configurations.")

            return
            
        for pool in pools:
            await self._verify_pool_wallet(pool, all_gateway_connections)
    
    async def _verify_pool_wallet(self, pool: Dict[str, Any], all_gateway_connections: List[Dict[str, Any]]):
        """Verify wallet connection for a specific pool configuration"""
        chain = pool.get("chain")
        network = pool.get("network")
        connector = pool.get("connector")
        pool_address = pool.get("pool_address")
        base_tokens = pool.get("base_tokens")
        quote_tokens = pool.get("quote_tokens")

        required_fields = {
            "chain": chain,
            "network": network, 
            "connector": connector,
            "pool_address": pool_address,
            "base_tokens": base_tokens,
            "quote_tokens": quote_tokens
        }
        missing_fields = [field for field, value in required_fields.items() if not value]
        
        if missing_fields:
            self.logger().error(f"Invalid pool configuration. Missing required fields: {', '.join(missing_fields)}")
            return

        gateway_connection = [
            gateway_connection for gateway_connection in all_gateway_connections
                 if gateway_connection["chain"] == chain and 
                    gateway_connection["connector"] == connector and 
                    gateway_connection["network"] == network
        ]

        if not gateway_connection:
            self.logger().error(f"""No gateway connection found for "{chain}/{connector}/{network}:{wallet_address}". Please connect using 'gateway connect'.""")
        else:
            wallet_address = gateway_connection[0]["wallet_address"]
            
            self.logger().info(f"""Found wallet connection for "{chain}/{connector}/{network}:{wallet_address}""")
            
            # Store wallet address in pool config for later use
            pool["wallet_address"] = wallet_address
            
            # Get pool info to get token information
            pool["info"] = await self.fetch_pool_info(chain, network, connector, wallet_address, pool_address)

    def format_status(self) -> str:
        return ""
