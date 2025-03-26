import os
from decimal import Decimal
from typing import Any, Dict, List, Optional, Union

from pydantic import Field

from hummingbot.client.config.config_data_types import BaseClientModel
from hummingbot.client.settings import GatewayConnectionSetting
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.event.events import TradeType
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

    gateway_is_ready = False
    gateway_http_client: Optional[GatewayHttpClient] = None

    def __init__(self, connectors: Dict[str, ConnectorBase], configuration: AMMRobustPositionManagerConfiguration):
        super().__init__(connectors)

        AMMRobustPositionManager.gateway_http_client = GatewayHttpClient.get_instance()

        self.initialize(configuration)

    def initialize(self, configuration: AMMRobustPositionManagerConfiguration):
        self.initialize_configuration(configuration)

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
            if await self.gateway_http_client.ping_gateway():
                self.gateway_is_ready = True

                self.logger().info("Gateway server is online!")

                await self._verify_wallet_connections()
            else:
                self._set_gateway_as_not_ready(
                    "Gateway server is offline! Make sure Gateway is running before using this strategy.")
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
            if gateway_connection["chain"] == chain and gateway_connection["connector"] == connector and gateway_connection["network"] == network
        ]

        if not gateway_connection:
            self.logger().error(
                f"""No gateway connection found for "{chain}/{connector}/{network}". Please connect using 'gateway connect'.""")
        else:
            wallet_address = gateway_connection[0]["wallet_address"]
            self.logger().info(f"""Found wallet connection for "{chain}/{connector}/{network}:{wallet_address}""")

            # Store wallet address in pool config for later use
            pool["wallet_address"] = wallet_address

            # Get pool info to get token information
            pool["information"] = await self.get_pool_information(pool)

    async def get_fetch_pools(self, chain: str, connector: str, network: str):
        """
        Fetch all available pools for a connector.

        Args:
            chain: Chain identifier
            connector: Connector identifier
            network: Network identifier

        Returns:
            Dictionary containing pools information
        """
        # Assume there's an amm_fetch_pools method in GatewayHttpClient
        return await self.gateway_http_client.amm_fetch_pools(connector, network)

    async def get_pool_information(self, pool: Dict[str, Any]):
        """
        Get detailed information about a liquidity pool.

        Args:
            pool: Dictionary containing pool configuration parameters

        Returns:
            Dictionary containing pool information
        """
        network = pool.get("network")
        connector = pool.get("connector")
        pool_address = pool.get("pool_address")

        if not all([network, connector, pool_address]):
            return None

        # This would be similar to clmm_pool_info method
        return await self.gateway_http_client.amm_pool_info(connector, network, pool_address)

    async def get_quote_swap(self, pool: Dict[str, Any], base_token: str, quote_token: str,
                             amount: str, side: str, slippage_percentage: str = "0.5"):
        """
        Get a quote for swapping tokens in a pool.

        Args:
            pool: Dictionary containing pool configuration parameters
            base_token: Symbol of the base token
            quote_token: Symbol of the quote token
            amount: Amount to swap
            side: Trade side (BUY or SELL)
            slippage_percentage: Maximum acceptable slippage as a percentage

        Returns:
            Dictionary containing swap quote information
        """
        network = pool.get("network")
        connector = pool.get("connector")
        pool_address = pool.get("pool_address")

        if not all([network, connector, pool_address]):
            return None

        trade_type = TradeType.BUY if side.upper() == "BUY" else TradeType.SELL

        return await self.gateway_http_client.quote_swap(
            network=network,
            connector=connector,
            base_asset=base_token,
            quote_asset=quote_token,
            amount=Decimal(amount),
            side=trade_type,
            slippage_pct=Decimal(slippage_percentage) if slippage_percentage else None,
            pool_address=pool_address
        )

    async def get_quote_liquidity(self, pool: Dict[str, Any], base_token_amount: str,
                                  quote_token_amount: str, slippage_percentage: str = "0.5"):
        """
        Get a quote for adding liquidity to a pool.

        Args:
            pool: Dictionary containing pool configuration parameters
            base_token_amount: Amount of base token to add
            quote_token_amount: Amount of quote token to add
            slippage_percentage: Maximum acceptable slippage as a percentage

        Returns:
            Dictionary containing liquidity quote information
        """
        network = pool.get("network")
        connector = pool.get("connector")
        pool_address = pool.get("pool_address")

        if not all([network, connector, pool_address]):
            return None

        # Assume there's an amm_quote_liquidity method in GatewayHttpClient
        return await self.gateway_http_client.amm_quote_liquidity(
            connector=connector,
            network=network,
            pool_address=pool_address,
            base_token_amount=base_token_amount,
            quote_token_amount=quote_token_amount,
            slippage_pct=slippage_percentage
        )

    async def post_execute_swap(self, pool: Dict[str, Any], base_token: str, quote_token: str,
                                amount: str, side: str, slippage_percentage: str = "0.5"):
        """
        Execute a token swap in the specified pool.

        Args:
            pool: Dictionary containing pool configuration parameters
            base_token: Symbol of the base token
            quote_token: Symbol of the quote token
            amount: Amount to swap
            side: Trade side (BUY or SELL)
            slippage_percentage: Maximum acceptable slippage as a percentage

        Returns:
            Dictionary containing swap execution result
        """
        network = pool.get("network")
        connector = pool.get("connector")
        pool_address = pool.get("pool_address")
        wallet_address = pool.get("wallet_address")

        if not all([network, connector, pool_address, wallet_address]):
            return None

        trade_type = TradeType.BUY if side.upper() == "BUY" else TradeType.SELL

        return await self.gateway_http_client.execute_swap(
            network=network,
            connector=connector,
            address=wallet_address,
            base_asset=base_token,
            quote_asset=quote_token,
            side=trade_type,
            amount=Decimal(amount),
            slippage_pct=Decimal(slippage_percentage) if slippage_percentage else None,
            pool_address=pool_address
        )

    async def post_add_liquidity(self, pool: Dict[str, Any], base_token_amount: str,
                                 quote_token_amount: str, slippage_percentage: str = "0.5"):
        """
        Add liquidity to the specified pool.

        Args:
            pool: Dictionary containing pool configuration parameters
            base_token_amount: Amount of base token to add
            quote_token_amount: Amount of quote token to add
            slippage_percentage: Maximum acceptable slippage as a percentage

        Returns:
            Dictionary containing add liquidity operation result
        """
        network = pool.get("network")
        connector = pool.get("connector")
        pool_address = pool.get("pool_address")
        wallet_address = pool.get("wallet_address")

        if not all([network, connector, pool_address, wallet_address]):
            return None

        # Assume there's an amm_add_liquidity method in GatewayHttpClient
        return await self.gateway_http_client.amm_add_liquidity(
            connector=connector,
            network=network,
            wallet_address=wallet_address,
            pool_address=pool_address,
            base_token_amount=base_token_amount,
            quote_token_amount=quote_token_amount,
            slippage_pct=slippage_percentage
        )

    async def post_remove_liquidity(self, pool: Dict[str, Any], percentage_to_remove: str):
        """
        Remove liquidity from the specified pool.

        Args:
            pool: Dictionary containing pool configuration parameters
            percentage_to_remove: Percentage of liquidity to remove

        Returns:
            Dictionary containing remove liquidity operation result
        """
        network = pool.get("network")
        connector = pool.get("connector")
        pool_address = pool.get("pool_address")
        wallet_address = pool.get("wallet_address")

        if not all([network, connector, pool_address, wallet_address]):
            return None

        # Assume there's an amm_remove_liquidity method in GatewayHttpClient
        return await self.gateway_http_client.amm_remove_liquidity(
            connector=connector,
            network=network,
            wallet_address=wallet_address,
            pool_address=pool_address,
            percentage_to_remove=percentage_to_remove
        )

    async def get_root_status(self):
        """
        Get the status of the Gateway server.

        Returns:
            Dictionary containing server status information
        """
        return await self.gateway_http_client.get_gateway_status()

    async def get_config(self, chain_or_connector: Optional[str] = None):
        """
        Get Gateway configuration settings.

        Args:
            chain_or_connector: Optional chain or connector to filter configuration

        Returns:
            Dictionary containing Gateway configuration
        """
        return await self.gateway_http_client.get_configuration(chain_or_connector)

    async def post_config_update(self, config_path: str, config_value: Any):
        """
        Update Gateway configuration setting.

        Args:
            config_path: Path to the configuration setting
            config_value: New value for the configuration setting

        Returns:
            Dictionary containing operation result
        """
        return await self.gateway_http_client.update_config(config_path, config_value)

    async def get_connectors(self):
        """
        Get all available connectors from Gateway.

        Returns:
            Dictionary containing available connectors information
        """
        return await self.gateway_http_client.get_connectors()

    async def get_wallet(self):
        """
        Get wallet information for all connected chains.

        Returns:
            Dictionary containing wallet information
        """
        return await self.gateway_http_client.get_wallets()

    async def post_wallet_add(self, chain: str, network: str, private_key: str):
        """
        Add a wallet to Gateway.

        Args:
            chain: Chain identifier
            network: Network identifier
            private_key: Private key or mnemonic for the wallet

        Returns:
            Dictionary containing operation result
        """
        return await self.gateway_http_client.add_wallet(chain, network, private_key)

    async def delete_wallet_remove(self, chain: str, address: str):
        """
        Remove a wallet from Gateway.

        Args:
            chain: Chain identifier
            address: Wallet address to remove

        Returns:
            Dictionary containing operation result
        """
        # Assuming there's a method to remove wallet in GatewayHttpClient or will be added
        return await self.gateway_http_client.remove_wallet(chain, address)

    async def get_chain_status(self, chain: str, network: str):
        """
        Get chain status.

        Args:
            chain: Chain identifier
            network: Network identifier

        Returns:
            Dictionary containing chain status information
        """
        return await self.gateway_http_client.get_network_status(chain, network)

    async def post_chain_poll(self, chain: str, network: str, tx_hash: str):
        """
        Poll for transaction status.

        Args:
            chain: Chain identifier
            network: Network identifier
            tx_hash: Transaction hash to poll

        Returns:
            Dictionary containing transaction status
        """
        return await self.gateway_http_client.get_transaction_status(chain, network, tx_hash)

    async def get_chain_tokens(self, chain: str, network: str, token_symbols: Optional[Union[str, List[str]]] = None):
        """
        Get token information.

        Args:
            chain: Chain identifier
            network: Network identifier
            token_symbols: Optional token symbols to filter

        Returns:
            Dictionary containing token information
        """
        return await self.gateway_http_client.get_tokens(chain, network, token_symbols)

    async def post_chain_balances(self, chain: str, network: str, address: str, token_symbols: Optional[Union[str, List[str]]] = None):
        """
        Get token balances for a wallet address.

        Args:
            chain: Chain identifier
            network: Network identifier
            address: Wallet address
            token_symbols: Optional token symbols to filter

        Returns:
            Dictionary containing token balances
        """
        return await self.gateway_http_client.get_balances(chain, network, address, token_symbols)

    def format_status(self) -> str:
        return ""
