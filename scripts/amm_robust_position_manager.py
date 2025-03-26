import os
from typing import Any, Dict, List, Optional, Union

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

        self.gateway_is_ready = False

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
            if await GatewayHttpClient.get_instance().ping_gateway():
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
        # AMM Connector Methods (remaining implementations)
        """
        Fetch all available pools for a connector.

        Args:
            chain: Chain identifier
            connector: Connector identifier
            network: Network identifier

        Returns:
            Dictionary containing pools information
        """
        try:
            gateway_http_client = GatewayHttpClient.get_instance()

            response = await gateway_http_client.get_request(
                f"/{connector}/amm/pools",
                params={
                    "network": network
                }
            )

            if response.get("success"):
                self.logger().info(f"Successfully fetched pools for {connector} on {network}")
                return response.get("pools")
            else:
                self.logger().error(f"Failed to fetch pools: {response.get('error')}")
                return None

        except Exception as exception:
            self.logger().error(f"Error fetching pools: {str(exception)}")
            return None

    async def get_pool_information(self, pool: Dict[str, Any]):
        """
        Get detailed information about a liquidity pool.

        Args:
            pool: Dictionary containing pool configuration parameters

        Returns:
            Dictionary containing pool information
        """
        try:
            gateway_http_client = GatewayHttpClient.get_instance()

            chain = pool.get("chain")
            network = pool.get("network")
            connector = pool.get("connector")
            pool_address = pool.get("pool_address")

            if not all([chain, network, connector, pool_address]):
                self.logger().error("Missing required pool parameters for pool information request")
                return None

            response = await gateway_http_client.get_request(
                f"/{connector}/amm/pool-info",
                params={
                    "network": network,
                    "poolAddress": pool_address
                }
            )

            if response.get("success"):
                self.logger().info(f"Successfully retrieved pool information for {pool_address}")
                return response.get("data")
            else:
                self.logger().error(f"Failed to retrieve pool information: {response.get('error')}")
                return None

        except Exception as exception:
            self.logger().error(f"Error retrieving pool information: {str(exception)}")
            return None

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
        try:
            gateway_http_client = GatewayHttpClient.get_instance()

            chain = pool.get("chain")
            network = pool.get("network")
            connector = pool.get("connector")
            pool_address = pool.get("pool_address")

            if not all([chain, network, connector, pool_address]):
                self.logger().error("Missing required pool parameters for quote swap request")
                return None

            response = await gateway_http_client.get_request(
                f"/{connector}/amm/quote-swap",
                params={
                    "network": network,
                    "baseToken": base_token,
                    "quoteToken": quote_token,
                    "amount": amount,
                    "side": side,
                    "poolAddress": pool_address,
                    "slippagePct": slippage_percentage
                }
            )

            if response.get("success"):
                self.logger().info(f"Successfully retrieved swap quote for {amount} {base_token}/{quote_token}")
                return response.get("data")
            else:
                self.logger().error(f"Failed to retrieve swap quote: {response.get('error')}")
                return None

        except Exception as exception:
            self.logger().error(f"Error retrieving swap quote: {str(exception)}")
            return None

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
        try:
            gateway_http_client = GatewayHttpClient.get_instance()

            chain = pool.get("chain")
            network = pool.get("network")
            connector = pool.get("connector")
            pool_address = pool.get("pool_address")

            if not all([chain, network, connector, pool_address]):
                self.logger().error("Missing required pool parameters for quote liquidity request")
                return None

            response = await gateway_http_client.get_request(
                f"/{connector}/amm/quote-liquidity",
                params={
                    "network": network,
                    "poolAddress": pool_address,
                    "baseTokenAmount": base_token_amount,
                    "quoteTokenAmount": quote_token_amount,
                    "slippagePct": slippage_percentage
                }
            )

            if response.get("success"):
                self.logger().info(f"Successfully retrieved liquidity quote for pool {pool_address}")
                return response.get("data")
            else:
                self.logger().error(f"Failed to retrieve liquidity quote: {response.get('error')}")
                return None

        except Exception as exception:
            self.logger().error(f"Error retrieving liquidity quote: {str(exception)}")
            return None

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
        try:
            gateway_http_client = GatewayHttpClient.get_instance()

            chain = pool.get("chain")
            network = pool.get("network")
            connector = pool.get("connector")
            pool_address = pool.get("pool_address")
            wallet_address = pool.get("wallet_address")

            if not all([chain, network, connector, pool_address, wallet_address]):
                self.logger().error("Missing required pool parameters for execute swap request")
                return None

            response = await gateway_http_client.post_request(
                f"/{connector}/amm/execute-swap",
                data={
                    "network": network,
                    "walletAddress": wallet_address,
                    "baseToken": base_token,
                    "quoteToken": quote_token,
                    "amount": amount,
                    "side": side,
                    "poolAddress": pool_address,
                    "slippagePct": slippage_percentage
                }
            )

            if response.get("success"):
                self.logger().info(f"Successfully executed swap of {amount} {base_token}/{quote_token}")
                return response.get("data")
            else:
                self.logger().error(f"Failed to execute swap: {response.get('error')}")
                return None

        except Exception as exception:
            self.logger().error(f"Error executing swap: {str(exception)}")
            return None

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
        try:
            gateway_http_client = GatewayHttpClient.get_instance()

            chain = pool.get("chain")
            network = pool.get("network")
            connector = pool.get("connector")
            pool_address = pool.get("pool_address")
            wallet_address = pool.get("wallet_address")

            if not all([chain, network, connector, pool_address, wallet_address]):
                self.logger().error("Missing required pool parameters for add liquidity request")
                return None

            response = await gateway_http_client.post_request(
                f"/{connector}/amm/add-liquidity",
                data={
                    "network": network,
                    "walletAddress": wallet_address,
                    "poolAddress": pool_address,
                    "baseTokenAmount": base_token_amount,
                    "quoteTokenAmount": quote_token_amount,
                    "slippagePct": slippage_percentage
                }
            )

            if response.get("success"):
                self.logger().info(f"Successfully added liquidity to pool {pool_address}")
                return response.get("data")
            else:
                self.logger().error(f"Failed to add liquidity: {response.get('error')}")
                return None

        except Exception as exception:
            self.logger().error(f"Error adding liquidity: {str(exception)}")
            return None

    async def post_remove_liquidity(self, pool: Dict[str, Any], percentage_to_remove: str):
        """
        Remove liquidity from the specified pool.

        Args:
            pool: Dictionary containing pool configuration parameters
            percentage_to_remove: Percentage of liquidity to remove

        Returns:
            Dictionary containing remove liquidity operation result
        """
        try:
            gateway_http_client = GatewayHttpClient.get_instance()

            chain = pool.get("chain")
            network = pool.get("network")
            connector = pool.get("connector")
            pool_address = pool.get("pool_address")
            wallet_address = pool.get("wallet_address")

            if not all([chain, network, connector, pool_address, wallet_address]):
                self.logger().error("Missing required pool parameters for remove liquidity request")
                return None

            response = await gateway_http_client.post_request(
                f"/{connector}/amm/remove-liquidity",
                data={
                    "network": network,
                    "walletAddress": wallet_address,
                    "poolAddress": pool_address,
                    "percentageToRemove": percentage_to_remove
                }
            )

            if response.get("success"):
                self.logger().info(f"Successfully removed {percentage_to_remove}% liquidity from pool {pool_address}")
                return response.get("data")
            else:
                self.logger().error(f"Failed to remove liquidity: {response.get('error')}")
                return None

        except Exception as exception:
            self.logger().error(f"Error removing liquidity: {str(exception)}")
            return None

    async def get_root_status(self):
        """
        Get the status of the Gateway server.

        Returns:
            Dictionary containing server status information
        """
        try:
            gateway_http_client = GatewayHttpClient.get_instance()

            response = await gateway_http_client.get_request("/")

            if response.get("status") == "ok":
                self.logger().info("Gateway server is running properly")
                return response
            else:
                self.logger().error("Gateway server status check failed")
                return None

        except Exception as exception:
            self.logger().error(f"Error checking Gateway status: {str(exception)}")
            return None

    async def get_config(self, chain_or_connector: Optional[str] = None):
        """
        Get Gateway configuration settings.

        Args:
            chain_or_connector: Optional chain or connector to filter configuration

        Returns:
            Dictionary containing Gateway configuration
        """
        try:
            gateway_http_client = GatewayHttpClient.get_instance()

            params = {}
            if chain_or_connector:
                params["chainOrConnector"] = chain_or_connector

            response = await gateway_http_client.get_request(
                "/config",
                params=params
            )

            if response.get("success"):
                self.logger().info("Successfully retrieved Gateway configuration")
                return response.get("data")
            else:
                self.logger().error(f"Failed to retrieve Gateway configuration: {response.get('error')}")
                return None

        except Exception as exception:
            self.logger().error(f"Error retrieving Gateway configuration: {str(exception)}")
            return None

    async def post_config_update(self, config_path: str, config_value: Any):
        """
        Update Gateway configuration setting.

        Args:
            config_path: Path to the configuration setting
            config_value: New value for the configuration setting

        Returns:
            Dictionary containing operation result
        """
        try:
            gateway_http_client = GatewayHttpClient.get_instance()

            response = await gateway_http_client.post_request(
                "/config/update",
                data={
                    "configPath": config_path,
                    "configValue": config_value
                }
            )

            if response.get("success"):
                self.logger().info(f"Successfully updated Gateway configuration at {config_path}")
                return response.get("data")
            else:
                self.logger().error(f"Failed to update Gateway configuration: {response.get('error')}")
                return None

        except Exception as exception:
            self.logger().error(f"Error updating Gateway configuration: {str(exception)}")
            return None

    async def get_connectors(self):
        """
        Get all available connectors from Gateway.

        Returns:
            Dictionary containing available connectors information
        """
        try:
            gateway_http_client = GatewayHttpClient.get_instance()

            response = await gateway_http_client.get_request("/connectors")

            if response.get("success"):
                self.logger().info("Successfully retrieved connector information")
                return response.get("connectors")
            else:
                self.logger().error(f"Failed to retrieve connector information: {response.get('error')}")
                return None

        except Exception as exception:
            self.logger().error(f"Error retrieving connector information: {str(exception)}")
            return None

    async def get_wallet(self):
        """
        Get wallet information for all connected chains.

        Returns:
            Dictionary containing wallet information
        """
        try:
            gateway_http_client = GatewayHttpClient.get_instance()

            response = await gateway_http_client.get_request("/wallet")

            if response.get("success"):
                self.logger().info("Successfully retrieved wallet information")
                return response.get("wallets")
            else:
                self.logger().error(f"Failed to retrieve wallet information: {response.get('error')}")
                return None

        except Exception as exception:
            self.logger().error(f"Error retrieving wallet information: {str(exception)}")
            return None

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
        try:
            gateway_http_client = GatewayHttpClient.get_instance()

            response = await gateway_http_client.post_request(
                "/wallet/add",
                data={
                    "chain": chain,
                    "network": network,
                    "privateKey": private_key
                }
            )

            if response.get("success"):
                self.logger().info(f"Successfully added wallet for {chain}/{network}")
                return response.get("data")
            else:
                self.logger().error(f"Failed to add wallet: {response.get('error')}")
                return None

        except Exception as exception:
            self.logger().error(f"Error adding wallet: {str(exception)}")
            return None

    async def delete_wallet_remove(self, chain: str, address: str):
        """
        Remove a wallet from Gateway.

        Args:
            chain: Chain identifier
            address: Wallet address to remove

        Returns:
            Dictionary containing operation result
        """
        try:
            gateway_http_client = GatewayHttpClient.get_instance()

            response = await gateway_http_client.delete_request(
                "/wallet/remove",
                data={
                    "chain": chain,
                    "address": address
                }
            )

            if response.get("success"):
                self.logger().info(f"Successfully removed wallet {address} from {chain}")
                return response.get("data")
            else:
                self.logger().error(f"Failed to remove wallet: {response.get('error')}")
                return None

        except Exception as exception:
            self.logger().error(f"Error removing wallet: {str(exception)}")
            return None

    async def get_chain_status(self, chain: str, network: str):
        """
        Get chain status.

        Args:
            chain: Chain identifier
            network: Network identifier

        Returns:
            Dictionary containing chain status information
        """
        try:
            gateway_http_client = GatewayHttpClient.get_instance()

            response = await gateway_http_client.get_request(
                f"/{chain}/status",
                params={
                    "network": network
                }
            )

            if response.get("success"):
                self.logger().info(f"Successfully retrieved status for {chain}/{network}")
                return response.get("data")
            else:
                self.logger().error(f"Failed to retrieve chain status: {response.get('error')}")
                return None

        except Exception as exception:
            self.logger().error(f"Error retrieving chain status: {str(exception)}")
            return None

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
        try:
            gateway_http_client = GatewayHttpClient.get_instance()

            response = await gateway_http_client.post_request(
                f"/{chain}/poll",
                data={
                    "network": network,
                    "txHash": tx_hash
                }
            )

            if response.get("success"):
                self.logger().info(f"Successfully polled transaction {tx_hash} on {chain}/{network}")
                return response.get("data")
            else:
                self.logger().error(f"Failed to poll transaction: {response.get('error')}")
                return None

        except Exception as exception:
            self.logger().error(f"Error polling transaction: {str(exception)}")
            return None

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
        try:
            gateway_http_client = GatewayHttpClient.get_instance()

            params = {"network": network}

            if token_symbols:
                if isinstance(token_symbols, list):
                    params["tokenSymbols"] = ",".join(token_symbols)
                else:
                    params["tokenSymbols"] = token_symbols

            response = await gateway_http_client.get_request(
                f"/{chain}/tokens",
                params=params
            )

            if response.get("success"):
                self.logger().info(f"Successfully retrieved token information for {chain}/{network}")
                return response.get("tokens")
            else:
                self.logger().error(f"Failed to retrieve token information: {response.get('error')}")
                return None

        except Exception as exception:
            self.logger().error(f"Error retrieving token information: {str(exception)}")
            return None

    async def post_chain_balances(self, chain: str, network: str, address: str,
                                  token_symbols: Optional[Union[str, List[str]]] = None):
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
        try:
            gateway_http_client = GatewayHttpClient.get_instance()

            data = {
                "network": network,
                "address": address
            }

            if token_symbols:
                data["tokenSymbols"] = token_symbols

            response = await gateway_http_client.post_request(
                f"/{chain}/balances",
                data=data
            )

            if response.get("success"):
                self.logger().info(f"Successfully retrieved balances for {address} on {chain}/{network}")
                return response.get("balances")
            else:
                self.logger().error(f"Failed to retrieve balances: {response.get('error')}")
                return None

        except Exception as exception:
            self.logger().error(f"Error retrieving balances: {str(exception)}")
            return None

    def format_status(self) -> str:
        return ""
