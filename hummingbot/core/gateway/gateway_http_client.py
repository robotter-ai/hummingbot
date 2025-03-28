import re
import ssl
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

import aiohttp
from aiohttp import ContentTypeError  # type: ignore

from hummingbot.client.config.security import Security  # type: ignore
from hummingbot.connector.gateway.common_types import ConnectorType, get_connector_type  # type: ignore
from hummingbot.core.event.events import TradeType  # type: ignore
from hummingbot.logger import HummingbotLogger  # type: ignore

if TYPE_CHECKING:
    from hummingbot.client.config.config_helpers import ClientConfigAdapter  # type: ignore


class GatewayError(Enum):
    """
    The gateway route error codes defined in /gateway/src/services/error-handler.ts
    """

    Network = 1001
    RateLimit = 1002
    OutOfGas = 1003
    TransactionGasPriceTooLow = 1004
    LoadWallet = 1005
    TokenNotSupported = 1006
    TradeFailed = 1007
    SwapPriceExceedsLimitPrice = 1008
    SwapPriceLowerThanLimitPrice = 1009
    ServiceUnitialized = 1010
    UnknownChainError = 1011
    InvalidNonceError = 1012
    PriceFailed = 1013
    UnknownError = 1099
    InsufficientBaseBalance = 1022
    InsufficientQuoteBalance = 1023
    SimulationError = 1024
    SwapRouteFetchError = 1025


def _transform_connector_route(connector: str) -> str:
    if "_" in connector:
        main, sub = connector.split("_", 1)
        return f"{main}/{sub}"
    return connector


class GatewayHttpClient:
    """
    An HTTP client for making requests to the gateway API.
    """

    _ghc_logger: Optional[HummingbotLogger] = None
    _shared_client: Optional[aiohttp.ClientSession] = None
    _base_url: str

    __instance = None

    @staticmethod
    def get_instance(client_config_map: Optional["ClientConfigAdapter"] = None) -> "GatewayHttpClient":
        if GatewayHttpClient.__instance is None:
            GatewayHttpClient(client_config_map)
        return GatewayHttpClient.__instance

    def __init__(self, client_config_map: Optional["ClientConfigAdapter"] = None):
        if client_config_map is None:
            from hummingbot.client.hummingbot_application import HummingbotApplication  # type: ignore
            client_config_map = HummingbotApplication.main_application().client_config_map
        api_host = client_config_map.gateway.gateway_api_host
        api_port = client_config_map.gateway.gateway_api_port
        if GatewayHttpClient.__instance is None:
            self._base_url = f"https://{api_host}:{api_port}"
        self._client_config_map = client_config_map
        GatewayHttpClient.__instance = self

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._ghc_logger is None:
            cls._ghc_logger = HummingbotLogger(__name__)
        return cls._ghc_logger

    @classmethod
    def _http_client(cls, client_config_map: "ClientConfigAdapter", re_init: bool = False) -> aiohttp.ClientSession:
        """
        :returns Shared client session instance
        """
        if cls._shared_client is None or re_init:
            cert_path = client_config_map.certs_path
            ssl_ctx = ssl.create_default_context(cafile=f"{cert_path}/ca_cert.pem")
            ssl_ctx.load_cert_chain(
                certfile=f"{cert_path}/client_cert.pem",
                keyfile=f"{cert_path}/client_key.pem",
                password=Security.secrets_manager.password.get_secret_value()
            )
            conn = aiohttp.TCPConnector(ssl_context=ssl_ctx)
            cls._shared_client = aiohttp.ClientSession(connector=conn)
        return cls._shared_client

    @classmethod
    def reload_certs(cls, client_config_map: "ClientConfigAdapter"):
        """
        Re-initializes the aiohttp.ClientSession. This should be called whenever there is any updates to the
        Certificates used to secure a HTTPS connection to the Gateway service.
        """
        cls._http_client(client_config_map, re_init=True)

    @property
    def base_url(self) -> str:
        return self._base_url

    @base_url.setter
    def base_url(self, url: str):
        self._base_url = url

    def log_error_codes(self, resp: Dict[str, Any]):
        """
        If the API returns an error code, interpret the code, log a useful
        message to the user, then raise an exception.
        """
        error_code: Optional[int] = resp.get("errorCode") if isinstance(resp, dict) else None
        if error_code is not None:
            if error_code == GatewayError.Network.value:
                self.logger().network(
                    "Gateway had a network error. Make sure it is still able to communicate with the node.")
            elif error_code == GatewayError.RateLimit.value:
                self.logger().network("Gateway was unable to communicate with the node because of rate limiting.")
            elif error_code == GatewayError.OutOfGas.value:
                self.logger().network("There was an out of gas error. Adjust the gas limit in the gateway config.")
            elif error_code == GatewayError.TransactionGasPriceTooLow.value:
                self.logger().network(
                    "The gas price provided by gateway was too low to create a blockchain operation. Consider increasing the gas price.")
            elif error_code == GatewayError.LoadWallet.value:
                self.logger().network(
                    "Gateway failed to load your wallet. Try running 'gateway connect' with the correct wallet settings.")
            elif error_code == GatewayError.TokenNotSupported.value:
                self.logger().network("Gateway tried to use an unsupported token.")
            elif error_code == GatewayError.TradeFailed.value:
                self.logger().network("The trade on gateway has failed.")
            elif error_code == GatewayError.PriceFailed.value:
                self.logger().network("The price query on gateway has failed.")
            elif error_code == GatewayError.InvalidNonceError.value:
                self.logger().network("The nonce was invalid.")
            elif error_code == GatewayError.ServiceUnitialized.value:
                self.logger().network("Some values was uninitialized. Please contact dev@hummingbot.io ")
            elif error_code == GatewayError.SwapPriceExceedsLimitPrice.value:
                self.logger().network(
                    "The swap price is greater than your limit buy price. The market may be too volatile or your slippage rate is too low. Try adjusting the strategy's allowed slippage rate.")
            elif error_code == GatewayError.SwapPriceLowerThanLimitPrice.value:
                self.logger().network(
                    "The swap price is lower than your limit sell price. The market may be too volatile or your slippage rate is too low. Try adjusting the strategy's allowed slippage rate.")
            elif error_code == GatewayError.UnknownChainError.value:
                self.logger().network(
                    "An unknown chain error has occurred on gateway. Make sure your gateway settings are correct.")
            elif error_code == GatewayError.InsufficientBaseBalance.value:
                self.logger().network("Insufficient base token balance needed to execute the trade.")
            elif error_code == GatewayError.InsufficientQuoteBalance.value:
                self.logger().network("Insufficient quote token balance needed to execute the trade.")
            elif error_code == GatewayError.SimulationError.value:
                self.logger().network("Transaction simulation failed.")
            elif error_code == GatewayError.SwapRouteFetchError.value:
                self.logger().network("Failed to fetch swap route.")
            elif error_code == GatewayError.UnknownError.value:
                self.logger().network(
                    "An unknown error has occurred on gateway. Please send your logs to operations@hummingbot.org.")
            else:
                self.logger().network(
                    "An unknown error has occurred on gateway. Please send your logs to operations@hummingbot.org.")

    @staticmethod
    def is_timeout_error(e) -> bool:
        """
        It is hard to consistently return a timeout error from gateway
        because it uses many different libraries to communicate with the
        chains with their own idiosyncracies and they do not necessarilly
        return HTTP status code 504 when there is a timeout error. It is
        easier to rely on the presence of the word 'timeout' in the error.
        """
        error_string = str(e)
        if re.search('timeout', error_string, re.IGNORECASE):
            return True
        return False

    async def api_request(
            self,
            method: str,
            path_url: str,
            params=None,
            fail_silently: bool = False,
            use_body: bool = False,
    ) -> Optional[Union[Dict[str, Any], List[Dict[str, Any]]]]:
        """
        Sends an aiohttp request and waits for a response.
        :param method: The HTTP method, e.g. get or post
        :param path_url: The path url or the API end point
        :param params: A dictionary of required params for the end point
        :param fail_silently: used to determine if errors will be raise or silently ignored
        :param use_body: used to determine if the request should sent the parameters in the body or as query string
        :returns A response in json format.
        """
        if params is None:
            params = {}
        url = f"{self.base_url}/{path_url}"
        client = self._http_client(self._client_config_map)

        parsed_response = {}
        try:
            if method == "get":
                if len(params) > 0:
                    if use_body:
                        response = await client.get(url, json=params)
                    else:
                        response = await client.get(url, params=params)
                else:
                    response = await client.get(url)
            elif method == "post":
                response = await client.post(url, json=params)
            elif method == 'put':
                response = await client.put(url, json=params)
            elif method == 'delete':
                response = await client.delete(url, json=params)
            else:
                raise ValueError(f"Unsupported request method {method}")
            if not fail_silently and response.status == 504:
                self.logger().network(f"The network call to {url} has timed out.")
            else:
                try:
                    parsed_response = await response.json()
                except ContentTypeError:
                    parsed_response = await response.text()
                if response.status != 200 and \
                        not fail_silently and \
                        not self.is_timeout_error(parsed_response):
                    self.log_error_codes(parsed_response)

                    if isinstance(parsed_response, dict) and "error" in parsed_response:
                        raise ValueError(f"Error on {method.upper()} {url} Error: {parsed_response['error']}")
                    else:
                        raise ValueError(f"Error on {method.upper()} {url} Error: {parsed_response}")

        except Exception as e:
            if not fail_silently:
                if self.is_timeout_error(e):
                    self.logger().network(f"The network call to {url} has timed out.")
                else:
                    self.logger().network(
                        'e',
                        exc_info=True,
                        app_warning_msg=f"Call to {url} failed. See logs for more details."
                    )
                raise e

        return parsed_response

    async def ping_gateway(self) -> bool:
        try:
            response: Dict[str, Any] = await self.api_request("get", "", fail_silently=True)
            return response["status"] == "ok"
        except (KeyError, TypeError):
            return False
        except aiohttp.ClientError:
            return False

    async def get_root(self) -> Dict[str, Any]:
        """
        Gets the root endpoint information from the gateway.
        Returns basic information about the gateway service.
        """
        return await self.api_request("get", "")

    async def get_gateway_status(self, fail_silently: bool = False) -> dict[str, Any] | list[dict[str, Any]] | None:
        """
        Calls the status endpoint on Gateway to know basic info about connected networks.
        """
        try:
            return await self.get_network_status(fail_silently=fail_silently)
        except Exception as e:
            self.logger().network(
                "Error fetching gateway status info",
                exc_info=True,
                app_warning_msg=str(e)
            )

    async def update_config(self, config_path: str, config_value: Any) -> Dict[str, Any]:
        response = await self.api_request("post", "config/update", {
            "configPath": config_path,
            "configValue": config_value,
        })
        self.logger().info("Detected change to Gateway config - restarting Gateway...", exc_info=False)
        await self.post_restart()
        return response

    async def post_restart(self):
        await self.api_request("post", "restart", fail_silently=False)

    async def get_connectors(self, fail_silently: bool = False) -> Dict[str, Any]:
        return await self.api_request("get", "connectors", fail_silently=fail_silently)

    async def get_wallets(self, fail_silently: bool = False) -> List[Dict[str, Any]]:
        return await self.api_request("get", "wallet", fail_silently=fail_silently)

    async def add_wallet(self, chain: str, network: str, private_key: str, **kwargs) -> Dict[str, Any]:
        request = {"chain": chain, "network": network, "privateKey": private_key}
        request.update(kwargs)
        return await self.api_request(method="post", path_url="wallet/add", params=request)

    async def remove_wallet(self, chain: str, address: str) -> Dict[str, Any]:
        """
        Remove a wallet from the Gateway.
        :param chain: The blockchain network (e.g., "solana", "ethereum")
        :param address: The wallet address to remove
        :return: Response from the Gateway API
        """
        url = f"{self.base_url}/wallet/remove"
        data = {"chain": chain, "address": address}

        return await self.api_request("DELETE", url, data)

    async def get_configuration(self, chain: str = None, connector: str = None, fail_silently: bool = False) -> Dict[str, Any]:
        params = {"chainOrConnector": chain} if chain is not None or connector is not None else {}
        return await self.api_request("get", "config", params=params, fail_silently=fail_silently)

    async def get_balances(
            self,
            chain: str,
            network: str,
            address: str,
            token_symbols: List[str],
            fail_silently: bool = False,
    ) -> Dict[str, Any]:
        if isinstance(token_symbols, list):
            token_symbols = [x for x in token_symbols if isinstance(x, str) and x.strip() != '']
            request_params = {
                "network": network,
                "address": address,
                "tokenSymbols": token_symbols
            }
            return await self.api_request(
                method="post",
                path_url=f"{chain}/balances",
                params=request_params,
                fail_silently=fail_silently,
            )
        else:
            return {}

    async def get_tokens(
            self,
            chain: str,
            network: str,
            _token_symbols: Optional[Union[str, List[str]]] = None,
            fail_silently: bool = True
    ) -> Dict[str, Any]:
        return await self.api_request("get", f"{chain}/tokens", {
            "network": network
        }, fail_silently=fail_silently)

    async def get_chain_token_info(
            self,
            chain: str,
            network: str,
            token_symbol: str,
            fail_silently: bool = False
    ) -> Dict[str, Any]:
        """
        Gets detailed information about a specific token on a chain.

        :param chain: The blockchain network (e.g., "solana", "ethereum")
        :param network: The network to use (e.g., "mainnet-beta")
        :param token_symbol: The symbol of the token to query (e.g., "SOL", "ETH")
        :param fail_silently: Whether to fail silently on error
        :return: Detailed token information including address, decimals, and other metadata
        """
        params = {
            "network": network,
            "tokenSymbols": token_symbol
        }

        return await self.api_request(
            "get",
            f"{chain}/tokens",
            params=params,
            fail_silently=fail_silently
        )

    async def get_chain_tokens(
            self,
            chain: str,
            network: str,
            fail_silently: bool = False
    ) -> Dict[str, Any]:
        """
        Gets information about all tokens on a specific chain and network.

        :param chain: The blockchain network (e.g., "solana", "ethereum")
        :param network: The network to use (e.g., "mainnet-beta")
        :param fail_silently: Whether to fail silently on error
        :return: Dictionary containing information about all tokens on the chain
        """
        params = {
            "network": network
        }

        return await self.api_request(
            "get",
            f"{chain}/tokens",
            params=params,
            fail_silently=fail_silently
        )

    async def get_chain_multiple_tokens(
            self,
            chain: str,
            network: str,
            native_token_symbol: str,
            base_token_symbol: str,
            quote_token_symbol: str,
            fail_silently: bool = False
    ) -> Dict[str, Any]:
        """
        Gets information about multiple tokens on a chain using the native, base and quote token symbols.

        :param chain: The blockchain network (e.g., "solana", "ethereum")
        :param network: The network to use (e.g., "mainnet-beta")
        :param native_token_symbol: The native token symbol (e.g., "SOL", "ETH")
        :param base_token_symbol: The base token symbol (e.g., "USDC")
        :param quote_token_symbol: The quote token symbol (e.g., "USDT")
        :param fail_silently: Whether to fail silently on error
        :return: Dictionary containing information about the specified tokens
        """
        # Join token symbols with comma for the API request
        token_symbols = f"{native_token_symbol},{base_token_symbol},{quote_token_symbol}"

        params = {
            "network": network,
            "tokenSymbols": token_symbols
        }

        return await self.api_request(
            "get",
            f"{chain}/tokens",
            params=params,
            fail_silently=fail_silently
        )

    async def get_network_status(
            self,
            chain: str = None,
            network: str = None,
            fail_silently: bool = False
    ) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        req_data: Dict[str, str] = {}
        if chain is not None and network is not None:
            req_data["network"] = network
            return await self.api_request("get", f"{chain}/status", req_data, fail_silently=fail_silently)
        return await self.api_request("get", "network/status", req_data, fail_silently=fail_silently)  # Default endpoint when chain is None

    async def approve_token(
            self,
            network: str,
            address: str,
            token: str,
            spender: str,
            nonce: Optional[int] = None,
            max_fee_per_gas: Optional[int] = None,
            max_priority_fee_per_gas: Optional[int] = None
    ) -> Dict[str, Any]:
        request_payload: Dict[str, Any] = {
            "network": network,
            "address": address,
            "token": token,
            "spender": spender
        }
        if nonce is not None:
            request_payload["nonce"] = nonce
        if max_fee_per_gas is not None:
            request_payload["maxFeePerGas"] = str(max_fee_per_gas)
        if max_priority_fee_per_gas is not None:
            request_payload["maxPriorityFeePerGas"] = str(max_priority_fee_per_gas)
        return await self.api_request(
            "post",
            "ethereum/approve",
            request_payload
        )

    async def get_allowances(
            self,
            # chain: str,
            network: str,
            address: str,
            token_symbols: List[str],
            spender: str,
            fail_silently: bool = False
    ) -> Dict[str, Any]:
        return await self.api_request("post", "ethereum/allowances", {
            "network": network,
            "address": address,
            "tokenSymbols": token_symbols,
            "spender": spender
        }, fail_silently=fail_silently)

    async def get_transaction_status(
            self,
            chain: str,
            network: str,
            transaction_hash: str,
            fail_silently: bool = False
    ) -> Dict[str, Any]:
        request = {
            "network": network,
            "txHash": transaction_hash
        }
        return await self.api_request("post", f"{chain}/poll", request, fail_silently=fail_silently)

    async def wallet_sign(
            self,
            chain: str,
            network: str,
            address: str,
            message: str,
    ) -> Dict[str, Any]:
        request = {
            "chain": chain,
            "network": network,
            "address": address,
            "message": message,
        }
        return await self.api_request("get", "wallet/sign", request)

    async def get_evm_nonce(
            self,
            # chain: str,
            network: str,
            address: str,
            fail_silently: bool = False
    ) -> Dict[str, Any]:
        return await self.api_request("post", "ethereum/nextNonce", {
            "network": network,
            "address": address
        }, fail_silently=fail_silently)

    async def cancel_evm_transaction(
            self,
            # chain: str,
            network: str,
            address: str,
            nonce: int
    ) -> Dict[str, Any]:
        return await self.api_request("post", "ethereum/cancel", {
            "network": network,
            "address": address,
            "nonce": nonce
        })

    async def amm_quote_swap(
        self,
        network: str,
        connector: str,
        base_asset: str,
        quote_asset: str,
        amount: Decimal,
        side: TradeType,
        slippage_pct: Optional[Decimal] = None,
        pool_address: Optional[str] = None,
        fail_silently: bool = False,
    ) -> Dict[str, Any]:
        if side not in [TradeType.BUY, TradeType.SELL]:
            raise ValueError("Only BUY and SELL prices are supported.")

        connector_type = get_connector_type(connector)

        request_payload = {
            "network": network,
            "baseToken": base_asset,
            "quoteToken": quote_asset,
            "amount": float(amount),
            "side": side.name
        }
        if slippage_pct is not None:
            request_payload["slippagePct"] = float(slippage_pct)
        if connector_type in (ConnectorType.CLMM, ConnectorType.AMM) and pool_address is not None:
            request_payload["poolAddress"] = pool_address

        return await self.api_request(
            "get",
            f"{connector}/amm/quote-swap",
            request_payload,
            fail_silently=fail_silently
        )

    async def clmm_quote_swap(
            self,
            network: str,
            connector: str,
            base_asset: str,
            quote_asset: str,
            amount: Decimal,
            side: TradeType,
            slippage_pct: Optional[Decimal] = None,
            pool_address: Optional[str] = None,
            fail_silently: bool = False,
    ) -> Dict[str, Any]:
        if side not in [TradeType.BUY, TradeType.SELL]:
            raise ValueError("Only BUY and SELL prices are supported.")

        connector_type = get_connector_type(connector)

        request_payload = {
            "network": network,
            "baseToken": base_asset,
            "quoteToken": quote_asset,
            "amount": float(amount),
            "side": side.name
        }
        if slippage_pct is not None:
            request_payload["slippagePct"] = float(slippage_pct)
        if connector_type in (ConnectorType.CLMM, ConnectorType.AMM) and pool_address is not None:
            request_payload["poolAddress"] = pool_address

        return await self.api_request(
            "get",
            f"{connector}/amm/quote-swap",
            request_payload,
            fail_silently=fail_silently
        )

    async def clmm_execute_swap(
        self,
        network: str,
        connector: str,
        address: str,
        base_asset: str,
        quote_asset: str,
        side: TradeType,
        amount: Decimal,
        slippage_pct: Optional[Decimal] = None,
        pool_address: Optional[str] = None,
        # limit_price: Optional[Decimal] = None,
        nonce: Optional[int] = None,
    ) -> Dict[str, Any]:
        if side not in [TradeType.BUY, TradeType.SELL]:
            raise ValueError("Only BUY and SELL prices are supported.")

        connector_type = get_connector_type(connector)

        request_payload: Dict[str, Any] = {
            "network": network,
            "walletAddress": address,
            "baseToken": base_asset,
            "quoteToken": quote_asset,
            "amount": float(amount),
            "side": side.name,
        }
        if slippage_pct is not None:
            request_payload["slippagePct"] = float(slippage_pct)
        # if limit_price is not None:
        #     request_payload["limitPrice"] = float(limit_price)
        if nonce is not None:
            request_payload["nonce"] = int(nonce)
        if connector_type in (ConnectorType.CLMM, ConnectorType.AMM) and pool_address is not None:
            request_payload["poolAddress"] = pool_address
        return await self.api_request(
            "post",
            f"{connector}/clmm/execute-swap",
            request_payload
        )

    async def amm_execute_swap(
        self,
        network: str,
        connector: str,
        address: str,
        base_asset: str,
        quote_asset: str,
        side: TradeType,
        amount: Decimal,
        slippage_pct: Optional[Decimal] = None,
        pool_address: Optional[str] = None,
        # limit_price: Optional[Decimal] = None,
        nonce: Optional[int] = None,
    ) -> Dict[str, Any]:
        if side not in [TradeType.BUY, TradeType.SELL]:
            raise ValueError("Only BUY and SELL prices are supported.")

        connector_type = get_connector_type(connector)

        request_payload: Dict[str, Any] = {
            "network": network,
            "walletAddress": address,
            "baseToken": base_asset,
            "quoteToken": quote_asset,
            "amount": float(amount),
            "side": side.name,
        }
        if slippage_pct is not None:
            request_payload["slippagePct"] = float(slippage_pct)
        # if limit_price is not None:
        #     request_payload["limitPrice"] = float(limit_price)
        if nonce is not None:
            request_payload["nonce"] = int(nonce)
        if connector_type in (ConnectorType.CLMM, ConnectorType.AMM) and pool_address is not None:
            request_payload["poolAddress"] = pool_address
        return await self.api_request(
            "post",
            f"{connector}/execute-swap",
            request_payload
        )

    async def estimate_gas(
            self,
            chain: str,
            network: str,
            gas_limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        return await self.api_request("post", f"{chain}/estimate-gas", {
            "chain": chain,
            "network": network,
            "gasLimit": gas_limit
        })

    async def amm_pool_info(
            self,
            connector: str,
            network: str,
            pool_address: str,
            fail_silently: bool = False
    ) -> Dict[str, Any]:
        """
        Gets information about a regular AMM pool
        :param connector: The connector/protocol (e.g., "hydration")
        :param network: The network to use (e.g., "mainnet")
        :param pool_address: The address of the pool
        :param fail_silently: Whether to fail silently on error
        :return: Pool information including price, liquidity, reserves, and fees
        """
        query_params = {
            "network": network,
            "poolAddress": pool_address,
        }
        return await self.api_request(
            "get",
            f"{connector}/clmm/pool-info",
            params=query_params,
            fail_silently=fail_silently,
        )

    async def amm_add_liquidity(
            self,
            connector: str,
            network: str,
            wallet_address: str,
            pool_address: str,
            base_token_amount: Optional[float] = None,
            quote_token_amount: Optional[float] = None,
            slippage_pct: float = 0.5,
            fail_silently: bool = False
    ) -> Dict[str, Any]:
        """
        Adds liquidity to a regular AMM pool
        :param connector: The connector/protocol (e.g., "hydration")
        :param network: The network to use (e.g., "mainnet")
        :param wallet_address: The wallet address adding liquidity
        :param pool_address: The address of the pool
        :param base_token_amount: The amount of base token to add (optional)
        :param quote_token_amount: The amount of quote token to add (optional)
        :param slippage_pct: Allowed slippage percentage (default: 0.5%)
        :param fail_silently: Whether to fail silently on error
        :return: Details of the liquidity addition including amounts and fees
        """
        request_payload = {
            "network": network,
            "walletAddress": wallet_address,
            "poolAddress": pool_address,
            "slippagePct": slippage_pct,
        }

        if base_token_amount is not None:
            request_payload["baseTokenAmount"] = base_token_amount
        if quote_token_amount is not None:
            request_payload["quoteTokenAmount"] = quote_token_amount

        return await self.api_request(
            "post",
            f"{connector}/amm/add-liquidity",
            request_payload,
            fail_silently=fail_silently,
        )

    async def amm_remove_liquidity(
            self,
            connector: str,
            network: str,
            wallet_address: str,
            pool_address: str,
            percentage_to_remove: float = 100.0,
            fail_silently: bool = False
    ) -> Dict[str, Any]:
        """
        Removes liquidity from a regular AMM pool
        :param connector: The connector/protocol (e.g., "hydration")
        :param network: The network to use (e.g., "mainnet")
        :param wallet_address: The wallet address removing liquidity
        :param pool_address: The address of the pool
        :param percentage_to_remove: Percentage of liquidity to remove (default: 100%)
        :param fail_silently: Whether to fail silently on error
        :return: Details of the liquidity removal including amounts and fees
        """
        request_payload = {
            "network": network,
            "walletAddress": wallet_address,
            "poolAddress": pool_address,
            "percentageToRemove": percentage_to_remove,
        }

        return await self.api_request(
            "post",
            f"{connector}/amm/remove-liquidity",
            request_payload,
            fail_silently=fail_silently,
        )

    async def post_amm_execute_swap(
            self,
            connector: str,
            network: str,
            wallet_address: str,
            base_token: str,
            quote_token: str,
            amount: float,
            side: str,
            pool_address: str,
            slippage_pct: float,
            fail_silently: bool = False
    ) -> Dict[str, Any]:
        """
        Executes a swap in an AMM pool.

        :param connector: The connector/protocol (e.g., "raydium")
        :param network: The network to use (e.g., "mainnet-beta")
        :param wallet_address: The wallet address executing the swap
        :param base_token: The token to swap from
        :param quote_token: The token to swap to
        :param amount: The amount to swap
        :param side: The side of the trade (buy/sell)
        :param pool_address: The address of the pool
        :param slippage_pct: The allowed slippage percentage
        :param fail_silently: Whether to fail silently on error
        :return: Transaction details including hash and status
        """
        request_payload = {
            "network": network,
            "walletAddress": wallet_address,
            "baseToken": base_token,
            "quoteToken": quote_token,
            "amount": str(amount),
            "side": side,
            "poolAddress": pool_address,
            "slippagePct": str(slippage_pct)
        }
        return await self.api_request(
            "post",
            f"{connector}/amm/execute-swap",
            request_payload,
            fail_silently=fail_silently
        )

    async def get_amm_quote_liquidity(
            self,
            connector: str,
            network: str,
            pool_address: str,
            base_token_amount: float,
            quote_token_amount: float,
            slippage_pct: float,
            fail_silently: bool = False
    ) -> Dict[str, Any]:
        """
        Gets a quote for adding liquidity to an AMM pool.

        :param connector: The connector/protocol (e.g., "raydium")
        :param network: The network to use (e.g., "mainnet-beta")
        :param pool_address: The address of the pool
        :param base_token_amount: The amount of base token to add
        :param quote_token_amount: The amount of quote token to add
        :param slippage_pct: The allowed slippage percentage
        :param fail_silently: Whether to fail silently on error
        :return: Quote information including expected pool tokens and price impact
        """
        params = {
            "network": network,
            "poolAddress": pool_address,
            "baseTokenAmount": base_token_amount,
            "quoteTokenAmount": quote_token_amount,
            "slippagePct": slippage_pct
        }
        return await self.api_request(
            "get",
            f"{connector}/amm/quote-liquidity",
            params=params,
            fail_silently=fail_silently
        )

    async def get_amm_quote_swap(
            self,
            connector: str,
            network: str,
            base_token: str,
            quote_token: str,
            amount: float,
            side: str,
            pool_address: str,
            slippage_pct: float,
            fail_silently: bool = False
    ) -> Dict[str, Any]:
        """
        Gets a quote for swapping tokens in an AMM pool.

        :param connector: The connector/protocol (e.g., "raydium", "hydration")
        :param network: The network to use (e.g., "mainnet-beta")
        :param base_token: The token to swap from
        :param quote_token: The token to swap to
        :param amount: The amount to swap
        :param side: The side of the trade (buy/sell)
        :param pool_address: The address of the pool
        :param slippage_pct: The allowed slippage percentage
        :param fail_silently: Whether to fail silently on error
        :return: Dictionary containing quote information including:
                - expected output amount
                - price impact
                - minimum received amount
                - fee information
        """
        request_payload = {
            "network": network,
            "baseToken": base_token,
            "quoteToken": quote_token,
            "amount": amount,
            "side": side,
            "poolAddress": pool_address,
            "slippagePct": slippage_pct
        }

        try:
            response = await self.api_request(
                "get",
                f"{connector}/amm/quote-swap",
                request_payload,
                fail_silently=fail_silently
            )
            return response
        except Exception as e:
            if not fail_silently:
                self.logger().network(
                    f"Failed to get swap quote for {connector} on {network}",
                    exc_info=True,
                    app_warning_msg=str(e)
                )
            raise e

    async def clmm_pool_info(
            self,
            connector: str,
            network: str,
            pool_address: str,
            fail_silently: bool = False
    ) -> Dict[str, Any]:
        """
        Gets information about a concentrated liquidity pool
        :param connector: The connector/protocol (e.g., "meteora")
        :param network: The network to use (e.g., "mainnet")
        :param pool_address: The address of the pool
        :param fail_silently: Whether to fail silently on error
        :return: Pool information including price, liquidity, and bin data
        """
        query_params = {
            "network": network,
            "poolAddress": pool_address,
        }
        return await self.api_request(
            "get",
            f"{connector}/pool-info",
            params=query_params,
            fail_silently=fail_silently,
        )

    async def clmm_position_info(
            self,
            connector: str,
            network: str,
            position_address: str,
            wallet_address: str,
            fail_silently: bool = False
    ) -> Dict[str, Any]:
        """
        Gets information about a concentrated liquidity position
        :param connector: The connector/protocol (e.g., "meteora")
        :param network: The network to use (e.g., "mainnet")
        :param position_address: The address of the position
        :param wallet_address: The wallet address that owns the position
        :param fail_silently: Whether to fail silently on error
        :return: Position information including amounts and price range
        """
        query_params = {
            "network": network,
            "positionAddress": position_address,
            "walletAddress": wallet_address,
        }
        return await self.api_request(
            "get",
            f"{connector}/clmm/position-info",
            params=query_params,
            fail_silently=fail_silently,
        )

    async def clmm_open_position(
            self,
            connector: str,
            network: str,
            wallet_address: str,
            pool_address: str,
            lower_price: float,
            upper_price: float,
            base_token_amount: Optional[float] = None,
            quote_token_amount: Optional[float] = None,
            slippage_pct: Optional[float] = None,
            fail_silently: bool = False
    ) -> Dict[str, Any]:
        """
        Opens a new concentrated liquidity position
        :param connector: The connector/protocol (e.g., "meteora")
        :param network: The network to use (e.g., "mainnet")
        :param wallet_address: The wallet address creating the position
        :param pool_address: The address of the pool
        :param lower_price: The lower price bound of the position
        :param upper_price: The upper price bound of the position
        :param base_token_amount: The amount of base token to add (optional)
        :param quote_token_amount: The amount of quote token to add (optional)
        :param slippage_pct: Allowed slippage percentage (optional)
        :param fail_silently: Whether to fail silently on error
        :return: Details of the opened position
        """
        request_payload = {
            "network": network,
            "walletAddress": wallet_address,
            "poolAddress": pool_address,
            "lowerPrice": lower_price,
            "upperPrice": upper_price,
        }
        if base_token_amount is not None:
            request_payload["baseTokenAmount"] = base_token_amount
        if quote_token_amount is not None:
            request_payload["quoteTokenAmount"] = quote_token_amount
        if slippage_pct is not None:
            request_payload["slippagePct"] = slippage_pct

        return await self.api_request(
            "post",
            f"{connector}/clmm/open-position",
            request_payload,
            fail_silently=fail_silently,
        )

    async def amm_pools(
            self,
            connector: str,
            network: str,
            wallet_address: str,
            position_address: str,
            fail_silently: bool = False
    ) -> dict[str, Any] | list[dict[str, Any]] | None:
        """
        Closes an existing concentrated liquidity position
        :param connector: The connector/protocol (e.g., "meteora")
        :param network: The network to use (e.g., "mainnet")
        :param wallet_address: The wallet address that owns the position
        :param position_address: The address of the position to close
        :param fail_silently: Whether to fail silently on error
        :return: Details of the closed position including refunded amounts
        """
        request_payload = {
            "network": network,
            "walletAddress": wallet_address,
            "positionAddress": position_address,
        }
        return await self.api_request(
            "post",
            f"{connector}/clmm/close-position",
            request_payload,
            fail_silently=fail_silently,
        )

        # async def amm_fetch_pools(
        #         self,
        #         connector: str,
        #         network: str,
        #         fail_silently: bool = False
        # ) -> Dict[str, Any]:
        #     """
        #     Fetches all available AMM pools for a given connector and network
        #     :param connector: The connector/protocol (e.g., "raydium")
        #     :param network: The network to use (e.g., "mainnet")
        #     :param fail_silently: Whether to fail silently on error
        #     :return: List of available pools with their information
        #     """
        #         query_params = {
        #             "network": network,
        #         }
        #         return await self.api_request(
        #             "get",
        #             f"{connector}/amm/pools",
        #             params=query_params,
        #             fail_silently=fail_silently,
        #         )

        # def __getattr__(self, name: str) -> Any:
        #     """
        #     Magic method to dynamically handle method calls based on naming convention.
        #     Format: {http_method}_{route_path} where underscores in route_path become slashes
        #     Example: get_network_status converts to GET /network/status

        #     :param name: The method name being called
        #     :return: An async function that makes the appropriate API request
        #     """

        #     async def dynamic_api_request(**kwargs) -> Dict[str, Any]:
        #         # Split the method name to extract HTTP method and route path
        #         parts = name.split('_')
        #         if not parts:
        #             raise ValueError(f"Invalid method name: {name}")

        #         # Extract the HTTP method (first part)
        #         http_method = parts[0].lower()
        #         if http_method not in ["get", "post", "put", "delete"]:
        #             raise ValueError(f"Unsupported HTTP method: {http_method}")

        #         # Convert remaining parts to route path with slashes
        #         route_path = '/'.join(parts[1:])

        #         # Extract fail_silently if provided, default to False
        #         fail_silently = kwargs.pop('fail_silently', False)

        #         # Make the API request
        #         return await self.api_request(http_method, route_path, kwargs, fail_silently=fail_silently)

        #     return dynamic_api_request

        # def __getattr__(self, name: str) -> Any:
        #     """
        #     Magic method to dynamically handle method calls based on naming convention.
        #     Format: {http_method}_{route_path} where underscores in route_path become slashes
        #     Example: get_network_status converts to GET /network/status
        #
        #     :param name: The method name being called
        #     :return: An async function that makes the appropriate API request
        #     """
        #
        #     async def dynamic_api_request(**kwargs) -> Dict[str, Any]:
        #         # Split the method name to extract HTTP method and route path
        #         parts = name.split('_')
        #         if not parts:
        #             raise ValueError(f"Invalid method name: {name}")
        #
        #         # Extract the HTTP method (first part)
        #         http_method = parts[0].lower()
        #         if http_method not in ["get", "post", "put", "delete"]:
        #             raise ValueError(f"Unsupported HTTP method: {http_method}")
        #
        #         # Convert remaining parts to route path with slashes
        #         route_path = '/'.join(parts[1:])
        #
        #         # Extract fail_silently if provided, default to False
        #         fail_silently = kwargs.pop('fail_silently', False)
        #
        #         # Make the API request
        #         return await self.api_request(http_method, route_path, kwargs, fail_silently=fail_silently)
        #
        #     return dynamic_api_request
        #
