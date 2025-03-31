import asyncio
import os
from typing import Any, Dict

from hummingbot.core.gateway.gateway_http_client import GatewayHttpClient
from hummingbot.core.utils.async_utils import safe_ensure_future

configuration = {
    "httpProtocol": "https",
    "webSocketProtocol": "wss",
    "host": "localhost",
    "port": "15888",
    "baseUrl": "",
    "chain": "solana",
    "network": "mainnet-beta",
    "connector": "raydium",
    "walletPublicKey": "7pWpBM8xtVHJq7C4BBivumFbzAC2J8XndTWvmg9GGXDb",
    "walletMnemonic": os.environ["WALLET_MNEMONIC"],
    "configPath": "raydium.allowedSlippage",
    "configValue": "2/100",
    "dummyMessage": "Hello, world!",
    "nativeTokenSymbol": "SOL",
    "baseTokenSymbol": "BONK",
    "quoteTokenSymbol": "USDC",
    "txHash": "55ukR6VCt1sQFMC8Nyeo51R1SMaTzUC7jikmkEJ2jjkQNdqBxXHraH7vaoaNmf8rX4Y55EXAj8XXoyzvvsrQqWZa",
    "poolAddress": "3kYeoo6jxTXGnTtoYtZXBBxAtTLwTVLEhwM2CH81i7x4",
    "amount": "900",
    "side": "buy",
    "slippagePercentage": "1.0",
    "baseTokenAmount": "900",
    "quoteTokenAmount": "0.01",
    "percentageToRemove": "1",
}


# ===================== ROOT ROUTE =====================
async def get_root() -> Dict[str, Any]:
    """
    Gets the root endpoint of the gateway.
    """
    return await GatewayHttpClient.get_instance().get_root()


# ===================== CONFIG ROUTES =====================
async def get_configuration() -> Dict[str, Any]:
    """
    Gets the general gateway configuration.
    """
    return await GatewayHttpClient.get_instance().get_configuration()


async def get_configuration_chain(options: Dict[str, Any]) -> Dict[str, Any]:
    """
    Gets the configuration for a specific chain.
    """
    return await GatewayHttpClient.get_instance().get_configuration(options["chain"])


async def get_configuration_connector(options: Dict[str, Any]) -> Dict[str, Any]:
    """
    Gets the configuration for a specific connector.
    """
    return await GatewayHttpClient.get_instance().get_configuration(options["connector"])


async def post_config_update(options: Dict[str, Any]) -> Dict[str, Any]:
    """
    Updates a configuration value.
    """
    return await GatewayHttpClient.get_instance().update_config(options["configPath"], options["configValue"])


# ===================== CONNECTOR ROUTES =====================
async def get_connectors() -> Dict[str, Any]:
    """
    Gets the list of available connectors.
    """
    return await GatewayHttpClient.get_instance().get_connectors()


# ===================== WALLET ROUTES =====================
async def get_wallets() -> list[dict[str, Any]]:
    """
    Gets the list of configured wallets.
    """
    return await GatewayHttpClient.get_instance().get_wallets()


async def post_wallet_add(options: Dict[str, Any]) -> Dict[str, Any]:
    """
    Adds a new wallet.
    """
    return await GatewayHttpClient.get_instance().add_wallet(
        options["chain"], options["network"], options["walletMnemonic"]
    )


async def delete_wallet_remove(options: Dict[str, Any]) -> Dict[str, Any]:
    """
    Removes a wallet.
    """
    return await GatewayHttpClient.get_instance().remove_wallet(options["chain"], options["walletPublicKey"])


# ===================== CHAIN ROUTES =====================
async def get_chain_status(options: Dict[str, Any]) -> Dict[str, Any]:
    """
    Gets the status of a chain network.
    """
    return await GatewayHttpClient.get_instance().get_network_status(options["chain"], options["network"])


# ===================== TRANSACTION ROUTES =====================
async def post_chain_poll(options: Dict[str, Any]) -> Dict[str, Any]:
    """
    Polls for transaction status.
    """
    return await GatewayHttpClient.get_instance().get_transaction_status(
        options["chain"], options["network"], options["txHash"]
    )


# ===================== TOKEN ROUTES =====================
async def get_chain_tokens_network(options: Dict[str, Any]) -> Dict[str, Any]:
    """
    Gets all tokens for a network.
    """
    return await GatewayHttpClient.get_instance().get_chain_tokens(options["network"])


async def get_token_single(options: Dict[str, Any]) -> Dict[str, Any]:
    """
    Gets information for a single token.
    """
    return await GatewayHttpClient.get_instance().get_chain_token_info(options["network"], options["nativeTokenSymbol"])


async def get_multiple_tokens(options: Dict[str, Any]) -> Dict[str, Any]:
    """
    Gets information for multiple tokens.
    """
    return await GatewayHttpClient.get_instance().get_chain_multiple_tokens(
        options["network"], [options["nativeTokenSymbol"], options["baseTokenSymbol"], options["quoteTokenSymbol"]]
    )


# ===================== BALANCE ROUTES =====================
async def post_chain_balances(options: Dict[str, Any]) -> Dict[str, Any]:
    """
    Gets all balances for a wallet.
    """
    return await GatewayHttpClient.get_instance().get_balances(
        options["chain"], options["network"], options["walletPublicKey"], [], False
    )


async def post_chain_balances_single_string(options: Dict[str, Any]) -> Dict[str, Any]:
    """
    Gets balance for a single token using string format.
    """
    return await GatewayHttpClient.get_instance().get_balances(
        options["chain"], options["network"], options["walletPublicKey"], [options["nativeTokenSymbol"]], False
    )


async def post_chain_balances_single_array(options: Dict[str, Any]) -> Dict[str, Any]:
    """
    Gets balance for a single token using array format.
    """
    return await GatewayHttpClient.get_instance().get_balances(
        options["chain"], options["network"], options["walletPublicKey"], [options["quoteTokenSymbol"]], False
    )


async def post_chain_balances_multiple(options: Dict[str, Any]) -> Dict[str, Any]:
    """
    Gets balances for multiple tokens.
    """
    return await GatewayHttpClient.get_instance().get_balances(
        options["chain"],
        options["network"],
        options["walletPublicKey"],
        [options["nativeTokenSymbol"], options["baseTokenSymbol"], options["quoteTokenSymbol"]],
        False,
    )


# ===================== AMM CONNECTOR ROUTES =====================
async def get_amm_pool_info(options: Dict[str, Any]) -> Dict[str, Any]:
    """
    Gets information about an AMM pool.
    """
    return await GatewayHttpClient.get_instance().amm_pool_info(
        options["connector"], options["network"], options["poolAddress"]
    )


async def get_amm_quote_swap(options: Dict[str, Any]) -> Dict[str, Any]:
    """
    Gets a quote for a swap operation.
    """
    return await GatewayHttpClient.get_instance().get_amm_quote_swap(
        options["connector"],
        options["network"],
        options["baseTokenSymbol"],
        options["quoteTokenSymbol"],
        options["amount"],
        options["side"],
        options["poolAddress"],
        options["slippagePercentage"],
    )


async def get_amm_quote_liquidity(options: Dict[str, Any]) -> Dict[str, Any]:
    """
    Gets a quote for adding liquidity.
    """
    return await GatewayHttpClient.get_instance().amm_quote_liquidity(
        options["connector"],
        options["network"],
        options["poolAddress"],
        options["baseTokenAmount"],
        options["quoteTokenAmount"],
        options["slippagePercentage"],
    )


async def post_amm_execute_swap(options: Dict[str, Any]) -> Dict[str, Any]:
    """
    Executes a swap operation.
    """
    return await GatewayHttpClient.get_instance().amm_execute_swap(
        options["connector"],
        options["network"],
        options["walletPublicKey"],
        options["baseTokenSymbol"],
        options["quoteTokenSymbol"],
        options["amount"],
        options["side"],
        options["poolAddress"],
        options["slippagePercentage"],
    )


async def post_amm_add_liquidity(options: Dict[str, Any]) -> Dict[str, Any]:
    """
    Adds liquidity to a pool.
    """
    return await GatewayHttpClient.get_instance().amm_add_liquidity(
        options["connector"],
        options["network"],
        options["walletPublicKey"],
        options["poolAddress"],
        options["baseTokenAmount"],
        options["quoteTokenAmount"],
        options["slippagePercentage"],
    )


async def post_amm_remove_liquidity(options: Dict[str, Any]) -> Dict[str, Any]:
    """
    Removes liquidity from a pool.
    """
    return await GatewayHttpClient.get_instance().amm_remove_liquidity(
        options["connector"],
        options["network"],
        options["walletPublicKey"],
        options["poolAddress"],
        options["percentageToRemove"],
    )


def on_tick():
    safe_ensure_future(async_on_tick())


async def async_on_tick():
    try:
        # Root and Config Routes
        await get_root()
        await get_configuration()
        await get_configuration_chain(configuration)
        await get_configuration_connector(configuration)
        await post_config_update(configuration)
        await get_configuration_connector(configuration)

        # Connector and Wallet Routes
        await get_connectors()
        await get_wallets()
        await delete_wallet_remove(configuration)
        await get_wallets()
        await post_wallet_add(configuration)
        await get_wallets()

        # Chain Routes
        await get_chain_status(configuration)
        await post_chain_poll(configuration)

        # Token and Balance Routes
        await get_chain_tokens_network(configuration)
        await get_token_single(configuration)
        await get_multiple_tokens(configuration)
        await post_chain_balances(configuration)
        await post_chain_balances_single_string(configuration)
        await post_chain_balances_single_array(configuration)
        await post_chain_balances_multiple(configuration)

        # AMM Routes
        await get_amm_pool_info(configuration)
        await get_amm_quote_swap(configuration)
        await get_amm_quote_liquidity(configuration)
        await post_amm_execute_swap(configuration)
        await post_amm_add_liquidity(configuration)
        await post_amm_remove_liquidity(configuration)

    except Exception as e:
        print(f"Error in async_on_tick: {str(e)}")


if __name__ == "__main__":
    asyncio.run(async_on_tick())
