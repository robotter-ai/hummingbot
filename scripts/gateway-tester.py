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
    "positionAddress": "",
    "amount": "900",
    "side": "buy",
    "slippagePercentage": "1.0",
    "baseTokenAmount": "900",
    "quoteTokenAmount": "0.01",
    "percentageToRemove": "1",
    "lowerPrice": "0.000010",
    "upperPrice": "0.000015"
}


async def get_root() -> Dict[str, Any]:
    return await GatewayHttpClient.get_instance().get_root()


# CONFIG ROUTES
# OK
async def get_configuration() -> Dict[str, Any]:
    """
    Gets the general gateway configuration.
    """
    return await GatewayHttpClient.get_instance().get_configuration()


# OK
async def get_configuration_chain(options: Dict[str, Any]) -> Dict[str, Any]:
    return await GatewayHttpClient.get_instance().get_configuration(options["chain"])


# OK
async def get_configuration_connector(options: Dict[str, Any]) -> Dict[str, Any]:
    return await GatewayHttpClient.get_instance().get_configuration(options["connector"])


# OK
async def post_config_update(options: Dict[str, Any]) -> Dict[str, Any]:
    return await GatewayHttpClient.get_instance().update_config(
        options["configPath"],
        options["configValue"]
    )


# OK
async def get_connectors() -> Dict[str, Any]:
    """
    Gets the list of available connectors in the gateway.
    Returns a dictionary containing information about all supported connectors.
    """
    return await GatewayHttpClient.get_instance().get_connectors()


# OK
async def get_wallets() -> list[dict[str, Any]]:
    return await GatewayHttpClient.get_instance().get_wallets()


# OK
async def post_wallet_add(options: Dict[str, Any]) -> Dict[str, Any]:
    return await GatewayHttpClient.get_instance().add_wallet(
        options["chain"],
        options["network"],
        options["walletMnemonic"]
    )


# OK
async def delete_wallet_remove(options: Dict[str, Any]) -> Dict[str, Any]:
    return await GatewayHttpClient.get_instance().remove_wallet(
        options["chain"],
        options["address"]
    )


# OK
async def get_chain_status(options: Dict[str, Any]) -> Dict[str, Any]:
    return await GatewayHttpClient.get_instance().get_network_status(
        options["chain"],
        options["network"]
    )


# OK
async def post_chain_poll(options: Dict[str, Any]) -> Dict[str, Any]:
    return await GatewayHttpClient.get_instance().get_transaction_status(
        options["chain"],
        options["network"],
        options["txHash"]
    )


# TOKEN ROUTES
# OK
async def get_chain_tokens_network(options: Dict[str, Any]) -> Dict[str, Any]:
    return await GatewayHttpClient.get_instance().get_chain_tokens(
        options["chain"],
        options["network"],
        options["failSilently"]
    )


# OK
async def get_token_single(options: Dict[str, Any]) -> Dict[str, Any]:
    return await GatewayHttpClient.get_instance().get_chain_token_info(
        options["chain"],
        options["network"],
        options["tokenSymbol"]
    )


# OK
async def get_multiple_tokens(options: Dict[str, Any]) -> Dict[str, Any]:
    return await GatewayHttpClient.get_instance().get_chain_multiple_tokens(
        options["chain"],
        options["network"],
        options["tokenSymbols"],
        options["baseTokenSymbol"],
        options["quoteTokenSymbol"],
    )


# BALANCE ROUTES
# OK
async def post_chain_balances(options: Dict[str, Any]) -> Dict[str, Any]:
    return await GatewayHttpClient.get_instance().get_balances(
        options["chain"],
        options["network"],
        options["address"],
        options["tokenSymbols"],
        options["walletPublicKey"],
        False
    )


# OK
async def post_chain_balances_single_string(options: Dict[str, Any]) -> Dict[str, Any]:
    """
    Gets the balance for a single token using string format.
    """
    return await GatewayHttpClient.get_instance().get_balances(
        options["chain"],
        options["network"],
        options["address"],
        options["nativeTokenSymbol"],
        options["walletPublicKey"],
    )


# OK
async def post_chain_balances_single_array(options: Dict[str, Any]) -> Dict[str, Any]:
    """
    Gets the balance for a single token using array format.
    """
    return await GatewayHttpClient.get_instance().get_balances(
        options["chain"],
        options["network"],
        options["address"],
        options["nativeTokenSymbol"],
        options["walletPublicKey"],
    )


# OK
async def post_chain_balances_multiple(options: Dict[str, Any]) -> Dict[str, Any]:
    """
    Gets balances for multiple tokens.
    """
    return await GatewayHttpClient.get_instance().get_balances(
        options["chain"],
        options["network"],
        options["address"],
        options["nativeTokenSymbol"],
        options["walletPublicKey"],
    )


# AMM Connector Routes
# OK
async def get_amm_pool_info(options: Dict[str, Any]) -> Dict[str, Any]:
    return await GatewayHttpClient.get_instance().amm_pool_info(
        options["connector"],
        options["network"],
        options["poolAddress"]
    )


# OK
async def get_amm_quote_swap(options: Dict[str, Any]) -> Dict[str, Any]:
    return await GatewayHttpClient.get_instance().get_amm_quote_swap(
        options["connector"],
        options["network"],
        options["baseToken"],
        options["quoteToken"],
        options["amount"],
        options["side"],
        options["poolAddress"],
        options["slippagePct"]
    )


# OK
async def get_amm_quote_liquidity(options: Dict[str, Any]) -> Dict[str, Any]:
    return await GatewayHttpClient.get_instance().get_amm_quote_liquidity(
        options["connector"],
        options["network"],
        options["poolAddress"],
        options["baseTokenAmount"],
        options["quoteTokenAmount"],
        options["slippagePct"]
    )


# OK
async def post_amm_execute_swap(options: Dict[str, Any]) -> Dict[str, Any]:
    return await GatewayHttpClient.get_instance().post_amm_execute_swap(
        options["connector"],
        options["network"],
        options["walletAddress"],
        options["baseToken"],
        options["quoteToken"],
        options["amount"],
        options["side"],
        options["poolAddress"],
        options["slippagePct"]
    )


# OK
async def post_amm_add_liquidity(options: Dict[str, Any]) -> Dict[str, Any]:
    return await GatewayHttpClient.get_instance().amm_add_liquidity(
        options["connector"],
        options["network"],
        options["walletAddress"],
        options["poolAddress"],
        options["baseTokenAmount"],
        options["quoteTokenAmount"],
        options["slippagePct"]
    )


# OK
async def post_amm_remove_liquidity(options: Dict[str, Any]) -> Dict[str, Any]:
    return await GatewayHttpClient.get_instance().amm_remove_liquidity(
        options["connector"],
        options["network"],
        options["walletPublicKey"],
        options["poolAddress"],
        options["percentageToRemove"]
    )


def on_tick():
    safe_ensure_future(async_on_tick())


async def async_on_tick():
    try:
        # Root and Config Routes
        await get_root()
        await get_configuration()
        await get_configuration_chain({})
        await get_configuration_connector({})
        await post_config_update(configuration)
        await get_configuration_connector({})

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
        await post_chain_balances(configuration)

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
