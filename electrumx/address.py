from nodes.electrumx import address_to_scripthash

async def get_address_overview(client, address: str):
    scripthash = address_to_scripthash(address)

    balance = await client.call(
        "blockchain.scripthash.get_balance",
        [scripthash]
    )

    utxos = await client.call(
        "blockchain.scripthash.listunspent",
        [scripthash]
    )

    history = await client.call(
        "blockchain.scripthash.get_history",
        [scripthash]
    )

    return {
        "address": address,
        "scripthash": scripthash,
        "balance": balance,
        "utxos": utxos,
        "history": history,
    }
