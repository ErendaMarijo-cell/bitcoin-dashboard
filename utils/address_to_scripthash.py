import hashlib
import base58
from bech32 import bech32_decode, convertbits


# --------------------------------------------------
# Legacy / P2SH → ScriptPubKey
# --------------------------------------------------
def legacy_to_scriptpubkey(address: str) -> bytes:

    decoded = base58.b58decode_check(address)
    version = decoded[0]
    payload = decoded[1:]

    # P2PKH (1…)
    if version == 0x00:
        return bytes.fromhex("76a914") + payload + bytes.fromhex("88ac")

    # P2SH (3…)
    elif version == 0x05:
        return bytes.fromhex("a914") + payload + bytes.fromhex("87")

    else:
        raise ValueError("unsupported legacy address version")


# --------------------------------------------------
# Bech32 / SegWit / Taproot
# --------------------------------------------------
def segwit_to_scriptpubkey(address: str) -> bytes:

    hrp, data = bech32_decode(address)

    if hrp != "bc":
        raise ValueError("unsupported bech32 hrp")

    witver = data[0]
    decoded = convertbits(data[1:], 5, 8, False)

    if decoded is None:
        raise ValueError("invalid bech32 payload")

    return bytes([witver]) + bytes([len(decoded)]) + bytes(decoded)


# --------------------------------------------------
# Universal Address → ScriptPubKey
# --------------------------------------------------
def address_to_scriptpubkey(address: str) -> bytes:

    if address.startswith(("1", "3")):
        return legacy_to_scriptpubkey(address)

    elif address.startswith("bc1"):
        return segwit_to_scriptpubkey(address)

    else:
        raise ValueError("unsupported address format")


# --------------------------------------------------
# ScriptPubKey → ScriptHash (ElectrumX)
# --------------------------------------------------
def address_to_scripthash(address: str) -> str:

    script = address_to_scriptpubkey(address)

    sha = hashlib.sha256(script).digest()

    # ElectrumX requires reversed hash
    return sha[::-1].hex()
