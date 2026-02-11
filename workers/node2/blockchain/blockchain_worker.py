# =============================
# 🔗 BLOCKCHAIN WORKER - NODE 2
# =============================

import os
import json
import time
import threading
import redis


# ================================
# 🛡️ HARD GUARD: NODE2 ONLY
# ================================
from nodes.config import NODE_CONFIG
from nodes.rpc import BitcoinRPC

RPC = BitcoinRPC(NODE_CONFIG["node2"])
print(f"[BLOCKCHAIN] Worker imported, RPC={RPC.url}")


# ================================
# 🔧 REDIS SETUP
# ================================
r = redis.Redis(
    host="localhost",
    port=6379,
    db=0,
    decode_responses=False
)


# ================================
# 🔑 REDIS KEYS + CONSTANTS
# ================================
from core.redis_keys import (

    BLOCKCHAIN_GETBLOCKCHAININFO_KEY,
    BLOCKCHAIN_LATEST_BLOCK_KEY,
    BLOCKCHAIN_STATIC_KEY,
    BLOCKCHAIN_LOCK_KEY,

    BLOCKCHAIN_DYNAMIC_CACHE,
    
    BLOCKCHAIN_DYNAMIC_BLOCKINFO_KEY,
    BLOCKCHAIN_DYNAMIC_HASHRATE_KEY,
    BLOCKCHAIN_DYNAMIC_HALVING_KEY,
    BLOCKCHAIN_DYNAMIC_WINNERHASH_KEY,

    HALVING_INTERVAL,
    LAST_HALVING_BLOCK,
    BLOCK_TIME_SECONDS,
    INITIAL_BLOCK_REWARD,

    BLOCKCHAIN_DYNAMIC_UPDATE_INTERVAL,
    BLOCKCHAIN_STATIC_UPDATE_INTERVAL,
    BLOCKCHAIN_LOCK_TTL_SECONDS,
)

# ================================
# 🧰 Helpers
# ================================
def _decode_if_bytes(x):
    if x is None:
        return None
    return x.decode() if isinstance(x, (bytes, bytearray)) else x


def _json_loads_safe(raw, default):
    raw = _decode_if_bytes(raw)
    if not raw:
        return default
    try:
        return json.loads(raw)
    except Exception:
        return default


# ================================
# 🔒 Locking (wie btc_top)
# ================================
def acquire_lock() -> bool:
    """
    Single-instance guard: verhindert, dass der Worker parallel mehrfach läuft.
    """
    pid = str(os.getpid())
    try:
        return bool(r.set(BLOCKCHAIN_LOCK_KEY, pid, nx=True, ex=BLOCKCHAIN_LOCK_TTL_SECONDS))
    except Exception as e:
        print(f"[BLOCKCHAIN LOCK ERROR] {e}")
        return False


def renew_lock() -> None:
    """
    Lock verlängern, solange wir ihn besitzen.
    """
    pid = str(os.getpid())
    cur = _decode_if_bytes(r.get(BLOCKCHAIN_LOCK_KEY))
    if cur == pid:
        r.expire(BLOCKCHAIN_LOCK_KEY, BLOCKCHAIN_LOCK_TTL_SECONDS)


def release_lock() -> None:
    """
    Lock nur löschen, wenn wir ihn besitzen.
    """
    pid = str(os.getpid())
    cur = _decode_if_bytes(r.get(BLOCKCHAIN_LOCK_KEY))
    if cur == pid:
        r.delete(BLOCKCHAIN_LOCK_KEY)


# ================================
# 🔄 STATE (Block Age)
# ================================
_LAST_BLOCK_HASH = None
_BLOCK_SEEN_TS = int(time.time())


# =================================================
# 🛑 INPUT UPDATE (RPC → Redis)
# =================================================
def update_blockchain_input():
    t_start = time.time()

    # ---------------------------------------------
    # 🔎 Core Input
    # ---------------------------------------------
    chain_info = RPC.call("getblockchaininfo") or {}
    r.set(BLOCKCHAIN_GETBLOCKCHAININFO_KEY, json.dumps(chain_info))

    # ---------------------------------------------
    # 📦 Letzten Block vollständig speichern
    #     (IMMER derselbe Key!)
    # ---------------------------------------------
    bestblockhash = chain_info.get("bestblockhash")
    if bestblockhash:
        try:
            block_data = RPC.call("getblock", [bestblockhash]) or {}
            r.set(
                BLOCKCHAIN_LATEST_BLOCK_KEY,
                json.dumps({
                    "hash": bestblockhash,
                    "height": chain_info.get("blocks", 0),
                    "tx": block_data.get("tx", [])
                })
            )
        except Exception as e:
            print(f"[BLOCKCHAIN INPUT ERROR] getblock failed: {e}")

    elapsed_ms = int((time.time() - t_start) * 1000)

    return {
        "scan_time_ms": elapsed_ms,
        "block_height": chain_info.get("blocks", 0),
    }



# =================================================
# 📦 STATIC UPDATE (aus Redis-Input, fallback RPC)
# =================================================
def update_blockchain_static():
    """
    Erstellt und persistiert statische Blockchain-Metadaten.
    - Kein TTL (State ≠ Cache)
    - Fallback auf RPC, falls Redis leer ist
    - Robust gegen fehlerhafte / leere Daten
    """

    # -------------------------------------------------
    # 🔎 Input: bevorzugt Redis, fallback RPC
    # -------------------------------------------------
    chain_info_raw = r.get(BLOCKCHAIN_GETBLOCKCHAININFO_KEY)
    chain_info = _json_loads_safe(chain_info_raw, default={})

    if not chain_info:
        try:
            chain_info = RPC.call("getblockchaininfo") or {}
        except Exception as e:
            print(f"[BLOCKCHAIN STATIC ERROR] RPC fallback failed: {e}")
            chain_info = {}

    # -------------------------------------------------
    # 🧮 Ableitungen (defensiv)
    # -------------------------------------------------
    current_block_height = int(chain_info.get("blocks") or 0)
    chain_name = chain_info.get("chain") or "—"

    try:
        halvings_passed = current_block_height // HALVING_INTERVAL
        current_block_reward = INITIAL_BLOCK_REWARD * (0.5 ** halvings_passed)
    except Exception:
        current_block_reward = None

    # -------------------------------------------------
    # 📦 Static Payload
    # -------------------------------------------------
    static_data = {
        "chain_name": chain_name,
        "current_block_height": current_block_height,
        "current_block_reward": current_block_reward,

        "HALVING_INTERVAL": HALVING_INTERVAL,
        "LAST_HALVING_BLOCK": LAST_HALVING_BLOCK,
        "INITIAL_BLOCK_REWARD": INITIAL_BLOCK_REWARD,

        "source": "node2",
        "updated_at": int(time.time()),
    }

    # -------------------------------------------------
    # 💾 Persist (kein TTL!)
    # -------------------------------------------------
    try:
        r.set(BLOCKCHAIN_STATIC_KEY, json.dumps(static_data))
    except Exception as e:
        print(f"[BLOCKCHAIN STATIC ERROR] Redis write failed: {e}")


# =================================================
# 🔸 DYNAMIC 1: Block Info + Age
# =================================================
def update_block_info():
    global _LAST_BLOCK_HASH, _BLOCK_SEEN_TS

    chain_info = _json_loads_safe(
        r.get(BLOCKCHAIN_GETBLOCKCHAININFO_KEY),
        default={}
    )

    current_block_height = chain_info.get("blocks", 0)
    bestblockhash = chain_info.get("bestblockhash")

    if bestblockhash and bestblockhash != _LAST_BLOCK_HASH:
        _LAST_BLOCK_HASH = bestblockhash
        _BLOCK_SEEN_TS = int(time.time())
        print("[BLOCK_AGE] Neuer Block erkannt → Timer reset")

    # ---------------------------------------------
    # 📦 TX-Count aus dem *einen* Block-Key
    # ---------------------------------------------
    tx_count = 0
    block = _json_loads_safe(
        r.get(BLOCKCHAIN_LATEST_BLOCK_KEY),
        default={}
    )

    if block.get("hash") == bestblockhash:
        tx_count = len(block.get("tx", []))

    elapsed = int(time.time()) - _BLOCK_SEEN_TS
    minutes, seconds = divmod(max(0, elapsed), 60)
    block_age_str = f"{minutes} minutes, {seconds} seconds"

    r.set(
        BLOCKCHAIN_DYNAMIC_BLOCKINFO_KEY,
        json.dumps({
            "current_block_height": current_block_height,
            "tx_count": tx_count,
            "block_age_str": block_age_str,
        })
    )


# =================================================
# 🔸 DYNAMIC 2: Hashrate
# =================================================
def update_hashrate():
    chain_info = _json_loads_safe(r.get(BLOCKCHAIN_GETBLOCKCHAININFO_KEY), default={})

    difficulty = chain_info.get("difficulty", 0) or 0
    hash_rate = (difficulty * (2 ** 32)) / BLOCK_TIME_SECONDS if difficulty else 0

    average_hash_rate = f"{hash_rate / 1e18:.2f} EH/s" if hash_rate else "No data"

    r.set(BLOCKCHAIN_DYNAMIC_HASHRATE_KEY, json.dumps({
        "average_hash_rate": average_hash_rate
    }))


# =================================================
# 🔸 DYNAMIC 3: Halving Countdown
# =================================================
def update_halving():
    chain_info = _json_loads_safe(r.get(BLOCKCHAIN_GETBLOCKCHAININFO_KEY), default={})

    current_block_height = chain_info.get("blocks", 0)
    remaining_blocks = max(0, HALVING_INTERVAL - (current_block_height - LAST_HALVING_BLOCK))
    remaining_seconds = remaining_blocks * BLOCK_TIME_SECONDS

    r.set(BLOCKCHAIN_DYNAMIC_HALVING_KEY, json.dumps({
        "remaining_blocks": remaining_blocks,
        "remaining_seconds": remaining_seconds
    }))


# =================================================
# 🔸 DYNAMIC 4: Winner Hash
# =================================================
def update_winnerhash():
    chain_info = _json_loads_safe(r.get(BLOCKCHAIN_GETBLOCKCHAININFO_KEY), default={})

    winner_hash = chain_info.get("bestblockhash", "—")
    block_height = chain_info.get("blocks", 0)

    r.set(BLOCKCHAIN_DYNAMIC_WINNERHASH_KEY, json.dumps({
        "winner_hash": winner_hash,
        "block_height": block_height
    }))


# =================================================
# 🔸 AGGREGATOR
# =================================================
def aggregate_blockchain_dynamic():
    combined = {}

    for key in (
        BLOCKCHAIN_DYNAMIC_BLOCKINFO_KEY,
        BLOCKCHAIN_DYNAMIC_HASHRATE_KEY,
        BLOCKCHAIN_DYNAMIC_HALVING_KEY,
        BLOCKCHAIN_DYNAMIC_WINNERHASH_KEY,
    ):
        raw = r.get(key)
        if raw:
            combined.update(_json_loads_safe(raw, default={}))

    if combined:
        r.set(BLOCKCHAIN_DYNAMIC_CACHE, json.dumps(combined))


# =================================================
# 🔁 MAIN LOOP
# =================================================
def blockchain_worker_loop():
    print("[BLOCKCHAIN WORKER] gestartet")
    time.sleep(1.5)

    # -------------------------------------------------
    # 🔒 Single-Instance Lock holen
    # -------------------------------------------------
    while not acquire_lock():
        print("[BLOCKCHAIN WORKER] Lock nicht erhalten, warte...")
        time.sleep(1.0)

    print("[BLOCKCHAIN WORKER] Lock erhalten")

    next_static_ts = 0.0

    while True:
        loop_start = time.time()

        try:
            renew_lock()

            # -----------------------------------------
            # 🔎 INPUT (RPC → Redis)
            # -----------------------------------------
            input_stats = update_blockchain_input()
            # erwartet:
            # {
            #   "scan_time_ms": int,
            #   "block_height": int
            # }

            # -----------------------------------------
            # ⚙️ DYNAMIC UPDATES
            # -----------------------------------------
            update_block_info()
            update_hashrate()
            update_halving()
            update_winnerhash()
            aggregate_blockchain_dynamic()

            # -----------------------------------------
            # 📦 STATIC (periodisch)
            # -----------------------------------------
            now = time.time()
            if now >= next_static_ts:
                update_blockchain_static()
                next_static_ts = now + BLOCKCHAIN_STATIC_UPDATE_INTERVAL

        except Exception as e:
            print(f"[BLOCKCHAIN WORKER ERROR] {e}")
            input_stats = {}

        # -----------------------------------------
        # 🧮 TIMING
        # -----------------------------------------
        loop_elapsed = time.time() - loop_start
        sleep_time = max(
            0.0,
            BLOCKCHAIN_DYNAMIC_UPDATE_INTERVAL - loop_elapsed
        )

        # -----------------------------------------
        # 📊 MONITORING (BTC_TOP-Style)
        # -----------------------------------------
        scan_ms = input_stats.get("scan_time_ms", "?")
        height = input_stats.get("block_height", "?")

        print(
            f"[BLOCKCHAIN WORKER] "
            f"rpc={RPC.info()} | "
            f"height={height} | "
            f"scan={scan_ms}ms | "
            f"sleep={sleep_time:.3f}s"
        )

        time.sleep(sleep_time)

