#!/usr/bin/env python3
# ==================================================
# 🔥 ADDRESS LAYER1B UPDATE WORKER (PRODUCTION)
# Live Address Event Sync (Tip Tracking, Append-only)
#
# ✅ Tracks chain tip and appends new address events
# ✅ Writes JSONL segments (10k blocks/file) identical to backfill format
# ✅ Crash-safe: state updates only after successful write + fsync
# ✅ Reorg-safe (pragmatic): processes only blocks older than FINALITY_DEPTH
#
# Output record (one per address-event):
# {"address":"...","txid":"...","height":N,"delta_sat":S}
#
# Requires:
# - txindex=1 (for prevout on inputs with verbosity=3)
# ==================================================

import os
import sys
import time
import json
from datetime import datetime, timezone
from typing import Any, Dict, Generator, Optional, Tuple

# ============================================
# 🔧 Project Root
# ============================================

BASE_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../../")
)
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

# ============================================
# 🔗 Shared Backfill Helper (State + Segments)
# ============================================

from workers.seo.helper.backfill_jsonl_helper import (
    load_state,
    save_state_atomic,
    segment_range_for_height,
    segment_filename,
)

# ============================================
# 🔗 Node RPC
# ============================================

from nodes.config import NODE_CONFIG
from nodes.rpc import BitcoinRPC

# ============================================
# ⚙️ Config
# ============================================

STATE_PATH = "/raid/data/seo/addresses/progress/addresses_backfill_state.json"
OUT_DIR = "/raid/data/seo/addresses/confirmed"

# Polling (when tip not advanced / not enough confirmations)
POLL_INTERVAL_SEC = 10

# Pragmatic reorg safety: process only blocks older than this depth
FINALITY_DEPTH = 6

# Durability
FSYNC_EVERY_N_BLOCKS = 1          # Update is low volume; keep strong durability
PROGRESS_SAVE_EVERY_N_BLOCKS = 1  # Save state every block (safe + simple)

# Logging
LOG_EVERY_N_BLOCKS = 1000

# RPC retry
RPC_RETRIES = 5
RPC_RETRY_SLEEP = 0.5

# ============================================
# 🔗 RPC Binding
# ============================================

RPC = BitcoinRPC(NODE_CONFIG["main"])
RPC.require_full_node()
print(f"[ADDRESS UPDATE] Bound to RPC {RPC.info()}")

# ============================================
# Helpers
# ============================================

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def ensure_out_dir() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)


def rpc_call(method: str, params: Optional[list] = None):
    params = params or []
    last_err = None
    for i in range(RPC_RETRIES):
        try:
            return RPC.call(method, params)
        except Exception as e:
            last_err = e
            time.sleep(RPC_RETRY_SLEEP * (i + 1))
    raise RuntimeError(
        f"RPC failed after {RPC_RETRIES} retries: {method} {params} :: {last_err}"
    )


def get_chain_tip() -> int:
    return int(rpc_call("getblockcount"))


def get_segment_file_path(entity: str, height: int, segment_size: int) -> str:
    start, end = segment_range_for_height(height, segment_size)
    fname = segment_filename(entity, start, end, pad=9, ext="jsonl")
    return os.path.join(OUT_DIR, fname)


def extract_address_from_scriptpubkey(spk: Dict[str, Any]) -> Optional[str]:
    """
    Common cases:
      - spk["address"] is present (modern)
      - spk["addresses"] list exists (older)
      - nonstandard/no address => return None
    """
    if not spk:
        return None

    addr = spk.get("address")
    if isinstance(addr, str) and addr:
        return addr

    addrs = spk.get("addresses")
    if isinstance(addrs, list) and addrs:
        a0 = addrs[0]
        if isinstance(a0, str) and a0:
            return a0

    return None


def satoshis_from_btc_value(v: Any) -> int:
    try:
        return int(round(float(v) * 100_000_000))
    except Exception:
        return 0


def write_event(fp, address: str, txid: str, height: int, delta_sat: int) -> None:
    rec = {
        "address": address,
        "txid": txid,
        "height": int(height),
        "delta_sat": int(delta_sat),
    }
    fp.write(json.dumps(rec, separators=(",", ":")) + "\n")


def iter_address_events_for_block(height: int) -> Generator[Tuple[str, str, int, int], None, None]:
    """
    Yields (address, txid, height, delta_sat) for one block.
    Uses verbosity=3 so vin.prevout is available (txindex=1 required).
    """
    block_hash = rpc_call("getblockhash", [height])
    block = rpc_call("getblock", [block_hash, 3])

    txs = block.get("tx") or []
    for tx in txs:
        txid = tx.get("txid")
        if not txid:
            continue

        # Outputs => positive delta
        for vout in (tx.get("vout") or []):
            spk = (vout.get("scriptPubKey") or {})
            addr = extract_address_from_scriptpubkey(spk)
            if not addr:
                continue
            delta = satoshis_from_btc_value(vout.get("value"))
            if delta:
                yield (addr, txid, height, delta)

        # Inputs => negative delta (from prevout)
        for vin in (tx.get("vin") or []):
            if "coinbase" in vin:
                continue
            prevout = vin.get("prevout") or {}
            spk = (prevout.get("scriptPubKey") or {})
            addr = extract_address_from_scriptpubkey(spk)
            if not addr:
                continue
            delta = satoshis_from_btc_value(prevout.get("value"))
            if delta:
                yield (addr, txid, height, -delta)


# ============================================
# File Handling
# ============================================

def open_segment_for_append(path: str):
    """
    Opens the segment file for append, ensures directory exists.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    return open(path, "a", encoding="utf-8")


# ============================================
# 🔄 Update Loop
# ============================================

def update_loop() -> None:
    print("[ADDRESS UPDATE] Worker started")
    ensure_out_dir()

    state = load_state(STATE_PATH)
    state.entity = "addresses"

    last_height = int(state.last_height)
    print(f"[ADDRESS UPDATE] Resume last_height → {last_height}")

    current_path = None
    fp = None

    blocks_since_fsync = 0
    blocks_since_state = 0
    last_log = time.time()

    try:
        while True:
            tip = get_chain_tip()
            target = tip - int(FINALITY_DEPTH)

            if target <= last_height:
                time.sleep(POLL_INTERVAL_SEC)
                continue

            # process only finalized range
            for height in range(last_height + 1, target + 1):
                seg_path = get_segment_file_path(
                    entity=state.entity,
                    height=height,
                    segment_size=state.segment_size,
                )

                # rotate segment if needed
                if seg_path != current_path:
                    if fp:
                        fp.flush()
                        os.fsync(fp.fileno())
                        fp.close()

                    current_path = seg_path
                    fp = open_segment_for_append(current_path)

                    seg_start, seg_end = segment_range_for_height(height, state.segment_size)
                    state.current_segment_start = seg_start
                    state.current_segment_end = seg_end

                    print(f"[ADDRESS UPDATE] Segment → {os.path.basename(current_path)}")

                # write events for this block
                events_written_this_block = 0
                for addr, txid, h, delta in iter_address_events_for_block(height):
                    write_event(fp, addr, txid, h, delta)
                    events_written_this_block += 1

                # durability first
                blocks_since_fsync += 1
                if blocks_since_fsync >= FSYNC_EVERY_N_BLOCKS:
                    fp.flush()
                    os.fsync(fp.fileno())
                    blocks_since_fsync = 0

                # update state ONLY after successful write+fsync
                last_height = height
                state.last_height = height
                state.events_written_total = int(state.events_written_total) + int(events_written_this_block)

                blocks_since_state += 1
                if blocks_since_state >= PROGRESS_SAVE_EVERY_N_BLOCKS:
                    state.segments_completed = state.current_segment_start // state.segment_size
                    save_state_atomic(STATE_PATH, state)
                    blocks_since_state = 0

                # logging
                if height % LOG_EVERY_N_BLOCKS == 0:
                    print(
                        f"[ADDRESS UPDATE] Height {height} "
                        f"→ events +{events_written_this_block} "
                        f"(total_events={state.events_written_total}) "
                        f"@ {utc_now_iso()}"
                    )

                # heartbeat (optional)
                if time.time() - last_log > 30:
                    print(
                        f"[ADDRESS UPDATE] heartbeat | "
                        f"last_height={last_height} tip={tip} target={target} "
                        f"@ {utc_now_iso()}"
                    )
                    last_log = time.time()

            # after catching up to target, sleep a bit
            time.sleep(POLL_INTERVAL_SEC)

    except KeyboardInterrupt:
        print("[ADDRESS UPDATE] stopped by Ctrl+C")

    finally:
        try:
            if fp:
                fp.flush()
                os.fsync(fp.fileno())
                fp.close()
        except Exception:
            pass

        try:
            state.segments_completed = state.current_segment_start // state.segment_size
            save_state_atomic(STATE_PATH, state)
        except Exception:
            pass


# ============================================
# ▶️ Entrypoint
# ============================================

if __name__ == "__main__":
    update_loop()
