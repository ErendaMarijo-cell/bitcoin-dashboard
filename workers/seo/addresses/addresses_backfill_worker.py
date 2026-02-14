#!/usr/bin/env python3
# ==================================================
# 🔥 ADDRESS BACKFILL WORKER (Layer 1B)
# Full Chain Address Event Backfill (Genesis → Tip)
# Writes JSONL segments (10k blocks/file)
#
# Output record (one per address-event):
# {"address":"...","txid":"...","height":N,"delta_sat":S}
#
# Uses getblock verbosity=3 to include prevout info.
# ==================================================

import os
import sys
import time
import json
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

# ============================================
# 🔧 Projekt-Root setzen
# ============================================

BASE_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../../")
)
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

# ============================================
# 🔗 Progress Loader
# ============================================

from workers.seo.addresses.addresses_backfill_progress import (
    load_state,
    save_state_atomic,
    segment_range_for_height,
    segment_filename,
)

# ============================================
# 🔗 Node RPC Layer
# ============================================

from nodes.config import NODE_CONFIG
from nodes.rpc import BitcoinRPC

# ============================================
# ⚙️ Config
# ============================================

STATE_PATH = "/raid/data/seo/addresses/progress/address_backfill_state.json"
OUT_DIR    = "/raid/data/seo/addresses/confirmed"

# Throttle (addresses is heavy; start conservative, tune later)
LOOP_SLEEP_SEC = 0.00

# Save progress every N blocks
PROGRESS_EVERY_N_BLOCKS = 10

# Durability: fsync every N blocks (addresses are huge; keep reasonable)
FSYNC_EVERY_N_BLOCKS = 10

# Log every N blocks
LOG_EVERY_N_BLOCKS = 100

# RPC retry
RPC_RETRIES = 5
RPC_RETRY_SLEEP = 0.5

# ============================================
# 🔗 RPC Binding
# ============================================

RPC = BitcoinRPC(NODE_CONFIG["main"])
RPC.require_full_node()
print(f"[ADDRESS BACKFILL] Bound to RPC {RPC.info()}")

# ============================================
# Helpers
# ============================================

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def rpc_call(method: str, params: Optional[list] = None):
    params = params or []
    last_err = None
    for i in range(RPC_RETRIES):
        try:
            return RPC.call(method, params)
        except Exception as e:
            last_err = e
            time.sleep(RPC_RETRY_SLEEP * (i + 1))
    raise RuntimeError(f"RPC failed after {RPC_RETRIES} retries: {method} {params} :: {last_err}")


def get_chain_tip() -> int:
    return int(rpc_call("getblockcount"))


def ensure_out_dir():
    os.makedirs(OUT_DIR, exist_ok=True)


def get_segment_file_path(entity: str, height: int, segment_size: int) -> str:
    start, end = segment_range_for_height(height, segment_size)
    fname = segment_filename(entity, start, end, pad=9, ext="jsonl")
    return os.path.join(OUT_DIR, fname)


def extract_address_from_scriptpubkey(spk: Dict[str, Any]) -> Optional[str]:
    """
    Bitcoin Core scriptPubKey formats vary by version/flags.
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
    """
    Core returns BTC amounts as JSON numbers (float-ish).
    Convert safely: round to nearest satoshi.
    """
    try:
        return int(round(float(v) * 100_000_000))
    except Exception:
        return 0


def write_event(fp, address: str, txid: str, height: int, delta_sat: int):
    rec = {
        "address": address,
        "txid": txid,
        "height": height,
        "delta_sat": int(delta_sat),
    }
    fp.write(json.dumps(rec, separators=(",", ":")) + "\n")


# ============================================
# Core extraction
# ============================================

def process_block(height: int):
    """
    Returns (tx_count, event_count_written)
    """
    block_hash = rpc_call("getblockhash", [height])

    # verbosity=3: includes decoded tx + vin.prevout info (best option for deltas)
    block = rpc_call("getblock", [block_hash, 3])

    txs = block.get("tx") or []
    event_count = 0

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
            value_btc = vout.get("value")
            delta = satoshis_from_btc_value(value_btc)
            if delta != 0:
                yield ("out", addr, txid, height, delta)

        # Inputs => negative delta (from prevout)
        for vin in (tx.get("vin") or []):
            # coinbase has no prevout
            if "coinbase" in vin:
                continue
            prevout = vin.get("prevout") or {}
            spk = (prevout.get("scriptPubKey") or {})
            addr = extract_address_from_scriptpubkey(spk)
            if not addr:
                continue
            value_btc = prevout.get("value")
            delta = satoshis_from_btc_value(value_btc)
            if delta != 0:
                yield ("in", addr, txid, height, -delta)


# ============================================
# 🔄 Backfill Loop
# ============================================

def backfill_loop():
    print("[ADDRESS BACKFILL] Worker started")
    ensure_out_dir()

    state = load_state(STATE_PATH)
    state.entity = "addresses"  # ensure correct

    start_height = state.next_height()
    tip = get_chain_tip()

    print(f"[ADDRESS BACKFILL] Resume height → {start_height}")
    print(f"[ADDRESS BACKFILL] Chain tip     → {tip}")

    if start_height > tip:
        print("[ADDRESS BACKFILL] Already fully backfilled")
        return

    current_path = None
    fp = None

    blocks_since_progress = 0
    blocks_since_fsync = 0

    try:
        for height in range(start_height, tip + 1):

            seg_path = get_segment_file_path(
                entity=state.entity,
                height=height,
                segment_size=state.segment_size
            )

            if seg_path != current_path:
                if fp:
                    fp.flush()
                    os.fsync(fp.fileno())
                    fp.close()

                current_path = seg_path
                fp = open(current_path, "a", encoding="utf-8")

                seg_start, seg_end = segment_range_for_height(height, state.segment_size)
                state.current_segment_start = seg_start
                state.current_segment_end = seg_end

                print(f"[ADDRESS BACKFILL] Segment → {os.path.basename(current_path)}")

            # --- Extract + write events
            events_written_this_block = 0
            tx_count = 0

            # We stream yields to avoid big in-memory structures
            for _role, addr, txid, h, delta in process_block(height):
                write_event(fp, addr, txid, h, delta)
                events_written_this_block += 1

            # Update state
            state.last_height = height
            state.events_written_total += events_written_this_block

            blocks_since_progress += 1
            blocks_since_fsync += 1

            if blocks_since_fsync >= FSYNC_EVERY_N_BLOCKS:
                fp.flush()
                os.fsync(fp.fileno())
                blocks_since_fsync = 0

            if blocks_since_progress >= PROGRESS_EVERY_N_BLOCKS:
                state.segments_completed = state.current_segment_start // state.segment_size
                save_state_atomic(STATE_PATH, state)
                blocks_since_progress = 0

            if height % LOG_EVERY_N_BLOCKS == 0:
                print(
                    f"[ADDRESS BACKFILL] Height {height} "
                    f"→ events +{events_written_this_block} "
                    f"(total_events={state.events_written_total}) "
                    f"@ {utc_now_iso()}"
                )

            if LOOP_SLEEP_SEC > 0:
                time.sleep(LOOP_SLEEP_SEC)

    except KeyboardInterrupt:
        print("[ADDRESS BACKFILL] stopped by Ctrl+C")

    except Exception as e:
        print(f"[ADDRESS BACKFILL ERROR] {e}")

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


if __name__ == "__main__":
    backfill_loop()
