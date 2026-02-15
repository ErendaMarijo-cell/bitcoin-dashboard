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
#
# PERF GOALS:
# - Max throughput (sequential appends, big batches)
# - NVMe friendly (rare fsync; fsync only on segment close)
# - Crash-safe progress (checkpoint only after flush)
# ==================================================

import os
import sys
import time
import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional

# ============================================
# 🔧 Projekt-Root setzen
# ============================================

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

# ============================================
# 🔗 Progress Loader (Shared)
# ============================================

from workers.seo.helper.backfill_jsonl_helper import (
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

STATE_PATH = "/raid/data/seo/addresses/progress/addresses_backfill_state.json"
OUT_DIR    = "/raid/data/seo/addresses/confirmed"

# Segment settings come from state.segment_size (e.g. 10k blocks/file)

# ---- Performance tuning ----
# Buffer events in RAM and write in large batches.
# 200k–500k is a good sweet spot for throughput without risking OOM.
BUFFER_MAX_EVENTS = 1_500_000

# Also flush at least every N blocks (even if buffer not full),
# to keep memory bounded and checkpoint progress.
FLUSH_EVERY_N_BLOCKS = 1000

# Save progress every N blocks (we will flush BEFORE saving to keep state consistent)
PROGRESS_EVERY_N_BLOCKS = 1000

# fsync only when closing a segment file (NVMe-friendly)
FSYNC_ON_SEGMENT_CLOSE = True

# Log every N blocks
LOG_EVERY_N_BLOCKS = 100

# Optional throttle (usually keep 0 for max throughput)
LOOP_SLEEP_SEC = 0.0

# RPC retry
RPC_RETRIES = 5
RPC_RETRY_SLEEP = 0.5

# File buffering (1–8MB is good)
FILE_BUFFER_BYTES = 4 * 1024 * 1024


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


def make_event_line(address: str, txid: str, height: int, delta_sat: int) -> str:
    # Build minimal JSON (no spaces) as a single line.
    rec = {
        "address": address,
        "txid": txid,
        "height": int(height),
        "delta_sat": int(delta_sat),
    }
    return json.dumps(rec, separators=(",", ":"))


# ============================================
# Core extraction (generator)
# ============================================

def process_block(height: int):
    block_hash = rpc_call("getblockhash", [height])
    block = rpc_call("getblock", [block_hash, 3])  # verbosity=3 includes prevout

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

        # Inputs => negative delta (prevout)
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
# IO helpers (buffered)
# ============================================

def flush_buffer(fp, buffer_lines) -> int:
    """
    Writes buffered lines as one big sequential append.
    Returns number of events flushed.
    """
    if not buffer_lines:
        return 0
    # One big write => NVMe happy, low syscall count
    fp.write("\n".join(buffer_lines))
    fp.write("\n")
    buffer_lines.clear()
    fp.flush()  # flush userspace buffer to OS (NO fsync)
    return 0  # not used, kept for clarity


def close_segment(fp):
    if not fp:
        return
    fp.flush()
    if FSYNC_ON_SEGMENT_CLOSE:
        os.fsync(fp.fileno())
    fp.close()


# ============================================
# 🔄 Backfill Loop
# ============================================

def backfill_loop():
    print("[ADDRESS BACKFILL] Worker started")
    ensure_out_dir()

    state = load_state(STATE_PATH)
    state.entity = "addresses"

    start_height = state.next_height()
    tip = get_chain_tip()

    print(f"[ADDRESS BACKFILL] Resume height → {start_height}")
    print(f"[ADDRESS BACKFILL] Chain tip     → {tip}")

    if start_height > tip:
        print("[ADDRESS BACKFILL] Already fully backfilled")
        return

    current_path = None
    fp = None

    # Big in-memory buffer
    buffer_lines = []
    buffered_events = 0

    blocks_since_flush = 0
    blocks_since_progress = 0

    try:
        for height in range(start_height, tip + 1):

            seg_path = get_segment_file_path(
                entity=state.entity,
                height=height,
                segment_size=state.segment_size
            )

            # Segment switch
            if seg_path != current_path:
                # Flush anything pending into old segment, then fsync+close old segment.
                if fp:
                    flush_buffer(fp, buffer_lines)
                    buffered_events = 0
                    close_segment(fp)

                current_path = seg_path
                fp = open(current_path, "a", encoding="utf-8", buffering=FILE_BUFFER_BYTES)

                seg_start, seg_end = segment_range_for_height(height, state.segment_size)
                state.current_segment_start = seg_start
                state.current_segment_end = seg_end

                print(f"[ADDRESS BACKFILL] Segment → {os.path.basename(current_path)}")

            # --- Extract + buffer events
            events_this_block = 0
            for addr, txid, h, delta in process_block(height):
                buffer_lines.append(make_event_line(addr, txid, h, delta))
                buffered_events += 1
                events_this_block += 1

                # Flush by size to avoid unbounded RAM
                if buffered_events >= BUFFER_MAX_EVENTS:
                    flush_buffer(fp, buffer_lines)
                    buffered_events = 0
                    blocks_since_flush = 0  # we just flushed

            # Update state (in-memory)
            state.last_height = height
            state.events_written_total += events_this_block

            blocks_since_flush += 1
            blocks_since_progress += 1

            # Periodic flush (NVMe friendly: flush often; fsync rarely)
            if blocks_since_flush >= FLUSH_EVERY_N_BLOCKS:
                flush_buffer(fp, buffer_lines)
                buffered_events = 0
                blocks_since_flush = 0

            # Progress checkpoint (IMPORTANT: flush BEFORE saving progress)
            if blocks_since_progress >= PROGRESS_EVERY_N_BLOCKS:
                flush_buffer(fp, buffer_lines)
                buffered_events = 0

                state.segments_completed = state.current_segment_start // state.segment_size
                save_state_atomic(STATE_PATH, state)
                blocks_since_progress = 0

            if height % LOG_EVERY_N_BLOCKS == 0:
                print(
                    f"[ADDRESS BACKFILL] Height {height} "
                    f"→ events +{events_this_block} "
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
                # Final flush of remaining buffer, then fsync+close
                flush_buffer(fp, buffer_lines)
                close_segment(fp)
        except Exception:
            pass

        try:
            state.segments_completed = state.current_segment_start // state.segment_size
            save_state_atomic(STATE_PATH, state)
        except Exception:
            pass


if __name__ == "__main__":
    backfill_loop()
