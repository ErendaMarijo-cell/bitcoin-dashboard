#!/usr/bin/env python3
# ==================================================
# 🔥 TXID BACKFILL WORKER
# Full Chain TXID Backfill (Genesis → Tip)
# Append-only JSONL segment writer
# ==================================================

import os
import sys
import time
import json
from datetime import datetime, timezone

# ============================================
# 🔧 Projekt-Root setzen
# ============================================

BASE_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../../")
)

if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

# ============================================
# 🔗 Progress Loader (Global Helper)
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
# 🔗 Global Config (Redis Keys File)
# ============================================

from core.redis_keys import (
    SEO_TXIDS_BACKFILL_STATE_PATH,
    SEO_TXIDS_BACKFILL_OUT_DIR,
    SEO_TXIDS_BACKFILL_LOOP_SLEEP_SEC,
    SEO_TXIDS_BACKFILL_PROGRESS_EVERY_N_BLOCKS,
    SEO_TXIDS_BACKFILL_FSYNC_EVERY_N_BLOCKS,
)


# ============================================
# 🔗 RPC Binding
# ============================================

RPC = BitcoinRPC(NODE_CONFIG["main"])
RPC.require_full_node()

print(f"[TXID BACKFILL] Bound → {RPC.info()}")

# ============================================
# Helpers
# ============================================

def utc_now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def ensure_out_dir():
    os.makedirs(SEO_TXIDS_BACKFILL_OUT_DIR, exist_ok=True)


def get_chain_tip():
    return int(RPC.call("getblockcount"))


def get_segment_file_path(entity, height, segment_size):
    start, end = segment_range_for_height(height, segment_size)
    fname = segment_filename(entity, start, end, pad=9, ext="jsonl")
    return os.path.join(SEO_TXIDS_BACKFILL_OUT_DIR, fname)


def append_block_txids(fp, height, block_hash, block_time, txids):
    """
    Append one JSONL record per TXID.
    Append-only → no dedupe needed.
    """

    for txid in txids:
        rec = {
            "height": height,
            "block_hash": block_hash,
            "block_time": block_time,
            "txid": txid,
        }

        fp.write(json.dumps(rec, separators=(",", ":")) + "\n")

# ============================================
# 🔄 Backfill Loop
# ============================================

def backfill_loop():

    print("[TXID BACKFILL] Worker started")

    ensure_out_dir()

    # --- Load progress state
    state = load_state(SEO_TXIDS_BACKFILL_STATE_PATH)

    start_height = state.next_height()
    tip          = get_chain_tip()

    print(f"[TXID BACKFILL] Resume → {start_height}")
    print(f"[TXID BACKFILL] Tip    → {tip}")

    if start_height > tip:
        print("[TXID BACKFILL] Already complete")
        return

    current_path = None
    fp = None

    blocks_since_progress = 0
    blocks_since_fsync    = 0

    try:
        for height in range(start_height, tip + 1):

            # ----------------------------------------
            # Segment Path
            # ----------------------------------------

            seg_path = get_segment_file_path(
                state.entity,
                height,
                state.segment_size
            )

            # ----------------------------------------
            # Rotate Segment File
            # ----------------------------------------

            if seg_path != current_path:

                if fp:
                    fp.flush()
                    os.fsync(fp.fileno())
                    fp.close()

                current_path = seg_path
                fp = open(current_path, "a", encoding="utf-8")

                seg_start, seg_end = segment_range_for_height(
                    height,
                    state.segment_size
                )

                state.current_segment_start = seg_start
                state.current_segment_end   = seg_end

                print(f"[TXID BACKFILL] Segment → {os.path.basename(seg_path)}")

            # ----------------------------------------
            # RPC Fetch
            # ----------------------------------------

            block_hash = RPC.call("getblockhash", [height])
            block      = RPC.call("getblock", [block_hash, 1])

            txids      = block.get("tx") or []
            block_time = int(block.get("time") or 0)

            # ----------------------------------------
            # Append JSONL
            # ----------------------------------------

            append_block_txids(
                fp,
                height,
                block_hash,
                block_time,
                txids
            )

            # ----------------------------------------
            # Counters
            # ----------------------------------------

            state.last_height = height
            state.events_written_total += len(txids)

            blocks_since_progress += 1
            blocks_since_fsync    += 1

            # ----------------------------------------
            # Durability Flush
            # ----------------------------------------

            if blocks_since_fsync >= SEO_TXIDS_BACKFILL_FSYNC_EVERY_N_BLOCKS:
                fp.flush()
                os.fsync(fp.fileno())
                blocks_since_fsync = 0

            # ----------------------------------------
            # Progress Save
            # ----------------------------------------

            if blocks_since_progress >= SEO_TXIDS_BACKFILL_PROGRESS_EVERY_N_BLOCKS:

                state.segments_completed = (
                    state.current_segment_start // state.segment_size
                )

                save_state_atomic(SEO_TXIDS_BACKFILL_STATE_PATH, state)

                blocks_since_progress = 0

            # ----------------------------------------
            # Logging
            # ----------------------------------------

            if height % 1000 == 0:
                print(
                    f"[TXID BACKFILL] Height {height} "
                    f"(events_total={state.events_written_total}) "
                    f"@ {utc_now_iso()}"
                )

            # ----------------------------------------
            # Throttle
            # ----------------------------------------

            if SEO_TXIDS_BACKFILL_LOOP_SLEEP_SEC > 0:
                time.sleep(SEO_TXIDS_BACKFILL_LOOP_SLEEP_SEC)

    except KeyboardInterrupt:
        print("[TXID BACKFILL] stopped")

    finally:

        # Final durability
        try:
            if fp:
                fp.flush()
                os.fsync(fp.fileno())
                fp.close()
        except Exception:
            pass

        # Final state save
        save_state_atomic(SEO_TXIDS_BACKFILL_STATE_PATH, state)

# ============================================
# ▶️ Entrypoint
# ============================================

if __name__ == "__main__":
    backfill_loop()
