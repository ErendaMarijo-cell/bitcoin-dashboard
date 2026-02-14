#!/usr/bin/env python3
# ==================================================
# 🔥 TXID UPDATE WORKER
# Live TXID Sync (Tip Tracking)
# Appends new TXIDs after backfill
# ==================================================

import os
import sys
import time
import json
from datetime import datetime, timezone

# ============================================
# 🔧 Project Root
# ============================================

BASE_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../../")
)

if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

# ============================================
# 🔗 Shared Backfill Helper
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
# 🔗 Global Config
# ============================================

from core.redis_keys import (
    SEO_TXIDS_BACKFILL_STATE_PATH,
    SEO_TXIDS_BACKFILL_OUT_DIR,
)

# ============================================
# ⚙️ Update Worker Config
# ============================================

POLL_INTERVAL_SEC = 10      # Tip polling
FSYNC_EVERY_BLOCK = True    # Max durability

# ============================================
# RPC Binding
# ============================================

RPC = BitcoinRPC(NODE_CONFIG["main"])
RPC.require_full_node()

print(f"[TXID UPDATE] Bound → {RPC.info()}")

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

    for txid in txids:
        rec = {
            "height": height,
            "block_hash": block_hash,
            "block_time": block_time,
            "txid": txid,
        }

        fp.write(json.dumps(rec, separators=(",", ":")) + "\n")


# ============================================
# 🔄 Live Update Loop
# ============================================

def update_loop():

    print("[TXID UPDATE] Worker started")

    ensure_out_dir()

    state = load_state(SEO_TXIDS_BACKFILL_STATE_PATH)

    # Safety: ensure entity
    state.entity = "txids"

    last_height = state.last_height

    print(f"[TXID UPDATE] Last indexed → {last_height}")

    current_path = None
    fp = None

    while True:

        try:

            tip = get_chain_tip()

            # ----------------------------------------
            # Tip reached → wait
            # ----------------------------------------

            if last_height >= tip:
                time.sleep(POLL_INTERVAL_SEC)
                continue

            print(f"[TXID UPDATE] Sync {last_height+1} → {tip}")

            # ----------------------------------------
            # Process new blocks
            # ----------------------------------------

            for height in range(last_height + 1, tip + 1):

                seg_path = get_segment_file_path(
                    state.entity,
                    height,
                    state.segment_size
                )

                # Segment rotation
                if seg_path != current_path:

                    if fp:
                        fp.flush()
                        os.fsync(fp.fileno())
                        fp.close()

                    current_path = seg_path
                    fp = open(current_path, "a", encoding="utf-8")

                    print(
                        f"[TXID UPDATE] Segment → "
                        f"{os.path.basename(seg_path)}"
                    )

                # ----------------------------------------
                # RPC Fetch
                # ----------------------------------------

                block_hash = RPC.call("getblockhash", [height])
                block      = RPC.call("getblock", [block_hash, 1])

                txids      = block.get("tx") or []
                block_time = int(block.get("time") or 0)

                append_block_txids(
                    fp,
                    height,
                    block_hash,
                    block_time,
                    txids
                )

                # ----------------------------------------
                # Durability
                # ----------------------------------------

                if FSYNC_EVERY_BLOCK:
                    fp.flush()
                    os.fsync(fp.fileno())

                # ----------------------------------------
                # State update
                # ----------------------------------------

                last_height = height
                state.last_height = height
                state.events_written_total += len(txids)

                save_state_atomic(
                    SEO_TXIDS_BACKFILL_STATE_PATH,
                    state
                )

                print(
                    f"[TXID UPDATE] Height {height} "
                    f"(tx={len(txids)}) "
                    f"@ {utc_now_iso()}"
                )

        except KeyboardInterrupt:
            print("[TXID UPDATE] stopped by Ctrl+C")
            break

        except Exception as e:
            print(f"[TXID UPDATE ERROR] {e}")
            time.sleep(5)

    # Final durability
    try:
        if fp:
            fp.flush()
            os.fsync(fp.fileno())
            fp.close()
    except Exception:
        pass


# ============================================
# ▶️ Entrypoint
# ============================================

if __name__ == "__main__":
    update_loop()
