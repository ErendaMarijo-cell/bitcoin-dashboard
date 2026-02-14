#!/usr/bin/env python3
# ==================================================
# 🔥 BLOCK LAYER 0 BACKFILL WORKER
# Full Chain Block Metadata Backfill (Genesis → Tip)
# Writes JSONL segments (10k blocks/file)
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
# 🔗 Progress Loader
# ============================================

from workers.seo.txids.txids_backfill_progress import (
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

STATE_PATH = "/raid/data/seo/blocks/progress/blocks_backfill_state.json"
OUT_DIR    = "/raid/data/seo/blocks/confirmed"

LOOP_SLEEP_SEC = 0.005
PROGRESS_EVERY_N_BLOCKS = 100
FSYNC_EVERY_N_BLOCKS = 100

# ============================================
# 🔗 RPC Binding
# ============================================

RPC = BitcoinRPC(NODE_CONFIG["main"])
RPC.require_full_node()

print(f"[BLOCK LAYER0] Bound to RPC {RPC.info()}")

# ============================================
# Helpers
# ============================================

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def get_chain_tip() -> int:
    return int(RPC.call("getblockcount"))


def ensure_out_dir():
    os.makedirs(OUT_DIR, exist_ok=True)


def get_segment_file_path(entity: str, height: int, segment_size: int) -> str:
    start, end = segment_range_for_height(height, segment_size)
    fname = segment_filename(entity, start, end, pad=9, ext="jsonl")
    return os.path.join(OUT_DIR, fname)


def append_block_jsonl(fp, height: int, block_hash: str, block_time: int):
    rec = {
        "height": height,
        "hash": block_hash,
        "time": block_time,
    }
    fp.write(json.dumps(rec, separators=(",", ":")) + "\n")


# ============================================
# 🔄 Backfill Loop
# ============================================

def backfill_loop():

    print("[BLOCK LAYER0] Worker started")
    ensure_out_dir()

    state = load_state(STATE_PATH)
    state.entity = "blocks"  # override entity

    start_height = state.next_height()
    tip = get_chain_tip()

    print(f"[BLOCK LAYER0] Resume height → {start_height}")
    print(f"[BLOCK LAYER0] Chain tip     → {tip}")

    if start_height > tip:
        print("[BLOCK LAYER0] Already fully backfilled")
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

                seg_start, seg_end = segment_range_for_height(
                    height, state.segment_size
                )
                state.current_segment_start = seg_start
                state.current_segment_end = seg_end

                print(f"[BLOCK LAYER0] Segment → {os.path.basename(current_path)}")

            # --- Fetch block header
            block_hash = RPC.call("getblockhash", [height])
            header = RPC.call("getblockheader", [block_hash])

            block_time = int(header.get("time") or 0)

            append_block_jsonl(
                fp=fp,
                height=height,
                block_hash=block_hash,
                block_time=block_time
            )

            state.last_height = height

            blocks_since_progress += 1
            blocks_since_fsync += 1

            if blocks_since_fsync >= FSYNC_EVERY_N_BLOCKS:
                fp.flush()
                os.fsync(fp.fileno())
                blocks_since_fsync = 0

            if blocks_since_progress >= PROGRESS_EVERY_N_BLOCKS:
                state.segments_completed = (
                    state.current_segment_start // state.segment_size
                )
                save_state_atomic(STATE_PATH, state)
                blocks_since_progress = 0

            if height % 5000 == 0:
                print(
                    f"[BLOCK LAYER0] Height {height} indexed "
                    f"@ {utc_now_iso()}"
                )

            if LOOP_SLEEP_SEC > 0:
                time.sleep(LOOP_SLEEP_SEC)

    except KeyboardInterrupt:
        print("[BLOCK LAYER0] stopped by Ctrl+C")

    except Exception as e:
        print(f"[BLOCK LAYER0 ERROR] {e}")

    finally:
        try:
            if fp:
                fp.flush()
                os.fsync(fp.fileno())
                fp.close()
        except Exception:
            pass

        try:
            state.segments_completed = (
                state.current_segment_start // state.segment_size
            )
            save_state_atomic(STATE_PATH, state)
        except Exception:
            pass


# ============================================
# ▶️ Entrypoint
# ============================================

if __name__ == "__main__":
    backfill_loop()
