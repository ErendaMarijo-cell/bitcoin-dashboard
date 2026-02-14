#!/usr/bin/env python3
# ==================================================
# 🔥 TXID BACKFILL WORKER
# Full Chain TXID Backfill (Genesis → Tip)
# Writes enriched JSONL segments (10k blocks/file)
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

STATE_PATH = "/raid/lightning/seo/txids/progress/txid_backfill_state.json"
OUT_DIR    = "/raid/lightning/seo/txids/confirmed"


# Throttle to protect node + other workers
LOOP_SLEEP_SEC = 0.02

# Save progress every N blocks (still safe + much less IO)
PROGRESS_EVERY_N_BLOCKS = 25

# Force flush/fsync every N blocks (tune later)
FSYNC_EVERY_N_BLOCKS = 25

# ============================================
# 🔗 RPC Binding
# ============================================

RPC = BitcoinRPC(NODE_CONFIG["main"])
RPC.require_full_node()

print(f"[TXID BACKFILL] Bound to RPC {RPC.info()}")

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


def append_block_txids_jsonl(fp, height: int, block_hash: str, block_time: int, txids: list[str]):
    """
    Append one JSONL record per txid.
    Enriched fields allow charts + joins later.
    """
    # Very compact JSON
    for txid in txids:
        rec = {
            "height": height,
            "block_hash": block_hash,
            "block_time": block_time,  # unix seconds
            "txid": txid,
        }
        fp.write(json.dumps(rec, separators=(",", ":")) + "\n")


# ============================================
# 🔄 Backfill Loop
# ============================================

def backfill_loop():

    print("[TXID BACKFILL] Worker started")
    ensure_out_dir()

    # Load progress
    state = load_state(STATE_PATH)

    start_height = state.next_height()
    tip = get_chain_tip()

    print(f"[TXID BACKFILL] Resume height → {start_height}")
    print(f"[TXID BACKFILL] Chain tip     → {tip}")

    if start_height > tip:
        print("[TXID BACKFILL] Already fully backfilled")
        return

    # Keep one file handle open per segment, rotate on segment change
    current_path = None
    fp = None

    blocks_since_progress = 0
    blocks_since_fsync = 0

    try:
        for height in range(start_height, tip + 1):

            # --- Determine segment file path
            seg_path = get_segment_file_path(
                entity=state.entity,
                height=height,
                segment_size=state.segment_size
            )

            # --- Rotate file if needed
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

                print(f"[TXID BACKFILL] Segment → {os.path.basename(current_path)}")

            # --- Fetch block
            block_hash = RPC.call("getblockhash", [height])

            # verbosity=1 => dict with txids + time
            block = RPC.call("getblock", [block_hash, 1])

            txids = block.get("tx") or []
            block_time = int(block.get("time") or 0)

            # --- Write JSONL
            append_block_txids_jsonl(
                fp=fp,
                height=height,
                block_hash=block_hash,
                block_time=block_time,
                txids=txids
            )

            # --- Counters
            state.last_height = height
            state.txids_indexed_total += len(txids)

            blocks_since_progress += 1
            blocks_since_fsync += 1

            # --- Periodic fsync (durability) without killing performance
            if blocks_since_fsync >= FSYNC_EVERY_N_BLOCKS:
                fp.flush()
                os.fsync(fp.fileno())
                blocks_since_fsync = 0

            # --- Periodic progress save
            if blocks_since_progress >= PROGRESS_EVERY_N_BLOCKS:
                # completed segments count (rough, but useful)
                state.segments_completed = state.current_segment_start // state.segment_size
                save_state_atomic(STATE_PATH, state)
                blocks_since_progress = 0

            # --- Logging (lightweight)
            if height % 1000 == 0:
                print(
                    f"[TXID BACKFILL] Height {height} "
                    f"→ +{len(txids)} txids "
                    f"(total_txids={state.txids_indexed_total}) "
                    f"@ {utc_now_iso()}"
                )

            # --- Throttle
            if LOOP_SLEEP_SEC > 0:
                time.sleep(LOOP_SLEEP_SEC)

    except KeyboardInterrupt:
        print("[TXID BACKFILL] stopped by Ctrl+C")

    except Exception as e:
        print(f"[TXID BACKFILL ERROR] {e}")

    finally:
        # Final durability + progress save
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
    backfill_loop()
