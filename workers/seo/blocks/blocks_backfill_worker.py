#!/usr/bin/env python3
# ==================================================
# 🔥 BLOCK LAYER0 BACKFILL WORKER (RAM BUFFERED)
# NVMe optimized metadata writer
# ==================================================

import os
import sys
import time
import json
from datetime import datetime, timezone

# ============================================
# 🔧 Projekt Root
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
# 🔗 Node RPC
# ============================================

from nodes.config import NODE_CONFIG
from nodes.rpc import BitcoinRPC

# ============================================
# ⚙️ Config
# ============================================

STATE_PATH = "/raid/data/seo/blocks/progress/blocks_backfill_state.json"
OUT_DIR    = "/raid/data/seo/blocks/confirmed"

LOOP_SLEEP_SEC = 0.005

# ============================================
# 🚀 RAM BUFFER CONFIG
# ============================================

BLOCK_BUFFER_MAX_EVENTS   = 1_000_000
BLOCK_BUFFER_FLUSH_BLOCKS = 5000

# ============================================
# RPC Binding
# ============================================

RPC = BitcoinRPC(NODE_CONFIG["main"])
RPC.require_full_node()

print(f"[BLOCK LAYER0] Bound to RPC {RPC.info()}")

# ============================================
# Helpers
# ============================================

def utc_now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def get_chain_tip():
    return int(RPC.call("getblockcount"))


def ensure_out_dir():
    os.makedirs(OUT_DIR, exist_ok=True)


def get_segment_file_path(entity, height, segment_size):
    start, end = segment_range_for_height(height, segment_size)
    fname = segment_filename(entity, start, end, pad=9, ext="jsonl")
    return os.path.join(OUT_DIR, fname)

# ============================================
# RAM BUFFER
# ============================================

class BlockBuffer:

    def __init__(self):
        self.records = []
        self.blocks_buffered = 0

    def add(self, height, block_hash, block_time):

        self.records.append({
            "height": height,
            "hash": block_hash,
            "time": block_time,
        })

        self.blocks_buffered += 1

    def should_flush(self):

        return (
            len(self.records) >= BLOCK_BUFFER_MAX_EVENTS
            or self.blocks_buffered >= BLOCK_BUFFER_FLUSH_BLOCKS
        )

    def flush(self, fp):

        for rec in self.records:
            fp.write(json.dumps(rec, separators=(",", ":")) + "\n")

        flushed = len(self.records)

        self.records.clear()
        self.blocks_buffered = 0

        return flushed

# ============================================
# 🔄 Backfill Loop
# ============================================

def backfill_loop():

    print("[BLOCK LAYER0] Worker started")
    ensure_out_dir()

    state = load_state(STATE_PATH)
    state.entity = "blocks"

    start_height = state.next_height()
    tip = get_chain_tip()

    print(f"[BLOCK LAYER0] Resume height → {start_height}")
    print(f"[BLOCK LAYER0] Chain tip     → {tip}")

    if start_height > tip:
        print("[BLOCK LAYER0] Already fully backfilled")
        return

    current_path = None
    fp = None

    buffer = BlockBuffer()

    try:
        for height in range(start_height, tip + 1):

            seg_path = get_segment_file_path(
                entity=state.entity,
                height=height,
                segment_size=state.segment_size
            )

            # Segment rotate
            if seg_path != current_path:

                if fp:
                    buffer.flush(fp)
                    fp.flush()
                    os.fsync(fp.fileno())
                    fp.close()

                current_path = seg_path
                fp = open(current_path, "a", encoding="utf-8", buffering=1024*1024)

                seg_start, seg_end = segment_range_for_height(
                    height, state.segment_size
                )

                state.current_segment_start = seg_start
                state.current_segment_end   = seg_end

                print(f"[BLOCK LAYER0] Segment → {os.path.basename(current_path)}")

            # RPC fetch
            block_hash = RPC.call("getblockhash", [height])
            header     = RPC.call("getblockheader", [block_hash])

            block_time = int(header.get("time") or 0)

            buffer.add(height, block_hash, block_time)

            state.last_height = height

            # Flush if needed
            if buffer.should_flush():

                flushed = buffer.flush(fp)

                fp.flush()
                os.fsync(fp.fileno())

                save_state_atomic(STATE_PATH, state)

                print(
                    f"[BLOCK LAYER0] Flush → {flushed} blocks "
                    f"@ {utc_now_iso()}"
                )

            # Log
            if height % 5000 == 0:
                print(
                    f"[BLOCK LAYER0] Height {height} indexed "
                    f"@ {utc_now_iso()}"
                )

            if LOOP_SLEEP_SEC > 0:
                time.sleep(LOOP_SLEEP_SEC)

    finally:

        try:
            if fp:
                buffer.flush(fp)
                fp.flush()
                os.fsync(fp.fileno())
                fp.close()
        except Exception:
            pass

        save_state_atomic(STATE_PATH, state)

# ============================================
# ▶️ Entrypoint
# ============================================

if __name__ == "__main__":
    backfill_loop()
