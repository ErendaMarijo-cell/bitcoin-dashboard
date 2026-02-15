#!/usr/bin/env python3
# ==================================================
# 🔥 TXID BACKFILL WORKER (RAM BUFFERED)
# NVMe optimized high-throughput writer
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
# 🔗 Global Config
# ============================================

from core.redis_keys import (
    SEO_TXIDS_BACKFILL_STATE_PATH,
    SEO_TXIDS_BACKFILL_OUT_DIR,
    SEO_TXIDS_BACKFILL_LOOP_SLEEP_SEC,
)

# ============================================
# 🚀 RAM BUFFER CONFIG
# ============================================

TXID_BUFFER_MAX_EVENTS = 2_000_000
TXID_BUFFER_FLUSH_BLOCKS = 1000

# ============================================
# RPC Binding
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

# ============================================
# RAM BUFFER
# ============================================

class TxidBuffer:

    def __init__(self):
        self.events = []
        self.blocks_buffered = 0

    def add_block(self, height, block_hash, block_time, txids):

        for txid in txids:
            self.events.append({
                "height": height,
                "block_hash": block_hash,
                "block_time": block_time,
                "txid": txid,
            })

        self.blocks_buffered += 1

    def should_flush(self):
        return (
            len(self.events) >= TXID_BUFFER_MAX_EVENTS
            or self.blocks_buffered >= TXID_BUFFER_FLUSH_BLOCKS
        )

    def flush(self, fp):

        for rec in self.events:
            fp.write(json.dumps(rec, separators=(",", ":")) + "\n")

        flushed = len(self.events)

        self.events.clear()
        self.blocks_buffered = 0

        return flushed

# ============================================
# 🔄 Backfill Loop
# ============================================

def backfill_loop():

    print("[TXID BACKFILL] Worker started")

    ensure_out_dir()

    state = load_state(SEO_TXIDS_BACKFILL_STATE_PATH)

    start_height = state.next_height()
    tip = get_chain_tip()

    print(f"[TXID BACKFILL] Resume → {start_height}")
    print(f"[TXID BACKFILL] Tip    → {tip}")

    if start_height > tip:
        print("[TXID BACKFILL] Already complete")
        return

    current_path = None
    fp = None

    buffer = TxidBuffer()

    try:
        for height in range(start_height, tip + 1):

            seg_path = get_segment_file_path(
                state.entity,
                height,
                state.segment_size
            )

            # Rotate segment
            if seg_path != current_path:

                if fp:
                    buffer.flush(fp)
                    fp.flush()
                    os.fsync(fp.fileno())
                    fp.close()

                current_path = seg_path
                fp = open(current_path, "a", encoding="utf-8", buffering=1024*1024)

                seg_start, seg_end = segment_range_for_height(
                    height,
                    state.segment_size
                )

                state.current_segment_start = seg_start
                state.current_segment_end   = seg_end

                print(f"[TXID BACKFILL] Segment → {os.path.basename(seg_path)}")

            # RPC
            block_hash = RPC.call("getblockhash", [height])
            block      = RPC.call("getblock", [block_hash, 1])

            txids      = block.get("tx") or []
            block_time = int(block.get("time") or 0)

            # Buffer add
            buffer.add_block(height, block_hash, block_time, txids)

            state.last_height = height
            state.events_written_total += len(txids)

            # Flush if needed
            if buffer.should_flush():

                flushed = buffer.flush(fp)

                fp.flush()
                os.fsync(fp.fileno())

                save_state_atomic(
                    SEO_TXIDS_BACKFILL_STATE_PATH,
                    state
                )

                print(
                    f"[TXID BACKFILL] Flush → {flushed} events "
                    f"@ {utc_now_iso()}"
                )

            # Log
            if height % 1000 == 0:
                print(
                    f"[TXID BACKFILL] Height {height} "
                    f"(events_total={state.events_written_total}) "
                    f"@ {utc_now_iso()}"
                )

            if SEO_TXIDS_BACKFILL_LOOP_SLEEP_SEC > 0:
                time.sleep(SEO_TXIDS_BACKFILL_LOOP_SLEEP_SEC)

    finally:

        try:
            if fp:
                buffer.flush(fp)
                fp.flush()
                os.fsync(fp.fileno())
                fp.close()
        except Exception:
            pass

        save_state_atomic(
            SEO_TXIDS_BACKFILL_STATE_PATH,
            state
        )

# ============================================
# ▶️ Entrypoint
# ============================================

if __name__ == "__main__":
    backfill_loop()
