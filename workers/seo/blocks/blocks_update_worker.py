#!/usr/bin/env python3
# ==================================================
# 🔥 BLOCK LAYER0 UPDATE WORKER
# Live Block Metadata Sync (Tip Tracking)
# Appends new blocks after backfill
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
# ⚙️ Config
# ============================================

STATE_PATH = "/raid/data/seo/blocks/progress/blocks_backfill_state.json"
OUT_DIR    = "/raid/data/seo/blocks/confirmed"

POLL_INTERVAL_SEC = 10

# ============================================
# RPC Bind
# ============================================

RPC = BitcoinRPC(NODE_CONFIG["main"])
RPC.require_full_node()

print(f"[BLOCK LAYER0 PROCESS] Bound to RPC {RPC.info()}")

# ============================================
# Helpers
# ============================================

def utc_now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def ensure_out_dir():
    os.makedirs(OUT_DIR, exist_ok=True)


def get_tip():
    return int(RPC.call("getblockcount"))


def get_segment_file_path(entity, height, segment_size):
    start, end = segment_range_for_height(height, segment_size)
    fname = segment_filename(entity, start, end, pad=9, ext="jsonl")
    return os.path.join(OUT_DIR, fname)


def append_block_jsonl(fp, height, block_hash, block_time):
    rec = {
        "height": height,
        "hash": block_hash,
        "time": block_time,
    }
    fp.write(json.dumps(rec, separators=(",", ":")) + "\n")


# ============================================
# 🔄 Live Sync Loop
# ============================================

def process_loop():

    print("[BLOCK LAYER0 PROCESS] Worker started")
    ensure_out_dir()

    state = load_state(STATE_PATH)
    state.entity = "blocks"

    last_height = state.last_height

    print(f"[BLOCK LAYER0 PROCESS] Last indexed height → {last_height}")

    current_path = None
    fp = None

    while True:

        try:
            tip = get_tip()

            if last_height >= tip:
                time.sleep(POLL_INTERVAL_SEC)
                continue

            for height in range(last_height + 1, tip + 1):

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

                    print(
                        f"[BLOCK LAYER0 PROCESS] Segment → "
                        f"{os.path.basename(current_path)}"
                    )

                block_hash = RPC.call("getblockhash", [height])
                header = RPC.call("getblockheader", [block_hash])

                block_time = int(header.get("time") or 0)

                append_block_jsonl(
                    fp,
                    height,
                    block_hash,
                    block_time
                )

                last_height = height
                state.last_height = height

                save_state_atomic(STATE_PATH, state)

                print(
                    f"[BLOCK LAYER0 PROCESS] Height {height} "
                    f"indexed @ {utc_now_iso()}"
                )

        except Exception as e:
            print(f"[BLOCK LAYER0 PROCESS ERROR] {e}")
            time.sleep(5)


# ============================================
# ▶️ Entrypoint
# ============================================

if __name__ == "__main__":
    process_loop()
