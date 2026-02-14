#!/usr/bin/env python3
# ==================================================
# 🔥 ADDRESS LAYER1B UPDATE WORKER
# Live Address Event Sync (Tip Tracking)
# ==================================================

import os
import sys
import time

BASE_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../../")
)

if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

# ============================================
# 🔗 State Loader
# ============================================

from workers.seo.addresses.addresses_backfill_worker import (
    load_state,
    save_state_atomic,
)

# ============================================
# 🔗 Node RPC
# ============================================

from nodes.config import NODE_CONFIG
from nodes.rpc import BitcoinRPC

# ============================================
# ⚙️ Config
# ============================================

STATE_PATH = "/raid/data/seo/addresses/progress/address_backfill_state.json"

POLL_INTERVAL_SEC = 10

RPC = BitcoinRPC(NODE_CONFIG["main"])
RPC.require_full_node()

print(f"[ADDRESS UPDATE] Bound to RPC {RPC.info()}")


# ============================================
# 🔄 Live Loop
# ============================================

def update_loop():

    print("[ADDRESS UPDATE] Worker started")

    state = load_state(STATE_PATH)

    last_height = state.last_height

    while True:

        try:
            tip = int(RPC.call("getblockcount"))

            if last_height >= tip:
                time.sleep(POLL_INTERVAL_SEC)
                continue

            for height in range(last_height + 1, tip + 1):

                print(f"[ADDRESS UPDATE] New block → {height}")

                # TODO:
                # call address extraction logic
                # append events
                # update state

                last_height = height
                state.last_height = height

                save_state_atomic(STATE_PATH, state)

        except Exception as e:
            print(f"[ADDRESS UPDATE ERROR] {e}")
            time.sleep(5)


if __name__ == "__main__":
    update_loop()
