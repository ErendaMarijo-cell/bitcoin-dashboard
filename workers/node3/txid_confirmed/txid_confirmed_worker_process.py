# ==========================================================
# 🔥 PROCESS WRAPPER
# File: txid_confirmed_worker_process.py
#
# Entrypoint wrapper for:
# workers/node3/txid_confirmed/txid_confirmed_worker.py
#
# Purpose:
# - Provides stable systemd entrypoint
# - Sets project root for imports
# - Starts realtime confirmed TXID writer loop
# ==========================================================

import os
import sys
import time
import traceback

# ============================================
# 🔧 Project Root
# ============================================
PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../../")
)

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# ============================================
# 🔗 Worker Import
# ============================================
try:
    from workers.node3.txid_confirmed.txid_confirmed_worker import (
        main as txid_confirmed_worker_loop
    )
except ImportError as e:
    print("[TXID CONFIRMED WORKER PROCESS] ❌ ImportError")
    print(str(e))
    traceback.print_exc()
    sys.exit(1)

# ============================================
# ▶️ Entrypoint
# ============================================
if __name__ == "__main__":

    print("[TXID CONFIRMED WORKER PROCESS] started")

    # optional boot delay
    time.sleep(1.0)

    try:
        txid_confirmed_worker_loop()

    except KeyboardInterrupt:
        print("[TXID CONFIRMED WORKER PROCESS] stopped by Ctrl+C")

    except Exception as e:
        print("[TXID CONFIRMED WORKER PROCESS] ❌ Worker crashed")
        print(str(e))
        traceback.print_exc()
        sys.exit(1)
