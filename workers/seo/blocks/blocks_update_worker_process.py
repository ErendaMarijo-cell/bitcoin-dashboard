#!/usr/bin/env python3
# ==================================================
# 🚀 BLOCK LAYER0 UPDATE WORKER PROCESS WRAPPER
# systemd Entrypoint → Live Tip Sync
# ==================================================

import os
import sys

# ============================================
# 🔧 Projekt-Root setzen
# ============================================

PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../../")
)

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# ============================================
# 🔗 Worker Import
# ============================================

from workers.seo.blocks.blocks_update_worker import process_loop

# ============================================
# ▶️ Entrypoint
# ============================================

if __name__ == "__main__":

    print("[BLOCK UPDATE PROCESS] started")

    try:
        process_loop()

    except KeyboardInterrupt:
        print("[BLOCK UPDATE PROCESS] stopped by Ctrl+C")
