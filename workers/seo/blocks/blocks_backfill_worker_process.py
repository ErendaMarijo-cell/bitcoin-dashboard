#!/usr/bin/env python3
# ==================================================
# 🚀 BLOCK LAYER0 BACKFILL WORKER PROCESS WRAPPER
# systemd Entrypoint → startet Backfill Loop
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

from workers.seo.blocks.blocks_backfill_worker import backfill_loop

# ============================================
# ▶️ Entrypoint
# ============================================

if __name__ == "__main__":

    print("[BLOCK BACKFILL PROCESS] started")

    try:
        backfill_loop()

    except KeyboardInterrupt:
        print("[BLOCK BACKFILL PROCESS] stopped by Ctrl+C")
