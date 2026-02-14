#!/usr/bin/env python3

# ==================================================
# 🔥 TXIDS BACKFILL WORKER (PROCESS WRAPPER)
# Systemd Entrypoint for TXID Backfill Worker
# Runs full chain append-only JSONL backfill
# ==================================================

import os
import sys

# ============================================
# 🔧 Projekt-Root setzen
# ============================================

BASE_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../../")
)

if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

# ============================================
# 🔗 Worker Import
# ============================================

from workers.seo.txids.txids_backfill_worker import backfill_loop

# ============================================
# ▶️ Entrypoint
# ============================================

if __name__ == "__main__":

    print("[TXIDS BACKFILL WORKER PROCESS] started")

    try:
        backfill_loop()

    except KeyboardInterrupt:
        print("[TXIDS BACKFILL WORKER PROCESS] stopped by Ctrl+C")
