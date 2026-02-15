#!/usr/bin/env python3
# ==================================================
# 🔥 ADDRESSES UPDATE WORKER (PROCESS WRAPPER)
# Systemd Entrypoint for Address Layer1B Update Worker
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

from workers.seo.addresses.addresses_update_worker import (
    update_loop
)

# ============================================
# ▶️ Entrypoint
# ============================================

if __name__ == "__main__":

    print("[ADDRESSES UPDATE WORKER PROCESS] started")

    try:
        update_loop()

    except KeyboardInterrupt:
        print("[ADDRESSES UPDATE WORKER PROCESS] stopped by Ctrl+C")
