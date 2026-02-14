#!/usr/bin/env python3
# =============================
# 🔗 ADDRESS BACKFILL PROCESS
# =============================

import os
import sys

# ============================================
# 🔧 PROJECT ROOT setzen
# ============================================

PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../../")
)

sys.path.insert(0, PROJECT_ROOT)

# ============================================
# 🔗 Worker Import
# ============================================

from workers.seo.addresses.addresses_backfill_worker import (
    backfill_loop
)

# ============================================
# ▶️ Entrypoint
# ============================================

if __name__ == "__main__":
    print("[ADDRESS BACKFILL PROCESS] started")

    try:
        backfill_loop()
    except KeyboardInterrupt:
        print("[ADDRESS BACKFILL PROCESS] stopped by Ctrl+C")
