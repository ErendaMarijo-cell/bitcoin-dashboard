#!/usr/bin/env python3
# ==================================================
# 🔥 TXIDS SITEMAP WORKER (PROCESS WRAPPER)
# Builds & Appends TXID URLs into SEO Sitemap Shards
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

from workers.seo.txids.txids_sitemap_worker import (
    txid_extractor_worker_loop
)

# ============================================
# ▶️ Entrypoint
# ============================================

if __name__ == "__main__":
    print("[TXIDS SITEMAP WORKER PROCESS] started")

    try:
        txid_extractor_worker_loop()

    except KeyboardInterrupt:
        print("[TXIDS SITEMAP WORKER PROCESS] stopped by Ctrl+C")
