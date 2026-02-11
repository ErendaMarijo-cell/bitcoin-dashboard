#!/usr/bin/env python3
import os
import sys
import time

# ===============================
# 🔧 Projekt-Root setzen
# ===============================
PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../")
)
sys.path.insert(0, PROJECT_ROOT)

# ===============================
# 🔗 TXID Extractor importieren
# ===============================
from workers.seo.txid_extractor_worker import (
    txid_extractor_worker_loop
)

# ===============================
# ▶️ ENTRYPOINT
# ===============================
if __name__ == "__main__":
    print("[TXID SEO WORKER PROCESS] started")

    try:
        txid_extractor_worker_loop()
    except KeyboardInterrupt:
        print("[TXID SEO WORKER PROCESS] stopped by Ctrl+C")
