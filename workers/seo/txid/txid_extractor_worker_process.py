#!/usr/bin/env python3
import os
import sys
import time
import traceback

# ===============================
# 🔧 Projekt-Root setzen
# ===============================
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# ===============================
# 🔗 TXID Extractor importieren
# ===============================
# Append-Only Worker erwartet weiterhin
# diese Funktionssignatur für systemd.
# Falls intern refactored → Alias Layer nutzen.

try:
    from workers.seo.txid.txid_extractor_worker import (
        txid_extractor_worker_loop
    )
except ImportError as e:
    print("[TXID SEO WORKER PROCESS] ❌ ImportError")
    print(str(e))
    traceback.print_exc()
    sys.exit(1)

# ===============================
# ▶️ ENTRYPOINT
# ===============================
if __name__ == "__main__":

    print("[TXID SEO WORKER PROCESS] started (append-only mode expected)")

    # Optional kurze Boot-Delay
    time.sleep(1.5)

    try:
        txid_extractor_worker_loop()

    except KeyboardInterrupt:
        print("[TXID SEO WORKER PROCESS] stopped by Ctrl+C")

    except Exception as e:
        print("[TXID SEO WORKER PROCESS] ❌ Worker crashed")
        print(str(e))
        traceback.print_exc()
        sys.exit(1)
