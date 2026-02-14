#!/usr/bin/env python3
# =============================
# 🔗 ADDRESS UPDATE PROCESS
# =============================

import os
import sys

PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../../")
)

sys.path.insert(0, PROJECT_ROOT)

from workers.seo.addresses.addresses_update_worker import (
    update_loop
)

if __name__ == "__main__":
    print("[ADDRESS UPDATE PROCESS] started")

    try:
        update_loop()
    except KeyboardInterrupt:
        print("[ADDRESS UPDATE PROCESS] stopped by Ctrl+C")
