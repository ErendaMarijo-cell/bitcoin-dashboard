#!/usr/bin/env python3
# ==================================================
# 🔥 TX AMOUNT HISTORICAL BUILDER
# Builds TX Leaderboards from Address JSONL
# One-shot / manual execution worker
# ==================================================

import os
import json
import glob
from collections import defaultdict
from datetime import datetime, timezone

# ==============================
# Config
# ==============================

ADDR_DIR = "/raid/data/seo/addresses/confirmed"
OUT_PATH = "/raid/data/bitcoin_dashboard/metrics_history/tx_amount_historical_snapshot.json"

TOP_LIMIT = 1000

# ==============================
# Helpers
# ==============================

def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()

# ==============================
# Scan Address Segments
# ==============================

def build_tx_amount_from_addresses():

    print("[TX_AMOUNT HIST] scanning address segments...")

    files = sorted(
        glob.glob(os.path.join(ADDR_DIR, "*.jsonl"))
    )

    if not files:
        print("[TX_AMOUNT HIST] no files found")
        return

    tx_sums = defaultdict(int)

    total_lines = 0
    positive_events = 0

    for path in files:

        print(f"[TX_AMOUNT HIST] scanning {os.path.basename(path)}")

        with open(path, "r") as f:

            for line in f:
                total_lines += 1

                try:
                    rec = json.loads(line)
                except:
                    continue

                delta = int(rec.get("delta_sat", 0))

                # Only outputs count for TX amount
                if delta <= 0:
                    continue

                txid = rec.get("txid")
                if not txid:
                    continue

                tx_sums[txid] += delta
                positive_events += 1

    print(
        f"[TX_AMOUNT HIST] lines={total_lines} "
        f"positive={positive_events} "
        f"unique_tx={len(tx_sums)}"
    )

    return tx_sums

# ==============================
# Build Leaderboards
# ==============================

def build_leaderboards(tx_sums):

    print("[TX_AMOUNT HIST] sorting leaderboards...")

    ranked = sorted(
        tx_sums.items(),
        key=lambda x: x[1],
        reverse=True
    )

    top_ever = ranked[:TOP_LIMIT]

    # Convert to output format
    leaderboard = []

    for txid, sat in top_ever:

        leaderboard.append({
            "txid": txid,
            "btc_value": sat / 1e8
        })

    return leaderboard

# ==============================
# Save Snapshot
# ==============================

def save_snapshot(leaderboard):

    payload = {
        "generated_utc": utc_now_iso(),
        "source": "address_backfill",
        "top_1000_ever": leaderboard
    }

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)

    with open(OUT_PATH, "w") as f:
        json.dump(payload, f, indent=2)

    print(f"[TX_AMOUNT HIST] snapshot saved → {OUT_PATH}")

# ==============================
# Main
# ==============================

def main():

    tx_sums = build_tx_amount_from_addresses()

    if not tx_sums:
        print("[TX_AMOUNT HIST] no tx data")
        return

    leaderboard = build_leaderboards(tx_sums)

    save_snapshot(leaderboard)

    print("[TX_AMOUNT HIST] done")

# ==============================

if __name__ == "__main__":
    main()
