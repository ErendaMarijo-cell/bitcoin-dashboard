# ==================================================
# 🔥 TXID SEEDER WORKER
# Streams backfill JSONL segments and seeds Redis SEO set
# Resume-safe via (file + byte offset)
# ==================================================

import os
import sys
import time
import json
from datetime import datetime, timezone

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

import redis
from core.redis_keys import (
    SEO_TXID_INDEXED_SET,
    SEO_TXID_SITEMAP_DIRTY_KEY,

)


# ----------------------------
# Paths
# ----------------------------
IN_DIR = "/raid/data/bitcoin_dashboard/backfill/txids_jsonl"
STATE_PATH = "/raid/data/bitcoin_dashboard/backfill/progress/txid_seeder_state.json"

# ----------------------------
# Redis
# ----------------------------
r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)


# ----------------------------
# Tuning
# ----------------------------
BATCH_SIZE = 1000
SLEEP_SEC = 0.05

# ----------------------------
# Helpers
# ----------------------------
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def atomic_write_json(path: str, payload: dict):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, separators=(",", ":"))
    os.replace(tmp, path)


def load_state() -> dict:
    default = {
        "current_file": None,
        "offset": 0,
        "seeded_total": 0,
        "updated_utc": None,
    }
    if not os.path.exists(STATE_PATH):
        atomic_write_json(STATE_PATH, default)
        return default

    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        # forward compatible merge
        merged = dict(default)
        merged.update(data or {})
        return merged
    except Exception:
        atomic_write_json(STATE_PATH, default)
        return default


def save_state(state: dict):
    state["updated_utc"] = utc_now_iso()
    atomic_write_json(STATE_PATH, state)


def list_segment_files() -> list[str]:
    if not os.path.isdir(IN_DIR):
        return []
    files = [
        f for f in os.listdir(IN_DIR)
        if f.startswith("txids_") and f.endswith(".jsonl")
    ]
    files.sort()  # relies on your 9-digit padding
    return [os.path.join(IN_DIR, f) for f in files]


def pick_start_file(files: list[str], current_file: str | None) -> tuple[str | None, int]:
    if not files:
        return None, 0

    if not current_file:
        return files[0], 0

    # if current_file exists in list -> resume there
    if current_file in files:
        return current_file, 0

    # if file moved/rotated -> resume at next greater file name
    for f in files:
        if os.path.basename(f) > os.path.basename(current_file):
            return f, 0

    return None, 0


def seed_batch(txids: list[str]) -> int:
    if not txids:
        return 0

    # Redis SADD returns number of new elements added
    added = r.sadd(SEO_TXID_INDEXED_SET, *txids)

    # Trigger sitemap rebuild asynchronously
    r.set(SEO_TXID_SITEMAP_DIRTY_KEY, "1")


    return int(added)


# ----------------------------
# Main loop
# ----------------------------
def txid_seeder_loop():
    print("[TXID SEEDER] started")
    print(f"[TXID SEEDER] IN_DIR={IN_DIR}")
    print(f"[TXID SEEDER] STATE={STATE_PATH}")
    print(f"[TXID SEEDER] Redis set={SEO_TXID_INDEXED_SET}")

    state = load_state()

    files = list_segment_files()
    current_file, _ = pick_start_file(files, state.get("current_file"))

    if not current_file:
        print("[TXID SEEDER] No segment files found")
        return

    if state.get("current_file") != current_file:
        state["current_file"] = current_file
        state["offset"] = 0
        save_state(state)

    batch = []
    last_log = time.time()

    while True:
        files = list_segment_files()
        if not files:
            print("[TXID SEEDER] No segment files found, sleeping...")
            time.sleep(2)
            continue

        # ensure current_file is valid (can happen if directory changes)
        if state["current_file"] not in files:
            next_file, _ = pick_start_file(files, state["current_file"])
            if not next_file:
                print("[TXID SEEDER] No next file available, sleeping...")
                time.sleep(2)
                continue
            state["current_file"] = next_file
            state["offset"] = 0
            save_state(state)

        path = state["current_file"]

        try:
            with open(path, "rb") as f:
                # Resume at byte offset
                f.seek(int(state.get("offset", 0)))

                while True:
                    line = f.readline()
                    if not line:
                        # EOF -> try next file
                        break

                    # Update offset immediately (crash-safe)
                    state["offset"] = f.tell()

                    try:
                        rec = json.loads(line.decode("utf-8"))
                    except Exception:
                        continue

                    txid = rec.get("txid")
                    if not txid:
                        continue

                    batch.append(txid)

                    if len(batch) >= BATCH_SIZE:
                        added = seed_batch(batch)
                        state["seeded_total"] = int(state.get("seeded_total", 0)) + added
                        save_state(state)

                        print(
                            f"[TXID SEEDER] +{added} new (batch={len(batch)}) "
                            f"total_seeded={state['seeded_total']} "
                            f"file={os.path.basename(path)} off={state['offset']}"
                        )

                        batch.clear()
                        time.sleep(SLEEP_SEC)

                    # periodic heartbeat
                    if time.time() - last_log > 10:
                        print(
                            f"[TXID SEEDER] heartbeat "
                            f"file={os.path.basename(path)} off={state['offset']} "
                            f"queued_batch={len(batch)}"
                        )
                        last_log = time.time()

            # EOF reached -> flush remaining batch
            if batch:
                added = seed_batch(batch)
                state["seeded_total"] = int(state.get("seeded_total", 0)) + added
                save_state(state)

                print(
                    f"[TXID SEEDER] +{added} new (final batch={len(batch)}) "
                    f"total_seeded={state['seeded_total']} "
                    f"file={os.path.basename(path)}"
                )
                batch.clear()

            # move to next file (if exists)
            idx = files.index(path)
            if idx + 1 < len(files):
                state["current_file"] = files[idx + 1]
                state["offset"] = 0
                save_state(state)
                print(f"[TXID SEEDER] next file → {os.path.basename(state['current_file'])}")
                continue

            # no next file yet: wait for new segments
            print("[TXID SEEDER] reached end of available files, waiting...")
            time.sleep(2)

        except KeyboardInterrupt:
            print("[TXID SEEDER] stopped by Ctrl+C")
            return
        except Exception as e:
            print(f"[TXID SEEDER ERROR] {e}")
            time.sleep(2)


if __name__ == "__main__":
    txid_seeder_loop()
