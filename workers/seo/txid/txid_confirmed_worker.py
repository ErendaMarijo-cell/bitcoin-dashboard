#!/usr/bin/env python3
# ==================================================
# ✅ TXID CONFIRMED WRITER (Realtime)
# - Polls Bitcoin Core RPC for new blocks
# - Writes confirmed TXIDs into JSONL segments (10k per file)
# - Crash-safe via state (last_processed_height + next_index)
# ==================================================

import os
import json
import time
import glob
from dataclasses import dataclass
from typing import Optional, Tuple
import requests

# ----------------------------
# Paths (NEW STRUCTURE)
# ----------------------------
OUT_DIR = "/raid/lightning/seo/txids/confirmed"
STATE_PATH = "/raid/lightning/seo/txids/progress/txid_confirmed_writer_state.json"

# ----------------------------
# Segmenting
# ----------------------------
SEGMENT_SIZE = 10_000  # matches txids_000930000_000939999.jsonl style

# ----------------------------
# Confirmations / polling
# ----------------------------
CONFIRMATIONS = int(os.getenv("TXID_WRITER_CONFIRMATIONS", "2"))
POLL_SEC = float(os.getenv("TXID_WRITER_POLL_SEC", "10"))

# ----------------------------
# Bitcoin Core RPC (set env vars or use defaults)
# ----------------------------
RPC_URL = os.getenv("BTC_RPC_URL", "http://127.0.0.1:8332")
RPC_USER = os.getenv("BTC_RPC_USER", "")
RPC_PASS = os.getenv("BTC_RPC_PASS", "")

# If your node uses cookie auth, set BTC_RPC_COOKIE=/path/to/.cookie and leave user/pass empty
RPC_COOKIE = os.getenv("BTC_RPC_COOKIE", "")

SESSION = requests.Session()


def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


def atomic_write_json(path: str, payload: dict) -> None:
    ensure_dir(os.path.dirname(path))
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, separators=(",", ":"), ensure_ascii=False)
    os.replace(tmp, path)


def load_state() -> dict:
    default = {
        "last_processed_height": None,
        "next_index": None,
        "updated_utc": None,
    }
    if not os.path.exists(STATE_PATH):
        atomic_write_json(STATE_PATH, default)
        return default
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f) or {}
        merged = dict(default)
        merged.update(data)
        return merged
    except Exception:
        atomic_write_json(STATE_PATH, default)
        return default


def save_state(state: dict) -> None:
    state["updated_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    atomic_write_json(STATE_PATH, state)


def rpc_auth():
    if RPC_COOKIE and os.path.exists(RPC_COOKIE):
        with open(RPC_COOKIE, "r", encoding="utf-8") as f:
            cookie = f.read().strip()
        user, pw = cookie.split(":", 1)
        return (user, pw)
    return (RPC_USER, RPC_PASS)


def rpc_call(method: str, params=None):
    if params is None:
        params = []
    payload = {"jsonrpc": "1.0", "id": "txid-writer", "method": method, "params": params}
    auth = rpc_auth()
    r = SESSION.post(RPC_URL, json=payload, auth=auth, timeout=30)
    r.raise_for_status()
    j = r.json()
    if j.get("error"):
        raise RuntimeError(f"RPC error: {j['error']}")
    return j["result"]


def parse_segment_range(filename: str) -> Optional[Tuple[int, int]]:
    # txids_000930000_000939999.jsonl
    base = os.path.basename(filename)
    if not (base.startswith("txids_") and base.endswith(".jsonl")):
        return None
    try:
        mid = base[len("txids_"):-len(".jsonl")]
        a, b = mid.split("_")
        return (int(a), int(b))
    except Exception:
        return None


def discover_next_index() -> int:
    ensure_dir(OUT_DIR)
    files = sorted(glob.glob(os.path.join(OUT_DIR, "txids_*.jsonl")))
    max_end = -1
    for f in files:
        r = parse_segment_range(f)
        if not r:
            continue
        _, end = r
        if end > max_end:
            max_end = end
    return max_end + 1 if max_end >= 0 else 0


def segment_path_for_index(i: int) -> Tuple[str, int, int]:
    start = (i // SEGMENT_SIZE) * SEGMENT_SIZE
    end = start + SEGMENT_SIZE - 1
    name = f"txids_{start:09d}_{end:09d}.jsonl"
    return os.path.join(OUT_DIR, name), start, end


def append_txids(txids):
    state = load_state()
    if state.get("next_index") is None:
        state["next_index"] = discover_next_index()

    i = int(state["next_index"])
    path, start, end = segment_path_for_index(i)
    ensure_dir(OUT_DIR)

    # append lines
    with open(path, "ab") as f:
        for txid in txids:
            line = json.dumps({"txid": txid}, separators=(",", ":")).encode("utf-8") + b"\n"
            f.write(line)
            i += 1
        f.flush()
        os.fsync(f.fileno())

    state["next_index"] = i
    save_state(state)
    return path, start, end, len(txids)


def main():
    print("[TXID WRITER] started")
    print(f"[TXID WRITER] OUT_DIR={OUT_DIR}")
    print(f"[TXID WRITER] STATE={STATE_PATH}")
    print(f"[TXID WRITER] RPC={RPC_URL} confs={CONFIRMATIONS} poll={POLL_SEC}s")

    ensure_dir(OUT_DIR)
    state = load_state()

    # Initialize last_processed_height to "tip - confirmations" if not set
    best = int(rpc_call("getblockcount"))
    safe_tip = max(0, best - CONFIRMATIONS)
    if state.get("last_processed_height") is None:
        # Start at safe_tip so we only do realtime from now on (no historic backfill)
        state["last_processed_height"] = safe_tip
        if state.get("next_index") is None:
            state["next_index"] = discover_next_index()
        save_state(state)
        print(f"[TXID WRITER] init last_processed_height={safe_tip} next_index={state['next_index']}")

    while True:
        try:
            best = int(rpc_call("getblockcount"))
            target = max(0, best - CONFIRMATIONS)

            last = int(state["last_processed_height"])
            if target <= last:
                time.sleep(POLL_SEC)
                continue

            # process blocks (last+1 .. target)
            for h in range(last + 1, target + 1):
                bh = rpc_call("getblockhash", [h])
                blk = rpc_call("getblock", [bh, 1])  # verbosity=1 => tx = [txid...]
                txids = blk.get("tx", []) or []
                out_path, seg_start, seg_end, n = append_txids(txids)

                state = load_state()
                state["last_processed_height"] = h
                save_state(state)

                print(f"[TXID WRITER] height={h} +{n} txids -> {os.path.basename(out_path)} ({seg_start}-{seg_end})")

            time.sleep(0.2)

        except KeyboardInterrupt:
            print("[TXID WRITER] stopped by Ctrl+C")
            return
        except Exception as e:
            print(f"[TXID WRITER ERROR] {e}")
            time.sleep(3)


if __name__ == "__main__":
    main()
