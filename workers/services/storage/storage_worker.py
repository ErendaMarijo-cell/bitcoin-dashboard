# =========================================================================
# 🗄️ STORAGE WORKER (Services) - Verantwortlich für persistente Sicherungen
# =========================================================================

import os
import re
import time
import json
import shutil
import redis
from datetime import datetime, timezone

# =============
# Konfiguration
# =============
INTERVAL_S = 60 * 20  # 20 Minuten (zentraler Takt)

BASE_DST_DIR = "/raid/data/bitcoin_dashboard"
LOCK_PATH = os.path.join(BASE_DST_DIR, ".storage_worker.lock")


# =======
# Locking
# =======
def acquire_lock_or_exit() -> None:
    os.makedirs(os.path.dirname(LOCK_PATH), exist_ok=True)
    try:
        fd = os.open(LOCK_PATH, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(fd, str(os.getpid()).encode())
        os.close(fd)
    except FileExistsError:
        print("[STORAGE] Worker läuft bereits – beende sauber")
        raise SystemExit(0)

def release_lock() -> None:
    try:
        os.remove(LOCK_PATH)
    except FileNotFoundError:
        pass


# =======
# Helpers
# =======
def utc_today_yyyymmdd() -> str:
    return time.strftime("%Y%m%d", time.gmtime())

def utc_segment_stamp() -> str:
    # Dateiname-sicher, eindeutig genug
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def safe_getsize(path: str) -> int:
    try:
        return os.path.getsize(path)
    except FileNotFoundError:
        return -1
    
def _safe_int_redis(r, key: str, default: int = 0) -> int:
    raw = r.get(key)
    if not raw:
        return default
    try:
        return int(raw.decode() if isinstance(raw, bytes) else raw)
    except Exception:
        return default

def atomic_copy(src: str, dst: str) -> None:
    """
    Copy nach tmp + os.replace() => atomar.
    copy2 übernimmt mtime/metadata (hilfreich für spätere Diagnosen).
    """
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    tmp = dst + ".tmp"
    shutil.copy2(src, tmp)
    os.replace(tmp, dst)

def atomic_touch(path: str) -> None:
    """
    Marker atomar erstellen.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        f.write(f"created_utc={datetime.now(timezone.utc).isoformat()}\n")
    os.replace(tmp, path)



# ================================================================================================================================= #
# ================================================================================================================================= #
# ================================================================================================================================= #



# ====================
# STORAGE_TXID-HISTORY
# ====================
def persist_txid_history():
    """
    Persistiert txid_history von RAM → NVMe
    (Prefix-Check, Marker-Latch, Segment-Mode)
    """

    SRC_DIR = "/raid/data/ramdisk_bitcoin_dashboard/txid_history"
    DST_DIR = os.path.join(BASE_DST_DIR, "txid_history")

    DEGRADED_MARKER_PREFIX = ".degraded_"
    FNAME_RE = re.compile(r"^all_mempool_seen_(\d{8})\.jsonl$")

    MIN_RAM_BYTES = 1024  # 1 KB Schutzgrenze

    try:
        files = sorted(f for f in os.listdir(SRC_DIR) if f.endswith(".jsonl"))
    except FileNotFoundError:
        return

    if not files:
        return

    today = utc_today_yyyymmdd()

    def degraded_marker(day: str) -> str:
        return os.path.join(DST_DIR, f"{DEGRADED_MARKER_PREFIX}{day}")

    for fname in files:
        m = FNAME_RE.match(fname)
        if not m:
            continue

        day = m.group(1)
        src = os.path.join(SRC_DIR, fname)
        dst_main = os.path.join(DST_DIR, fname)

        if day > today:
            continue

        # Alte Tage: copy-once
        if day < today:
            if not os.path.exists(dst_main) and os.path.exists(src):
                atomic_copy(src, dst_main)
                print(f"[STORAGE][TXID] finalized {fname}")
            continue

        # Heute
        if not os.path.exists(src):
            continue

        if os.path.exists(degraded_marker(day)):
            stamp = utc_segment_stamp()
            seg = os.path.join(
                DST_DIR,
                f"all_mempool_seen_{day}.segment_{stamp}.jsonl"
            )
            atomic_copy(src, seg)
            print(f"[STORAGE][TXID] segment written {os.path.basename(seg)}")
            continue

        ram_size = safe_getsize(src)
        nvme_size = safe_getsize(dst_main)

        if nvme_size < 0:
            atomic_copy(src, dst_main)
            print(f"[STORAGE][TXID] initial write {fname}")
            continue

        if ram_size < MIN_RAM_BYTES:
            print(
                f"[STORAGE][TXID] skip overwrite "
                f"(ram too small: {ram_size} bytes): {fname}"
            )
            continue

        if ram_size >= nvme_size:
            atomic_copy(src, dst_main)
            print(f"[STORAGE][TXID] overwrite ok {fname}")
            continue

        # Degraded
        atomic_touch(degraded_marker(day))
        stamp = utc_segment_stamp()
        seg = os.path.join(
            DST_DIR,
            f"all_mempool_seen_{day}.segment_{stamp}.jsonl"
        )
        atomic_copy(src, seg)
        print(f"[STORAGE][TXID] degraded → segment {os.path.basename(seg)}")


# ================================================================================================================================= #


# ===========================
# STORAGE_BTC_VOLUME_SNAPSHOT
# ===========================

def persist_btc_volume():
    """
    Persistiert BTC Volume Snapshot (overwrite, daily JSON)
    Quelle: Redis BTC_VOL_DYNAMIC_CACHE
    """

    from core.redis_keys import BTC_VOL_DYNAMIC_CACHE

    r = redis.Redis(
        host="localhost",
        port=6379,
        db=0,
        decode_responses=False
    )

    DST_DIR = os.path.join(
        BASE_DST_DIR,
        "metrics_history",
        "btc_volume_history"
    )
    os.makedirs(DST_DIR, exist_ok=True)

    raw = r.get(BTC_VOL_DYNAMIC_CACHE)
    if not raw:
        return

    try:
        payload = json.loads(raw.decode() if isinstance(raw, bytes) else raw)
    except Exception:
        return

    # 🔒 Safety Gate
    if payload.get("mempool_tx_count", 0) <= 0:
        print("[STORAGE][BTC_VOL] skip (empty or invalid)")
        return

    ts = payload.get("ts")
    if not ts:
        return

    day = time.strftime("%Y-%m-%d", time.gmtime(ts))
    path = os.path.join(DST_DIR, f"btc_volume_{day}.json")
    tmp  = path + ".tmp"

    payload_out = {
        "generated_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        **payload
    }

    with open(tmp, "w") as f:
        json.dump(payload_out, f, separators=(",", ":"))

    os.replace(tmp, path)

    print(f"[STORAGE][BTC_VOL] snapshot → {os.path.basename(path)}")


# ================================================================================================================================= #


# ==============================
# STORAGE_BTC_TX_VOLUME_SNAPSHOT
# ==============================

def persist_btc_tx_volume():
    """
    Persistiert BTC TX Volume Chart-State (overwrite, daily JSON)
    Quelle: Redis METRICS_BTC_TX_VOLUME_*
    """

    from core.redis_keys import (
        BTC_TX_VOLUME_1H,
        BTC_TX_VOLUME_24H,
        BTC_TX_VOLUME_1W,
        BTC_TX_VOLUME_1M,
        BTC_TX_VOLUME_1Y,
        BTC_TX_VOLUME_OPEN_BUCKETS,
    )

    r = redis.Redis(
        host="localhost",
        port=6379,
        db=0,
        decode_responses=False
    )

    keys = {
        "1h":  BTC_TX_VOLUME_1H,
        "24h": BTC_TX_VOLUME_24H,
        "1w":  BTC_TX_VOLUME_1W,
        "1m":  BTC_TX_VOLUME_1M,
        "1y":  BTC_TX_VOLUME_1Y,
    }

    buckets = {}

    for name, key in keys.items():
        raw = r.get(key)
        if not raw:
            continue

        try:
            data = json.loads(raw.decode() if isinstance(raw, bytes) else raw)
        except Exception:
            continue

        if "history" in data and data["history"]:
            buckets[name] = { "history": data["history"] }

    if not buckets:
        print("[STORAGE][BTC_TX_VOLUME] skip (no data)")
        return


    open_buckets = {}

    raw_open = r.get(BTC_TX_VOLUME_OPEN_BUCKETS)
    if raw_open:
        try:
            open_buckets = json.loads(
                raw_open.decode() if isinstance(raw_open, bytes) else raw_open
            )
        except Exception:
            open_buckets = {}


    # 🔑 letzten Timestamp bestimmen
    last_ts_ms = max(
        [
            p["x"]
            for b in buckets.values()
            for p in b["history"]
            if "x" in p
        ]
        + [
            v["cur_bucket"]
            for v in open_buckets.values()
            if isinstance(v, dict) and "cur_bucket" in v
        ]
    )

    DST_DIR = os.path.join(
        BASE_DST_DIR,
        "metrics_history",
        "btc_tx_volume_history"
    )
    os.makedirs(DST_DIR, exist_ok=True)

    day = time.strftime("%Y-%m-%d", time.gmtime())
    path = os.path.join(DST_DIR, f"btc_tx_volume_{day}.json")
    tmp  = path + ".tmp"

    payload = {
        "generated_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "last_ts_ms": last_ts_ms,
        "buckets": buckets,
    }

    if open_buckets:
        payload["open_buckets"] = open_buckets

    with open(tmp, "w") as f:
        json.dump(payload, f, separators=(",", ":"))

    os.replace(tmp, path)

    print(f"[STORAGE][BTC_TX_VOLUME] snapshot → {os.path.basename(path)}")


# ================================================================================================================================= #


# =====================
# STORAGE_BTC_TX_AMOUNT
# =====================

def persist_btc_tx_amount():
    """
    Persistiert BTC TX Amount State (overwrite, daily JSON)
    Quelle: Redis BTC_TX_AMOUNT_HISTORY_KEY
    """

    from core.redis_keys import BTC_TX_AMOUNT_HISTORY_KEY

    r = redis.Redis(
        host="localhost",
        port=6379,
        db=0,
        decode_responses=False
    )

    DST_DIR = os.path.join(
        BASE_DST_DIR,
        "metrics_history",
        "btc_tx_amount_history"
    )
    os.makedirs(DST_DIR, exist_ok=True)

    raw = r.get(BTC_TX_AMOUNT_HISTORY_KEY)
    if not raw:
        return

    try:
        payload = json.loads(raw.decode() if isinstance(raw, bytes) else raw)
    except Exception:
        return

    ts = payload.get("generated_ts_ms")
    if not ts:
        return

    # 🔒 Safety Gate
    if not payload.get("now"):
        print("[STORAGE][TX_AMOUNT] skip (empty payload)")
        return

    day = time.strftime("%Y-%m-%d", time.gmtime(ts / 1000))
    path = os.path.join(DST_DIR, f"btc_tx_amount_{day}.json")
    tmp  = path + ".tmp"

    with open(tmp, "w") as f:
        json.dump(payload, f, separators=(",", ":"))

    os.replace(tmp, path)

    print(f"[STORAGE][TX_AMOUNT] snapshot → {os.path.basename(path)}")


# ================================================================================================================================= #


# ============================
# STORAGE_BTC_TX_FEES_SNAPSHOT
# ============================

def persist_btc_tx_fees():
    """
    Persistiert BTC TX Fees Chart-State (overwrite, daily JSON)
    Quelle: Redis METRICS_BTC_TX_FEES_*
    """

    from core.redis_keys import (
        BTC_TX_FEES_24H,
        BTC_TX_FEES_1W,
        BTC_TX_FEES_1M,
        BTC_TX_FEES_1Y,
        BTC_TX_FEES_OPEN_BUCKETS,
    )

    r = redis.Redis(
        host="localhost",
        port=6379,
        db=0,
        decode_responses=False
    )

    keys = {
        "24h": BTC_TX_FEES_24H,
        "1w":  BTC_TX_FEES_1W,
        "1m":  BTC_TX_FEES_1M,
        "1y":  BTC_TX_FEES_1Y,
    }

    buckets = {}

    # ---------------------------
    # 📊 History Buckets
    # ---------------------------
    for name, key in keys.items():
        raw = r.get(key)
        if not raw:
            continue

        try:
            data = json.loads(raw.decode() if isinstance(raw, bytes) else raw)
        except Exception:
            continue

        if "history" in data and data["history"]:
            buckets[name] = {"history": data["history"]}

    if not buckets:
        print("[STORAGE][BTC_TX_FEES] skip (no data)")
        return

    # ---------------------------
    # 🔁 Open Buckets
    # ---------------------------
    open_buckets = {}

    raw_open = r.get(BTC_TX_FEES_OPEN_BUCKETS)
    if raw_open:
        try:
            open_buckets = json.loads(
                raw_open.decode() if isinstance(raw_open, bytes) else raw_open
            )
        except Exception:
            open_buckets = {}

    # ---------------------------
    # 🔑 last_ts_ms bestimmen
    # ---------------------------
    last_ts_ms = max(
        p["x"]
        for b in buckets.values()
        for p in b["history"]
        if "x" in p
    )

    # ---------------------------
    # 💾 Write Snapshot
    # ---------------------------
    DST_DIR = os.path.join(
        BASE_DST_DIR,
        "metrics_history",
        "btc_tx_fees_history"
    )
    os.makedirs(DST_DIR, exist_ok=True)

    day = time.strftime("%Y-%m-%d", time.gmtime())
    path = os.path.join(DST_DIR, f"btc_tx_fees_{day}.json")
    tmp  = path + ".tmp"

    payload = {
        "generated_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "last_ts_ms": last_ts_ms,
        "buckets": buckets,
    }

    if open_buckets:
        payload["open_buckets"] = open_buckets

    with open(tmp, "w") as f:
        json.dump(payload, f, separators=(",", ":"))

    os.replace(tmp, path)

    print(f"[STORAGE][BTC_TX_FEES] snapshot → {os.path.basename(path)}")


# ================================================================================================================================= #


# ==============================
# STORAGE_DASHBOARD_TRAFFIC
# ==============================

def persist_dashboard_traffic():
    """
    Persistiert Dashboard Traffic State (overwrite, daily JSON)
    Quelle:
      - Redis INFO_DASHBOARD_TRAFFIC_*
      - Redis DASHBOARD_TRAFFIC_*
      - Redis DASHBOARD_TRAFFIC_OPEN_BUCKETS
    """

    from core.redis_keys import (
        INFO_DASHBOARD_TRAFFIC_1H,
        INFO_DASHBOARD_TRAFFIC_24H,
        INFO_DASHBOARD_TRAFFIC_1W,
        INFO_DASHBOARD_TRAFFIC_1M,
        INFO_DASHBOARD_TRAFFIC_1Y,
        DASHBOARD_TRAFFIC_TOTAL,
        DASHBOARD_TRAFFIC_TODAY,
        DASHBOARD_TRAFFIC_DAY,
        DASHBOARD_TRAFFIC_LAST_TS,
        DASHBOARD_TRAFFIC_LAUNCH_TS,
        DASHBOARD_TRAFFIC_OPEN_BUCKETS,
    )

    r = redis.Redis(
        host="localhost",
        port=6379,
        db=0,
        decode_responses=False
    )

    # ------------------------------
    # Buckets lesen
    # ------------------------------
    keys = {
        "1h":  INFO_DASHBOARD_TRAFFIC_1H,
        "24h": INFO_DASHBOARD_TRAFFIC_24H,
        "1w":  INFO_DASHBOARD_TRAFFIC_1W,
        "1m":  INFO_DASHBOARD_TRAFFIC_1M,
        "1y":  INFO_DASHBOARD_TRAFFIC_1Y,
    }

    buckets = {}

    for name, key in keys.items():
        raw = r.get(key)
        if not raw:
            continue

        try:
            data = json.loads(raw.decode() if isinstance(raw, bytes) else raw)
        except Exception:
            continue

        if data.get("history"):
            buckets[name] = {"history": data["history"]}

    if not buckets:
        print("[STORAGE][DASHBOARD_TRAFFIC] skip (no data)")
        return

    # ------------------------------
    # Open Buckets lesen (🔥 NEU)
    # ------------------------------
    open_buckets = {}

    raw_open = r.get(DASHBOARD_TRAFFIC_OPEN_BUCKETS)
    if raw_open:
        try:
            open_buckets = json.loads(
                raw_open.decode() if isinstance(raw_open, bytes) else raw_open
            )
        except Exception:
            open_buckets = {}

    # ------------------------------
    # Meta-Daten
    # ------------------------------
    last_ts_ms = _safe_int_redis(r, DASHBOARD_TRAFFIC_LAST_TS)
    total      = _safe_int_redis(r, DASHBOARD_TRAFFIC_TOTAL)
    today      = _safe_int_redis(r, DASHBOARD_TRAFFIC_TODAY)
    launch_ts  = _safe_int_redis(r, DASHBOARD_TRAFFIC_LAUNCH_TS)

    day_raw = r.get(DASHBOARD_TRAFFIC_DAY)
    day_utc = (
        day_raw.decode() if isinstance(day_raw, bytes) else day_raw
    ) if day_raw else None

    # ------------------------------
    # Zielpfad
    # ------------------------------
    DST_DIR = os.path.join(
        BASE_DST_DIR,
        "info",
        "dashboard_traffic_history"
    )
    os.makedirs(DST_DIR, exist_ok=True)

    day = time.strftime("%Y-%m-%d", time.gmtime())
    path = os.path.join(DST_DIR, f"dashboard_traffic_{day}.json")
    tmp  = path + ".tmp"

    payload = {
        "generated_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "last_ts_ms": last_ts_ms,
        "launch_ts": launch_ts,
        "total_requests": total,
        "today_requests": today,
        "day_utc": day_utc,
        "buckets": buckets,
    }

    # 🔥 open buckets nur wenn vorhanden
    if open_buckets:
        payload["open_buckets"] = open_buckets

    with open(tmp, "w") as f:
        json.dump(payload, f, separators=(",", ":"))

    os.replace(tmp, path)

    print(f"[STORAGE][DASHBOARD_TRAFFIC] snapshot → {os.path.basename(path)}")



# ================================================================================================================================= #
# ================================================================================================================================= #
# ================================================================================================================================= #



# ===========
# AGGREGIEREN
# ===========

def run_once():
    """
    Führt alle Storage-Jobs auf einmal aus
    """
    persist_txid_history()
    persist_btc_volume()
    persist_btc_tx_volume()
    persist_btc_tx_amount()
    persist_btc_tx_fees()
    persist_dashboard_traffic()
    


def main():
    acquire_lock_or_exit()
    print("[STORAGE] Worker gestartet")

    try:
        while True:
            t0 = time.time()

            try:
                run_once()
            except Exception as e:
                print(f"[STORAGE ERROR] {e}")

            elapsed = time.time() - t0
            sleep_time = max(0.0, INTERVAL_S - elapsed)
            time.sleep(sleep_time)

    finally:
        release_lock()


def start():
    import threading
    t = threading.Thread(
        target=main,
        name="storage-worker",
        daemon=True
    )
    t.start()
    return t


if __name__ == "__main__":
    main()
