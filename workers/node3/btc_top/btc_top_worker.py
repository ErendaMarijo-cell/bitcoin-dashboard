# ============================
# 🔗 BTC_TOP_WORKER  -  NODE 3
# ============================

import os
import json
import time
import threading
import redis

from core.redis_keys import (
    BTC_TOP_SEEN_KEY,
    BTC_TOP_TXS_KEY,
    BTC_TOP_STATS_KEY,
    BTC_TOP_LOCK_KEY,
    BTC_TOP_SEEN_VALUE_KEY,
    BTC_TOP_UPDATE_INTERVAL,
    BTC_TOP_TOP_N,
    BTC_TOP_LOCK_TTL,
)


r = redis.Redis(
    host="localhost",
    port=6379,
    db=0,
    decode_responses=False
)

# ================================
# 🛡️ HARD GUARD: NODE3 ONLY
# ================================
from nodes.config import NODE_CONFIG
from nodes.rpc import BitcoinRPC

RPC = BitcoinRPC(NODE_CONFIG["node3"])
print(f"[BTC_TOP] Module imported, RPC={RPC.url}")


# ================================
# 🔧 Worker-Konfiguration (Paths)
# ================================

# 🔥 RAM-Disk: TXID-History (nur temporär, wird von Storage gesichert)
TXID_HISTORY_DIR = "/raid/data/ramdisk_bitcoin_dashboard/txid_history"

# 💾 Persistente History: BTC Top 50 Ever
BTC_TOP_50_EVER_PATH = (
    "/raid/data/bitcoin_dashboard/metrics_history/"
    "btc_top_history/btc_top_50_history.json"
)

# -------------------------------
# Fail-Fast Checks
# -------------------------------
if not os.path.isdir(TXID_HISTORY_DIR):
    raise RuntimeError(
        f"[FATAL] TXID_HISTORY_DIR fehlt oder RAM-Disk nicht gemountet: {TXID_HISTORY_DIR}"
    )

os.makedirs(os.path.dirname(BTC_TOP_50_EVER_PATH), exist_ok=True)


_LAST_PRUNE_TS = 0
# =========================
# 🧹 RAM-Disk Pruning
# =========================
def prune_ramdisk_history(max_age_days: int = 10):
    """
    Löscht JSONL-Dateien in der RAM-Disk (TXID_HISTORY_DIR),
    die älter sind als max_age_days.

    Ziel: RAM-Disk dauerhaft klein halten (nur letzte X Tage),
    während das NVMe-Backup die komplette History behält.
    """
    now = time.time()
    cutoff = now - max_age_days * 86400

    try:
        for fname in os.listdir(TXID_HISTORY_DIR):
            if not fname.endswith(".jsonl"):
                continue

            fpath = os.path.join(TXID_HISTORY_DIR, fname)
            try:
                mtime = os.path.getmtime(fpath)
            except FileNotFoundError:
                # Falls Datei zwischenzeitlich entfernt wurde → ignorieren
                continue

            if mtime < cutoff:
                try:
                    os.remove(fpath)
                    print(f"[PRUNE] Entfernt alte Datei aus RAM: {fname}")
                except Exception as e:
                    print(f"[PRUNE ERROR] Konnte {fname} nicht löschen: {e}")

    except Exception as e:
        print(f"[PRUNE ERROR] Allgemeiner Fehler: {e}")


# =========================
# 🛑 Top-BTC Worker – Logik
# =========================
def save_top50_ever_if_changed(top50_ever):
    existing_data = []
    if os.path.exists(BTC_TOP_50_EVER_PATH):
        try:
            with open(BTC_TOP_50_EVER_PATH, "r") as f:
                existing_data = json.load(f)
        except Exception:
            existing_data = []

    if existing_data != top50_ever:
        try:
            with open(BTC_TOP_50_EVER_PATH, "w") as f:
                json.dump(top50_ever, f, indent=4)
            print(f"[INFO] {BTC_TOP_50_EVER_PATH} aktualisiert ({len(top50_ever)} Einträge)")
        except Exception as e:
            print(f"[ERROR] Fehler beim Schreiben von {BTC_TOP_50_EVER_PATH}: {e}")


def update_btc_top():
    """Scannt den Mempool und aktualisiert die Top-Liste inkl. SEEN_VALUE"""
    worker_pid = os.getpid()
    
    # Lock setzen
    have_lock = r.set(BTC_TOP_LOCK_KEY, worker_pid, nx=True, ex=BTC_TOP_LOCK_TTL)
    if not have_lock:
        print("[BTC_TOP WORKER] Kein Lock (läuft schon woanders) → skip")
        return

    t_start = time.time()

    # 🔹 RAM-Disk Pruning:
    # Prüft 1× pro Stunde und entfernt Dateien, die älter als 10 Tage sind
    global _LAST_PRUNE_TS
    now = time.time()
    if now - _LAST_PRUNE_TS >= 3600:
        prune_ramdisk_history(max_age_days=10)
        _LAST_PRUNE_TS = now


    try:
        # Mempool abfragen
        mempool = RPC.call("getrawmempool", [True])
        if not mempool:
            return

        mempool_items = list(mempool.items())
        mempool_txids = set(txid for txid, _ in mempool_items)

        # Seen-Set aus Redis
        seen_raw = r.smembers(BTC_TOP_SEEN_KEY) or set()
        seen = set(x.decode() if isinstance(x, bytes) else x for x in seen_raw)

        # Entferne TXs, die nicht mehr im Mempool sind
        for tx in list(seen):
            if tx not in mempool_txids:
                r.srem(BTC_TOP_SEEN_KEY, tx)
                r.hdel(BTC_TOP_SEEN_VALUE_KEY, tx)
                seen.remove(tx)

        # Aktuelle Top-Liste aus Redis
        raw_top = r.get(BTC_TOP_TXS_KEY)
        current_top = []
        if raw_top:
            try:
                data = json.loads(raw_top)
                current_top = data.get("top10", [])
            except Exception:
                current_top = []

        current_top = sorted(current_top, key=lambda x: x["btc_value"], reverse=True)[:BTC_TOP_TOP_N]

        # Top50-Ever laden
        try:
            with open(BTC_TOP_50_EVER_PATH, "r") as f:
                top50_ever = json.load(f)
        except Exception:
            top50_ever = []

        top50_ever = sorted(top50_ever, key=lambda x: x["btc_value"], reverse=True)[:BTC_TOP_TOP_N]
        ever_seen = set(tx["txid"] for tx in top50_ever)

        # Kandidaten: TXs, die noch nicht gesehen oder in top-ever
        candidates = [(txid, info) for txid, info in mempool_items if txid not in seen and txid not in ever_seen]

        rpc_fetched = 0
        today_txid_history_path = os.path.join(
            TXID_HISTORY_DIR,
            f"all_mempool_seen_{time.strftime('%Y%m%d', time.gmtime())}.jsonl" # ZEIT ist in UTC!!!
        )

        for txid, info in candidates:
            tx_detail = RPC.call("getrawtransaction", [txid, True])
            if not tx_detail:
                continue

            btc_value = sum(vout.get("value", 0) for vout in tx_detail.get("vout", []))

            # Top-Liste aktualisieren
            if len(current_top) < BTC_TOP_TOP_N or btc_value > current_top[-1]["btc_value"]:
                current_top.append({"txid": txid, "btc_value": btc_value})
                current_top = sorted(current_top, key=lambda x: x["btc_value"], reverse=True)[:BTC_TOP_TOP_N]

            # Top50-Ever aktualisieren
            if len(top50_ever) < BTC_TOP_TOP_N or btc_value > top50_ever[-1]["btc_value"]:
                top50_ever.append({"txid": txid, "btc_value": btc_value})
                top50_ever = sorted(top50_ever, key=lambda x: x["btc_value"], reverse=True)[:BTC_TOP_TOP_N]
                ever_seen.add(txid)

            # TX als gesehen markieren
            r.sadd(BTC_TOP_SEEN_KEY, txid)

            # Neu: SEEN_VALUE mit Value + Timestamp
            seen_value_entry = {
                "btc_value": btc_value,
                "timestamp_ms": int(time.time() * 1000)
            }
            r.hset(BTC_TOP_SEEN_VALUE_KEY, txid, json.dumps(seen_value_entry))

            rpc_fetched += 1

            # JSONL Logging
            weight = tx_detail.get("weight", 0)

            fee_btc = info.get("fees", {}).get("base", 0) or 0
            fee_sat = int(fee_btc * 100_000_000)  # BTC → Satoshi

            entry = {
                "txid": txid,
                "timestamp_ms": int(time.time() * 1000),
                "btc_value": round(btc_value, 8),
                "weight": weight,
                "fee_sat": fee_sat,
                "mempool_size": len(mempool)
            }

            with open(today_txid_history_path, "a", buffering=1) as f:
                f.write(json.dumps(entry) + "\n")


        # Nur TXs behalten, die noch im Mempool sind
        current_top = [tx for tx in current_top if tx["txid"] in mempool_txids]

        # Ergebnisse in Redis speichern (Top10 + Top50 Ever zusammen)
        r.set(
            BTC_TOP_TXS_KEY,
            json.dumps({
                "top10": current_top,
                "top50_ever": top50_ever,
                "last_updated": time.time()
            }, separators=(",", ":"))
        )
        
        # Persistenz weiterhin beibehalten (optional, aber sinnvoll)
        save_top50_ever_if_changed(top50_ever)

        # Monitoring
        t_end = time.time()
        elapsed_ms = int((t_end - t_start) * 1000)
        stats = {
            "last_run_ts": str(time.time()),
            "mempool_examined": str(len(mempool_items)),
            "candidates_fetched": str(len(candidates)),
            "rpc_fetched": str(rpc_fetched),
            "scan_time_ms": str(elapsed_ms),
        }
        r.hset(BTC_TOP_STATS_KEY, mapping=stats)

    finally:
        # Lock freigeben
        cur = r.get(BTC_TOP_LOCK_KEY)
        if cur and (cur.decode() if isinstance(cur, bytes) else cur) == str(worker_pid):
            r.delete(BTC_TOP_LOCK_KEY)


# ==============
# 🔹 Worker Loop
# ==============
def btc_top_worker_loop():
    print(f"[BTC_TOP WORKER] Startet in {BTC_TOP_UPDATE_INTERVAL}s...")
    time.sleep(BTC_TOP_UPDATE_INTERVAL)

    while True:
        loop_start = time.time()

        try:
            update_btc_top()
        except Exception as e:
            print(f"[BTC_TOP WORKER ERROR] {e}")

        loop_elapsed = time.time() - loop_start
        sleep_time = max(0.0, BTC_TOP_UPDATE_INTERVAL - loop_elapsed)

        # -------------------------
        # 🔎 Stats aus Redis lesen
        # -------------------------
        raw = r.hgetall(BTC_TOP_STATS_KEY) or {}

        def _d(x):
            return x.decode() if isinstance(x, bytes) else x

        mempool = _d(raw.get(b"mempool_examined", b"?"))
        candidates = _d(raw.get(b"candidates_fetched", b"?"))
        rpc_fetched = _d(raw.get(b"rpc_fetched", b"?"))
        scan_ms = _d(raw.get(b"scan_time_ms", b"?"))

        print(
            f"[BTC_TOP WORKER] "
            f"rpc={RPC.info()} | "
            f"mempool={mempool} | "
            f"scan={scan_ms}ms | "
            f"sleep={sleep_time:.3f}s | "
            f"candidates={candidates} | "
            f"rpc_fetched={rpc_fetched} "    
        )

        time.sleep(sleep_time)




