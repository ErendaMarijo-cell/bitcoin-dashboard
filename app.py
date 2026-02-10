# ==========================================
# BITCOIN-DASHBOARD – Flask Backend / app.py
# ==========================================

# ================
# 🔗 Standard Libs
# ================
import os
import time
import json
import threading
import glob
import heapq
import subprocess
import uuid
import asyncio
from datetime import datetime, timezone
from pathlib import Path

# ==============
# 🌐 Third Party
# ==============
import redis
import requests
import psutil
from bitcoinrpc.authproxy import AuthServiceProxy
from dotenv import load_dotenv

# =====================
# 🧪 Flask & Extensions
# =====================
from flask import (
    Flask,
    render_template,
    jsonify,
    request,
    Response,
    make_response,
    g,
    send_from_directory,
    abort,              
)
from flask_cors import CORS

# ======================
# ⚙️ Concurrency / Utils
# ======================
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

# ====================
# 🧠 Project Internals
# ====================
from nodes.electrumx import ElectrumXClient
from electrumx.address import get_address_overview
from core.electrumx_service import get_electrumx_client



# ===================================================
# 🔑 Environment Configuration – External API Secrets
# ===================================================
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
ENV_DIR = os.path.join(BASE_DIR, "env")

load_dotenv(os.path.join(ENV_DIR, ".env.api"), override=False)



## ================================================================================================================================================================ ##
## ================================================================================================================================================================ ##
## ================================================================================================================================================================ ##



# ============================================
# 🔑 Project Internals – Redis Keys (ZENTRAL!)
# ============================================
from core.redis_keys import (

    # ---- BLOCKCHAIN
    BLOCKCHAIN_DYNAMIC_CACHE,
    BLOCKCHAIN_STATIC_KEY,

    # ---- MEMPOOL
    MEMPOOL_STATIC_KEY,
    MEMPOOL_DYNAMIC_CACHE,

    # ---- NETWORK
    NETWORK_DYNAMIC_CACHE,



    # ---- HOME_BTC_PRICE
    HOME_BTC_PRICE_CACHE,
    HOME_PRICE_LOCK,
    HOME_BTC_PRICE_CACHE_TTL,
    HOME_BTC_PRICE_LOCK_TTL,
    HOME_BTC_PRICE_MAX_WAIT,
    HOME_BTC_PRICE_WAIT_STEP,
    
    # ---- HOME_META
    HOME_META_CACHE,
    HOME_META_CACHE_TTL,
    HOME_META_LOCK ,
    HOME_META_LOCK_TTL,  
    HOME_META_DAY_SECONDS,

    # ---- HOME_BTC_TOP
    BTC_TOP_50_EVER_PATH,
    BTC_TOP_TXS_KEY,

    # ---- HOME_BTC_VOLUME
    BTC_VOL_DYNAMIC_CACHE,

    # ---- HOME_DASHBOARD_TRAFFIC
    DASHBOARD_TRAFFIC_RAW_PREFIX,
    DASHBOARD_TRAFFIC_TOTAL,



    # ---- NETWORK_NODES
    NETWORK_NODES_CACHE_KEY,
    NETWORK_NODES_LOCK_KEY,
    NETWORK_NODES_REFRESH_INTERVAL,
    NETWORK_NODES_LOCK_TTL,
    NETWORK_NODES_SHORT_CACHE_TTL,
    NETWORK_NODES_SUBTAB_CACHE_TTL,

    # ---- NETWORK_MINER
    NETWORK_MINER_CACHE_KEY,
    NETWORK_MINER_LOCK_KEY,
    NETWORK_MINER_REFRESH_INTERVAL,
    NETWORK_MINER_LOCK_TTL,
    NETWORK_MINER_STC_KEY,
    NETWORK_MINER_STC_TTL,



    # ---- METRICS_BTC_USD_EUR
    METRICS_BTC_USD_EUR_CACHE_KEY,
    METRICS_BTC_USD_EUR_LOCK_KEY,
    METRICS_BTC_USD_EUR_REFRESH_INTERVAL,
    METRICS_BTC_USD_EUR_LOCK_TTL,
    METRICS_BTC_USD_EUR_SHORT_CACHE_TTL,
    METRICS_BTC_USD_EUR_SUBTAB_TTL,

    # ---- METRICS_BTC_TX_AMOUNT
    BTC_TX_AMOUNT_HISTORY_KEY,

    # ---- METRICS_BTC_TX_VOLUME
    BTC_TX_VOLUME_1H,
    BTC_TX_VOLUME_24H,
    BTC_TX_VOLUME_1W,
    BTC_TX_VOLUME_1M,
    BTC_TX_VOLUME_1Y,
    BTC_TX_VOLUME_STATS,

    # ---- METRICS_BTC_TX_FEES
    BTC_TX_FEES_24H,
    BTC_TX_FEES_1W,
    BTC_TX_FEES_1M,
    BTC_TX_FEES_1Y,



    # ---- REVIEW_BTC_VALUE
    REVIEW_BASE_PATH,



    # ---- EXPLORER_ADRESSES
    EXPLORER_ADDRESSES_MAX_ADDRESSES_KEY,
    EXPLORER_ADDRESSES_MAX_ADDRESSES_DEFAULT,



    # ---- TREASURIES
    TREASURIES_BASE_PATH,

    # ---- TREASURIES_COMPANIES
    TREASURIES_COMPANIES_FILENAME,
    TREASURIES_COMPANIES_RESPONSE_CACHE_KEY,

    # ---- TREASURIES_INSTITUTIONS
    TREASURIES_INSTITUTIONS_FILENAME,        
    TREASURIES_INSTITUTIONS_RESPONSE_CACHE_KEY,

    # ---- TREASURIES_COUNTRIES
    TREASURIES_COUNTRIES_FILENAME,
    TREASURIES_COUNTRIES_RESPONSE_CACHE_KEY,



    # ---- MARKET_CAP
    MARKET_CAP_BASE_PATH,

    # ---- MARKET_CAP_COINS
    MARKET_CAP_COINS_CACHE_KEY,
    MARKET_CAP_COINS_LOCK_KEY,
    MARKET_CAP_COINS_REFRESH_INTERVAL,
    MARKET_CAP_COINS_LOCK_TTL,

    # ---- MARKET_CAP_COMPANIES
    MARKET_CAP_COMPANIES_CACHE_NOW,           
    MARKET_CAP_COMPANIES_CACHE_OLD,               
    MARKET_CAP_COMPANIES_LOCK_KEY,           
    MARKET_CAP_COMPANIES_REFRESH_INTERVAL,             
    MARKET_CAP_COMPANIES_LOCK_TTL,          
    MARKET_CAP_COMPANIES_REFRESH_COOLDOWN ,
    MARKET_CAP_COMPANIES_REFRESH_COOLDOWN_TTL,

    # ---- MARKET_CAP_CURRENCIES
    MARKET_CAP_CURRENCIES_FX_FILENAME,
    MARKET_CAP_CURRENCIES_FIAT_FILENAME,       
    MARKET_CAP_CURRENCIES_RESPONSE_CACHE_KEY,
    MARKET_CAP_CURRENCIES_RESPONSE_TTL,

    # ---- MARKET_CAP_COMMODITIES
    MARKET_CAP_COMMODITIES_FILENAME,
    MARKET_CAP_COMMODITIES_RESPONSE_CACHE_KEY,
    MARKET_CAP_COMMODITIES_RESPONSE_TTL,



    # ---- INFO_DASHBOARD_TRAFFIC
    INFO_DASHBOARD_TRAFFIC_1H, 
    INFO_DASHBOARD_TRAFFIC_24H, 
    INFO_DASHBOARD_TRAFFIC_1W,  
    INFO_DASHBOARD_TRAFFIC_1M,  
    INFO_DASHBOARD_TRAFFIC_1Y,  


)


# ===========================
# 🌐 Time-Helpers
from utils.time_helpers import (
    utc_now,
    utc_now_ts,
    utc_now_ts_ms,
    utc_today_str,
)


# ===========================
# 🌐 Flask App initialisieren
# ===========================
app = Flask(
    __name__,
    static_folder='static',
    static_url_path='/static',
    template_folder='templates'
)


# =====================
# 🔸 Flask & CORS Setup
# =====================
allowed_origins = [
    "https://bitcoin-dashboard.de",
    "https://bitcoin-dashboard.com",
    "https://bitcoin-dashboard.net"  # neue Domain
]

CORS(app, resources={
    r"/api/*": {"origins": allowed_origins},
    r"/stream/*": {"origins": allowed_origins}
})



## ================================================================================================================================================================ ##
## ================================================================================================================================================================ ##
## ================================================================================================================================================================ ##



# ==========================
# 🟢 --- HILFSFUNKTIONEN ---
# ==========================

# ==============
# 🟢 REDIS SETUP
try:
    r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    r.ping()
    print("✅ Verbindung zu Redis hergestellt.")
except Exception as e:
    print(f"❌ Fehler bei Redis-Verbindung: {e}")
    r = None


# =====================
# 🟢 JSON-LOAD-FUNKTION
def load_json(path, default=None):
    """Liest JSON sicher ein, verhindert 500er Fehler."""
    try:
        if not os.path.exists(path):
            return default
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"[JSON] ❌ Fehler ({path}): {e}")
        return default


# =====================
# 🟢 INDEX-Route
@app.route("/")
def index():
    btc_address = "bc1qa5xegm6gxe408rszj0uyjmtwhq76vg9n2e7gzq"

    return render_template(
        "index.html",
        btc_address=btc_address
    )

## ================================================================================================================================================================ ##
## ================================================================================================================================================================ ##
## ================================================================================================================================================================ ##



# =========================================================
# 🟧 CoinGecko BTC Price mit Distributed Lock + Redis Cache
# =========================================================

# =================================
# 🟦 Hilfsfunktion: CoinGecko-Fetch
def _fetch_home_btc_price_from_api():
    """Holt BTC-Preis von CoinGecko (USD, EUR, JPY)."""
    
    print("🌍 [HOME-BTC-PRICE] Hole Live-Daten von CoinGecko …")

    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        "ids": "bitcoin",
        "vs_currencies": "usd,eur,jpy"
    }

    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()

    print("✅ [HOME-BTC-PRICE] CoinGecko Daten erfolgreich geladen.")
    return resp.text   # direkt JSON-String zurückgeben


# ======================================
# 🟦 Zentrale Funktion: BTC Preise holen
def get_home_btc_prices():
    """Race-Condition-freies Holen der BTC-Preise mit Redis-Lock."""

    if r is None:
        return '{"error": "Redis not initialized"}'

    # 1️⃣ Cache prüfen
    cached = r.get(HOME_BTC_PRICE_CACHE)
    if cached:
        print("📦 [HOME-BTC-PRICE] Cache-Hit.")
        return cached

    print("📭 [HOME-BTC-PRICE] Kein Cache vorhanden → Fetch benötigt.")

    # 2️⃣ Lock versuchen → wir werden der Updater-Thread
    got_lock = r.set(
        HOME_PRICE_LOCK,
        "1",
        nx=True,   # nur setzen, wenn nicht existiert
        ex=HOME_BTC_PRICE_LOCK_TTL
    )

    if got_lock:
        # 🔒 Wir sind der einzige Thread, der den API-Fetch machen darf
        try:
            print("🔒 [HOME-BTC-PRICE] Lock erhalten – führe API-Fetch aus …")
            data = _fetch_home_btc_price_from_api()

            r.set(HOME_BTC_PRICE_CACHE, data, ex=HOME_BTC_PRICE_CACHE_TTL)
            print("🟢 [HOME-BTC-PRICE] Cache aktualisiert.")
            return data

        except Exception as e:
            print(f"❌ [BTC-PRICE] Fehler beim API-Fetch: {e}")
            fallback = '{"error": "CoinGecko API failed"}'
            r.set(HOME_BTC_PRICE_CACHE, fallback, ex=10)
            return fallback

        finally:
            try:
                r.delete(HOME_PRICE_LOCK)
            except:
                pass

    # 3️⃣ Kein Lock erhalten → ein anderer Thread aktualisiert gerade
    print("⏳ [BTC-PRICE] Lock aktiv – warte auf Cache …")

    waited = 0
    while waited < HOME_BTC_PRICE_MAX_WAIT:
        time.sleep(HOME_BTC_PRICE_WAIT_STEP)
        waited += HOME_BTC_PRICE_WAIT_STEP

        cached = r.get(HOME_BTC_PRICE_CACHE)
        if cached:
            print("📦 [HOME-BTC-PRICE] Cache nach Wartezeit verfügbar.")
            return cached

    # 4️⃣ Timeout → Fallback statt Fehler
    print("⛔ [BTC-PRICE] Timeout – gebe Fallback-Daten zurück.")
    return '{"error": "cache timeout"}'


# ============
# 🟧 API Route
@app.route("/api/home_btc_price")
def api_home_btc_price():
    try:
        data = get_home_btc_prices()
        return Response(data, mimetype="application/json")
    except Exception as e:
        return Response(
            json.dumps({"error": str(e)}),
            status=500,
            mimetype="application/json"
        )

## ================================================================================================================================================================ ##

# =======================================================
# 🔹 BTC_VOLUME_WORKER                       --RAM-ONLY--
# =======================================================

# =========================
# 🔸 API: BTC_TOP (dynamic)
@app.route("/api/BTC_VOL", methods=["GET"])
def api_3_btc_vol():
    raw = r.get(BTC_VOL_DYNAMIC_CACHE)

    if not raw:
        # 🔒 IMMER gültiges JSON zurückgeben
        return Response(
            json.dumps({
                "mempool_volume": 0.0,
                "volume_1h": 0.0,
                "volume_24h": 0.0,
                "ts": int(time.time()),
            }),
            mimetype="application/json",
            status=200,
        )

    return Response(
        raw.decode() if isinstance(raw, bytes) else raw,
        mimetype="application/json",
        status=200,
    )


## ================================================================================================================================================================ ##


# =============================
# DASHBOARD TRAFFIC – CONSTANTS
# =============================
# --- Active Sessions (Model B) ---
DASHBOARD_ACTIVE_PREFIX = "DASHBOARD_ACTIVE_"
DASHBOARD_ACTIVE_TTL    = 30  # seconds

# --- Page Views (Model A) ---
RAW_EVENT_TTL_SECONDS = 60 * 60 * 24  # 24h safety


# =============
# REDIS HELPERS
# =============
def _get_int_redis(key: str, default: int = 0) -> int:
    """Safe integer read"""
    raw = r.get(key)
    if not raw:
        return default
    try:
        return int(raw.decode() if isinstance(raw, bytes) else raw)
    except Exception:
        return default

# =========================
# MODEL A – PAGE VIEW EVENT
# =========================
def emit_dashboard_traffic_event() -> None:
    """
    Emits exactly ONE dashboard page view.
    Called only on real page load.
    """
    ts_ms = int(time.time() * 1000)
    redis_key = f"{DASHBOARD_TRAFFIC_RAW_PREFIX}{ts_ms}"
    r.incr(redis_key)
    r.expire(redis_key, RAW_EVENT_TTL_SECONDS)


# ==================================
# MODEL B – ACTIVE SESSION HEARTBEAT
# ==================================
@app.route("/api/track/dashboard_alive", methods=["POST", "GET", "HEAD"])
def api_dashboard_alive():

    # ==========================
    # Google / Crawler (GET, HEAD)
    # ==========================
    if request.method in ("GET", "HEAD"):
        resp = Response(status=204)  # No Content
        resp.headers["X-Robots-Tag"] = "noindex, nofollow"
        return resp

    # ==========================
    # Dashboard Heartbeat (POST)
    # ==========================
    session_id = request.headers.get("X-Dashboard-Session")
    if not session_id:
        session_id = uuid.uuid4().hex

    redis_key = f"{DASHBOARD_ACTIVE_PREFIX}{session_id}"
    r.set(redis_key, "1", ex=DASHBOARD_ACTIVE_TTL)

    return jsonify({"session_id": session_id})


# ============================
# PAGE VIEW TRACKING (Model A)
# ============================
@app.route("/api/track/dashboard_pageview", methods=["POST"])
def api_track_dashboard_pageview():
    """
    Dashboard page view.
    - called once per page load
    """
    emit_dashboard_traffic_event()
    return "", 204

# ===================
# HOME DASHBOARD KPIs
# ===================
@app.route("/api/home_traffic", methods=["GET"])
def api_home_traffic():
    """
    Home dashboard metrics.
    - LIVE   = active sessions (Model B)
    - TOTAL  = page views since launch (Model A)
    """

    # count active sessions
    active_sessions = sum(
        1 for _ in r.scan_iter(match=f"{DASHBOARD_ACTIVE_PREFIX}*")
    )

    return jsonify({
        "live": active_sessions,
        "total_requests": _get_int_redis(DASHBOARD_TRAFFIC_TOTAL, 0),
        "ts_utc": utc_now_ts(),
    })

# ========================================
# DASHBOARD TRAFFIC – TIME SERIES (CHARTS)
# ========================================
@app.route("/api/dashboard_traffic/<range>", methods=["GET"])
def api_dashboard_traffic(range: str):
    """
    Traffic time series.
    - bucketed
    - read only
    """

    key_map = {
        "1h":  INFO_DASHBOARD_TRAFFIC_1H,
        "24h": INFO_DASHBOARD_TRAFFIC_24H,
        "1w":  INFO_DASHBOARD_TRAFFIC_1W,
        "1m":  INFO_DASHBOARD_TRAFFIC_1M,
        "1y":  INFO_DASHBOARD_TRAFFIC_1Y,
    }

    redis_key = key_map.get(range)
    if not redis_key:
        return jsonify({"history": []})

    raw = r.get(redis_key)
    if not raw:
        return jsonify({"history": []})

    return raw, 200, {"Content-Type": "application/json"}


## ================================================================================================================================================================ ##


# =================
# 🔹 META-DASHBOARD
# =================

# =======
# 🔹 Lock
def acquire_lock():
    return r.set(HOME_META_LOCK, "1", nx=True, ex=HOME_META_LOCK_TTL)

def release_lock():
    r.delete(HOME_META_LOCK)

# =============================================================
# 🔹 Request Stats (rollend 24h)  -  ACHTUNG RAM - KEIN REDIS!!
HOME_META_REQUEST_STATS = {"timestamps": []}


def update_home_meta_request_stats():
    now = time.time()
    HOME_META_REQUEST_STATS["timestamps"].append(now)
    HOME_META_REQUEST_STATS["timestamps"] = [t for t in HOME_META_REQUEST_STATS["timestamps"] if now - t <= HOME_META_DAY_SECONDS]

def get_home_meta_request_stats():
    now = time.time()
    last_minute = [t for t in HOME_META_REQUEST_STATS["timestamps"] if now - t <= 60]
    rps = len(last_minute)/60
    total_day = len(HOME_META_REQUEST_STATS["timestamps"])
    return {
        "reqs_per_sec": round(rps,1),
        "reqs_percent": min(100, round(rps/250*100,1)),  # max 250 RPS
        "api_requests": total_day,
        "api_percent": min(100, round(total_day/1_000_000*100,1))
    }

# =============
# 🔹 Redis Info
def get_redis_info():
    if not r:
        return {"used":0,"usedPercent":0,"hits":0,"misses":0}

    info = r.info()
    used = info.get("used_memory", 0) / (1024**2)

    system_ram_mb = psutil.virtual_memory().total / (1024**2)
    used_percent = round((used / system_ram_mb) * 100, 2)

    hits = info.get("keyspace_hits", 0)
    misses = info.get("keyspace_misses", 0)
    total = hits + misses or 1

    return {
        "used": round(used,1),
        "usedPercent": used_percent,
        "hits": hits,
        "hitsPercent": round((hits/total)*100,1),
        "misses": misses,
        "missesPercent": round((misses/total)*100,1)
    }

# =========================================
# 🔹 NVMe IO  -  ACHTUNG RAM - KEIN REDIS!!
HOME_META_NVME_DEVICES = ["nvme0n1","nvme1n1"]
def get_nvme_io(interval=1):
    io_before = psutil.disk_io_counters(perdisk=True)
    time.sleep(interval)
    io_after = psutil.disk_io_counters(perdisk=True)
    read_total = sum(io_after[d].read_bytes - io_before[d].read_bytes
                     for d in HOME_META_NVME_DEVICES if d in io_before and d in io_after)
    write_total = sum(io_after[d].write_bytes - io_before[d].write_bytes
                      for d in HOME_META_NVME_DEVICES if d in io_before and d in io_after)
    return {"read": round(read_total/(1024**2)/interval,1),
            "write": round(write_total/(1024**2)/interval,1)}

# ===========
# 🔹 Netzwerk
def get_default_gateway_iface():
    try:
        route = subprocess.check_output("ip route show default", shell=True).decode()
        return route.split("dev")[1].split()[0]
    except:
        return None

def get_network_speed(default_iface, interval=1):
    net_before = psutil.net_io_counters(pernic=True)
    time.sleep(interval)
    net_after = psutil.net_io_counters(pernic=True)
    upload_internet = download_internet = upload_lan = download_lan = 0
    for iface in net_before:
        up = (net_after[iface].bytes_sent - net_before[iface].bytes_sent)/interval
        down = (net_after[iface].bytes_recv - net_before[iface].bytes_recv)/interval
        if iface==default_iface:
            upload_internet += up
            download_internet += down
        else:
            upload_lan += up
            download_lan += down
    bps_to_mbit=lambda x:(x*8)/1_000_000
    return {"internet":(bps_to_mbit(upload_internet),bps_to_mbit(download_internet)),
            "lan":(bps_to_mbit(upload_lan),bps_to_mbit(download_lan))}

default_iface = get_default_gateway_iface()
if not default_iface:
    raise RuntimeError("Standard-Gateway konnte nicht ermittelt werden")

# =====================
# 🔹 Hintergrund-Worker
def meta_cache_worker():
    while True:
        try:
            if acquire_lock():
                update_home_meta_request_stats()
                req_stats = get_home_meta_request_stats()

                cpu = psutil.cpu_percent(interval=0.1)
                mem = psutil.virtual_memory()
                swap = psutil.swap_memory().percent
                ram_percent = round(mem.used/mem.total*100,1)
                nvme_usage_percent = psutil.disk_usage("/").percent
                bitcoin_disk = psutil.disk_usage("/raid/bitcoin")
                nvme_free = bitcoin_disk.free/(1024**3)
                nvme_free_percent = bitcoin_disk.free/bitcoin_disk.total*100
                redis_info = get_redis_info()

                # Parallel NVMe + Netzwerk messen
                with ThreadPoolExecutor() as executor:
                    nvme_future = executor.submit(get_nvme_io, 1)
                    net_future = executor.submit(get_network_speed, default_iface, 1)
                    nvme_io = nvme_future.result()
                    speeds = net_future.result()

                payload = {
                    "cpuLoad":cpu,
                    "ramUsage":mem.used/(1024**3),
                    "ramTotal":mem.total/(1024**3),
                    "ramUsagePercent":ram_percent,
                    "swapUsage":swap,
                    "nvmeUsagePercent":nvme_usage_percent,
                    "nvmeFree":nvme_free,
                    "nvmeFreePercent":nvme_free_percent,
                    "nvmeIoRead":nvme_io["read"],
                    "nvmeIoWrite":nvme_io["write"],
                    "reqsPerSec":req_stats["reqs_per_sec"],
                    "reqsPercent":req_stats["reqs_percent"],
                    "apiRequests":req_stats["api_requests"],
                    "apiPercent":req_stats["api_percent"],
                    "redisUsed":redis_info["used"],
                    "redisUsedPercent":redis_info["usedPercent"],
                    "redisHits":redis_info["hits"],
                    "redisHitsPercent":redis_info["hitsPercent"],
                    "redisMisses":redis_info["misses"],
                    "redisMissesPercent":redis_info["missesPercent"],
                    "internet":{"upload":round(speeds["internet"][0],2),
                                "download":round(speeds["internet"][1],2)},
                    "lan":{"upload":round(speeds["lan"][0],2),
                           "download":round(speeds["lan"][1],2)}
                }

                r.set(HOME_META_CACHE,json.dumps(payload),ex=HOME_META_CACHE_TTL)
                release_lock()
        except Exception as e:
            print("[Worker Error]", e)

        time.sleep(1)  # Polling

threading.Thread(target=meta_cache_worker, daemon=True).start()

# ===========
# 🔹 Endpoint
@app.route('/api/system-health')
def system_health():
    cached = r.get(HOME_META_CACHE)
    if cached:
        return jsonify(json.loads(cached)),200
    return jsonify({
        "cpuLoad":0,"ramUsage":0,"ramTotal":0,"ramUsagePercent":0,
        "swapUsage":0,"nvmeUsagePercent":0,"nvmeFree":0,"nvmeFreePercent":0,
        "nvmeIoRead":0,"nvmeIoWrite":0,
        "reqsPerSec":0,"reqsPercent":0,"apiRequests":0,"apiPercent":0,
        "redisUsed":0,"redisUsedPercent":0,"redisHits":0,"redisHitsPercent":0,
        "redisMisses":0,"redisMissesPercent":0,
        "internet":{"upload":0,"download":0},
        "lan":{"upload":0,"download":0}
    }),200



## ================================================================================================================================================================ ##
## ================================================================================================================================================================ ##
## ================================================================================================================================================================ ##



# ==============================================
# REVOLUTION_HISTORY – SSR Master Template Route
# Hybrid SPA + SSR Deep-Link Entry Point
# ==============================================
@app.route("/revolution/history")
def revolution_history():
    return render_template(
        "pages/revolution/history.html"
    )


## ================================================================================================================================================================ ##


# ===================================================
# REVOLUTION_PIONEERS – SSR Content Page (Static SEO)
# Bitcoin Cypherpunks & Early Contributors
# ===================================================
@app.route("/revolution/pioneers")
def revolution_pioneers():
    return render_template(
        "pages/revolution/pioneers.html"
    )


## ================================================================================================================================================================ ##


# =====================================================
# REVOLUTION_WHITEPAPER – SSR Content Page (Static SEO)
# Original Bitcoin Whitepaper by Satoshi Nakamoto
# =====================================================
@app.route("/revolution/whitepaper")
def revolution_whitepaper():
    return render_template(
        "pages/revolution/whitepaper.html"
    )



## ================================================================================================================================================================ ##
## ================================================================================================================================================================ ##
## ================================================================================================================================================================ ##



# =================================================
# NETWORK_STRUCTURE – SSR Content Page (Static SEO)
# Bitcoin Node Topology & Network Architecture
# =================================================
@app.route("/network/structure")
def network_structure():
    return render_template(
        "pages/network/structure.html"
    )


## ================================================================================================================================================================ ##


# =================================================
# NETWORK_NODES – Redis + Short-Term + Subtab Cache
# =================================================

# -----------------------
# Short-Term / RAM Cache
# -----------------------
bitnodes_last_response = None
bitnodes_last_response_time = 0


def fetch_bitnodes_data():
    global bitnodes_last_response, bitnodes_last_response_time
    now = time.time()

    # -------------------
    # Short-Term Cache prüfen (RAM)
    # -------------------
    if (
        bitnodes_last_response
        and now - bitnodes_last_response_time < NETWORK_NODES_SHORT_CACHE_TTL
    ):
        return bitnodes_last_response

    worker_pid = os.getpid()
    key = NETWORK_NODES_CACHE_KEY
    lock_key = NETWORK_NODES_LOCK_KEY

    # -------------------
    # Redis Cache prüfen
    # -------------------
    if r:
        cached = r.get(key)
        if cached:
            payload = json.loads(cached)
            print(
                f"[Worker {worker_pid}] ✅ Bitnodes-Daten aus Redis Cache geladen "
                f"({payload.get('total', '?')} Nodes)"
            )
            bitnodes_last_response = payload
            bitnodes_last_response_time = now
            return payload

        # Lock setzen, um parallele Updates zu verhindern
        got_lock = r.set(lock_key, "1", nx=True, ex=NETWORK_NODES_LOCK_TTL)
        if not got_lock:
            print(f"[Worker {worker_pid}] ⏳ Bitnodes-Cache wird gerade aktualisiert")
            return {"error": "Cache wird aktualisiert, bitte später erneut versuchen"}

    # -------------------
    # Live-Daten abrufen
    # -------------------
    try:
        res = requests.get(
            "https://bitnodes.io/api/v1/snapshots/latest/", timeout=10
        )
        if res.status_code == 200:
            api_data = res.json()

            countries_raw = [
                ("Vereinigte Staaten", 2460),
                ("Deutschland", 1280),
                ("Frankreich", 710),
                ("Kanada", 420),
                ("Finnland", 380),
                ("Niederlande", 340),
                ("Vereinigtes Königreich", 310),
                ("Schweiz", 240),
                ("Russische Föderation", 200),
            ]
            by_country = [{"country": c, "nodes": n} for c, n in countries_raw]

            from datetime import datetime, timezone

            payload = {
                "total": api_data.get("total_nodes", 0),
                "last_update": datetime.now(timezone.utc).strftime(
                    "%Y-%m-%d %H:%M:%S UTC"
                ),
                "status": "OK",
                "by_country": by_country,
            }

            # Redis Cache aktualisieren
            if r:
                r.set(
                    key,
                    json.dumps(payload),
                    ex=NETWORK_NODES_REFRESH_INTERVAL,
                )
                r.delete(lock_key)
                print(
                    f"[Worker {worker_pid}] 🔍 Bitnodes aktualisiert: "
                    f"{payload['total']} Nodes"
                )

            # Short-Term Cache aktualisieren (RAM)
            bitnodes_last_response = payload
            bitnodes_last_response_time = now

            return payload

        return {"error": f"Bitnodes API Status {res.status_code}"}

    except Exception as e:
        print(
            f"[Worker {worker_pid}] Fehler beim Abrufen der Bitnodes-Daten: {e}"
        )
        return {"error": str(e)}


# ---------------
# 🟢 API Endpoint
# ---------------
subtab_nodes_cache = None
subtab_nodes_cache_time = 0

@app.route("/api/network/nodes")
def network_nodes():
    global subtab_nodes_cache, subtab_nodes_cache_time
    now = time.time()

    # -------------------
    # Subtab-Cache prüfen
    # -------------------
    if subtab_nodes_cache and now - subtab_nodes_cache_time < NETWORK_NODES_SUBTAB_CACHE_TTL:
        return jsonify(subtab_nodes_cache)

    # -------------------
    # Endpoint-Daten abrufen
    # -------------------
    payload = fetch_bitnodes_data()

    # Subtab-Cache aktualisieren
    subtab_nodes_cache = payload
    subtab_nodes_cache_time = now

    return jsonify(payload)



## ================================================================================================================================================================ ##
## ================================================================================================================================================================ ##



# ========================================================
# NETWORK_MINER – Redis + Distributed Lock + mempool.space
# ========================================================
def fetch_network_miner_data():
    worker_pid = os.getpid()
    cache_key = NETWORK_MINER_CACHE_KEY
    lock_key = NETWORK_MINER_LOCK_KEY

    # ------------------------------
    # Redis Langzeitcache prüfen
    # ------------------------------
    if r:
        cached = r.get(cache_key)
        if cached:
            payload = json.loads(cached)
            print(
                f"[Worker {worker_pid}] ✅ Miner-Daten aus Redis Cache geladen "
                f"({len(payload)} Pools)"
            )
            return payload

        # ------------------------------
        # Distributed Lock setzen
        # ------------------------------
        got_lock = r.set(
            lock_key,
            "1",
            nx=True,
            ex=NETWORK_MINER_LOCK_TTL
        )

        if not got_lock:
            print(
                f"[Worker {worker_pid}] ⏳ Miner-Cache wird gerade aktualisiert"
            )
            return []

    # ------------------------------
    # API Abruf (mempool.space)
    # ------------------------------
    try:
        response = requests.get(
            "https://mempool.space/api/v1/mining/pools/24h",
            timeout=10
        )
        response.raise_for_status()
        raw = response.json()

        pools = raw.get("pools", [])
        total_blocks = raw.get("blockCount", 0)

        if not pools or total_blocks == 0:
            raise ValueError("Ungültige Pool-Daten von mempool.space")

        # ------------------------------
        # Top 5 Pools + Marktanteil
        # ------------------------------
        data = []

        for pool in pools[:5]:
            name = pool.get("name", "Unknown")
            blocks = pool.get("blockCount", 0)

            share = round((blocks / total_blocks) * 100, 2)

            data.append({
                "pool": name,
                "share": share
            })

        # ------------------------------
        # Redis Cache speichern
        # ------------------------------
        if r:
            r.set(
                cache_key,
                json.dumps(data),
                ex=NETWORK_MINER_REFRESH_INTERVAL
            )
            r.delete(lock_key)

            print(
                f"[Worker {worker_pid}] 🔍 Miner-Daten aktualisiert "
                f"({len(data)} Pools)"
            )

        return data

    except Exception as e:
        print(
            f"[Worker {worker_pid}] ❌ Fehler beim Abrufen der Miner-Daten: {e}"
        )

        if r:
            r.delete(lock_key)

        return []

# ==============================
# 🟢 API Endpoint – Mining Pools
@app.route("/api/network/miner")
def api_network_miner():
    try:
        if r:
            cached = r.get(NETWORK_MINER_STC_KEY)
            if cached:
                print("[NETWORK_MINER API] ⚡ Short-Term Cache genutzt")
                return cached, 200, {"Content-Type": "application/json"}

        data = fetch_network_miner_data()
        payload = jsonify(data)
        text = payload.get_data(as_text=True)

        if r:
            r.set(
                NETWORK_MINER_STC_KEY,
                text,
                ex=NETWORK_MINER_STC_TTL
            )
            print("[NETWORK_MINER API] 🧊 Short-Term Cache gesetzt")

        return payload

    except Exception as e:
        print(
            f"[NETWORK_MINER API] ❌ Fehler im API Endpoint: {e}"
        )
        return jsonify([]), 500



## ================================================================================================================================================================ ##
## ================================================================================================================================================================ ##
## ================================================================================================================================================================ ##



# =======================================================
# METRICS_BTC_USD_EUR – Redis + Short-Term + Subtab Cache
# =======================================================

# ---------------------------------
# 🔹 Short-Term Cache Einstellungen (RAM)
# ---------------------------------
metrics_last_response = None
metrics_last_response_time = 0

# ---------------------------------
# 🔹 Subtab Cache Einstellungen (RAM)
# ---------------------------------
metrics_subtab_cache = None
metrics_subtab_cache_time = 0

# --------------------------------------------------
# 🔹 Hilfsfunktion: CoinGecko-Request mit Fehlercheck
# --------------------------------------------------
def _cg_get(url, params=None):
    resp = requests.get(url, params=params, timeout=10)
    if resp.status_code != 200:
        raise RuntimeError(f"CoinGecko HTTP {resp.status_code}: {resp.text[:200]}")
    return resp.json()

# ----------------------------------------
# 🔹 Funktion: Metrics-Daten aus Redis / API
# ----------------------------------------
def get_metrics_btc_usd_eur_redis():
    worker_pid = os.getpid()

    if not r:
        print(f"[Worker {worker_pid}] ⚠️ Kein Redis-Client verfügbar – direkter API-Fetch.")
        return _fetch_metrics_btc_usd_eur_from_api()

    max_wait_seconds = 10
    wait_step = 0.5
    waited = 0.0

    while True:
        # 1️⃣ Redis Cache prüfen
        cached = r.get(METRICS_BTC_USD_EUR_CACHE_KEY)
        if cached:
            payload = json.loads(cached)
            print(f"[Worker {worker_pid}] ✅ Metrics aus Redis Cache geladen.")
            return payload

        # 2️⃣ Lock versuchen
        got_lock = r.set(
            METRICS_BTC_USD_EUR_LOCK_KEY,
            "1",
            nx=True,
            ex=METRICS_BTC_USD_EUR_LOCK_TTL
        )

        if got_lock:
            print(f"[Worker {worker_pid}] 🔒 Lock erhalten – lade Metrics von CoinGecko …")
            try:
                result = _fetch_metrics_btc_usd_eur_from_api()

                r.set(
                    METRICS_BTC_USD_EUR_CACHE_KEY,
                    json.dumps(result),
                    ex=METRICS_BTC_USD_EUR_REFRESH_INTERVAL
                )
                print(f"[Worker {worker_pid}] 🟢 Metrics in Redis aktualisiert.")
                return result

            except Exception as e:
                print(f"[Worker {worker_pid}] ❌ Fehler beim Abrufen der Metrics: {e}")
                fallback = {
                    "live": {"usd": None, "eur": None},
                    "history": {"usd": [], "eur": []}
                }
                r.set(
                    METRICS_BTC_USD_EUR_CACHE_KEY,
                    json.dumps(fallback),
                    ex=60
                )
                return fallback

            finally:
                try:
                    r.delete(METRICS_BTC_USD_EUR_LOCK_KEY)
                except Exception as e:
                    print(f"[Worker {worker_pid}] ⚠️ Konnte Lock-Key nicht löschen: {e}")

        # 3️⃣ Warten
        print(f"[Worker {worker_pid}] ⏳ Warte auf Cache-Update (Lock aktiv) …")
        time.sleep(wait_step)
        waited += wait_step

        if waited >= max_wait_seconds:
            print(f"[Worker {worker_pid}] ⛔ Timeout beim Warten auf Cache.")
            return {
                "live": {"usd": None, "eur": None},
                "history": {"usd": [], "eur": []}
            }

# -------------------------------------------------
# 🔹 Funktion: Reiner API-Fetch + Resultat aufbauen
# -------------------------------------------------
def _fetch_metrics_btc_usd_eur_from_api():
    worker_pid = os.getpid()
    print(f"[Worker {worker_pid}] 🌍 Hole BTC/USD/EUR Metrics von CoinGecko …")

    live_url = "https://api.coingecko.com/api/v3/simple/price"
    live_params = {
        "ids": "bitcoin",
        "vs_currencies": "usd,eur"
    }
    live_json = _cg_get(live_url, params=live_params)
    live_usd = live_json["bitcoin"]["usd"]
    live_eur = live_json["bitcoin"]["eur"]

    base_url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"
    hist_usd = _cg_get(base_url, params={"vs_currency": "usd", "days": 365})
    hist_eur = _cg_get(base_url, params={"vs_currency": "eur", "days": 365})

    result = {
        "live": {"usd": live_usd, "eur": live_eur},
        "history": {
            "usd": hist_usd["prices"],
            "eur": hist_eur["prices"]
        }
    }

    print(f"[Worker {worker_pid}] ✅ CoinGecko Metrics erfolgreich geladen.")
    return result

# ---------------------------------------------
# 🔹 Funktion: Metrics mit Short-Term Cache (RAM)
# ---------------------------------------------
def get_metrics_btc_usd_eur_short_term():
    global metrics_last_response, metrics_last_response_time
    now = time.time()

    if (
        metrics_last_response
        and (now - metrics_last_response_time) < METRICS_BTC_USD_EUR_SHORT_CACHE_TTL
    ):
        return metrics_last_response

    data = get_metrics_btc_usd_eur_redis()
    metrics_last_response = data
    metrics_last_response_time = now
    return data

# ----------------------------------------
# 🔹 Funktion: Metrics mit Subtab-Cache (RAM)
# ----------------------------------------
def get_metrics_btc_usd_eur_subtab():
    global metrics_subtab_cache, metrics_subtab_cache_time
    now = time.time()

    if (
        metrics_subtab_cache
        and (now - metrics_subtab_cache_time) < METRICS_BTC_USD_EUR_SUBTAB_TTL
    ):
        return metrics_subtab_cache

    data = get_metrics_btc_usd_eur_short_term()
    metrics_subtab_cache = data
    metrics_subtab_cache_time = now
    print(f"[Worker {os.getpid()}] 🟢 Metrics Subtab-Cache aktualisiert")
    return data

# -------------------------------
# 🟢 API Endpoint BTC/USD/EUR
# -------------------------------
@app.route("/api/metrics/btc_usd_eur")
def metrics_btc_usd_eur():
    try:
        return jsonify(get_metrics_btc_usd_eur_subtab())
    except Exception as e:
        print(f"[Worker {os.getpid()}] Fehler im Metrics-Endpoint: {e}")
        return jsonify({
            "live": {"usd": None, "eur": None},
            "history": {"usd": [], "eur": []}
        })

## ================================================================================================================================================================ ##

# ======================================================
# 🌐 METRICS_DIFFICULTY API     --RAM-ONLY--  --NODE I--
# ======================================================

@app.route("/api/difficulty/1y")
def api_difficulty_1y():
    raw = r.get("CHART_BTC_DIFFICULTY_1y")
    if not raw:
        return jsonify({"history": []}), 200
    return Response(raw, mimetype="application/json")

@app.route("/api/difficulty/5y")
def api_difficulty_5y():
    raw = r.get("CHART_BTC_DIFFICULTY_5y")
    if not raw:
        return jsonify({"history": []}), 200
    return Response(raw, mimetype="application/json")

@app.route("/api/difficulty/10y")
def api_difficulty_10y():
    raw = r.get("CHART_BTC_DIFFICULTY_10y")
    if not raw:
        return jsonify({"history": []}), 200
    return Response(raw, mimetype="application/json")

@app.route("/api/difficulty/ever")
def api_difficulty_ever():
    raw = r.get("CHART_BTC_DIFFICULTY_ever")
    if not raw:
        return jsonify({"history": []}), 200
    return Response(raw, mimetype="application/json")

## ================================================================================================================================================================ ##

# ===================================================================
# 🔹 BTC_TX_VOLUME_WORKER                                --RAM-ONLY--
# ===================================================================

def _redis_chart_response(key: str):
    raw = r.get(key)

    if not raw:
        return Response(
            json.dumps({"history": []}),
            mimetype="application/json"
        )

    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode()

    try:
        json.loads(raw)
    except Exception:
        return Response(
            json.dumps({"history": []}),
            mimetype="application/json"
        )

    return Response(raw, mimetype="application/json")


# ===============
# Chart Endpoints
@app.route("/api/btc_tx_volume/1h")
def api_btc_tx_volume_1h():
    return _redis_chart_response(BTC_TX_VOLUME_1H)

@app.route("/api/btc_tx_volume/24h")
def api_btc_tx_volume_24h():
    return _redis_chart_response(BTC_TX_VOLUME_24H)

@app.route("/api/btc_tx_volume/1w")
def api_btc_tx_volume_1w():
    return _redis_chart_response(BTC_TX_VOLUME_1W)

@app.route("/api/btc_tx_volume/1m")
def api_btc_tx_volume_1m():
    return _redis_chart_response(BTC_TX_VOLUME_1M)

@app.route("/api/btc_tx_volume/1y")
def api_btc_tx_volume_1y():
    return _redis_chart_response(BTC_TX_VOLUME_1Y)


# =====
# Stats
@app.route("/api/btc_tx_volume/stats")
def api_btc_tx_volume_stats():
    raw = r.hgetall(BTC_TX_VOLUME_STATS)

    if not raw:
        return Response(
            json.dumps({}),
            mimetype="application/json"
        )

    stats = {k.decode(): v.decode() for k, v in raw.items()}

    return Response(
        json.dumps(stats),
        mimetype="application/json"
    )

## ================================================================================================================================================================ ##

# ===================================================================
# 🔹 BTC_TX_AMOUNT_WORKER                                --RAM-ONLY--
# ===================================================================

# =============================
# 🔸 API: BTC TX Amount History
@app.route("/api/txamount/history", methods=["GET"])
def api_txamount_history():
    try:
        data = r.get(BTC_TX_AMOUNT_HISTORY_KEY)

        if not data:
            return Response(
                json.dumps({"error": "tx amount data not available"}),
                status=503,
                mimetype="application/json"
            )

        return Response(
            data.decode() if isinstance(data, bytes) else data,
            mimetype="application/json"
        )

    except Exception as e:
        return Response(
            json.dumps({"error": str(e)}),
            status=500,
            mimetype="application/json"
        )

## ================================================================================================================================================================ ##

# ===================================================================
# 🔹 BTC_TX_FEE_WORKER                                --RAM-ONLY--
# ===================================================================

# ======================================
# 📊 Redis Chart Helper (RAM-ONLY, SAFE)
def _redis_chart_response(key: str):
    raw = r.get(key)

    if not raw:
        return Response(
            json.dumps({"history": []}),
            mimetype="application/json"
        )

    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode()

    try:
        json.loads(raw)
    except Exception:
        return Response(
            json.dumps({"history": []}),
            mimetype="application/json"
        )

    return Response(raw, mimetype="application/json")


# =============================================
# 📊 API: BTC_TX_FEES              --RAM-ONLY--
# =============================================

@app.route("/api/btc_tx_fees/24h")
def api_btc_tx_fees_24h():
    return _redis_chart_response(BTC_TX_FEES_24H)

@app.route("/api/btc_tx_fees/1w")
def api_btc_tx_fees_1w():
    return _redis_chart_response(BTC_TX_FEES_1W)

@app.route("/api/btc_tx_fees/1m")
def api_btc_tx_fees_1m():
    return _redis_chart_response(BTC_TX_FEES_1M)

@app.route("/api/btc_tx_fees/1y")
def api_btc_tx_fees_1y():
    return _redis_chart_response(BTC_TX_FEES_1Y)

## ================================================================================================================================================================ ##

# =======================================================
# 🌐 Hashrate API                --RAM-ONLY--  --NODE I--
# =======================================================
@app.route("/api/hashrate/1y")
def api_hashrate_1y():
    raw = r.get("CHART_BTC_HASHRATE_1y")
    if not raw:
        return jsonify({"history": []}), 200
    return Response(raw, mimetype="application/json")

@app.route("/api/hashrate/5y")
def api_hashrate_5y():
    raw = r.get("CHART_BTC_HASHRATE_5y")
    if not raw:
        return jsonify({"history": []}), 200
    return Response(raw, mimetype="application/json")

@app.route("/api/hashrate/10y")
def api_hashrate_10y():
    raw = r.get("CHART_BTC_HASHRATE_10y")
    if not raw:
        return jsonify({"history": []}), 200
    return Response(raw, mimetype="application/json")

@app.route("/api/hashrate/ever")
def api_hashrate_ever():
    raw = r.get("CHART_BTC_HASHRATE_ever")
    if not raw:
        return jsonify({"history": []}), 200
    return Response(raw, mimetype="application/json")



## ================================================================================================================================================================ ##
## ================================================================================================================================================================ ##
## ================================================================================================================================================================ ##




# ============
# 🔹 API-ROUTE
@app.route("/data/review/<path:filename>")
def review_data(filename):
    return send_from_directory(REVIEW_BASE_PATH, filename)



## ================================================================================================================================================================ ##
## ================================================================================================================================================================ ##
## ================================================================================================================================================================ ##



# ======================
# 🔹 [EXPLORER] - ADRESS
# ======================

# ============
# 🔹 API-ROUTE
@app.route("/api/address/<address>")
def api_address(address: str):
    try:
        client = get_electrumx_client()

        # ⚠️ asyncio.run darf NICHT laufen,
        # wenn bereits ein Event-Loop existiert
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            # Falls Flask / Gunicorn bereits einen Loop hat
            data = loop.run_until_complete(
                get_address_overview(client, address)
            )
        else:
            # Normalfall
            data = asyncio.run(
                get_address_overview(client, address)
            )

        return jsonify({
            "status": "ok",
            "data": {
                "address": data["address"],
                "balance": data["balance"],
                "utxos": data["utxos"],
                "history": data["history"],
            }
        })

    except ValueError as e:
        # z. B. ungültiges Address-Format
        return jsonify({
            "status": "error",
            "error": str(e)
        }), 400

    except Exception as e:
        # 🔴 WICHTIG: Traceback erzwingen
        import traceback
        traceback.print_exc()

        return jsonify({
            "status": "error",
            "error": str(e)  # ← temporär, damit wir sehen WAS passiert
        }), 500

## ================================================================================================================================================================ ##

# ===========================
# 🔹 [EXPLORER_TXID] – Worker
# ===========================

def _decode_if_bytes(x):
    if x is None:
        return None
    return x.decode() if isinstance(x, (bytes, bytearray)) else x


def _json_loads_safe(raw, default):
    raw = _decode_if_bytes(raw)
    if not raw:
        return default
    try:
        return json.loads(raw)
    except Exception:
        return default


def get_cached_chain_height() -> int | None:
    """
    Holt die aktuelle Chainhöhe aus Redis.
    Erwartet aggregate_blockchain_dynamic() → BLOCKCHAIN_DYNAMIC_CACHE
    mit z.B. {"current_block_height": 929261, ...}
    """
    combined = _json_loads_safe(r.get(BLOCKCHAIN_DYNAMIC_CACHE), default={})
    h = combined.get("current_block_height")

    try:
        return int(h) if h is not None else None
    except Exception:
        return None


async def get_explorer_txid_details(client, txid: str) -> dict:
    """
    Lädt eine Transaktion inkl. Fee-Berechnung über ElectrumX
    + Blockhöhe-Fallback via Redis-Chainheight
    """

    # Haupt-TX laden (verbose!)
    tx = await client.call("blockchain.transaction.get", [txid, True])

    if not tx or "txid" not in tx:
        raise ValueError("Transaktion nicht gefunden")

    # --------------------------------------------------
    # Outputs
    # --------------------------------------------------
    total_out = sum(v["value"] for v in tx.get("vout", []))

    # --------------------------------------------------
    # Inputs (Prev-TX nachladen)
    # --------------------------------------------------
    total_in = 0.0

    for vin in tx.get("vin", []):
        # Coinbase-TX → keine Inputs, keine Fee
        if "coinbase" in vin:
            total_in = total_out
            break

        prev_txid = vin["txid"]
        prev_vout = vin["vout"]

        prev = await client.call("blockchain.transaction.get", [prev_txid, True])

        try:
            total_in += prev["vout"][prev_vout]["value"]
        except (IndexError, KeyError):
            raise RuntimeError("Fehler beim Auflösen eines Inputs")

    fee = round(total_in - total_out, 8)

    # --------------------------------------------------
    # Status / Blockdaten
    # --------------------------------------------------
    confirmations = int(tx.get("confirmations", 0) or 0)
    confirmed = confirmations > 0

    # ElectrumX liefert blockheight oft nicht → Fallback:
    block_height = tx.get("blockheight")

    if confirmed and block_height is None:
        chain_height = get_cached_chain_height()
        if chain_height is not None:
            block_height = int(chain_height) - confirmations + 1
        else:
            # Notfalls bleibt es None → Frontend zeigt "–"
            block_height = None

    return {
        "txid": tx["txid"],
        "confirmed": confirmed,
        "confirmations": confirmations,
        "block_height": block_height,
        "timestamp": tx.get("blocktime"),
        "total_in": round(total_in, 8),
        "total_out": round(total_out, 8),
        "fee": fee,
    }

# ========================
# 🔹 [EXPLORER_TXID] – API
@app.route("/api/explorer_txid/<txid>")
def api_explorer_txid(txid: str):
    try:
        client = get_electrumx_client()

        # Event-Loop Handling (identisch zu /api/address)
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            data = loop.run_until_complete(
                get_explorer_txid_details(client, txid)
            )
        else:
            data = asyncio.run(
                get_explorer_txid_details(client, txid)
            )

        return jsonify({
            "status": "ok",
            "data": data
        })

    except ValueError as e:
        return jsonify({
            "status": "error",
            "error": str(e)
        }), 400

    except Exception as e:
        import traceback
        traceback.print_exc()

        return jsonify({
            "status": "error",
            "error": str(e)
        }), 500


## ================================================================================================================================================================ ##


# =================================
# 🔹 [EXPLORER_WALLET] – Redis Init
def init_explorer_defaults():
    r.setnx(
        EXPLORER_ADDRESSES_MAX_ADDRESSES_KEY,
        EXPLORER_ADDRESSES_MAX_ADDRESSES_DEFAULT
    )

# =============================
# 🔹 [EXPLORER_WALLET] – HELPER
def get_explorer_addresses_max() -> int:
    try:
        raw = r.get(EXPLORER_ADDRESSES_MAX_ADDRESSES_KEY)
        if raw is None:
            return EXPLORER_ADDRESSES_MAX_ADDRESSES_DEFAULT
        return max(1, int(raw))
    except Exception:
        return EXPLORER_ADDRESSES_MAX_ADDRESSES_DEFAULT

# =============================
# 🔹 [EXPLORER_WALLET] – Worker
# =============================
async def get_wallet_overview(client, addresses: list[str]) -> dict:
    if not addresses:
        raise ValueError("Keine Adressen übergeben")

    seen = set()
    clean = []

    for addr in addresses:
        addr = addr.strip()
        if not addr:
            continue
        if addr in seen:
            continue
        seen.add(addr)
        clean.append(addr)

    if not clean:
        raise ValueError("Keine gültigen Adressen")

    results = []
    total_confirmed = 0
    total_unconfirmed = 0
    total_utxos = 0

    for addr in clean:
        data = await get_address_overview(client, addr)

        total_confirmed   += data["balance"]["confirmed"]
        total_unconfirmed += data["balance"]["unconfirmed"]
        total_utxos       += len(data["utxos"])

        results.append(data)

    return {
        "address_count": len(results),
        "balance": {
            "confirmed": total_confirmed,
            "unconfirmed": total_unconfirmed,
        },
        "utxo_count": total_utxos,
        "addresses": results,
    }

# ================================
# 🔹 [EXPLORER_WALLET] – API-Route
@app.route("/api/explorer_wallet", methods=["POST"])
def api_explorer_wallet():
    try:
        payload = request.get_json(force=True)
        addresses = payload.get("addresses", [])

        # ----------------------------
        # 🔒 Wallet Address Limit
        # ----------------------------
        MAX_ADDRESSES = get_explorer_addresses_max()

        if not isinstance(addresses, list):
            raise ValueError("Ungültiges Adressformat")

        if len(addresses) > MAX_ADDRESSES:
            raise ValueError(
                f"Maximal {MAX_ADDRESSES} Adressen erlaubt"
            )

        client = get_electrumx_client()

        # Event-Loop Handling
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            data = loop.run_until_complete(
                get_wallet_overview(client, addresses)
            )
        else:
            data = asyncio.run(
                get_wallet_overview(client, addresses)
            )

        return jsonify({
            "status": "ok",
            "data": data
        })

    except ValueError as e:
        return jsonify({
            "status": "error",
            "error": str(e)
        }), 400

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({
            "status": "error",
            "error": str(e)
        }), 500



## ================================================================================================================================================================ ##
## ================================================================================================================================================================ ##
## ================================================================================================================================================================ ##



# =====================================
# 🔹 TREASURIES_COMPANIES - DATENLOADER
# =====================================
def load_treasuries_companies_data():
    """
    Lädt IMMER die produktive Companies-Datei aus der RAM-Disk.
    Keine Umschaltung, keine Versionierung hier.
    """

    path = os.path.join(
        TREASURIES_BASE_PATH,
        TREASURIES_COMPANIES_FILENAME
    )

    if not os.path.exists(path):
        raise FileNotFoundError(
            "Treasury-Companies-Datei nicht gefunden (RAM-Disk)"
        )

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    return data


# ===================================
# 🔹 TREASURIES_COMPANIES - API_ROUTE
# ===================================
@app.route("/api/treasuries_companies")
def api_treasuries_companies():

    # -------------------------------------------------
    # 🟥 1) Redis HIT → sofort antworten
    # -------------------------------------------------
    try:
        cached = r.get(TREASURIES_COMPANIES_RESPONSE_CACHE_KEY)
        if cached:
            return Response(
                cached,
                mimetype="application/json"
            )
    except Exception:
        # Redis darf die API nie blockieren
        pass

    # -------------------------------------------------
    # 🟩 2) Redis MISS → RAM-Disk lesen
    # -------------------------------------------------
    try:
        data = load_treasuries_companies_data()

        response_obj = {
            "status": "ok",
            "data": data,
            "meta": {
                "file": TREASURIES_COMPANIES_FILENAME,
                "count": len(data)
            }
        }

        response_json = json.dumps(response_obj)

        # -------------------------------------------------
        # 💾 Cache speichern (kein TTL, bewusst)
        # -------------------------------------------------
        try:
            r.set(
                TREASURIES_COMPANIES_RESPONSE_CACHE_KEY,
                response_json
            )
        except Exception:
            pass

        return Response(
            response_json,
            mimetype="application/json"
        )

    except Exception as e:
        return jsonify({
            "status": "error",
            "error": str(e)
        }), 500



## ================================================================================================================================================================ ##



# ========================================
# 🔹 TREASURIES_INSTITUTIONS - DATENLOADER
# ========================================
def load_treasuries_institutions_data():
    """
    Lädt IMMER die produktive Institutions-Datei aus der RAM-Disk.
    Keine Active-Files, keine Versionierung hier.
    """

    path = os.path.join(
        TREASURIES_BASE_PATH,
        TREASURIES_INSTITUTIONS_FILENAME
    )

    if not os.path.exists(path):
        raise FileNotFoundError(
            "Institutions-Datei nicht gefunden (RAM-Disk)"
        )

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    return data


# ======================================
# 🔹 TREASURIES_INSTITUTIONS - API_ROUTE
# ======================================
@app.route("/api/treasuries_institutions")
def api_treasuries_institutions():

    # -------------------------------------------------
    # 🟥 1) Redis HIT → sofort antworten
    # -------------------------------------------------
    try:
        cached = r.get(TREASURIES_INSTITUTIONS_RESPONSE_CACHE_KEY)
        if cached:
            return Response(
                cached,
                mimetype="application/json"
            )
    except Exception:
        pass

    # -------------------------------------------------
    # 🟩 2) Redis MISS → RAM-Disk lesen
    # -------------------------------------------------
    try:
        data = load_treasuries_institutions_data()

        response_obj = {
            "status": "ok",
            "data": data,
            "meta": {
                "file": TREASURIES_INSTITUTIONS_FILENAME,
                "count": len(data)
            }
        }

        response_json = json.dumps(response_obj)

        # -------------------------------------------------
        # 💾 Cache speichern (kein TTL, bewusst)
        # -------------------------------------------------
        try:
            r.set(
                TREASURIES_INSTITUTIONS_RESPONSE_CACHE_KEY,
                response_json
            )
        except Exception:
            pass

        return Response(
            response_json,
            mimetype="application/json"
        )

    except Exception as e:
        return jsonify({
            "status": "error",
            "error": str(e)
        }), 500



## ================================================================================================================================================================ ##



# =====================================
# 🔹 TREASURIES_COUNTRIES - DATENLOADER
# =====================================
def load_treasuries_countries_data():
    """
    Lädt IMMER die produktive Countries-Datei aus der RAM-Disk.
    Keine Active-Files, keine Versionierung hier.
    """

    path = os.path.join(
        TREASURIES_BASE_PATH,
        TREASURIES_COUNTRIES_FILENAME
    )

    if not os.path.exists(path):
        raise FileNotFoundError(
            "Treasury-Countries-Datei nicht gefunden (RAM-Disk)"
        )

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    return data


# ===================================
# 🔹 TREASURIES_COUNTRIES - API_ROUTE
# ===================================
@app.route("/api/treasuries_countries")
def api_treasuries_countries():

    # -------------------------------------------------
    # 🟥 1) Redis HIT → sofort antworten
    # -------------------------------------------------
    try:
        cached = r.get(TREASURIES_COUNTRIES_RESPONSE_CACHE_KEY)
        if cached:
            return Response(
                cached,
                mimetype="application/json"
            )
    except Exception:
        # Redis darf API niemals blockieren
        pass

    # -------------------------------------------------
    # 🟩 2) Redis MISS → RAM-Disk lesen
    # -------------------------------------------------
    try:
        data = load_treasuries_countries_data()

        response_obj = {
            "status": "ok",
            "data": data,
            "meta": {
                "file": TREASURIES_COUNTRIES_FILENAME,
                "count": len(data)
            }
        }

        response_json = json.dumps(response_obj)

        # -------------------------------------------------
        # 💾 Cache speichern (kein TTL, bewusst)
        # -------------------------------------------------
        try:
            r.set(
                TREASURIES_COUNTRIES_RESPONSE_CACHE_KEY,
                response_json
            )
        except Exception:
            pass

        return Response(
            response_json,
            mimetype="application/json"
        )

    except Exception as e:
        return jsonify({
            "status": "error",
            "error": str(e)
        }), 500



## ================================================================================================================================================================ ##
## ================================================================================================================================================================ ##
## ================================================================================================================================================================ ##



# ================================================================
# MARKET_CAP_COINS – Top Kryptowährungen nach Market Cap (Coins)
# Server-side Refresh Loop + Single Redis Cache + Distributed Lock
# ================================================================

# ===================
# 🔹 API Fetch Helper
def _fetch_market_cap_from_api():
    """Holt Top Coins nach Market Cap von CoinGecko."""
    pid = os.getpid()
    print(f"[Worker {pid}] 🌍 Hole MARKET_CAP Daten von CoinGecko …")

    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 20,
        "page": 1,
        "sparkline": "false",
        "price_change_percentage": "24h"
    }

    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()

    data = resp.json()
    print(f"[Worker {pid}] ✅ MARKET_CAP API erfolgreich ({len(data)} Einträge).")
    return data


# ==========================================================
# 🔹 Hintergrund-Refresh Loop (alle X Stunden, client-unabhängig)
def market_cap_coins_refresh_loop():
    """
    Sturer Server-Loop:
    - refresht MARKET_CAP_COINS alle X Stunden
    - genau ein Worker gewinnt via Redis-Lock
    """
    while True:
        try:
            if not r:
                time.sleep(60)
                continue

            got_lock = r.set(
                MARKET_CAP_COINS_LOCK_KEY,
                "1",
                nx=True,
                ex=MARKET_CAP_COINS_LOCK_TTL
            )

            if got_lock:
                pid = os.getpid()
                print(f"[Worker {pid}] ⏰ MARKET_CAP_COINS scheduled refresh")

                data = _fetch_market_cap_from_api()

                r.set(
                    MARKET_CAP_COINS_CACHE_KEY,
                    json.dumps(data),
                    # TTL nur als Failsafe, nicht als Steuerung
                    ex=MARKET_CAP_COINS_REFRESH_INTERVAL * 2
                )

                print(f"[Worker {pid}] 🟢 MARKET_CAP_COINS Cache aktualisiert")

        except Exception as e:
            print(f"[MARKET_CAP_COINS][LOOP] ❌ {e}")

        finally:
            try:
                r.delete(MARKET_CAP_COINS_LOCK_KEY)
            except Exception:
                pass

        # ⏳ schlafen bis zum nächsten festen Intervall
        time.sleep(MARKET_CAP_COINS_REFRESH_INTERVAL)


# ==========================================================
# 🔹 Loop beim App-Start genau EINMAL starten
def start_market_cap_coins_loop():
    t = threading.Thread(
        target=market_cap_coins_refresh_loop,
        daemon=True
    )
    t.start()
    print("[INIT] MARKET_CAP_COINS refresh loop gestartet")


# ⚠️ WICHTIG:
# Diese Funktion genau EINMAL beim App-Start aufrufen
start_market_cap_coins_loop()


# ==========================================================
# 🔹 Read-only Zugriff für API (kein Refresh, kein Lock)
def get_market_cap_coin_data():
    if not r:
        return []

    cached = r.get(MARKET_CAP_COINS_CACHE_KEY)
    if not cached:
        return []

    try:
        return json.loads(cached)
    except Exception:
        return []


# ============
# 🔹 API Endpoint
@app.route('/api/market_cap_coins')
def api_market_cap_coins():
    try:
        data = get_market_cap_coin_data()
        return jsonify(data)
    except Exception as e:
        print(f"[MARKET_CAP API] ❌ Fehler im Endpoint: {e}")
        return jsonify([]), 500



## ================================================================================================================================================================ ##



# ================================================================
# 🔹 MARKET_CAP - COMPANIES (+ BTC aus Redis)   -- REDIS CACHED --
# ================================================================

# ==========================================================
# 🔹 BTC aus MARKET_CAP_COINS Redis-Cache lesen (READ-ONLY)
def get_btc_from_market_cap_coins_cache():
    try:
        if not r:
            return None

        cached = r.get(MARKET_CAP_COINS_CACHE_KEY)
        if not cached:
            return None

        coins = json.loads(cached)

        for c in coins:
            if c.get("symbol", "").lower() == "btc":
                return {
                    "symbol": "BTC",
                    "name": "Bitcoin",
                    "sector": "CRYPTO",
                    "market_cap": int(c.get("market_cap", 0)),
                    "__is_btc": True
                }

        return None

    except Exception as e:
        print(f"[BTC CACHE] ❌ Fehler: {e}")
        return None


# ====================================
# 🔹 Companies von Alpha Vantage holen
def _fetch_market_cap_companies_from_api():

    pid = os.getpid()
    print(f"[Worker {pid}] 🌍 Hole MARKET_CAP_COMPANIES von Alpha Vantage …")

    # 🔑 API-Key zur Laufzeit laden (ENV)
    MARKET_CAP_COMPANIES_API_KEY = os.getenv("MARKET_CAP_COMPANIES_API_KEY")

    if not MARKET_CAP_COMPANIES_API_KEY:
        raise RuntimeError("MARKET_CAP_COMPANIES_API_KEY not configured")

    symbols = [
        "NVDA", "AAPL", "GOOG", "MSFT", "AMZN",
        "META", "TSLA", "AVGO", "TSM", "BRK-B",
        "LLY", "WMT", "JPM", "TCEHY", "V",
        "ORCL", "MA", "XOM", "JNJ", "UNH"
    ]

    result = []

    for symbol in symbols:
        try:
            resp = requests.get(
                "https://www.alphavantage.co/query",
                params={
                    "function": "OVERVIEW",
                    "symbol": symbol,
                    "apikey": MARKET_CAP_COMPANIES_API_KEY
                },
                timeout=10
            )
            resp.raise_for_status()
            data = resp.json()

            # ⛔ Rate-Limit erkannt → Abbruch
            if "Note" in data or "Information" in data:
                print(f"[Worker {pid}] ⛔ Rate-Limit erreicht – Abbruch")
                break

            if "MarketCapitalization" in data:
                result.append({
                    "symbol": symbol,
                    "name": data.get("Name"),
                    "sector": data.get("Sector"),
                    "market_cap": int(data["MarketCapitalization"])
                })

            # Alpha Vantage: max. 5 Calls / Minute
            time.sleep(65)

        except Exception as e:
            print(f"[Worker {pid}] ❌ Fehler bei {symbol}: {e}")

    print(f"[Worker {pid}] ✅ Companies geladen ({len(result)} Einträge).")
    return result


# ======================================================
# 🔹 Refresh-Logik (ASYNC ONLY!)
def _refresh_market_cap_companies():
    pid = os.getpid()
    print(f"[Worker {pid}] 🔄 MARKET_CAP_COMPANIES Refresh gestartet")

    companies = _fetch_market_cap_companies_from_api()
    btc = get_btc_from_market_cap_coins_cache()

    if btc and "BTC" not in {c.get("symbol", "").upper() for c in companies}:
        companies.append(btc)

    # ❌ Sicherheits-Gate: keine kaputten Builds
    if len(companies) < 10:
        print(f"[Worker {pid}] ❌ Refresh verworfen – zu wenige Daten ({len(companies)})")
        return

    companies.sort(key=lambda x: int(x.get("market_cap", 0)), reverse=True)
    companies = companies[:20]

    try:
        pipe = r.pipeline()

        # 🔍 Aktuellen NOW lesen (für Stale-Handling)
        cache_now = r.get(MARKET_CAP_COMPANIES_CACHE_NOW)

        if cache_now:
            # 🟡 Normalfall: altes NOW → OLD
            pipe.set(
                MARKET_CAP_COMPANIES_CACHE_OLD,
                cache_now,
                ex=MARKET_CAP_COMPANIES_REFRESH_INTERVAL * 2
            )
            old_source = "from_now"
        else:
            # 🟡 Initial-Build: OLD = NOW (neu)
            pipe.set(
                MARKET_CAP_COMPANIES_CACHE_OLD,
                json.dumps(companies),
                ex=MARKET_CAP_COMPANIES_REFRESH_INTERVAL * 2
            )
            old_source = "initial"

        # 🟢 Neues NOW setzen
        pipe.set(
            MARKET_CAP_COMPANIES_CACHE_NOW,
            json.dumps(companies),
            ex=MARKET_CAP_COMPANIES_REFRESH_INTERVAL
        )

        pipe.execute()

        print(
            f"[Worker {pid}] 🟢 MARKET_CAP_COMPANIES Refresh abgeschlossen "
            f"(NOW={len(companies)}, OLD={old_source})"
        )

    except Exception as e:
        print(f"[Worker {pid}] ❌ Redis-Fehler im Refresh: {e}")


# ======================================================
# 🔹 Async-Trigger mit Lock
def _trigger_market_cap_companies_refresh_async():
    if not r:
        return

    # ⛔ Cooldown aktiv? → kein neuer Refresh
    if r.get(MARKET_CAP_COMPANIES_REFRESH_COOLDOWN):
        return

    got_lock = r.set(
        MARKET_CAP_COMPANIES_LOCK_KEY,
        "1",
        nx=True,
        ex=MARKET_CAP_COMPANIES_LOCK_TTL
    )

    if not got_lock:
        return

    # 🧊 Cooldown setzen (sobald wir wirklich refreshen)
    r.set(
        MARKET_CAP_COMPANIES_REFRESH_COOLDOWN,
        "1",
        ex=MARKET_CAP_COMPANIES_REFRESH_COOLDOWN_TTL
    )


    def _run():
        try:
            _refresh_market_cap_companies()
        finally:
            r.delete(MARKET_CAP_COMPANIES_LOCK_KEY)

    threading.Thread(target=_run, daemon=True).start()


# ===================================================
# 🔹 Zentrale Cache-Logik (NIE BLOCKIEREND!)
def get_market_cap_companies_data():
    pid = os.getpid()

    if not r:
        return []

    cached_now = r.get(MARKET_CAP_COMPANIES_CACHE_NOW)
    if cached_now:
        print(f"[Worker {pid}] 📦 CACHE_NOW ausgeliefert")
        _trigger_market_cap_companies_refresh_async()
        return json.loads(cached_now)

    cached_old = r.get(MARKET_CAP_COMPANIES_CACHE_OLD)
    if cached_old:
        print(f"[Worker {pid}] 🟡 CACHE_OLD ausgeliefert")
        _trigger_market_cap_companies_refresh_async()
        return json.loads(cached_old)

    # ❗ KEIN Initial-Build im Request!
    print(f"[Worker {pid}] 🔴 Kein Cache – Async-Build gestartet")
    _trigger_market_cap_companies_refresh_async()
    return []


# ============
# 🔹 API-ROUTE
@app.route("/api/companies")
def api_market_cap_companies():
    try:
        data = get_market_cap_companies_data()
        return jsonify(data)
    except Exception as e:
        print(f"[MARKET_CAP_COMPANIES API] ❌ Fehler: {e}")
        return jsonify([]), 500



## ================================================================================================================================================================ ##



# ============================================================
# 🔹 MARKET_CAP – CURRENCIES (BTC + Fiat M2, USD-normalisiert)
# ============================================================

# ==========================================================
# 🔹 BTC aus MARKET_CAP_COINS Redis-Cache lesen (READ-ONLY)
def get_btc_currency_from_market_cap_coins_cache():
    try:
        cached = r.get(MARKET_CAP_COINS_CACHE_KEY)
        if not cached:
            return None

        coins = json.loads(cached)

        for c in coins:
            if c.get("symbol", "").lower() == "btc":
                market_cap = int(c.get("market_cap", 0))
                return {
                    "name": "Bitcoin",
                    "symbol": "BTC",
                    "type": "Crypto",
                    "market_cap_native": market_cap,
                    "market_cap_usd": market_cap,
                    "__is_btc": True
                }

        return None

    except Exception as e:
        print(f"[MARKET_CAP_CURRENCIES] BTC Cache Fehler: {e}")
        return None


# =================================
# 🔹 MARKET_CAP_CURRENCIES - LOADER
def load_market_cap_currencies_data():

    fx_path = os.path.join(
        MARKET_CAP_BASE_PATH,
        MARKET_CAP_CURRENCIES_FX_FILENAME
    )

    fiat_path = os.path.join(
        MARKET_CAP_BASE_PATH,
        MARKET_CAP_CURRENCIES_FIAT_FILENAME
    )

    if not os.path.exists(fx_path):
        raise FileNotFoundError("FX-Datei fehlt (RAM-Disk)")

    if not os.path.exists(fiat_path):
        raise FileNotFoundError("Fiat-Datei fehlt (RAM-Disk)")

    with open(fx_path, "r", encoding="utf-8") as f:
        fx_rates = json.load(f)

    with open(fiat_path, "r", encoding="utf-8") as f:
        fiat_m2 = json.load(f)

    return fx_rates, fiat_m2


# ===========================================
# 🔹 AGGREGATOR (USD-normalisiert, PURE VIEW)
def build_market_cap_currencies():
    currencies = []

    # BTC zuerst (read-only)
    btc = get_btc_currency_from_market_cap_coins_cache()
    if btc:
        currencies.append(btc)

    fx_rates, fiat_m2 = load_market_cap_currencies_data()

    for item in fiat_m2:
        symbol = item.get("symbol")
        native = int(item.get("market_cap", 0))
        fx = fx_rates.get(symbol)

        if not symbol or not fx:
            continue

        currencies.append({
            "name": item.get("name", "—"),
            "symbol": symbol,
            "type": item.get("type", "Fiat"),
            "market_cap_native": native,
            "market_cap_usd": int(native * fx)
        })

    currencies.sort(key=lambda x: x["market_cap_usd"], reverse=True)
    return currencies


# ============
# 🔹 API Route
@app.route("/api/market-cap-currencies")
def api_market_cap_currencies():

    # -------------------------------------------------
    # 🟥 1) Redis HIT
    # -------------------------------------------------
    try:
        cached = r.get(MARKET_CAP_CURRENCIES_RESPONSE_CACHE_KEY)
        if cached:
            return Response(cached, mimetype="application/json")
    except Exception:
        pass

    # -------------------------------------------------
    # 🟩 2) Redis MISS → RAM-Disk + Aggregation
    # -------------------------------------------------
    try:
        data = build_market_cap_currencies()

        response_obj = {
            "status": "ok",
            "data": data,
            "meta": {
                "fx_file": MARKET_CAP_CURRENCIES_FX_FILENAME,
                "fiat_file": MARKET_CAP_CURRENCIES_FIAT_FILENAME,
                "count": len(data)
            }
        }

        response_json = json.dumps(response_obj)

        try:
            r.set(
                MARKET_CAP_CURRENCIES_RESPONSE_CACHE_KEY,
                response_json,
                ex=MARKET_CAP_CURRENCIES_RESPONSE_TTL
            )

        except Exception:
            pass

        return Response(response_json, mimetype="application/json")

    except Exception as e:
        return jsonify({
            "status": "error",
            "error": str(e)
        }), 500



## ================================================================================================================================================================ ##



# ==========================================================
# 🔹 BTC aus MARKET_CAP_COINS Redis-Cache lesen (READ-ONLY)
# ==========================================================
def get_btc_from_market_cap_coins_cache():
    """
    Liest Bitcoin-Marktkapitalisierung aus dem
    MARKET_CAP_COINS Redis-Cache.

    Quelle:
    - wird von der Coins-API befüllt
    - Single Source of Truth für BTC Market Cap

    Rückgabe:
    - dict mit einheitlicher Struktur
    - oder None bei Fehler / Cache-Miss
    """
    try:
        # Redis nicht verfügbar
        if not r:
            return None

        cached = r.get(MARKET_CAP_COINS_CACHE_KEY)
        if not cached:
            return None

        coins = json.loads(cached)

        for c in coins:
            if c.get("symbol", "").lower() == "btc":
                return {
                    "symbol": "BTC",
                    "name": "Bitcoin",
                    "sector": "CRYPTO",
                    "market_cap": int(c.get("market_cap", 0)),
                    "__is_btc": True
                }

        return None

    except Exception as e:
        print(f"[BTC CACHE] ❌ Fehler beim Lesen von BTC aus Redis: {e}")
        return None


# =======================================
# 🔹 MARKET_CAP_COMMODITIES - DATENLOADER
# =======================================
def load_market_cap_commodities_data():
    """
    Lädt Commodities aus RAM-Disk
    + ersetzt/ergänzt BTC dynamisch aus Redis.
    """

    path = os.path.join(
        MARKET_CAP_BASE_PATH,
        MARKET_CAP_COMMODITIES_FILENAME
    )

    if not os.path.exists(path):
        raise FileNotFoundError(
            "Market-Cap-Commodities-Datei nicht gefunden (RAM-Disk)"
        )

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # -------------------------------------------------
    # 🔹 BTC aus Redis einfügen (falls vorhanden)
    # -------------------------------------------------
    btc = get_btc_from_market_cap_coins_cache()
    if btc:
        # evtl. vorhandenen BTC aus JSON entfernen
        data = [x for x in data if x.get("symbol") != "BTC"]
        data.insert(0, btc)

    return data


# =====================================
# 🔹 MARKET_CAP_COMMODITIES - API-ROUTE
# =====================================
@app.route("/api/market_cap_commodities")
def api_market_cap_commodities():

    # -------------------------------------------------
    # 🟥 1) Redis HIT → sofort antworten
    # -------------------------------------------------
    try:
        cached = r.get(MARKET_CAP_COMMODITIES_RESPONSE_CACHE_KEY)
        if cached:
            return Response(
                cached,
                mimetype="application/json"
            )
    except Exception:
        pass

    # -------------------------------------------------
    # 🟩 2) Redis MISS → RAM-Disk + BTC bauen
    # -------------------------------------------------
    try:
        data = load_market_cap_commodities_data()

        response_obj = {
            "status": "ok",
            "data": data,
            "meta": {
                "file": MARKET_CAP_COMMODITIES_FILENAME,
                "count": len(data)
            }
        }

        response_json = json.dumps(response_obj)

        try:
            r.set(
                MARKET_CAP_COMMODITIES_RESPONSE_CACHE_KEY,
                response_json,
                ex=MARKET_CAP_COMMODITIES_RESPONSE_TTL
            )
        except Exception:
            pass

        return Response(
            response_json,
            mimetype="application/json"
        )

    except Exception as e:
        return jsonify({
            "status": "error",
            "error": str(e)
        }), 500





## ================================================================================================================================================================ ##
## ================================================================================================================================================================ ##
## ================================================================================================================================================================ ##
## ================================================================================================================================================================ ##
## ================================================================================================================================================================ ##
## ================================================================================================================================================================ ##
## ================================================================================================================================================================ ##
## ================================================================================================================================================================ ##
## ================================================================================================================================================================ ##
## ================================================================================================================================================================ ##





# =======================================================================
# 🔗 BLOCKCHAIN_WORKER                          --RAM-ONLY--  --NODE II--
# =======================================================================

# ===========================
# 🔸 API: BLOCKCHAIN (static)
@app.route("/api/blockchain", methods=["GET", "HEAD"])
def api_blockchain_static():

    # ==========================
    # 🤖 Crawler / Direct Access
    # Google & Co. rufen per GET/HEAD ohne Kontext ab
    if request.method in ("GET", "HEAD") and not request.headers.get("X-Requested-With"):
        resp = Response(status=204)  # No Content
        resp.headers["X-Robots-Tag"] = "noindex, nofollow"
        return resp

    # =================
    # 🔧 Real API Logic
    raw = r.get(BLOCKCHAIN_STATIC_KEY)
    if not raw:
        return Response(
            json.dumps({"error": "Blockchain static data not available"}),
            status=503,
            mimetype="application/json"
        )

    return Response(
        raw.decode() if isinstance(raw, bytes) else raw,
        mimetype="application/json"
    )


# ============================
# 🔸 API: BLOCKCHAIN (dynamic)
@app.route("/api/blockchain2", methods=["GET"])
def api_blockchain_dynamic():
    raw = r.get(BLOCKCHAIN_DYNAMIC_CACHE)
    if not raw:
        return Response(
            json.dumps({"error": "Blockchain dynamic data not available"}),
            status=503,
            mimetype="application/json"
        )

    return Response(
        raw.decode() if isinstance(raw, bytes) else raw,
        mimetype="application/json"
    )



## ================================================================================================================================================================ ##
## ================================================================================================================================================================ ##



# =====================================================================
# 🔗 MEMPOOL_WORKER                           --RAM-ONLY--  --NODE II--
# =====================================================================

# ========================
# 🔸 API: MEMPOOL (static)
@app.route("/api/mempool", methods=["GET"])
def api_mempool():
    raw = r.get(MEMPOOL_STATIC_KEY)

    if not raw:
        return Response(
            json.dumps({"error": "mempool static data not available"}),
            status=503,
            mimetype="application/json"
        )

    return Response(
        raw.decode() if isinstance(raw, bytes) else raw,
        mimetype="application/json"
    )

# =========================
# 🔸 API: MEMPOOL (dynamic)
@app.route("/api/mempool2", methods=["GET"])
def api_mempool2():
    raw = r.get(MEMPOOL_DYNAMIC_CACHE)

    if not raw:
        return Response(
            json.dumps({"error": "mempool data not available"}),
            status=503,
            mimetype="application/json"
        )

    return Response(
        raw.decode() if isinstance(raw, bytes) else raw,
        mimetype="application/json"
    )



## ================================================================================================================================================================ ##
## ================================================================================================================================================================ ##



# ===================================================================
# 🔗 NETWORK_WORKER                         --RAM-ONLY--  --NODE II--
# ===================================================================

from workers.node2.network.network_worker import start_network_worker
start_network_worker()

# =========================
# 🔸 API: NETWORK (dynamic)
@app.route("/api/network2", methods=["GET"])
def api_network_dynamic():
    data = r.get(NETWORK_DYNAMIC_CACHE)

    if not data:
        return Response(
            json.dumps({"error": "network data not available"}),
            status=503,
            mimetype="application/json"
        )

    if isinstance(data, bytes):
        data = data.decode()

    return Response(data, mimetype="application/json")





## ================================================================================================================================================================ ##
## ================================================================================================================================================================ ##
## ================================================================================================================================================================ ##
## ================================================================================================================================================================ ##
## ================================================================================================================================================================ ##
## ================================================================================================================================================================ ##
## ================================================================================================================================================================ ##
## ================================================================================================================================================================ ##
## ================================================================================================================================================================ ##
## ================================================================================================================================================================ ##





# ====================================================================
# 🔹 BTC_TOP_WORKER                         --RAM-ONLY--  --NODE III--
# ====================================================================

# =========================
# 🔸 API: BTC_TOP (dynamic)
@app.route("/api/3_BTC_TOP", methods=["GET"])
def api_3_btc_top():
    try:
        raw = r.get(BTC_TOP_TXS_KEY)
        mempool_top10 = json.loads(raw).get("top10", []) if raw else []

        try:
            with open(BTC_TOP_50_EVER_PATH, "r") as f:
                top50_ever = json.load(f)
        except Exception:
            top50_ever = []

        return Response(
            json.dumps({
                "mempool_top10": mempool_top10,
                "top50_ever": top50_ever
            }),
            mimetype="application/json"
        )

    except Exception as e:
        return Response(
            json.dumps({"error": str(e)}),
            status=500,
            mimetype="application/json"
        )





## ================================================================================================================================================================ ##
## ================================================================================================================================================================ ##
## ================================================================================================================================================================ ##
## ================================================================================================================================================================ ##
## ================================================================================================================================================================ ##
## ================================================================================================================================================================ ##
## ================================================================================================================================================================ ##
## ================================================================================================================================================================ ##
## ================================================================================================================================================================ ##
## ================================================================================================================================================================ ##





@app.route("/api/dashboard/core", methods=["GET"])
def api_dashboard_core():
    now = int(time.time())

    result = {
        "blockchain": None,
        "blockchain_static": None,
        "mempool": None,
        "network": None,
        "errors": {},
        "generated_at": now
    }

    # ----------------------------
    # 🔗 Blockchain (dynamic)
    # ----------------------------
    try:
        raw = r.get(BLOCKCHAIN_DYNAMIC_CACHE)
        if raw:
            result["blockchain"] = json.loads(
                raw.decode() if isinstance(raw, bytes) else raw
            )
        else:
            result["errors"]["blockchain"] = "no_data"
    except Exception as e:
        result["errors"]["blockchain"] = str(e)

    # ----------------------------
    # ⛓ Blockchain STATIC (1x/Tag aus Worker)
    # ----------------------------
    try:
        raw = r.get(BLOCKCHAIN_STATIC_KEY)
        if raw:
            result["blockchain_static"] = json.loads(
                raw.decode() if isinstance(raw, bytes) else raw
            )
        else:
            result["errors"]["blockchain_static"] = "no_data"
    except Exception as e:
        result["errors"]["blockchain_static"] = str(e)

    # ----------------------------
    # 🔗 Mempool (dynamic)
    # ----------------------------
    try:
        raw = r.get(MEMPOOL_DYNAMIC_CACHE)
        if raw:
            result["mempool"] = json.loads(
                raw.decode() if isinstance(raw, bytes) else raw
            )
        else:
            result["errors"]["mempool"] = "no_data"
    except Exception as e:
        result["errors"]["mempool"] = str(e)

    # ----------------------------
    # 🔗 Network (dynamic)
    # ----------------------------
    try:
        raw = r.get(NETWORK_DYNAMIC_CACHE)
        if raw:
            result["network"] = json.loads(
                raw.decode() if isinstance(raw, bytes) else raw
            )
        else:
            result["errors"]["network"] = "no_data"
    except Exception as e:
        result["errors"]["network"] = str(e)




    # ----------------------------
    # 🟢 System Health (META)
    # ----------------------------
    try:
        raw = r.get(HOME_META_CACHE)
        if raw:
            result["system_health"] = json.loads(
                raw.decode() if isinstance(raw, bytes) else raw
            )
        else:
            result["errors"]["system_health"] = "no_data"
    except Exception as e:
        result["errors"]["system_health"] = str(e)




    # ----------------------------
    # 🟣 BTC TOP (Top10 + Top50 Ever aus Redis)
    # ----------------------------
    try:
        raw = r.get(BTC_TOP_TXS_KEY)
        if raw:
            result["btc_top"] = json.loads(
                raw.decode() if isinstance(raw, bytes) else raw
            )
        else:
            result["errors"]["btc_top"] = "no_data"
    except Exception as e:
        result["errors"]["btc_top"] = str(e)

    # ----------------------------
    # 🔵 BTC VOL (aggregated volume aus Redis)
    # ----------------------------
    try:
        raw = r.get(BTC_VOL_DYNAMIC_CACHE)
        if raw:
            result["btc_vol"] = json.loads(
                raw.decode() if isinstance(raw, bytes) else raw
            )
        else:
            result["errors"]["btc_vol"] = "no_data"
    except Exception as e:
        result["errors"]["btc_vol"] = str(e)

    return Response(
        json.dumps(result),
        mimetype="application/json"
    )





## ================================================================================================================================================================ ##
## ================================================================================================================================================================ ##
## ================================================================================================================================================================ ##
## ================================================================================================================================================================ ##
## ================================================================================================================================================================ ##





BASE_DIR = Path(__file__).resolve().parent
TEMPLATE_DIR = BASE_DIR / "templates"

# ====================
# 🗺️ SEO – XML Sitemap
# ====================
@app.route("/sitemap.xml")
def sitemap():
    return send_from_directory(BASE_DIR, "sitemap.xml", mimetype="application/xml")


## ================================================================================================================================================================ ##


# =======================
# 🧭 SPA Fallback Routing
# =======================
@app.route("/", defaults={"path": ""})
@app.route("/<path:path>")
def spa_fallback(path):

    # echte statische Assets direkt ausliefern
    if path.startswith("static/"):
        return send_from_directory(BASE_DIR, path)

    # alles mit Dateiendung bewusst 404en
    if "." in path:
        abort(404)

    # alle App-Routen → SPA
    return send_from_directory(TEMPLATE_DIR, "index.html")

## ================================================================================================================================================================ ##


# ===============================
# 🚀 App starten & Worker starten
# ===============================
if __name__ == "__main__":
    app.run(debug=False, host='127.0.0.1', port=5000)


