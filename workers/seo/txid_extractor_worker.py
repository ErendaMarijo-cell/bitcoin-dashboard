# ==================================================
# 🔥 TXID SEO EXTRACTOR WORKER
# Sharded Sitemap Builder + Hourly Rebuild Policy
# ==================================================

import glob
import sys
import os
import time
import json
import redis

# ============================================
# 🔧 Project Root
# ============================================

BASE_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../")
)

if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

# ============================================
# 🔑 Redis Keys / Config
# ============================================

from core.redis_keys import (
    BTC_TX_AMOUNT_HISTORY_KEY,
    SEO_TXID_INDEXED_SET,
    SEO_TXID_EXTRACT_INTERVAL,
    SEO_TXID_BATCH_LIMIT,
    SEO_TXID_SITEMAP_DIRTY_KEY,
    SEO_TXID_SITEMAP_LAST_BUILD_KEY,
    SITEMAP_REBUILD_INTERVAL_SEC,
)

# ============================================
# 🔧 Redis Client
# ============================================

r = redis.Redis(
    host="localhost",
    port=6379,
    db=0,
    decode_responses=True
)

# ============================================
# 📥 Load TX Buckets
# ============================================

def load_all_txids():

    raw = r.get(BTC_TX_AMOUNT_HISTORY_KEY)
    if not raw:
        return []

    try:
        data = json.loads(raw)
    except Exception:
        return []

    txids = set()

    for bucket, entries in data.items():

        if not isinstance(entries, list):
            continue

        for tx in entries:
            txid = tx.get("txid")
            if txid:
                txids.add(txid)

    return list(txids)

# ============================================
# 🧱 Sharded Sitemap Builder
# ============================================

SITEMAP_MAX_URLS = 50000
SITEMAP_SHARD_PAD = 6

TXID_URL_PREFIX = "https://bitcoin-dashboard.net/explorer/tx/"
SITEMAP_BASE_URL = "https://bitcoin-dashboard.net/static/sitemaps/txids/"

def _write_urlset_header(fp):
    fp.write('<?xml version="1.0" encoding="UTF-8"?>\n')
    fp.write('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n')

def _write_urlset_footer(fp):
    fp.write("</urlset>\n")

def _write_sitemapindex(path, shard_files):

    tmp = path + ".tmp"

    with open(tmp, "w", encoding="utf-8") as fp:
        fp.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        fp.write('<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n')

        for name in shard_files:
            fp.write("  <sitemap>\n")
            fp.write(f"    <loc>{SITEMAP_BASE_URL}{name}</loc>\n")
            fp.write("  </sitemap>\n")

        fp.write("</sitemapindex>\n")
        fp.flush()
        os.fsync(fp.fileno())

    os.replace(tmp, path)

def rebuild_txid_sitemaps_sharded():

    sitemap_dir = os.path.join(BASE_DIR, "static", "sitemaps", "txids")
    os.makedirs(sitemap_dir, exist_ok=True)

    def shard_name(i: int) -> str:
        return f"sitemap_txids_{i:0{SITEMAP_SHARD_PAD}d}.xml"

    # Cleanup old shards
    for old in glob.glob(os.path.join(sitemap_dir, "sitemap_txids_*.xml")):
        if not old.endswith("index.xml"):
            os.remove(old)

    shard_files = []
    shard_idx = 1
    urls_in_shard = 0
    total_written = 0

    current_path = os.path.join(sitemap_dir, shard_name(shard_idx))
    fp = open(current_path, "w", encoding="utf-8")

    _write_urlset_header(fp)
    shard_files.append(shard_name(shard_idx))

    cursor = 0

    try:
        while True:

            cursor, members = r.sscan(
                SEO_TXID_INDEXED_SET,
                cursor=cursor,
                count=5000
            )

            if members:
                for txid in members:

                    if urls_in_shard >= SITEMAP_MAX_URLS:
                        _write_urlset_footer(fp)
                        fp.flush()
                        os.fsync(fp.fileno())
                        fp.close()

                        shard_idx += 1
                        urls_in_shard = 0

                        current_path = os.path.join(
                            sitemap_dir,
                            shard_name(shard_idx)
                        )

                        fp = open(current_path, "w", encoding="utf-8")
                        _write_urlset_header(fp)
                        shard_files.append(shard_name(shard_idx))

                    fp.write("  <url>\n")
                    fp.write(f"    <loc>{TXID_URL_PREFIX}{txid}</loc>\n")
                    fp.write("    <changefreq>weekly</changefreq>\n")
                    fp.write("    <priority>0.8</priority>\n")
                    fp.write("  </url>\n")

                    urls_in_shard += 1
                    total_written += 1

            if cursor == 0:
                break

        _write_urlset_footer(fp)
        fp.flush()
        os.fsync(fp.fileno())
        fp.close()

        # Write shard index
        index_path = os.path.join(
            sitemap_dir,
            "sitemap_txids_index.xml"
        )

        _write_sitemapindex(index_path, shard_files)

        print(
            f"[TXID SEO] Sharded sitemaps rebuilt: "
            f"{len(shard_files)} files, {total_written} urls"
        )

        # 🔥 Set last rebuild timestamp
        r.set(
            SEO_TXID_SITEMAP_LAST_BUILD_KEY,
            str(time.time())
        )

    finally:
        try:
            if not fp.closed:
                fp.close()
        except Exception:
            pass

# ============================================
# 🕒 Rebuild Policy
# ============================================

def should_rebuild_sitemap():

    # Dirty trigger
    dirty = r.get(SEO_TXID_SITEMAP_DIRTY_KEY)
    if dirty:
        print("[TXID SEO] Rebuild trigger → DIRTY FLAG")
        r.delete(SEO_TXID_SITEMAP_DIRTY_KEY)
        return True

    # URL count trigger
    total = r.scard(SEO_TXID_INDEXED_SET)

    if total >= 48000:
        print("[TXID SEO] Rebuild trigger → URL THRESHOLD")
        return True

    # Hourly fallback
    last = r.get(SEO_TXID_SITEMAP_LAST_BUILD_KEY)

    if not last:
        return True

    elapsed = time.time() - float(last)

    if elapsed >= SITEMAP_REBUILD_INTERVAL_SEC:
        print("[TXID SEO] Rebuild trigger → HOURLY")
        return True

    return False


# ============================================
# 🔄 Extract Cycle
# ============================================

def extract_txids_cycle():

    all_txids = load_all_txids()

    if not all_txids:
        print("[TXID SEO] No TXIDs found")
        return

    indexed = r.smembers(SEO_TXID_INDEXED_SET)

    new_txids = [
        txid for txid in all_txids
        if txid not in indexed
    ]

    if not new_txids:
        print("[TXID SEO] No new TXIDs")
        return

    new_txids = new_txids[:SEO_TXID_BATCH_LIMIT]

    r.sadd(SEO_TXID_INDEXED_SET, *new_txids)

    print(
        f"[TXID SEO] Added {len(new_txids)} new TXIDs"
    )

    if should_rebuild_sitemap():
        rebuild_txid_sitemaps_sharded()

# ============================================
# 🔁 Worker Loop
# ============================================

def txid_extractor_worker_loop():

    print("[TXID SEO WORKER] Started")
    time.sleep(2)

    while True:

        loop_start = time.time()

        try:
            extract_txids_cycle()
        except Exception as e:
            print(f"[TXID SEO ERROR] {e}")

        sleep_time = max(
            0,
            SEO_TXID_EXTRACT_INTERVAL - (time.time() - loop_start)
        )

        time.sleep(sleep_time)

# ============================================
# ▶️ ENTRYPOINT
# ============================================

if __name__ == "__main__":
    txid_extractor_worker_loop()
