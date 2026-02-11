# ==================================================
# 🔥 TXID SEO EXTRACTOR WORKER
# Seeds TX Explorer Pages + Sitemap Flywheel
# ==================================================


import sys
import os

BASE_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../")
)

if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)



import time
import json
import redis
from xml.etree.ElementTree import Element, SubElement, ElementTree

from core.redis_keys import (
    BTC_TX_AMOUNT_HISTORY_KEY,
    SEO_TXID_INDEXED_SET,
    SEO_TXID_SITEMAP_PATH,
    SEO_TXID_EXTRACT_INTERVAL,
    SEO_TXID_BATCH_LIMIT,
)

# ============================================
# 🔧 Redis
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
# 🧱 Sitemap Builder
# ============================================

def build_sitemap(txids):

    urlset = Element(
        "urlset",
        xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"
    )

    for txid in txids:

        url = SubElement(urlset, "url")

        loc = SubElement(url, "loc")
        loc.text = f"https://bitcoin-dashboard.net/explorer/tx/{txid}"

        changefreq = SubElement(url, "changefreq")
        changefreq.text = "weekly"

        priority = SubElement(url, "priority")
        priority.text = "0.8"

    tree = ElementTree(urlset)
    tree.write(
        SEO_TXID_SITEMAP_PATH,
        encoding="utf-8",
        xml_declaration=True
    )

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

    # Batch Limit
    new_txids = new_txids[:SEO_TXID_BATCH_LIMIT]

    # Update Indexed Set
    if new_txids:
        r.sadd(SEO_TXID_INDEXED_SET, *new_txids)

    # Rebuild Sitemap
    all_indexed = r.smembers(SEO_TXID_INDEXED_SET)
    build_sitemap(all_indexed)

    print(
        f"[TXID SEO] Added {len(new_txids)} new TXIDs "
        f"(Total: {len(all_indexed)})"
    )

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
