# ==================================================
# 🔥 TXID SEO EXTRACTOR WORKER (APPEND-ONLY, CONFIRMED-ONLY)
#
# ✅ Reads CONFIRMED TXIDs from JSONL segment files
# ✅ Appends TXID URLs into sharded sitemaps (50,000 URLs/file)
# ✅ Never deletes existing shard files
# ✅ Rebuilds ONLY the root sitemap_index.xml when a NEW shard is created
# ✅ Resume-safe via (segment file + byte offset) + small txid ring to avoid duplicates on crash replay
#
# IMPORTANT:
# - Does NOT use BTC_TX_AMOUNT_HISTORY_KEY.
# - Does NOT scan Redis sets.
# ==================================================

import os
import sys
import time
import json
import glob
from datetime import datetime, timezone
from typing import List, Tuple, Optional, Deque
from collections import deque

import redis

# ============================================
# 🔧 Project Root
# ============================================
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

# ============================================
# 🔑 Redis Keys (optional metadata only)
# ============================================
from core.redis_keys import (
    SEO_TXID_EXTRACT_INTERVAL,
    SEO_TXID_BATCH_LIMIT,
    SEO_TXID_SITEMAP_LAST_BUILD_KEY,
)

# ============================================
# 🔧 Redis Client (optional metadata only)
# ============================================
r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)

# ============================================
# Paths
# ============================================
IN_DIR = "/raid/lightning/seo/txids/confirmed"
SITEMAP_DIR = "/raid/lightning/seo/txids/sitemaps/shards"
ROOT_SITEMAP_INDEX_PATH = os.path.join(BASE_DIR, "static", "sitemaps", "sitemap_txids.xml")
STATE_PATH = "/raid/lightning/seo/txids/progress/txid_extractor_append_state.json"

# ============================================
# Sitemap settings
# ============================================
SITEMAP_MAX_URLS = 50000
SITEMAP_SHARD_PAD = 6

TXID_URL_PREFIX = "https://bitcoin-dashboard.net/explorer/tx/"
SITEMAP_BASE_URL = "https://bitcoin-dashboard.net/static/sitemaps/txids/"  # URL prefix, not filesystem

# ============================================
# Crash-safety / dedupe window
# ============================================
TXID_RING_SIZE = 50000


# ============================================
# Helpers
# ============================================
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def atomic_write_json(path: str, payload: dict) -> None:
    ensure_dir(os.path.dirname(path))
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, separators=(",", ":"), ensure_ascii=False)
    os.replace(tmp, path)


def load_state() -> dict:
    default = {
        "current_file": None,
        "offset": 0,
        "shard_idx": None,
        "urls_in_shard": None,
        "written_total": 0,
        "last_txids": [],
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
        if not isinstance(merged.get("last_txids"), list):
            merged["last_txids"] = []
        return merged
    except Exception:
        atomic_write_json(STATE_PATH, default)
        return default


def save_state(state: dict) -> None:
    state["updated_utc"] = utc_now_iso()
    atomic_write_json(STATE_PATH, state)


def list_segment_files() -> List[str]:
    if not os.path.isdir(IN_DIR):
        return []
    files = [f for f in os.listdir(IN_DIR) if f.startswith("txids_") and f.endswith(".jsonl")]
    files.sort()
    return [os.path.join(IN_DIR, f) for f in files]


def pick_start_file(files: List[str], current_file: Optional[str]) -> Tuple[Optional[str], int]:
    if not files:
        return None, 0
    if not current_file:
        return files[0], 0
    if current_file in files:
        return current_file, 0
    for f in files:
        if os.path.basename(f) > os.path.basename(current_file):
            return f, 0
    return None, 0


def shard_name(i: int) -> str:
    return f"sitemap_txids_{i:0{SITEMAP_SHARD_PAD}d}.xml"


def _write_urlset_header_text() -> str:
    return '<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'


def _write_urlset_footer_text() -> str:
    return "</urlset>\n"


def count_urls_in_shard(path: str) -> int:
    # Only used for LAST shard (<=50k) so OK.
    try:
        with open(path, "r", encoding="utf-8") as f:
            return sum(1 for line in f if "<loc>" in line)
    except FileNotFoundError:
        return 0


def detect_last_shard() -> Tuple[int, int]:
    ensure_dir(SITEMAP_DIR)
    files = sorted(glob.glob(os.path.join(SITEMAP_DIR, "sitemap_txids_*.xml")))
    if not files:
        return 1, 0

    last_path = files[-1]
    base = os.path.basename(last_path)
    try:
        shard_idx = int(base.split("_")[-1].split(".")[0])
    except Exception:
        return 1, 0

    urls = count_urls_in_shard(last_path)
    return shard_idx, urls


def ensure_shard_exists(shard_idx: int) -> str:
    ensure_dir(SITEMAP_DIR)
    path = os.path.join(SITEMAP_DIR, shard_name(shard_idx))
    if not os.path.exists(path):
        with open(path, "w", encoding="utf-8") as f:
            f.write(_write_urlset_header_text())
            f.write(_write_urlset_footer_text())
    return path


def open_shard_for_append(shard_idx: int) -> Tuple[object, int, str]:
    """
    Opens shard file in binary append mode with the file positioned BEFORE </urlset>.
    Returns (fp, urls_in_shard, shard_path)
    """
    shard_path = ensure_shard_exists(shard_idx)

    # Determine count fresh (only for last shard; safe)
    urls_in_shard = count_urls_in_shard(shard_path)

    footer = b"</urlset>"
    with open(shard_path, "rb") as rf:
        data = rf.read()
    pos = data.rfind(footer)
    if pos == -1:
        # Repair: append footer then retry
        with open(shard_path, "ab") as af:
            if not data.endswith(b"\n"):
                af.write(b"\n")
            af.write(b"</urlset>\n")
        with open(shard_path, "rb") as rf:
            data = rf.read()
        pos = data.rfind(footer)
        if pos == -1:
            raise RuntimeError(f"Shard footer not found and could not be repaired: {shard_path}")

    fp = open(shard_path, "r+b")
    fp.seek(pos)
    fp.truncate()               # remove footer
    fp.seek(0, os.SEEK_END)     # move to end for appends
    return fp, urls_in_shard, shard_path


def close_shard(fp) -> None:
    fp.write(_write_urlset_footer_text().encode("utf-8"))
    fp.flush()
    os.fsync(fp.fileno())
    fp.close()


def rebuild_root_sitemap_index() -> None:
    ensure_dir(os.path.dirname(ROOT_SITEMAP_INDEX_PATH))
    shard_files = sorted(
        os.path.basename(p)
        for p in glob.glob(os.path.join(SITEMAP_DIR, "sitemap_txids_*.xml"))
    )

    tmp = ROOT_SITEMAP_INDEX_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fp:
        fp.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        fp.write('<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n')

        fp.write("  <sitemap>\n")
        fp.write("    <loc>https://bitcoin-dashboard.net/static/sitemaps/sitemap_pages.xml</loc>\n")
        fp.write("  </sitemap>\n")

        for name in shard_files:
            fp.write("  <sitemap>\n")
            fp.write(f"    <loc>{SITEMAP_BASE_URL}{name}</loc>\n")
            fp.write("  </sitemap>\n")

        fp.write("</sitemapindex>\n")

    os.replace(tmp, ROOT_SITEMAP_INDEX_PATH)


def write_url_entry(fp, txid: str) -> None:
    entry = (
        "  <url>\n"
        f"    <loc>{TXID_URL_PREFIX}{txid}</loc>\n"
        "    <changefreq>weekly</changefreq>\n"
        "    <priority>0.8</priority>\n"
        "  </url>\n"
    )
    fp.write(entry.encode("utf-8"))


# ============================================
# Core append loop
# ============================================
def txid_extractor_append_loop():
    print("[TXID SEO APPEND] started")
    print(f"[TXID SEO APPEND] IN_DIR={IN_DIR}")
    print(f"[TXID SEO APPEND] SITEMAP_DIR={SITEMAP_DIR}")
    print(f"[TXID SEO APPEND] ROOT_INDEX={ROOT_SITEMAP_INDEX_PATH}")
    print(f"[TXID SEO APPEND] STATE={STATE_PATH}")

    ensure_dir(SITEMAP_DIR)
    state = load_state()

    # init shard info from filesystem
    if not isinstance(state.get("shard_idx"), int) or not isinstance(state.get("urls_in_shard"), int):
        shard_idx, urls_in_shard = detect_last_shard()
        state["shard_idx"] = shard_idx
        state["urls_in_shard"] = urls_in_shard
        save_state(state)

    ring: Deque[str] = deque(state.get("last_txids", []), maxlen=TXID_RING_SIZE)

    # init file state
    files = list_segment_files()
    current_file, _ = pick_start_file(files, state.get("current_file"))
    if not current_file:
        print("[TXID SEO APPEND] No segment files found, waiting...")
        while True:
            time.sleep(5)
            files = list_segment_files()
            current_file, _ = pick_start_file(files, state.get("current_file"))
            if current_file:
                break

    if state.get("current_file") != current_file:
        state["current_file"] = current_file
        state["offset"] = 0
        save_state(state)

    # open shard
    fp, urls_in_shard, _ = open_shard_for_append(int(state["shard_idx"]))

    batch_txids: List[str] = []
    last_log = time.time()

    batch_limit = max(1, int(SEO_TXID_BATCH_LIMIT))
    idle_sleep = max(1.0, float(SEO_TXID_EXTRACT_INTERVAL))

    while True:
        files = list_segment_files()
        if not files:
            print("[TXID SEO APPEND] No segment files found, sleeping...")
            time.sleep(5)
            continue

        if state["current_file"] not in files:
            next_file, _ = pick_start_file(files, state["current_file"])
            if not next_file:
                print("[TXID SEO APPEND] No next file available, sleeping...")
                time.sleep(5)
                continue
            state["current_file"] = next_file
            state["offset"] = 0
            save_state(state)

        path = state["current_file"]

        try:
            with open(path, "rb") as f:
                f.seek(int(state.get("offset", 0)))

                while True:
                    line = f.readline()
                    if not line:
                        break  # EOF

                    try:
                        rec = json.loads(line.decode("utf-8"))
                    except Exception:
                        # skip bad line, but move forward safely after batch commit (so just ignore)
                        continue

                    txid = rec.get("txid")
                    if not txid:
                        continue

                    if txid in ring:
                        continue

                    batch_txids.append(txid)

                    if len(batch_txids) >= batch_limit:
                        # commit this batch at the CURRENT file position
                        commit_offset = f.tell()

                        new_shard_created = False
                        for t in batch_txids:
                            if urls_in_shard >= SITEMAP_MAX_URLS:
                                close_shard(fp)
                                state["shard_idx"] = int(state["shard_idx"]) + 1
                                fp, urls_in_shard, _ = open_shard_for_append(int(state["shard_idx"]))
                                new_shard_created = True

                            write_url_entry(fp, t)
                            urls_in_shard += 1
                            state["written_total"] = int(state.get("written_total", 0)) + 1
                            ring.append(t)

                        # finalize shard to keep it always valid on disk
                        close_shard(fp)
                        fp, _, _ = open_shard_for_append(int(state["shard_idx"]))

                        # state commit AFTER successful fsync
                        state["urls_in_shard"] = urls_in_shard
                        state["offset"] = commit_offset
                        state["last_txids"] = list(ring)
                        save_state(state)

                        if new_shard_created:
                            rebuild_root_sitemap_index()

                        try:
                            r.set(SEO_TXID_SITEMAP_LAST_BUILD_KEY, str(time.time()))
                        except Exception:
                            pass

                        print(
                            f"[TXID SEO APPEND] +{len(batch_txids)} urls | "
                            f"shard={int(state['shard_idx'])} urls_in_shard={urls_in_shard} | "
                            f"file={os.path.basename(path)} off={state['offset']}"
                        )
                        batch_txids.clear()

                    if time.time() - last_log > 15:
                        print(
                            f"[TXID SEO APPEND] heartbeat | "
                            f"shard={int(state['shard_idx'])} urls_in_shard={urls_in_shard} | "
                            f"file={os.path.basename(path)} off={int(state.get('offset', 0))} | "
                            f"queued={len(batch_txids)}"
                        )
                        last_log = time.time()

            # EOF flush
            if batch_txids:
                new_shard_created = False
                for t in batch_txids:
                    if urls_in_shard >= SITEMAP_MAX_URLS:
                        close_shard(fp)
                        state["shard_idx"] = int(state["shard_idx"]) + 1
                        fp, urls_in_shard, _ = open_shard_for_append(int(state["shard_idx"]))
                        new_shard_created = True

                    write_url_entry(fp, t)
                    urls_in_shard += 1
                    state["written_total"] = int(state.get("written_total", 0)) + 1
                    ring.append(t)

                close_shard(fp)
                fp, _, _ = open_shard_for_append(int(state["shard_idx"]))

                state["urls_in_shard"] = urls_in_shard
                state["offset"] = os.path.getsize(path)
                state["last_txids"] = list(ring)
                save_state(state)

                if new_shard_created:
                    rebuild_root_sitemap_index()

                try:
                    r.set(SEO_TXID_SITEMAP_LAST_BUILD_KEY, str(time.time()))
                except Exception:
                    pass

                print(
                    f"[TXID SEO APPEND] +{len(batch_txids)} urls (flush) | "
                    f"shard={int(state['shard_idx'])} urls_in_shard={urls_in_shard} | "
                    f"file={os.path.basename(path)}"
                )
                batch_txids.clear()

            # next file?
            idx = files.index(path)
            if idx + 1 < len(files):
                state["current_file"] = files[idx + 1]
                state["offset"] = 0
                save_state(state)
                print(f"[TXID SEO APPEND] next file → {os.path.basename(state['current_file'])}")
                continue

            # tip reached
            time.sleep(idle_sleep)

        except KeyboardInterrupt:
            print("[TXID SEO APPEND] stopped by Ctrl+C")
            try:
                close_shard(fp)
            except Exception:
                pass
            return

        except Exception as e:
            print(f"[TXID SEO APPEND ERROR] {e}")
            # ensure shard valid
            try:
                close_shard(fp)
            except Exception:
                pass
            # reopen shard
            try:
                fp, urls_in_shard, _ = open_shard_for_append(int(state["shard_idx"]))
            except Exception:
                time.sleep(5)
                continue
            time.sleep(2)


# ============================================
# ▶️ COMPAT ENTRYPOINT (systemd expects this)
# ============================================
def txid_extractor_worker_loop():
    """
    Compatibility wrapper for systemd process script.
    """
    txid_extractor_append_loop()


# ============================================
# ▶️ Optional direct run
# ============================================
if __name__ == "__main__":
    txid_extractor_worker_loop()
