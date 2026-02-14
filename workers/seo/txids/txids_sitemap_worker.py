#!/usr/bin/env python3
# ==================================================
# 🔥 TXIDS SEO SITEMAP WORKER (HIGH PERFORMANCE)
#
# ✅ Reads CONFIRMED TXIDs from JSONL segment files (append-only)
# ✅ Builds sharded TXID sitemaps (50,000 URLs/file)
# ✅ Append-only (never deletes shards)
# ✅ Resume-safe (file + byte offset)
# ✅ Footer-safe appends
# ✅ Batch writing (single write per batch)
# ✅ Optional crash-replay dedupe via small txid ring
#
# NOTE:
# - Redis is metadata-only (last build timestamp)
# - No Redis set scans
# ==================================================

import os
import sys
import time
import json
import glob
from datetime import datetime, timezone
from typing import List, Optional, Tuple, Deque
from collections import deque

import redis

# ============================================
# 🔧 Project Root
# ============================================

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

# ============================================
# 🔑 Global Config (NO HARDCODES)
# ============================================

from core.redis_keys import (
    # Loop / Batch
    SEO_TXIDS_SITEMAP_EXTRACT_INTERVAL_SEC,
    SEO_TXIDS_SITEMAP_BATCH_LIMIT,

    # Metadata
    SEO_TXIDS_SITEMAP_LAST_BUILD_KEY,

    # Paths
    SEO_TXIDS_CONFIRMED_SEGMENTS_DIR,
    SEO_TXIDS_SITEMAP_SHARDS_DIR,
    SEO_TXIDS_SITEMAP_ROOT_INDEX_PATH,
    SEO_TXIDS_SITEMAP_STATE_PATH,

    # Sitemap Settings
    SEO_TXIDS_SITEMAP_MAX_URLS_PER_FILE,
    SEO_TXIDS_SITEMAP_SHARD_PAD,

    # URLs
    SEO_TXIDS_TX_URL_PREFIX,
    SEO_TXIDS_SITEMAP_BASE_URL,

    # Crash Safety
    SEO_TXIDS_SITEMAP_TXID_RING_SIZE,
)

# Optional extra sitemap entry (no hardcoded URL here)
try:
    from core.redis_keys import SEO_SITEMAP_PAGES_LOC  # e.g. "https://.../sitemap_pages.xml"
except Exception:
    SEO_SITEMAP_PAGES_LOC = ""

# ============================================
# Runtime Constants
# ============================================

IN_DIR = SEO_TXIDS_CONFIRMED_SEGMENTS_DIR
SITEMAP_DIR = SEO_TXIDS_SITEMAP_SHARDS_DIR
ROOT_INDEX_PATH = SEO_TXIDS_SITEMAP_ROOT_INDEX_PATH
STATE_PATH = SEO_TXIDS_SITEMAP_STATE_PATH

TXID_URL_PREFIX = SEO_TXIDS_TX_URL_PREFIX
SITEMAP_BASE_URL = SEO_TXIDS_SITEMAP_BASE_URL

SITEMAP_MAX_URLS = int(SEO_TXIDS_SITEMAP_MAX_URLS_PER_FILE)
SITEMAP_PAD = int(SEO_TXIDS_SITEMAP_SHARD_PAD)

BATCH_SIZE = max(1, int(SEO_TXIDS_SITEMAP_BATCH_LIMIT))
POLL_INTERVAL = max(1.0, float(SEO_TXIDS_SITEMAP_EXTRACT_INTERVAL_SEC))

TXID_RING_SIZE = max(0, int(SEO_TXIDS_SITEMAP_TXID_RING_SIZE))

# ============================================
# Redis (metadata only)
# ============================================

r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)

# ============================================
# Helpers
# ============================================

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


def atomic_write_json(path: str, payload: dict) -> None:
    ensure_dir(os.path.dirname(path))
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, separators=(",", ":"), ensure_ascii=False)
    os.replace(tmp, path)


def shard_name(i: int) -> str:
    return f"sitemap_txids_{i:0{SITEMAP_PAD}d}.xml"


def list_segments() -> List[str]:
    files = glob.glob(os.path.join(IN_DIR, "txids_*.jsonl"))
    files.sort()
    return files


# ============================================
# XML
# ============================================

_URLSET_HEADER = (
    '<?xml version="1.0" encoding="UTF-8"?>\n'
    '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
).encode("utf-8")

_URLSET_FOOTER = b"</urlset>\n"


def url_entry_bytes(txid: str) -> bytes:
    # Keep formatting stable & minimal
    return (
        "  <url>\n"
        f"    <loc>{TXID_URL_PREFIX}{txid}</loc>\n"
        "    <changefreq>weekly</changefreq>\n"
        "    <priority>0.8</priority>\n"
        "  </url>\n"
    ).encode("utf-8")


# ============================================
# State
# ============================================

def load_state() -> dict:
    default = {
        "current_file": None,
        "offset": 0,
        "shard_idx": 1,
        "urls_in_shard": 0,
        "written_total": 0,
        # crash-replay dedupe (optional)
        "last_txids": [],
        "updated_utc": None,
    }

    if not os.path.exists(STATE_PATH):
        atomic_write_json(STATE_PATH, default)
        return default

    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f) or {}

        # backward-compatible defaults
        for k, v in default.items():
            data.setdefault(k, v)

        if not isinstance(data.get("last_txids"), list):
            data["last_txids"] = []

        # sanitize numeric fields
        data["offset"] = int(data.get("offset", 0) or 0)
        data["shard_idx"] = int(data.get("shard_idx", 1) or 1)
        data["urls_in_shard"] = int(data.get("urls_in_shard", 0) or 0)
        data["written_total"] = int(data.get("written_total", 0) or 0)

        return data

    except Exception:
        atomic_write_json(STATE_PATH, default)
        return default


def save_state(s: dict) -> None:
    s["updated_utc"] = utc_now_iso()
    atomic_write_json(STATE_PATH, s)


# ============================================
# Shards (footer-safe append)
# ============================================

def ensure_shard_file(idx: int) -> str:
    ensure_dir(SITEMAP_DIR)
    path = os.path.join(SITEMAP_DIR, shard_name(idx))

    if not os.path.exists(path):
        with open(path, "wb") as fp:
            fp.write(_URLSET_HEADER)
            fp.write(_URLSET_FOOTER)

    return path


def _find_footer_pos(path: str) -> int:
    """
    Find position of '</urlset>' footer.
    Efficient approach: scan from tail window; fallback to full read if needed.
    """
    footer = b"</urlset>"
    try:
        size = os.path.getsize(path)
        window = min(size, 1024 * 256)  # 256KB tail window
        with open(path, "rb") as f:
            f.seek(max(0, size - window))
            data = f.read()
        pos = data.rfind(footer)
        if pos != -1:
            return max(0, size - window) + pos
    except Exception:
        pass

    # fallback (rare)
    with open(path, "rb") as f:
        data = f.read()
    pos = data.rfind(footer)
    if pos == -1:
        return -1
    return pos


def open_shard_for_append(idx: int) -> Tuple[object, str]:
    """
    Opens shard in r+b, positioned BEFORE footer (footer removed).
    """
    path = ensure_shard_file(idx)

    pos = _find_footer_pos(path)

    if pos == -1:
        # Repair attempt: append footer then re-scan
        with open(path, "ab") as af:
            af.write(b"\n" + _URLSET_FOOTER)

        pos = _find_footer_pos(path)
        if pos == -1:
            raise RuntimeError(f"Footer missing and could not be repaired: {path}")

    fp = open(path, "r+b")
    fp.seek(pos)
    fp.truncate()            # remove footer
    fp.seek(0, os.SEEK_END)  # move to end for appends
    return fp, path


def close_shard(fp) -> None:
    fp.write(_URLSET_FOOTER)
    fp.flush()
    os.fsync(fp.fileno())
    fp.close()


# ============================================
# Root Index
# ============================================

def rebuild_root_index() -> None:
    ensure_dir(os.path.dirname(ROOT_INDEX_PATH))

    shards = sorted(
        os.path.basename(p)
        for p in glob.glob(os.path.join(SITEMAP_DIR, "sitemap_txids_*.xml"))
    )

    tmp = ROOT_INDEX_PATH + ".tmp"

    with open(tmp, "w", encoding="utf-8") as fp:
        fp.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        fp.write('<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n')

        if SEO_SITEMAP_PAGES_LOC:
            fp.write("  <sitemap>\n")
            fp.write(f"    <loc>{SEO_SITEMAP_PAGES_LOC}</loc>\n")
            fp.write("  </sitemap>\n")

        for name in shards:
            fp.write("  <sitemap>\n")
            fp.write(f"    <loc>{SITEMAP_BASE_URL}{name}</loc>\n")
            fp.write("  </sitemap>\n")

        fp.write("</sitemapindex>\n")

    os.replace(tmp, ROOT_INDEX_PATH)


# ============================================
# Worker Loop
# ============================================

def sitemap_loop() -> None:
    print("[TXIDS SITEMAP] started")
    print(f"[TXIDS SITEMAP] IN_DIR={IN_DIR}")
    print(f"[TXIDS SITEMAP] SITEMAP_DIR={SITEMAP_DIR}")
    print(f"[TXIDS SITEMAP] ROOT_INDEX={ROOT_INDEX_PATH}")
    print(f"[TXIDS SITEMAP] STATE={STATE_PATH}")
    print(f"[TXIDS SITEMAP] BATCH={BATCH_SIZE} POLL={POLL_INTERVAL}s MAX_URLS={SITEMAP_MAX_URLS}")

    state = load_state()

    # ring for crash-replay dedupe (optional)
    ring: Deque[str]
    if TXID_RING_SIZE > 0:
        ring = deque(state.get("last_txids", []), maxlen=TXID_RING_SIZE)
    else:
        ring = deque([], maxlen=0)

    # open current shard
    shard_fp, _ = open_shard_for_append(int(state["shard_idx"]))

    batch: List[str] = []
    last_log = time.time()

    while True:
        files = list_segments()

        if not files:
            print("[TXIDS SITEMAP] no segment files found")
            time.sleep(5)
            continue

        # init file if missing
        if not state.get("current_file"):
            state["current_file"] = files[0]
            state["offset"] = 0
            save_state(state)

        # if current file vanished, jump to next
        if state["current_file"] not in files:
            # pick first file lexicographically greater
            cur_base = os.path.basename(state["current_file"])
            next_file = None
            for f in files:
                if os.path.basename(f) > cur_base:
                    next_file = f
                    break
            if not next_file:
                print("[TXIDS SITEMAP] current segment missing and no next available, sleeping...")
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

                    # NOTE: commit offset is when we flush batch, not per-line, to stay atomic
                    try:
                        rec = json.loads(line.decode("utf-8"))
                    except Exception:
                        continue

                    txid = rec.get("txid")
                    if not txid:
                        continue

                    if TXID_RING_SIZE > 0 and txid in ring:
                        continue

                    batch.append(txid)

                    if len(batch) >= BATCH_SIZE:
                        commit_offset = f.tell()
                        _commit_batch(batch, state, ring, shard_fp, path, commit_offset)
                        batch.clear()

                    # heartbeat
                    if time.time() - last_log > 15:
                        print(
                            f"[TXIDS SITEMAP] heartbeat | "
                            f"file={os.path.basename(path)} off={int(state.get('offset', 0))} | "
                            f"shard={state['shard_idx']} urls_in_shard={state['urls_in_shard']} | "
                            f"queued={len(batch)}"
                        )
                        last_log = time.time()

            # EOF flush (remaining)
            if batch:
                commit_offset = os.path.getsize(path)
                _commit_batch(batch, state, ring, shard_fp, path, commit_offset, eof_flush=True)
                batch.clear()

            # next file?
            idx = files.index(path)
            if idx + 1 < len(files):
                state["current_file"] = files[idx + 1]
                state["offset"] = 0
                save_state(state)
                print(f"[TXIDS SITEMAP] next file → {os.path.basename(state['current_file'])}")
                continue

            # tip reached
            time.sleep(POLL_INTERVAL)

        except KeyboardInterrupt:
            print("[TXIDS SITEMAP] stopped by Ctrl+C")
            try:
                close_shard(shard_fp)
            except Exception:
                pass
            return

        except Exception as e:
            print(f"[TXIDS SITEMAP ERROR] {e}")
            # keep shard valid
            try:
                close_shard(shard_fp)
            except Exception:
                pass
            # reopen
            try:
                shard_fp, _ = open_shard_for_append(int(state["shard_idx"]))
            except Exception:
                time.sleep(5)
                continue
            time.sleep(2)


def _commit_batch(
    batch: List[str],
    state: dict,
    ring: Deque[str],
    shard_fp,
    current_path: str,
    commit_offset: int,
    eof_flush: bool = False,
) -> None:
    """
    Writes batch to shard(s), rotates shard if needed, fsync, then commits state.
    """
    new_shard_created = False
    written_now = 0

    for txid in batch:
        if int(state["urls_in_shard"]) >= SITEMAP_MAX_URLS:
            close_shard(shard_fp)
            state["shard_idx"] = int(state["shard_idx"]) + 1
            state["urls_in_shard"] = 0
            shard_fp, _ = open_shard_for_append(int(state["shard_idx"]))
            new_shard_created = True

        shard_fp.write(url_entry_bytes(txid))
        state["urls_in_shard"] = int(state["urls_in_shard"]) + 1
        state["written_total"] = int(state.get("written_total", 0)) + 1
        written_now += 1

        if ring.maxlen and ring.maxlen > 0:
            ring.append(txid)

    # finalize shard on disk (valid XML always)
    close_shard(shard_fp)
    shard_fp, _ = open_shard_for_append(int(state["shard_idx"]))

    # state commit AFTER fsync/close+reopen
    state["offset"] = int(commit_offset)
    if ring.maxlen and ring.maxlen > 0:
        state["last_txids"] = list(ring)
    save_state(state)

    if new_shard_created:
        rebuild_root_index()

    # metadata ping
    try:
        r.set(SEO_TXIDS_SITEMAP_LAST_BUILD_KEY, str(time.time()))
    except Exception:
        pass

    tag = "flush" if eof_flush else "batch"
    print(
        f"[TXIDS SITEMAP] +{written_now} urls ({tag}) | "
        f"total={state.get('written_total', 0)} | "
        f"shard={state['shard_idx']} ({state['urls_in_shard']}) | "
        f"file={os.path.basename(current_path)} off={state['offset']}"
    )


# ============================================
# Entrypoint
# ============================================

if __name__ == "__main__":
    sitemap_loop()
