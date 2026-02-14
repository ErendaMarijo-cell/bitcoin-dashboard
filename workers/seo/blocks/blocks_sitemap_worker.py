#!/usr/bin/env python3
# ==================================================
# 🔥 BLOCKS SEO SITEMAP WORKER
#
# Builds block sitemap shards from JSONL segments
# Append-only, resume-safe
# ==================================================

import os
import sys
import time
import json
import glob
from datetime import datetime, timezone

# ============================================
# 🔧 Project Root
# ============================================

BASE_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../../")
)

if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

# ============================================
# Paths
# ============================================

IN_DIR = "/raid/data/seo/blocks/confirmed"
SITEMAP_DIR = "/raid/data/seo/blocks/sitemaps/shards"
ROOT_INDEX_PATH = "/raid/data/seo/blocks/sitemaps/sitemap_blocks.xml"

STATE_PATH = "/raid/data/seo/blocks/progress/blocks_sitemap_state.json"

BLOCK_URL_PREFIX = "https://bitcoin-dashboard.net/explorer/block/"

SITEMAP_MAX_URLS = 50000
SITEMAP_PAD = 6

POLL_INTERVAL = 10

# ============================================
# Helpers
# ============================================

def utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def ensure_dir(p):
    os.makedirs(p, exist_ok=True)


def shard_name(i):
    return f"sitemap_blocks_{i:0{SITEMAP_PAD}d}.xml"


def write_header(fp):
    fp.write(
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
    )


def write_footer(fp):
    fp.write("</urlset>\n")


def write_url(fp, height):
    fp.write(
        "  <url>\n"
        f"    <loc>{BLOCK_URL_PREFIX}{height}</loc>\n"
        "    <changefreq>weekly</changefreq>\n"
        "    <priority>0.7</priority>\n"
        "  </url>\n"
    )


# ============================================
# State
# ============================================

def load_state():
    if not os.path.exists(STATE_PATH):
        return {
            "current_file": None,
            "offset": 0,
            "shard_idx": 1,
            "urls_in_shard": 0
        }

    with open(STATE_PATH) as f:
        return json.load(f)


def save_state(s):
    ensure_dir(os.path.dirname(STATE_PATH))
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w") as f:
        json.dump(s, f)
    os.replace(tmp, STATE_PATH)


# ============================================
# Files
# ============================================

def list_segments():
    files = glob.glob(os.path.join(IN_DIR, "blocks_*.jsonl"))
    files.sort()
    return files


# ============================================
# Worker Loop
# ============================================

def sitemap_loop():

    print("[BLOCKS SITEMAP] started")

    ensure_dir(SITEMAP_DIR)

    state = load_state()

    shard_path = os.path.join(
        SITEMAP_DIR,
        shard_name(state["shard_idx"])
    )

    if not os.path.exists(shard_path):
        with open(shard_path, "w") as fp:
            write_header(fp)
            write_footer(fp)

    while True:

        files = list_segments()

        if not files:
            time.sleep(5)
            continue

        if not state["current_file"]:
            state["current_file"] = files[0]
            state["offset"] = 0

        path = state["current_file"]

        with open(path, "r") as f:
            f.seek(state["offset"])

            for line in f:

                rec = json.loads(line)
                height = rec["height"]

                # shard rollover
                if state["urls_in_shard"] >= SITEMAP_MAX_URLS:
                    state["shard_idx"] += 1
                    state["urls_in_shard"] = 0

                    shard_path = os.path.join(
                        SITEMAP_DIR,
                        shard_name(state["shard_idx"])
                    )

                    with open(shard_path, "w") as fp:
                        write_header(fp)
                        write_footer(fp)

                # append
                with open(shard_path, "r+") as fp:
                    fp.seek(0, os.SEEK_END)
                    pos = fp.tell()
                    fp.seek(pos - len("</urlset>\n"))
                    write_url(fp, height)
                    write_footer(fp)

                state["urls_in_shard"] += 1
                state["offset"] = f.tell()

                save_state(state)

        time.sleep(POLL_INTERVAL)


# ============================================
# Entrypoint
# ============================================

if __name__ == "__main__":
    sitemap_loop()
