#!/usr/bin/env python3
# ==================================================
# 🔥 BLOCKS SEO SITEMAP WORKER (HIGH PERFORMANCE)
#
# ✅ Reads block JSONL segments
# ✅ Builds sharded block sitemaps
# ✅ Append-only
# ✅ Resume-safe (file + offset)
# ✅ Footer-safe appends
# ✅ Batch writing
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

BATCH_SIZE = 1000
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


def list_segments():
    files = glob.glob(os.path.join(IN_DIR, "blocks_*.jsonl"))
    files.sort()
    return files


# ============================================
# XML Helpers
# ============================================

def url_entry(height):
    return (
        "  <url>\n"
        f"    <loc>{BLOCK_URL_PREFIX}{height}</loc>\n"
        "    <changefreq>weekly</changefreq>\n"
        "    <priority>0.7</priority>\n"
        "  </url>\n"
    )


def write_header(fp):
    fp.write(
        (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
        ).encode("utf-8")
    )


def write_footer(fp):
    fp.write("</urlset>\n".encode("utf-8"))



# ============================================
# State
# ============================================

def load_state():
    if not os.path.exists(STATE_PATH):
        return {
            "current_file": None,
            "offset": 0,
            "shard_idx": 1,
            "urls_in_shard": 0,
            "written_total": 0
        }

    with open(STATE_PATH) as f:
        s = json.load(f)

    # 🔧 Backward compatibility defaults
    s.setdefault("current_file", None)
    s.setdefault("offset", 0)
    s.setdefault("shard_idx", 1)
    s.setdefault("urls_in_shard", 0)
    s.setdefault("written_total", 0)

    return s


def save_state(s):
    ensure_dir(os.path.dirname(STATE_PATH))
    tmp = STATE_PATH + ".tmp"
    s["updated_utc"] = utc_now()

    with open(tmp, "w") as f:
        json.dump(s, f)

    os.replace(tmp, STATE_PATH)


# ============================================
# Shard Handling
# ============================================

def open_shard_for_append(idx):

    ensure_dir(SITEMAP_DIR)

    path = os.path.join(SITEMAP_DIR, shard_name(idx))

    if not os.path.exists(path):
        with open(path, "wb") as fp:
            write_header(fp)
            write_footer(fp)

    footer = b"</urlset>"

    with open(path, "rb") as rf:
        data = rf.read()

    pos = data.rfind(footer)

    if pos == -1:
        raise RuntimeError(f"Footer missing in {path}")

    fp = open(path, "r+b")
    fp.seek(pos)
    fp.truncate()
    fp.seek(0, os.SEEK_END)

    return fp, path


def close_shard(fp):
    write_footer(fp)
    fp.flush()
    os.fsync(fp.fileno())
    fp.close()


# ============================================
# Root Index
# ============================================

def rebuild_root_index():

    ensure_dir(os.path.dirname(ROOT_INDEX_PATH))

    shards = sorted(
        os.path.basename(p)
        for p in glob.glob(os.path.join(SITEMAP_DIR, "sitemap_blocks_*.xml"))
    )

    tmp = ROOT_INDEX_PATH + ".tmp"

    with open(tmp, "w") as fp:

        fp.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        fp.write('<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n')

        for name in shards:
            fp.write("  <sitemap>\n")
            fp.write(
                f"    <loc>https://bitcoin-dashboard.net/static/sitemaps/blocks/{name}</loc>\n"
            )
            fp.write("  </sitemap>\n")

        fp.write("</sitemapindex>\n")

    os.replace(tmp, ROOT_INDEX_PATH)


# ============================================
# Worker Loop
# ============================================

def sitemap_loop():

    print("[BLOCKS SITEMAP] started")

    state = load_state()

    shard_fp, shard_path = open_shard_for_append(
        state["shard_idx"]
    )

    print(
        f"[BLOCKS SITEMAP] shard={state['shard_idx']} "
        f"urls={state['urls_in_shard']}"
    )

    batch = []
    last_log = time.time()

    while True:

        files = list_segments()

        if not files:
            print("[BLOCKS SITEMAP] no segment files found")
            time.sleep(5)
            continue

        if not state["current_file"]:
            state["current_file"] = files[0]
            state["offset"] = 0

        path = state["current_file"]

        print(f"[BLOCKS SITEMAP] scanning → {os.path.basename(path)}")

        with open(path, "r") as f:

            f.seek(state["offset"])

            while True:

                line = f.readline()

                if not line:
                    break

                state["offset"] = f.tell()

                rec = json.loads(line)
                height = rec["height"]

                batch.append(height)

                if len(batch) >= BATCH_SIZE:

                    for h in batch:

                        if state["urls_in_shard"] >= SITEMAP_MAX_URLS:

                            close_shard(shard_fp)

                            state["shard_idx"] += 1
                            state["urls_in_shard"] = 0

                            shard_fp, shard_path = open_shard_for_append(
                                state["shard_idx"]
                            )

                            rebuild_root_index()

                            print(
                                f"[BLOCKS SITEMAP] new shard → {state['shard_idx']}"
                            )

                        shard_fp.write(url_entry(h).encode("utf-8"))
                        state["urls_in_shard"] += 1
                        state["written_total"] += 1

                    shard_fp.flush()
                    os.fsync(shard_fp.fileno())

                    save_state(state)

                    print(
                        f"[BLOCKS SITEMAP] +{len(batch)} urls | "
                        f"total={state['written_total']} | "
                        f"shard={state['shard_idx']} "
                        f"({state['urls_in_shard']})"
                    )

                    batch.clear()

                # heartbeat
                if time.time() - last_log > 15:

                    print(
                        f"[BLOCKS SITEMAP] heartbeat | "
                        f"offset={state['offset']} | "
                        f"file={os.path.basename(path)}"
                    )

                    last_log = time.time()

        # next segment
        idx = files.index(path)

        if idx + 1 < len(files):
            state["current_file"] = files[idx + 1]
            state["offset"] = 0
            save_state(state)

            print(
                f"[BLOCKS SITEMAP] next file → "
                f"{os.path.basename(state['current_file'])}"
            )

            continue

        print("[BLOCKS SITEMAP] tip reached → sleeping")
        time.sleep(POLL_INTERVAL)


# ============================================
# Entrypoint
# ============================================

if __name__ == "__main__":
    sitemap_loop()
