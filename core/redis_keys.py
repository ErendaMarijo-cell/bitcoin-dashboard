# ================================================================================================================================= #
# ------------------------------------🔑 REDIS KEYS & SHARED CONSTANTS (NO SIDE EFFECTS)-------------------------------------------
# ================================================================================================================================= #


# ================================================================================================================================= #
# [HOME] 🔸 BLOCKCHAIN                                       🛑 INPUT-WORKER 🛑                                         --NODE II--
# ================================================================================================================================= #
BLOCKCHAIN_PREFIX = "2_BLOCKCHAIN_"

# ---- Block Detail
BLOCKCHAIN_LATEST_BLOCK_KEY = f"{BLOCKCHAIN_PREFIX}GETBLOCK_LATEST"


# ---- Core Keys
BLOCKCHAIN_GETBLOCKCHAININFO_KEY = f"{BLOCKCHAIN_PREFIX}GETBLOCKCHAININFO"
BLOCKCHAIN_STATIC_KEY            = f"{BLOCKCHAIN_PREFIX}STATIC"
BLOCKCHAIN_LOCK_KEY              = f"{BLOCKCHAIN_PREFIX}LOCK"

# ---- Cache / Stats
BLOCKCHAIN_DYNAMIC_CACHE = f"{BLOCKCHAIN_PREFIX}DYNAMIC_CACHE"
BLOCKCHAIN_STATS_KEY     = f"{BLOCKCHAIN_PREFIX}INPUT_STATS"

# ---- Dynamic Subkeys
BLOCKCHAIN_DYNAMIC_BLOCKINFO_KEY  = f"{BLOCKCHAIN_PREFIX}DYNAMIC_BLOCKINFO"
BLOCKCHAIN_DYNAMIC_HASHRATE_KEY   = f"{BLOCKCHAIN_PREFIX}DYNAMIC_HASHRATE"
BLOCKCHAIN_DYNAMIC_HALVING_KEY    = f"{BLOCKCHAIN_PREFIX}DYNAMIC_HALVING"
BLOCKCHAIN_DYNAMIC_WINNERHASH_KEY = f"{BLOCKCHAIN_PREFIX}DYNAMIC_WINNERHASH"

# ---- Blockchain Constants
HALVING_INTERVAL      = 210_000
LAST_HALVING_BLOCK    = 840_000
BLOCK_TIME_SECONDS    = 10 * 60
INITIAL_BLOCK_REWARD  = 50

# ---- Locking
BLOCKCHAIN_LOCK_TTL_SECONDS = 10

# ---- Worker Intervals
BLOCKCHAIN_DYNAMIC_UPDATE_INTERVAL = 1 # UPDATE-INTERVALL
BLOCKCHAIN_STATIC_UPDATE_INTERVAL  = 60 * 60 * 6


# ================================================================================================================================= #


# ================================================================================================================================= #
# [HOME] 🔸 MEMPOOL                                         🛑 INPUT-WORKER 🛑                                          --NODE II--
# ================================================================================================================================= #
MEMPOOL_PREFIX = "2_MEMPOOL_"

# ---- Core Keys
MEMPOOL_GETMEMPOOLINFO = f"{MEMPOOL_PREFIX}GETMEMPOOLINFO"
MEMPOOL_STATIC_KEY     = f"{MEMPOOL_PREFIX}STATIC"
MEMPOOL_LOCK_KEY       = f"{MEMPOOL_PREFIX}LOCK"

# ---- Cache / Stats
MEMPOOL_DYNAMIC_CACHE  = f"{MEMPOOL_PREFIX}DYNAMIC_CACHE"
MEMPOOL_STATS_KEY      = f"{MEMPOOL_PREFIX}INPUT_STATS"

# ---- Dynamic Subkeys
MEMPOOL_DYNAMIC_SIZEFEE_KEY   = f"{MEMPOOL_PREFIX}DYNAMIC_SIZEFEE"
MEMPOOL_DYNAMIC_AVGTX_KEY     = f"{MEMPOOL_PREFIX}DYNAMIC_AVGTX"
MEMPOOL_DYNAMIC_WAITTIME_KEY  = f"{MEMPOOL_PREFIX}DYNAMIC_WAITTIME"

# ---- Intervals
MEMPOOL_DYNAMIC_UPDATE_INTERVAL = 1   # UPDATE-INTERVALL
MEMPOOL_STATIC_UPDATE_INTERVAL  = 60 * 60 * 24


# ================================================================================================================================= #


# ================================================================================================================================= #
# [HOME] 🔸 NETWORK                                                                                                     --NODE II--
# ================================================================================================================================= #
NETWORK_PREFIX = "2_NETWORK_"

# ---- Core Keys
NETWORK_GETNETWORKINFO = f"{NETWORK_PREFIX}GETNETWORKINFO"
NETWORK_STATIC_KEY     = f"{NETWORK_PREFIX}STATIC"
NETWORK_LOCK_KEY       = f"{NETWORK_PREFIX}LOCK"
NETWORK_DYNAMIC_CACHE  = f"{NETWORK_PREFIX}DYNAMIC_CACHE"

# ---- Intervals
NETWORK_DYNAMIC_UPDATE_INTERVAL = 10   # UPDATE-INTERVALL
NETWORK_STATIC_UPDATE_INTERVAL  = 60 * 60 * 6


# ================================================================================================================================= #


# ================================================================================================================================= #
# [HOME] 🔸 BTC_TOP                                       🛑 INPUT-WORKER 🛑                                           --NODE III--
# ================================================================================================================================= #
BTC_TOP_50_EVER_PATH = ("/raid/data/bitcoin_dashboard/metrics_history/btc_top_history/btc_top_50_history.json")

BTC_TOP_PREFIX = "3_BTC_TOP_"

# ---- Core Keys
BTC_TOP_SEEN_KEY        = f"{BTC_TOP_PREFIX}SEEN"
BTC_TOP_TXS_KEY         = f"{BTC_TOP_PREFIX}TXS"
BTC_TOP_STATS_KEY       = f"{BTC_TOP_PREFIX}STATS"
BTC_TOP_LOCK_KEY        = f"{BTC_TOP_PREFIX}LOCK"

BTC_TOP_SEEN_VALUE_KEY  = f"{BTC_TOP_PREFIX}SEEN_VALUE"     # 🛑 ZENTRALE QUELLE ALLER TX-WERTE (READ-ONLY für andere Worker)

BTC_TOP_TOP_N = 50
BTC_TOP_LOCK_TTL = 20               
BTC_TOP_UPDATE_INTERVAL = 2.5          # UPDATE-INTERVALL


# ================================================================================================================================= #


# ================================================================================================================================= #
# [HOME] 🔸 BTC_VOLUME                                                                                               --REDIS ONLY--
# ================================================================================================================================= #
BTC_VOL_PREFIX = "HOME_BTC_VOL_"

# ---- Core Keys - aktueller Volume-Zustand (mempool_volume, 1h / 24h, avg_tx, counts)
BTC_VOL_DYNAMIC_CACHE = f"{BTC_VOL_PREFIX}DYNAMIC_CACHE"

# Distributed Lock für BTC-Volume-Worker
BTC_VOL_LOCK_KEY = f"{BTC_VOL_PREFIX}LOCK"

# ---- Cache / Stats - Laufzeit-, Scan- & Health-Daten des Volume-Workers
BTC_VOL_STATS_KEY = f"{BTC_VOL_PREFIX}STATS"

# UPDATE-INTERVALL
BTC_VOL_UPDATE_INTERVAL = 2.5
BTC_VOL_LOCK_TTL = 10                  


# ================================================================================================================================= #


# ================================================================================================================================= #
# [HOME] 🔸 BTC_PRICE                                                                                                --REDIS ONLY--
# ================================================================================================================================= #
HOME_BTC_PRICE_CACHE        =          "HOME_BTC_PRICE_CACHE"
HOME_PRICE_LOCK             =               "HOME_PRICE_LOCK"

HOME_BTC_PRICE_CACHE_TTL    = 60     # 60 Sekunden Redis-Caching
HOME_BTC_PRICE_LOCK_TTL     = 50     # 55 Sekunden Lock für API-Schutz

# Wartezeit, wenn Lock von anderem Thread gehalten wird
HOME_BTC_PRICE_MAX_WAIT     = 5       # max 5 Sekunden warten
HOME_BTC_PRICE_WAIT_STEP    = 0.25   # alle 250 ms erneut prüfen


# ================================================================================================================================= #


# ================================================================================================================================= #
# [HOME] 🔸 DASHBOARD_TRAFFIC                                                                                          --RAM ONLY--
# ================================================================================================================================= #
DASHBOARD_TRAFFIC_PREFIX        = "DASHBOARD_TRAFFIC_"
DASHBOARD_TRAFFIC_RAW_PREFIX    = "DASHBOARD_TRAFFIC_RAW_TS_"

DASHBOARD_TRAFFIC_TOTAL         = f"{DASHBOARD_TRAFFIC_PREFIX}TOTAL"
DASHBOARD_TRAFFIC_TODAY         = f"{DASHBOARD_TRAFFIC_PREFIX}TODAY"
DASHBOARD_TRAFFIC_DAY           = f"{DASHBOARD_TRAFFIC_PREFIX}DAY_UTC"      # "YYYY-MM-DD"
DASHBOARD_TRAFFIC_LAUNCH_TS     = f"{DASHBOARD_TRAFFIC_PREFIX}LAUNCH_TS_MS" # epoch ms
DASHBOARD_TRAFFIC_LIVE_10S      = f"{DASHBOARD_TRAFFIC_PREFIX}LIVE_10S"     # current 10s bucket count
DASHBOARD_TRAFFIC_LAST_TS       = f"{DASHBOARD_TRAFFIC_PREFIX}LAST_TS_MS"

DASHBOARD_TRAFFIC_STATS         = f"{DASHBOARD_TRAFFIC_PREFIX}STATS"


# ================================================================================================================================= #


# ================================================================================================================================= #
# [HOME] 🔸 META-DASHBOARD                                                                                             --RAM ONLY--
# ================================================================================================================================= #

# META Cache (System Health Payload)
HOME_META_CACHE         = "HOME_META_CACHE"
HOME_META_CACHE_TTL     = 3   # Sekunden (etwas länger als Messdauer)

# META Lock (Worker-Synchronisation)
HOME_META_LOCK          = "HOME_META_LOCK"
HOME_META_LOCK_TTL      = 3   # Sekunden

HOME_META_DAY_SECONDS   = 24*60*60


# ================================================================================================================================= #
# ================================================================================================================================= #
# ================================================================================================================================= #
# ================================================================================================================================= #
# ================================================================================================================================= #


# ================================================================================================================================= #
# [NETWORK] 🔹 NODES                                                                                                      --REDIS--
# ================================================================================================================================= #
NETWORK_NODES_CACHE_KEY        = "NETWORK_NODES_CACHE"
NETWORK_NODES_SUBTAB_CACHE_KEY = "NETWORK_NODES_SUBTAB_CACHE"
NETWORK_NODES_LOCK_KEY         = "NETWORK_NODES_LOCK"


NETWORK_NODES_REFRESH_INTERVAL = 60 * 60
NETWORK_NODES_LOCK_TTL         = 60
NETWORK_NODES_SUBTAB_CACHE_TTL = 60 * 60 * 24
NETWORK_NODES_SHORT_CACHE_TTL  = 60 * 5


# ================================================================================================================================= #


# ================================================================================================================================= #
# [NETWORK] 🔹 MINER                                                                                                      --REDIS--
# ================================================================================================================================= #
NETWORK_MINER_CACHE_KEY        = "NETWORK_MINER_CACHE"
NETWORK_MINER_LOCK_KEY         = "NETWORK_MINER_LOCK"
NETWORK_MINER_STC_KEY          = "NETWORK_MINER_STC"

NETWORK_MINER_REFRESH_INTERVAL = 60 * 8       # 8 Minuten Redis Worker Cache
NETWORK_MINER_LOCK_TTL         = 60           # 60 Sekunden Lock
NETWORK_MINER_STC_TTL          = 150          # 150 Sekunden API Short-Term Cache


# ================================================================================================================================= #
# ================================================================================================================================= #
# ================================================================================================================================= #
# ================================================================================================================================= #
# ================================================================================================================================= #


# ================================================================================================================================= #
# [METRICS] 🔹 BTC_USD_EUR                                                                                                  --REDIS--
# ================================================================================================================================= #
METRICS_BTC_USD_EUR_CACHE_KEY           =               "METRICS_BTC_USD_EUR_CACHE_KEY"
METRICS_BTC_USD_EUR_LOCK_KEY            =               "METRICS_BTC_USD_EUR_LOCK_KEY"

METRICS_BTC_USD_EUR_REFRESH_INTERVAL    = 60 * 15       # 15 Minuten Redis Cache
METRICS_BTC_USD_EUR_LOCK_TTL            = 60 * 5        # 5 Minuten Lock
METRICS_BTC_USD_EUR_SHORT_CACHE_TTL     = 60 * 2        # 2 Minuten RAM Short-Term
METRICS_BTC_USD_EUR_SUBTAB_TTL          = 60 * 15       # 15 Minuten RAM Subtab


# ================================================================================================================================= #


# ================================================================================================================================= #
# [METRICS] 🔹 DIFFICULTY                                     🛑 INPUT-WORKER 🛑                                         --NODE I--
# ================================================================================================================================= #
BTC_DIFFICULTY_PREFIX = "METRICS_BTC_DIFFICULTY_"

BTC_DIFFICULTY_1Y   = f"{BTC_DIFFICULTY_PREFIX}1Y"
BTC_DIFFICULTY_5Y   = f"{BTC_DIFFICULTY_PREFIX}5Y"
BTC_DIFFICULTY_10Y  = f"{BTC_DIFFICULTY_PREFIX}10Y"
BTC_DIFFICULTY_EVER = f"{BTC_DIFFICULTY_PREFIX}EVER"


# ================================================================================================================================= #
UPDATE_INTERVAL_HOURS   = 23         # SOWOHL FÜR DIFFICULTY ALS AUCH HASHRATE
RETRY_INTERVAL_SECONDS  = 10         # SOWOHL FÜR DIFFICULTY ALS AUCH HASHRATE
# ================================================================================================================================= #


# ================================================================================================================================== #
# [METRICS] 🔹 HASHRATE                                  🛑 INPUT-WORKER 🛑                                               --NODE I--
# ================================================================================================================================== #
BTC_HASHRATE_PREFIX = "METRICS_BTC_HASHRATE_"

BTC_HASHRATE_1Y   = f"{BTC_HASHRATE_PREFIX}1Y"
BTC_HASHRATE_5Y   = f"{BTC_HASHRATE_PREFIX}5Y"
BTC_HASHRATE_10Y  = f"{BTC_HASHRATE_PREFIX}10Y"
BTC_HASHRATE_EVER = f"{BTC_HASHRATE_PREFIX}EVER"


# ================================================================================================================================= #


# ================================================================================================================================= #
# [METRICS] 🔹 BTC_TX_VOLUME  -                                                                                        --RAM ONLY--
# ================================================================================================================================= #
BTC_TX_VOLUME_PREFIX = "METRICS_BTC_TX_VOLUME_"

BTC_TX_VOLUME_1H            = f"{BTC_TX_VOLUME_PREFIX}1H"
BTC_TX_VOLUME_24H           = f"{BTC_TX_VOLUME_PREFIX}24H"
BTC_TX_VOLUME_1W            = f"{BTC_TX_VOLUME_PREFIX}1W"
BTC_TX_VOLUME_1M            = f"{BTC_TX_VOLUME_PREFIX}1M"
BTC_TX_VOLUME_1Y            = f"{BTC_TX_VOLUME_PREFIX}1Y"

BTC_TX_VOLUME_STATS         = f"{BTC_TX_VOLUME_PREFIX}STATS"

BTC_TX_VOLUME_OPEN_BUCKETS  = f"{BTC_TX_VOLUME_PREFIX}OPEN_BUCKETS"


# ================================================================================================================================= #
POLL_SECONDS = 10  # UPDATE-INTERVALL (smallest bucket - 10s) => SOWOHL FÜR BTC_TX_VOLUME ALS AUCH FÜR BTC_TX_FEES
# ================================================================================================================================= #


# ================================================================================================================================= #
# [METRICS] 🔹 BTC_TX_FEES                                                                                             --RAM ONLY--
# ================================================================================================================================= #

BTC_TX_FEES_PREFIX = "METRICS_BTC_TX_FEES_"

BTC_TX_FEES_24H = f"{BTC_TX_FEES_PREFIX}24H"
BTC_TX_FEES_1W  = f"{BTC_TX_FEES_PREFIX}1W"
BTC_TX_FEES_1M  = f"{BTC_TX_FEES_PREFIX}1M"
BTC_TX_FEES_1Y  = f"{BTC_TX_FEES_PREFIX}1Y"

BTC_TX_FEES_STATS = f"{BTC_TX_FEES_PREFIX}STATS"

BTC_TX_FEES_OPEN_BUCKETS = f"{BTC_TX_FEES_PREFIX}OPEN_BUCKETS"

# ================================================================================================================================= #


# ================================================================================================================================= #
# [METRICS] 🔹 BTC_TX_AMOUNT                                                                                           --RAM ONLY--
# ================================================================================================================================= #

BTC_TX_AMOUNT_PREFIX = "METRICS_BTC_TX_AMOUNT_"

# ---- Aggregated History - aggregierte Top-TX-Historie (now / 24h / 1w / 1m / 1y / halving / ever)
BTC_TX_AMOUNT_HISTORY_KEY = f"{BTC_TX_AMOUNT_PREFIX}HISTORY"

# ---- Cache / Stats - Laufzeit-, Scan- & Health-Daten des TX-Amount-Workers
BTC_TX_AMOUNT_STATS_KEY = f"{BTC_TX_AMOUNT_PREFIX}STATS"

# UPDATE-INTERVALL
BTC_TX_AMOUNT_TOP_NOW = 50
BTC_TX_AMOUNT_TOP_OTHER = 1000
BTC_TX_AMOUNT_AGG_INTERVAL = 10        


# ================================================================================================================================= #
# ================================================================================================================================= #
# ================================================================================================================================= #
# ================================================================================================================================= #
# ================================================================================================================================= #


# ================================================================================================================================= #
# [REVIEW] 🔹 BTC_VALUE                                                                                                --RAM ONLY--
# ================================================================================================================================= #
REVIEW_BASE_PATH = "/raid/data/ramdisk_bitcoin_dashboard/review"
# ================================================================================================================================= #


# ================================================================================================================================= #
# ================================================================================================================================= #
# ================================================================================================================================= #
# ================================================================================================================================= #
# ================================================================================================================================= #


# ================================================================================================================================= #
# [EXPLORER] 🔹 ADDRESS                                                                                                --RAM ONLY--
# ================================================================================================================================= #
EXPLORER_ADDRESSES_MAX_ADDRESSES_KEY = "EXPLORER_ADDRESSES_MAX_ADDRESSES"

EXPLORER_ADDRESSES_MAX_ADDRESSES_DEFAULT = 25


# ================================================================================================================================= #


# ================================================================================================================================= #
# [EXPLORER] 🔹 TX                                                                                                     --RAM ONLY--
# ================================================================================================================================= #
SEO_TXIDS_BACKFILL_STATE_PATH               = "/raid/lightning/seo/txids/progress/txids_backfill_state.json"
SEO_TXIDS_BACKFILL_OUT_DIR                  = "/raid/lightning/seo/txids/confirmed"

SEO_TXIDS_BACKFILL_LOOP_SLEEP_SEC           = 0.02      # Sleep zwischen Block-RPC Calls
SEO_TXIDS_BACKFILL_PROGRESS_EVERY_N_BLOCKS  = 25        # Wie viele Blöcke verarbeitet werden, bevor der Progress-State gespeichert wird
SEO_TXIDS_BACKFILL_FSYNC_EVERY_N_BLOCKS     = 25        # Erzwingt fsync() Flush auf Segment-Datei → garantiert, dass JSONL physisch auf Disk liegt


# ================================================================================================================================= #


# ================================================================================================================================= #
# [EXPLORER] 🔹 TXIDS SITEMAP EXTRACTOR                                                                                --RAM ONLY-- #
# ================================================================================================================================= #

# --- Runtime / Loop ---
SEO_TXIDS_SITEMAP_EXTRACT_INTERVAL_SEC   = 2        # Sleep zwischen Extract Runs
SEO_TXIDS_SITEMAP_BATCH_LIMIT            = 5000     # Max TXIDs pro Loop Iteration

# --- Build Metadata ---
SEO_TXIDS_SITEMAP_LAST_BUILD_KEY         = "seo:txids:sitemap:last_build_ts"
SEO_TXIDS_SITEMAP_LAST_SHARD_ID_KEY      = "seo:txids:sitemap:last_shard_id"

# --- Crash Resume State ---
SEO_TXIDS_SITEMAP_STATE_PATH             = "/raid/lightning/seo/txids/progress/txids_sitemap_state.json"

# --- Input / Output Dirs ---
SEO_TXIDS_CONFIRMED_SEGMENTS_DIR         = "/raid/lightning/seo/txids/confirmed"
SEO_TXIDS_SITEMAP_SHARDS_DIR             = "/raid/lightning/seo/txids/sitemaps/shards"

# --- Root Index ---
SEO_TXIDS_SITEMAP_ROOT_INDEX_PATH        = "/home/marijo/Dokumente/NODE_DASHBOARD/static/sitemaps/sitemap_txids.xml"

# --- Sitemap Limits ---
SEO_TXIDS_SITEMAP_MAX_URLS_PER_FILE      = 50000
SEO_TXIDS_SITEMAP_SHARD_PAD              = 6

# --- URL Prefix ---
SEO_TXIDS_TX_URL_PREFIX                  = "https://bitcoin-dashboard.net/explorer/tx/"
SEO_TXIDS_SITEMAP_BASE_URL               = "https://bitcoin-dashboard.net/static/sitemaps/txids/"

# --- Crash Safety ---
SEO_TXIDS_SITEMAP_TXID_RING_SIZE         = 50000

# ================================================================================================================================= #



# ================================================================================================================================= #
# ================================================================================================================================= #
# ================================================================================================================================= #
# ================================================================================================================================= #
# ================================================================================================================================= #


# ================================================================================================================================= #
TREASURIES_BASE_PATH = "/raid/data/ramdisk_bitcoin_dashboard/treasuries"
# ================================================================================================================================= #

# ================================================================================================================================= #
# [TREASURIES] 🔹 COMPANIES                                                                                             --RAM ONLY--
# ================================================================================================================================= #
TREASURIES_COMPANIES_FILENAME               = "treasuries_companies.json"
TREASURIES_COMPANIES_RESPONSE_CACHE_KEY     = "TREASURIES_COMPANIES_RESPONSE_CACHE_KEY"


# ================================================================================================================================= #


# ================================================================================================================================= #
# [TREASURIES] 🔹 INSTITUTIONS                                                                                             --RAM ONLY--
# ================================================================================================================================= #
TREASURIES_INSTITUTIONS_FILENAME            = "treasuries_institutions.json"
TREASURIES_INSTITUTIONS_RESPONSE_CACHE_KEY  = "TREASURIES_INSTITUTIONS_RESPONSE_CACHE_KEY"


# ================================================================================================================================= #


# ================================================================================================================================= #
# [TREASURIES] 🔹 COUNTRIES                                                                                            --RAM ONLY--
# ================================================================================================================================= #
TREASURIES_COUNTRIES_FILENAME               = "treasuries_countries.json"
TREASURIES_COUNTRIES_RESPONSE_CACHE_KEY     = "TREASURIES_COUNTRIES_RESPONSE_CACHE_KEY"



# ================================================================================================================================= #
# ================================================================================================================================= #
# ================================================================================================================================= #
# ================================================================================================================================= #
# ================================================================================================================================= #


# ================================================================================================================================= #
MARKET_CAP_BASE_PATH = "/raid/data/ramdisk_bitcoin_dashboard/market_cap"
# ================================================================================================================================= #


# ================================================================================================================================= #
# [MARKET_CAP] 🔹 COINS                                                                                                   --REDIS--
# ================================================================================================================================= #
MARKET_CAP_COINS_CACHE_KEY     = "MARKET_CAP_COINS_CACHE_KEY"       # 🛑 SOURCE OF TRUE
MARKET_CAP_COINS_LOCK_KEY      = "MARKET_CAP_COINS_LOCK"

MARKET_CAP_COINS_REFRESH_INTERVAL = 60 * 60 * 25  # 4 Stunden Redis Langzeitcache
MARKET_CAP_COINS_LOCK_TTL         = 60 * 30       # 60 Sekunden Lock-Time


# ================================================================================================================================= #


# ================================================================================================================================= #
# [MARKET_CAP] 🔹 COMPANIES                                                                                               --REDIS--
# ================================================================================================================================= #
MARKET_CAP_COMPANIES_CACHE_NOW              = "MARKET_CAP_COMPANIES_CACHE_NOW"
MARKET_CAP_COMPANIES_CACHE_OLD              = "MARKET_CAP_COMPANIES_CACHE_OLD"
MARKET_CAP_COMPANIES_LOCK_KEY               = "MARKET_CAP_COMPANIES_LOCK_KEY"
MARKET_CAP_COMPANIES_REFRESH_COOLDOWN       = "MARKET_CAP_COMPANIES_REFRESH_COOLDOWN"

MARKET_CAP_COMPANIES_REFRESH_COOLDOWN_TTL   = 60 * 60 * 24   # 24 Stunden COOLDOWN
MARKET_CAP_COMPANIES_REFRESH_INTERVAL       = 60 * 60 * 25   # 25 Stunden Cache
MARKET_CAP_COMPANIES_LOCK_TTL               = 60 * 30        # 30 Minuten Lock


# ================================================================================================================================= #


# ================================================================================================================================= #
# [MARKET_CAP] 🔹 CURRENCIES                                                                                               --REDIS--
# ================================================================================================================================= #
MARKET_CAP_CURRENCIES_FX_FILENAME           =   "market_cap_currencies_fx_to_usd.json"
MARKET_CAP_CURRENCIES_FIAT_FILENAME         =   "market_cap_currencies_fiat_m2.json"
MARKET_CAP_CURRENCIES_RESPONSE_CACHE_KEY    =   "MARKET_CAP_CURRENCIES_RESPONSE_CACHE_KEY"

MARKET_CAP_CURRENCIES_RESPONSE_TTL = 60 * 60 * 4  # 4 Stunden Redis Long-Term Cache


# ================================================================================================================================= #


# ================================================================================================================================= #
# [MARKET_CAP] 🔹 COMMODITIES                                                                                               --REDIS--
# ================================================================================================================================= #
MARKET_CAP_COMMODITIES_FILENAME             =   "market_cap_commodities.json"
MARKET_CAP_COMMODITIES_RESPONSE_CACHE_KEY   =   "MARKET_CAP_COMMODITIES_RESPONSE_CACHE_KEY"

MARKET_CAP_COMMODITIES_RESPONSE_TTL = 60 * 60 * 4  # 4 Stunden Redis Long-Term Cache


# ================================================================================================================================= #


# ================================================================================================================================= #
# [INFO] 🔹 DASHBOARD_TRAFFIC                                                                                             --REDIS--
# ================================================================================================================================= #
INFO_DASHBOARD_TRAFFIC_PREFIX = "INFO_DASHBOARD_TRAFFIC_"

INFO_DASHBOARD_TRAFFIC_1H  = f"{INFO_DASHBOARD_TRAFFIC_PREFIX}1H"
INFO_DASHBOARD_TRAFFIC_24H = f"{INFO_DASHBOARD_TRAFFIC_PREFIX}24H"
INFO_DASHBOARD_TRAFFIC_1W  = f"{INFO_DASHBOARD_TRAFFIC_PREFIX}1W"
INFO_DASHBOARD_TRAFFIC_1M  = f"{INFO_DASHBOARD_TRAFFIC_PREFIX}1M"
INFO_DASHBOARD_TRAFFIC_1Y  = f"{INFO_DASHBOARD_TRAFFIC_PREFIX}1Y"

DASHBOARD_TRAFFIC_OPEN_BUCKETS = f"{DASHBOARD_TRAFFIC_PREFIX}OPEN_BUCKETS"


# ================================================================================================================================= #


