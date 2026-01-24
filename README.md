# Bitcoin Dashboard

🚀 **Live system:** https://bitcoin-dashboard.net  
📊 **Production-grade Bitcoin analytics platform** with a self-hosted Python backend,
distributed background workers, Redis-based synchronization and a server-driven frontend.

This repository mirrors the **real production structure** of a live system.
This is **not a demo or toy project**.

---

## 🧠 What this project is

A backend-heavy Bitcoin analytics platform built around:

- real blockchain, network and market data
- long-running, independent worker processes
- explicit caching, locking and synchronization strategies
- production-safe defaults and defensive system design

The system runs continuously and powers a public-facing analytics dashboard.

---

## 📸 Live System – Selected Views

### Bitcoin Overview & Blockchain Status
![Bitcoin Overview](docs/screenshots/overview.jpg)

Real-time blockchain and network state:
- time since genesis block
- halving countdown & remaining blocks
- block height and network hashrate
- multi-currency BTC price aggregation (USD / EUR / JPY)

---

### BTC Price – USD & EUR
![BTC Price Chart](docs/screenshots/price_chart.jpg)

Backend-aggregated Bitcoin price data:
- synchronized multi-currency time series
- long-range historical context
- server-side normalization and caching

---

### Bitcoin Difficulty – Protocol-Level Adjustments
![Difficulty Chart](docs/screenshots/difficulty.jpg)

Bitcoin difficulty visualized across multiple time frames:
- discrete protocol-level difficulty changes
- no artificial smoothing
- accurate representation of consensus mechanics

---

### Transaction Amount
![Transaction Amount](docs/screenshots/tx_amount.jpg)

Analysis of on-chain transaction values:
- aggregated transaction sizes
- historical trends
- backend-driven computation and caching

---

### Transaction Fees
![Transaction Fees](docs/screenshots/tx_fees.jpg)

Transaction fee dynamics:
- network congestion visibility
- fee market behavior
- time-based aggregation

---

### Bitcoin Explorer
![Explorer](docs/screenshots/explorer.jpg)

Lightweight Bitcoin explorer features:
- address lookup
- transaction inspection
- wallet-level aggregation
- ElectrumX-backed queries

---

### Market Capitalization – Commodities
![Market Cap Commodities](docs/screenshots/market_cap_commodities.jpg)

Bitcoin market capitalization compared to:
- global commodities
- normalized economic benchmarks
- structured and ranked datasets

---

## 🏗 Architecture (High-Level)

![System Architecture](docs/architecture.jpg)

The system follows a **backend-first architecture**:

- workers ingest, process and aggregate data
- Redis acts as synchronization and state backbone
- frontend is a pure presentation layer
- no business logic in the browser

---

## 🧩 Key Characteristics

- multi-process background workers
- Redis-based caching, locking and shared state coordination
- direct Bitcoin Core RPC and ElectrumX integration
- external market and metrics APIs with rate-limit protection
- server-driven frontend (Flask + HTML/CSS/JavaScript)
- no frontend framework dependency

---

## 📂 Repository Structure

**app.py**  
API layer and orchestration entry point.

- serves the server-driven frontend
- aggregates worker output from Redis
- exposes stable API endpoints
- starts safely even if optional subsystems are unavailable

**workers/**  
Independent background processes responsible for:

- blockchain state ingestion
- mempool analysis
- transaction metrics (volume, amount, fees)
- hashrate and difficulty computation
- market capitalization (coins, companies, commodities)
- dashboard traffic and system health

Workers:
- run independently
- coordinate via Redis locks
- use TTL-based caching and cooldowns
- fail gracefully without crashing the system

**core/redis_keys.py**  
Central, side-effect-free definition of all Redis keys and shared constants.

- single source of truth for Redis schema
- strict namespacing
- no runtime logic or side effects

**nodes/**  
Bitcoin infrastructure integration:

- Bitcoin Core RPC abstraction
- ElectrumX client logic
- multi-node configuration handling

**static/** & **templates/**  
Server-driven frontend:

- Flask-rendered HTML templates
- vanilla JavaScript for data fetching
- CSS-based responsive layout
- no frontend framework dependency

---

## 🔁 Redis Strategy

Redis is a **core system component**, not just a cache.

Used for:
- cross-process synchronization (distributed locks)
- shared state between workers
- long- and short-term caching via explicit TTLs
- worker statistics and health monitoring

Design principles:
- no implicit key creation
- no magic strings
- strict namespacing
- explicit TTL and lock durations

---

## 🔐 Configuration & Secrets

All configuration and secrets are provided via environment variables
and intentionally not part of this repository.

Expected files (examples, not included):

- env/.env.api   – external API keys
- env/.env.main  – main node configuration
- env/.env.node2 – secondary node configuration
- env/.env.node3 – additional node configuration

The application starts safely even if optional configuration is missing
and fails only when a dependent feature is accessed.

---

## 🧭 Production Philosophy

This project intentionally follows real production constraints:

- no global side effects on import
- no hard crashes on missing optional configuration
- rate-limit aware external API usage
- defensive JSON parsing and fallbacks
- explicit locking for shared resources
- clear separation of input, processing and presentation

Focus: **robustness, clarity and long-term operation**.

---

## 📌 Status

- actively used in production
- continuously evolving
- architecture-first, feature-driven development

---

## 👤 Author

**Marijo Erenda**  
Backend & Automation Engineer

Focus:
- backend systems
- data pipelines
- distributed workers
- production infrastructure
