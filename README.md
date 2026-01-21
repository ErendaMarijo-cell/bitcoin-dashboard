# Bitcoin Dashboard

Production-grade Bitcoin analytics dashboard with a Python backend,
distributed background workers, Redis-based synchronization and a server-driven frontend.

---

## Overview

This project powers a live Bitcoin analytics platform built around real blockchain,
network and market data.

It is designed as a backend-heavy system with long-running worker processes,
explicit caching strategies and production-safe defaults.

Key characteristics:

- multi-process background workers
- Redis-based caching, locking and shared state coordination
- direct Bitcoin Core RPC and ElectrumX integration
- external market and metrics APIs with rate-limit protection
- server-driven frontend (Flask + HTML/CSS/JavaScript)

The repository mirrors the real production structure.
This is not a demo or toy project.

---

## Architecture (High-Level)

┌──────────────────────────┐
│ Bitcoin Nodes            │
│ (Bitcoin Core / RPC)     │
│ ElectrumX                │
└─────────────┬────────────┘
              │
┌─────────────▼────────────┐
│ Input Workers             │
│ - blockchain              │
│ - mempool                 │
│ - network                 │
└─────────────┬────────────┘
              │
┌─────────────▼────────────┐
│ Redis                     │
│ - cache (TTL-based)       │
│ - locks (NX + EX)         │
│ - shared state            │
└─────────────┬────────────┘
              │
┌─────────────▼────────────┐
│ Aggregation Workers       │
│ - derived metrics         │
│ - summaries               │
│ - cooldown handling       │
└─────────────┬────────────┘
              │
┌─────────────▼────────────┐
│ Flask Backend (app.py)    │
│ - API endpoints           │
│ - server-rendered views   │
└─────────────┬────────────┘
              │
┌─────────────▼────────────┐
│ Frontend                  │
│ HTML / CSS / JavaScript   │
│ (data-driven UI)          │
└──────────────────────────┘

---

## Repository Structure

app.py  
API layer and orchestration entry point.

- Serves the server-driven frontend
- Aggregates worker output from Redis
- Provides stable API endpoints for frontend data loading
- Designed to start even if optional subsystems are unavailable

workers/  
Independent background processes responsible for:

- blockchain state ingestion
- mempool analysis
- network and node monitoring
- hashrate and difficulty metrics
- market capitalization (coins, companies, commodities)
- dashboard traffic and system health

Workers:
- run independently
- coordinate via Redis locks
- use TTL-based caching and cooldowns
- fail gracefully without crashing the system

core/redis_keys.py  
Central, side-effect-free definition of all Redis keys and shared constants.

- Single source of truth for Redis schema
- Strict namespacing of keys
- No runtime logic, imports or side effects
- Ensures consistency across all workers and services

nodes/  
Bitcoin infrastructure integration:

- Bitcoin Core RPC abstraction
- ElectrumX client logic
- Node-specific configuration handling

static/ and templates/  
Server-driven frontend:

- HTML templates rendered by Flask
- Vanilla JavaScript for data fetching and updates
- CSS-based responsive layout
- No frontend framework dependency

---

## Redis Strategy

Redis is a core system component, not just a cache.

It is used for:

- cross-process synchronization (distributed locks)
- shared state between workers
- long-term and short-term caching via explicit TTLs
- worker statistics and health monitoring

Design principles:

- no implicit key creation
- no magic strings
- strict namespacing
- explicit TTL and lock durations

---

## Configuration & Secrets

All configuration and secrets are provided via environment variables.
They are intentionally not part of this repository.

Expected files (examples, not included):

- env/.env.api   – external API keys
- env/.env.main  – main node configuration
- env/.env.node2 – secondary node configuration
- env/.env.node3 – additional node configuration

The application is designed to start safely even if optional configuration
is missing, and only fail when a dependent feature is accessed.

---

## Production Philosophy

This project intentionally follows real production constraints:

- no global side effects on import
- no hard crashes on missing optional configuration
- rate-limit aware external API usage
- defensive JSON parsing and fallbacks
- explicit locking for shared resources
- clear separation of input, processing and presentation

The focus is robustness, clarity and long-term operation.

---

## Status

- Actively used in production
- Continuously evolving
- Architecture-first, feature-driven development

---

## Author

Marijo Erenda  
Backend & Automation Engineer

Focus:
- backend systems
- data pipelines
- distributed workers
- production infrastructure
