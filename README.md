# Bitcoin Dashboard

Production-grade Bitcoin analytics dashboard with a Python backend,
background workers, Redis-based synchronization and a server-driven frontend.

## Overview

This project powers a live Bitcoin analytics platform with:

- multi-process background workers
- Redis-based caching, locking and state coordination
- direct Bitcoin Core and ElectrumX integration
- server-driven frontend (Flask + HTML/CSS/JavaScript)

The repository mirrors the real production structure.
This is not a demo or toy project.

## Architecture

- **app.py**  
  API layer and orchestration. Serves frontend and aggregates worker output.

- **workers/**  
  Independent background processes responsible for data ingestion,
  aggregation and periodic updates.

- **core/redis_keys.py**  
  Central, side-effect-free definition of all Redis keys and shared constants.
  Acts as a single source of truth for Redis schema and coordination.

- **nodes/**  
  Bitcoin Core RPC and ElectrumX integration.

- **static/** / **templates/**  
  Server-driven frontend with client-side data loading.

## Redis Strategy

Redis is used for:
- cross-process synchronization (locks)
- shared state
- caching with explicit TTLs
- worker health and statistics

Redis keys are strictly namespaced and centrally defined.

## Configuration

All configuration and secrets are provided via environment variables.
They are intentionally not part of this repository.

Expected files (examples, not included):

- `env/.env.api`
- `env/.env.main`
- `env/.env.node2`
- `env/.env.node3`

## Status

This project is actively used in production and continuously evolving.
