# Market Data Backend — Summary

**Status:** Complete, tested and reviewed. The 2026-11-04 review's findings are resolved (see `MARKET_DATA_REVIEW.md` §8); live verification against a real Massive key is still pending.

## What Was Built

A complete market data subsystem in `backend/app/market/` (9 modules) providing live price simulation and real market data via a unified interface.

### Architecture

```
MarketDataSource (ABC)
├── SimulatorDataSource  →  GBM simulator (default, no API key needed)
└── MassiveDataSource    →  Polygon.io REST poller (when MASSIVE_API_KEY set)
        │
        ▼
   PriceCache (thread-safe, in-memory)
        │
        ├──→ SSE stream endpoint (/api/stream/prices)
        ├──→ Portfolio valuation
        └──→ Trade execution
```

### Modules

| File | Purpose |
|------|---------|
| `models.py` | `PriceUpdate` — immutable frozen dataclass (ticker, price, previous_price, timestamp, optional reference_price; derived change, change_percent, day_change_percent, direction) |
| `interface.py` | `MarketDataSource` ABC (`start/stop/add_ticker/remove_ticker/get_tickers`) plus ticker helpers (`normalize_ticker`, `validate_ticker`, `clean_tickers`) and `UnknownTickerError` |
| `cache.py` | `PriceCache` — thread-safe price store; rejects NaN/non-positive prices; version counter for SSE change detection |
| `seed_prices.py` | Realistic seed prices, per-ticker GBM params (drift/volatility), correlation groups |
| `simulator.py` | `GBMSimulator` (Geometric Brownian Motion with Cholesky-correlated moves) + `SimulatorDataSource` |
| `massive_client.py` | `MassiveDataSource` — REST polling client via the `massive` package, with price fallbacks, ns timestamp handling, 429 backoff, rate-limited fast add, bounded first poll |
| `factory.py` | `create_market_data_source()` — selects simulator or Massive based on `MASSIVE_API_KEY` env var |
| `stream.py` | `create_stream_router()` — FastAPI SSE endpoint factory (new router per call) with version-based change detection and a keep-alive heartbeat |

### Key Design Decisions

- **Strategy pattern** — both data sources implement the same ABC; downstream code is source-agnostic
- **PriceCache as single point of truth** — producers write, consumers read; no direct coupling
- **GBM with correlated moves** — Cholesky decomposition of sector-based correlation matrix; tech stocks correlate at 0.6, finance at 0.5, cross-sector at 0.3
- **Random shock events** — ~0.1% chance per tick per ticker of a 2-5% move for visual drama
- **SSE over WebSockets** — simpler, one-way push, universal browser support

## Test Suite

**185 passed, 1 skipped** (the skipped test is an optional live smoke test that needs `MASSIVE_API_KEY`), on Python 3.13 and 3.12. Overall coverage 98%.

| Module | Coverage |
|--------|----------|
| models, cache, interface, factory, stream, seed_prices | 100% |
| simulator.py | 98% |
| massive_client.py | 96% |

Notable test design: Massive tests build snapshots with the real `TickerSnapshot.from_dict` (never `MagicMock`, which hid a wrong attribute name), SSE is tested through the generator and the route, and one contract test runs the same assertions against both sources.

## Review History

- **Review 1 (2026-02-10):** 7 issues (hatch config, lazy imports, SSE annotation, `get_tickers`, correlation constants, test imports, Massive test mocks), all fixed.
- **Review 2 (2026-11-04):** found that the Massive path dropped every snapshot against the real client (`LastTrade` has `sip_timestamp`, not `timestamp`), plus 4 high, 8 medium and 5 low findings. All addressed; see `MARKET_DATA_REVIEW.md` §8.

## Remaining Before the App Is Complete

These belong to the watchlist/portfolio/API work, not the market layer: keep tickers with open positions tracked, map `ValueError`/`UnknownTickerError` to HTTP 400/404, and run the live smoke test with a real key.

## Demo

A Rich terminal demo is available at `backend/market_data_demo.py`:

```bash
cd backend
uv run market_data_demo.py
```

Displays a live-updating dashboard with all 10 tickers, sparklines, color-coded direction arrows, and an event log for notable price moves. Runs 60 seconds or until Ctrl+C.

## Usage for Downstream Code

```python
from app.market import PriceCache, create_market_data_source

# Startup
cache = PriceCache()
source = create_market_data_source(cache)  # Reads MASSIVE_API_KEY
await source.start(["AAPL", "GOOGL", "MSFT", ...])

# Read prices
update = cache.get("AAPL")          # PriceUpdate or None (has .reference_price / .day_change_percent)
price = cache.get_price("AAPL")     # float or None
all_prices = cache.get_all()        # dict[str, PriceUpdate]

# Dynamic watchlist
await source.add_ticker("TSLA")     # ValueError if malformed; UnknownTickerError (Massive) if unknown
await source.remove_ticker("GOOGL")

# Shutdown
await source.stop()
```
