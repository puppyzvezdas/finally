# Market Data Backend — Design

Implementation-ready design for the FinAlly market data subsystem: the unified API, the GBM simulator, the Massive (Polygon.io) client, the shared price cache, SSE streaming, and FastAPI integration.

All code lives in `backend/app/market/`. This document supersedes `planning/archive/MARKET_DATA_DESIGN.md`. It is written against the code as it exists today and marks every place where the design **changes** that code (tagged **[CHANGE]**). Untagged snippets match the current implementation.

Related docs: `MARKET_INTERFACE.md` (API summary), `MARKET_SIMULATOR.md` (simulator model), `MASSIVE_API.md` (REST reference), `MARKET_DATA_SUMMARY.md` (status).

---

## Table of Contents

1. [Goals and principles](#1-goals-and-principles)
2. [Architecture and file layout](#2-architecture-and-file-layout)
3. [Data model: `PriceUpdate`](#3-data-model-priceupdate)
4. [Price cache: `PriceCache`](#4-price-cache-pricecache)
5. [Unified interface: `MarketDataSource`](#5-unified-interface-marketdatasource)
6. [Factory](#6-factory)
7. [Simulator](#7-simulator)
8. [Massive API client](#8-massive-api-client)
9. [SSE streaming](#9-sse-streaming)
10. [FastAPI integration](#10-fastapi-integration)
11. [Watchlist and trade coordination](#11-watchlist-and-trade-coordination)
12. [Testing](#12-testing)
13. [Error handling and edge cases](#13-error-handling-and-edge-cases)
14. [Configuration reference](#14-configuration-reference)
15. [Change list vs. current code](#15-change-list-vs-current-code)

---

## 1. Goals and principles

| # | Principle | Consequence |
|---|---|---|
| 1 | **Push, not pull** | Sources run a background task and *write* to `PriceCache`. Consumers only read the cache, never a source. A 500 ms simulator and a 15 s poller look identical downstream. |
| 2 | **One data type** | `PriceUpdate` is the only object that leaves the layer. |
| 3 | **Select once, at startup** | `create_market_data_source()` reads `MASSIVE_API_KEY`; nothing else branches on the source. |
| 4 | **Failure isolation** | A failed tick or poll is logged and retried; the cache keeps serving the last known price. A background task must never die from an exception. |
| 5 | **Never block the event loop** | The Massive client is synchronous, so it runs in `asyncio.to_thread`. The cache is guarded by `threading.Lock` because of this. |
| 6 | **Simulator needs nothing** | No network, no API key, works offline. |

---

## 2. Architecture and file layout

```
MASSIVE_API_KEY set? ──yes──▶ MassiveDataSource ─┐  one batched snapshot call / 15 s
        │                                         ├──▶ PriceCache ──▶ SSE  GET /api/stream/prices
        └──no───────────▶ SimulatorDataSource ───┘     (in-memory,    ├▶ portfolio valuation
                           GBM step / 500 ms            thread-safe)   ├▶ trade execution
                                                                       └▶ watchlist API (prices)
```

```
backend/app/market/
  __init__.py        # public exports
  models.py          # PriceUpdate (frozen dataclass)
  cache.py           # PriceCache
  interface.py       # MarketDataSource ABC
  factory.py         # create_market_data_source()
  seed_prices.py     # seed prices, GBM params, correlation constants
  simulator.py       # GBMSimulator (math) + SimulatorDataSource (asyncio adapter)
  massive_client.py  # MassiveDataSource (+ pure parsing helpers)
  stream.py          # create_stream_router(): SSE endpoint
```

Public surface (`from app.market import ...`): `PriceUpdate`, `PriceCache`, `MarketDataSource`, `create_market_data_source`, `create_stream_router`.

---

## 3. Data model: `PriceUpdate`

`backend/app/market/models.py` (unchanged)

```python
from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class PriceUpdate:
    """Immutable snapshot of a single ticker's price at a point in time."""

    ticker: str
    price: float
    previous_price: float
    timestamp: float = field(default_factory=time.time)  # Unix seconds

    @property
    def change(self) -> float:
        return round(self.price - self.previous_price, 4)

    @property
    def change_percent(self) -> float:
        if self.previous_price == 0:
            return 0.0
        return round((self.price - self.previous_price) / self.previous_price * 100, 4)

    @property
    def direction(self) -> str:
        if self.price > self.previous_price:
            return "up"
        if self.price < self.previous_price:
            return "down"
        return "flat"

    def to_dict(self) -> dict:
        return {
            "ticker": self.ticker,
            "price": self.price,
            "previous_price": self.previous_price,
            "timestamp": self.timestamp,
            "change": self.change,
            "change_percent": self.change_percent,
            "direction": self.direction,
        }
```

Semantics worth stating explicitly:

- `previous_price` is the **previous update's price**, not the prior close. It drives the green/red flash, so it behaves identically for the simulator (every 0.5 s) and Massive (every 15 s).
- The first update for a ticker has `previous_price == price` → `direction == "flat"`.
- Derived fields are properties, so they can never be inconsistent with `price`/`previous_price`.
- "Daily change %" (vs. prior close) is a different quantity. It is deliberately **not** part of `PriceUpdate`; see §8.7 for the optional extension.

Example SSE/REST shape:

```json
{"ticker": "AAPL", "price": 190.52, "previous_price": 190.5, "timestamp": 1730000000.5,
 "change": 0.02, "change_percent": 0.0105, "direction": "up"}
```

---

## 4. Price cache: `PriceCache`

`backend/app/market/cache.py`

The cache is the single point of truth. Writers: exactly one data source. Readers: SSE, portfolio valuation, trade execution, watchlist API.

```python
from __future__ import annotations

import time
from threading import Lock

from .models import PriceUpdate


class PriceCache:
    def __init__(self) -> None:
        self._prices: dict[str, PriceUpdate] = {}
        self._lock = Lock()
        self._version: int = 0  # bumped on every write

    def update(self, ticker: str, price: float, timestamp: float | None = None) -> PriceUpdate:
        with self._lock:
            ts = timestamp or time.time()
            prev = self._prices.get(ticker)
            previous_price = prev.price if prev else price
            update = PriceUpdate(
                ticker=ticker,
                price=round(price, 2),
                previous_price=round(previous_price, 2),
                timestamp=ts,
            )
            self._prices[ticker] = update
            self._version += 1
            return update

    def get(self, ticker: str) -> PriceUpdate | None:
        with self._lock:
            return self._prices.get(ticker)

    def get_price(self, ticker: str) -> float | None:
        update = self.get(ticker)
        return update.price if update else None

    def get_all(self) -> dict[str, PriceUpdate]:
        with self._lock:
            return dict(self._prices)  # shallow copy; PriceUpdate is immutable

    def remove(self, ticker: str) -> None:
        with self._lock:
            self._prices.pop(ticker, None)

    @property
    def version(self) -> int:
        with self._lock:                      # [CHANGE] read under the lock (review item 3.4)
            return self._version

    def __len__(self) -> int:
        with self._lock:
            return len(self._prices)

    def __contains__(self, ticker: str) -> bool:
        with self._lock:
            return ticker in self._prices
```

Design notes:

- **`threading.Lock`, not `asyncio.Lock`.** Massive polling parses results on the event loop, but future code (or a thread-offloaded simulator step) may write from worker threads. `threading.Lock` is safe from both. The critical sections are a dict lookup and assignment, so contention is negligible.
- **Version counter.** The SSE loop compares `cache.version` to the last value it sent and skips serialization when nothing changed. With Massive at 15 s polling this avoids resending identical payloads 30× per poll.
- **`remove()` bumps no version.** Removed tickers simply vanish from the next payload. If you need the SSE stream to notice a pure removal immediately, also bump `_version` in `remove()` when the key existed (one-line change; optional).
- **Memory is O(tickers).** The cache stores only the latest price. History for sparklines lives in the browser; portfolio history lives in `portfolio_snapshots`.

Usage:

```python
cache = PriceCache()
cache.update("AAPL", 190.00)
u = cache.update("AAPL", 190.25)
assert (u.direction, u.change, u.previous_price) == ("up", 0.25, 190.0)
cache.get_price("AAPL")      # 190.25
cache.get_price("NOPE")      # None   → callers must handle (trade rejection, etc.)
```

---

## 5. Unified interface: `MarketDataSource`

`backend/app/market/interface.py` (unchanged)

```python
from __future__ import annotations

from abc import ABC, abstractmethod


class MarketDataSource(ABC):
    """Contract for market data providers.

    Implementations push updates into a shared PriceCache on their own schedule.
    Downstream code never asks a source for a price; it reads the cache.
    """

    @abstractmethod
    async def start(self, tickers: list[str]) -> None:
        """Begin producing updates. Spawns a background task. Call exactly once."""

    @abstractmethod
    async def stop(self) -> None:
        """Cancel the background task and release resources. Idempotent."""

    @abstractmethod
    async def add_ticker(self, ticker: str) -> None:
        """Track a ticker. No-op if already tracked."""

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """Stop tracking a ticker and drop it from the cache. No-op if absent."""

    @abstractmethod
    def get_tickers(self) -> list[str]:
        """Currently tracked tickers."""
```

Contract rules both implementations must satisfy (and the shared test in §12.5 enforces):

1. After `await start(t)` returns, the cache is populated for every `t` that *can* be priced (simulator: always; Massive: after the immediate first poll).
2. `stop()` twice is safe; after `stop()` nothing writes to the cache.
3. `add_ticker`/`remove_ticker` are idempotent and normalize case (`" tsla "` → `"TSLA"`).
4. `remove_ticker` always clears the cache entry.
5. No method raises on provider errors. Errors are logged; callers see "no price yet" as `cache.get_price(...) is None`.

> **[CHANGE] Ticker normalization.** The simulator currently does not upper-case/strip tickers while Massive does. Move normalization into one shared helper (below) and call it in both sources so rule 3 holds uniformly.

```python
# interface.py (add)
def normalize_ticker(ticker: str) -> str:
    """Canonical form used as the cache/watchlist key."""
    return ticker.strip().upper()
```

---

## 6. Factory

`backend/app/market/factory.py` (unchanged)

```python
import logging
import os

from .cache import PriceCache
from .interface import MarketDataSource
from .massive_client import MassiveDataSource
from .simulator import SimulatorDataSource

logger = logging.getLogger(__name__)


def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    """MASSIVE_API_KEY set and non-blank → Massive; otherwise → simulator.

    Returns an UNSTARTED source; the caller awaits source.start(tickers).
    """
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()
    if api_key:
        logger.info("Market data source: Massive API (real data)")
        return MassiveDataSource(api_key=api_key, price_cache=price_cache)
    logger.info("Market data source: GBM Simulator")
    return SimulatorDataSource(price_cache=price_cache)
```

Optional **[CHANGE]**: let the poll interval be tuned for paid plans without code changes.

```python
poll = float(os.environ.get("MASSIVE_POLL_INTERVAL", "15"))
return MassiveDataSource(api_key=api_key, price_cache=price_cache, poll_interval=poll)
```

(Add `MASSIVE_POLL_INTERVAL` to `.env.example` as optional; default 15 is safe for the free tier.)

---

## 7. Simulator

Two classes in `simulator.py`: `GBMSimulator` (synchronous math, no asyncio, trivially unit-testable) and `SimulatorDataSource` (asyncio lifecycle adapter).

### 7.1 Model

Per tick, per ticker:

```
S(t+dt) = S(t) · exp( (μ − σ²/2)·dt + σ·√dt·Z )
```

- `exp()` keeps prices strictly positive; the `−σ²/2` Itô term makes expected growth equal `μ`.
- `dt = 0.5 s / (252 · 6.5 · 3600 s) ≈ 8.48e-8` of a trading year → per-tick σ·√dt ≈ σ·2.9e-4 (≈ $0.014 on a $190 stock at σ = 0.25). Motion is gentle but accumulates realistically (daily σ ≈ σ/√252).
- `Z` is correlated across tickers: `Z = L @ z_ind` where `C = L·Lᵀ` is the Cholesky factorization of the sector correlation matrix.
- **Events:** each tick, each ticker has probability `0.001` of a shock `price *= 1 ± U(2%, 5%)`. With 10 tickers at 2 ticks/s that is roughly one event every ~50 s.

### 7.2 Constants — `seed_prices.py` (unchanged)

```python
SEED_PRICES = {"AAPL": 190.00, "GOOGL": 175.00, "MSFT": 420.00, "AMZN": 185.00,
               "TSLA": 250.00, "NVDA": 800.00, "META": 500.00, "JPM": 195.00,
               "V": 280.00, "NFLX": 600.00}

TICKER_PARAMS = {                      # annualized
    "AAPL": {"sigma": 0.22, "mu": 0.05}, "GOOGL": {"sigma": 0.25, "mu": 0.05},
    "MSFT": {"sigma": 0.20, "mu": 0.05}, "AMZN":  {"sigma": 0.28, "mu": 0.05},
    "TSLA": {"sigma": 0.50, "mu": 0.03}, "NVDA":  {"sigma": 0.40, "mu": 0.08},
    "META": {"sigma": 0.30, "mu": 0.05}, "JPM":   {"sigma": 0.18, "mu": 0.04},
    "V":    {"sigma": 0.17, "mu": 0.04}, "NFLX":  {"sigma": 0.35, "mu": 0.05},
}
DEFAULT_PARAMS = {"sigma": 0.25, "mu": 0.05}          # tickers added at runtime

CORRELATION_GROUPS = {
    "tech": {"AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "NFLX"},
    "finance": {"JPM", "V"},
}
INTRA_TECH_CORR = 0.6
INTRA_FINANCE_CORR = 0.5
CROSS_GROUP_CORR = 0.3      # cross-sector pairs and unknown tickers
TSLA_CORR = 0.3             # TSLA is in the tech set but trades on its own
```

All off-diagonals are in [0.3, 0.6] with a unit diagonal, so the matrix is positive definite and `np.linalg.cholesky` cannot fail.

### 7.3 `GBMSimulator`

```python
from __future__ import annotations

import math
import random

import numpy as np

from .seed_prices import (CORRELATION_GROUPS, CROSS_GROUP_CORR, DEFAULT_PARAMS,
                          INTRA_FINANCE_CORR, INTRA_TECH_CORR, SEED_PRICES,
                          TICKER_PARAMS, TSLA_CORR)


class GBMSimulator:
    TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600          # 5,896,800
    DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR          # ≈ 8.48e-8

    def __init__(self, tickers: list[str], dt: float = DEFAULT_DT,
                 event_probability: float = 0.001,
                 seed: int | None = None) -> None:       # [CHANGE] seed for reproducible tests/E2E
        self._dt = dt
        self._event_prob = event_probability
        self._rng = np.random.default_rng(seed)          # [CHANGE] own RNGs instead of globals
        self._py_rng = random.Random(seed)
        self._tickers: list[str] = []
        self._prices: dict[str, float] = {}
        self._params: dict[str, dict[str, float]] = {}
        self._cholesky: np.ndarray | None = None
        for t in tickers:
            self._add_ticker_internal(t)
        self._rebuild_cholesky()

    # ---- public ----
    def step(self) -> dict[str, float]:
        """Advance every ticker one dt. Returns {ticker: price rounded to cents}."""
        n = len(self._tickers)
        if n == 0:
            return {}
        z = self._rng.standard_normal(n)
        if self._cholesky is not None:
            z = self._cholesky @ z                       # correlate

        out: dict[str, float] = {}
        for i, t in enumerate(self._tickers):
            mu, sigma = self._params[t]["mu"], self._params[t]["sigma"]
            drift = (mu - 0.5 * sigma**2) * self._dt
            diffusion = sigma * math.sqrt(self._dt) * z[i]
            self._prices[t] *= math.exp(drift + diffusion)

            if self._py_rng.random() < self._event_prob:  # shock event
                mag = self._py_rng.uniform(0.02, 0.05)
                sign = self._py_rng.choice((-1, 1))
                self._prices[t] *= 1 + mag * sign

            out[t] = round(self._prices[t], 2)            # internal state keeps full precision
        return out

    def add_ticker(self, ticker: str) -> None:
        if ticker in self._prices:
            return
        self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    def remove_ticker(self, ticker: str) -> None:
        if ticker not in self._prices:
            return
        self._tickers.remove(ticker)
        del self._prices[ticker], self._params[ticker]
        self._rebuild_cholesky()

    def get_price(self, ticker: str) -> float | None:
        return self._prices.get(ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    # ---- internals ----
    def _add_ticker_internal(self, ticker: str) -> None:
        if ticker in self._prices:
            return
        self._tickers.append(ticker)
        self._prices[ticker] = SEED_PRICES.get(ticker, self._py_rng.uniform(50.0, 300.0))
        self._params[ticker] = TICKER_PARAMS.get(ticker, dict(DEFAULT_PARAMS))

    def _rebuild_cholesky(self) -> None:
        n = len(self._tickers)
        if n <= 1:
            self._cholesky = None
            return
        corr = np.eye(n)
        for i in range(n):
            for j in range(i + 1, n):
                rho = self._pairwise_correlation(self._tickers[i], self._tickers[j])
                corr[i, j] = corr[j, i] = rho
        self._cholesky = np.linalg.cholesky(corr)

    @staticmethod
    def _pairwise_correlation(t1: str, t2: str) -> float:
        tech, finance = CORRELATION_GROUPS["tech"], CORRELATION_GROUPS["finance"]
        if t1 == "TSLA" or t2 == "TSLA":                  # checked first: TSLA is in `tech`
            return TSLA_CORR
        if t1 in tech and t2 in tech:
            return INTRA_TECH_CORR
        if t1 in finance and t2 in finance:
            return INTRA_FINANCE_CORR
        return CROSS_GROUP_CORR
```

The two **[CHANGE]**s (private RNGs + optional `seed`) are backward compatible: with `seed=None` behavior is unchanged except that it no longer depends on global `np.random`/`random` state. Seeded runs let E2E tests and statistical unit tests be reproducible, and keep two simulators in one process from interfering.

### 7.4 `SimulatorDataSource`

```python
class SimulatorDataSource(MarketDataSource):
    def __init__(self, price_cache: PriceCache, update_interval: float = 0.5,
                 event_probability: float = 0.001, seed: int | None = None) -> None:
        self._cache = price_cache
        self._interval = update_interval
        self._event_prob = event_probability
        self._seed = seed
        self._sim: GBMSimulator | None = None
        self._task: asyncio.Task | None = None

    async def start(self, tickers: list[str]) -> None:
        tickers = [normalize_ticker(t) for t in tickers]
        self._sim = GBMSimulator(tickers, event_probability=self._event_prob, seed=self._seed)
        for t in self._sim.get_tickers():                 # seed the cache BEFORE the loop starts
            self._cache.update(ticker=t, price=self._sim.get_price(t))
        self._task = asyncio.create_task(self._run_loop(), name="simulator-loop")

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None

    async def add_ticker(self, ticker: str) -> None:
        ticker = normalize_ticker(ticker)
        if self._sim:
            self._sim.add_ticker(ticker)
            self._cache.update(ticker=ticker, price=self._sim.get_price(ticker))  # priced instantly

    async def remove_ticker(self, ticker: str) -> None:
        ticker = normalize_ticker(ticker)
        if self._sim:
            self._sim.remove_ticker(ticker)
        self._cache.remove(ticker)

    def get_tickers(self) -> list[str]:
        return self._sim.get_tickers() if self._sim else []

    async def _run_loop(self) -> None:
        while True:
            try:
                if self._sim:
                    for t, p in self._sim.step().items():
                        self._cache.update(ticker=t, price=p)
            except Exception:
                logger.exception("Simulator step failed")   # never let the loop die
            await asyncio.sleep(self._interval)
```

Behavioral notes:

- Because the simulator prices any symbol (unknown ones start at a random $50–$300), `add_ticker("ZZZZ")` always "succeeds". With the simulator, the API cannot distinguish real from fake symbols. That is acceptable for the demo; see §11 for validation policy.
- Prices reset to the seeds on restart. The simulator holds no persistent state.
- `step()` takes microseconds for n < 50, so no thread offload is needed.
- Tuning: raise `dt` or `sigma` for livelier motion; `event_probability=0` for calm/deterministic tests.

---

## 8. Massive API client

`massive_client.py` polls the REST **snapshot** endpoint: one batched call returns the latest state for every requested ticker.

```
GET https://api.massive.com/v2/snapshot/locale/us/markets/stocks/tickers?tickers=AAPL,MSFT,...
Authorization: Bearer <MASSIVE_API_KEY>
```

Python client (synchronous): `RESTClient(api_key=...).get_snapshot_all(SnapshotMarketType.STOCKS, tickers=[...])` → list of `TickerSnapshot`.

### 8.1 Rate-limit-driven design

| Plan | Limit | Poll interval |
|---|---|---|
| Free | ~5 req/min | 15 s (default; 12 s minimum) |
| Paid | effectively unlimited | 2–5 s |

Rule: **exactly one batched call per poll**, never one call per ticker.

### 8.2 Snapshot → price mapping **[CHANGE]**

The current code reads only `last_trade.price` and divides `last_trade.timestamp` by 1000. Two defects, both documented in `MARKET_INTERFACE.md`:

1. `lastTrade.t` is **nanoseconds**, not ms. Dividing by 1000 yields timestamps ~10⁶× too large.
2. `lastTrade` is absent on plans without trade data, and pre-market. The ticker is then skipped forever.

Fix with pure, individually testable helpers (no I/O):

```python
# massive_client.py
from __future__ import annotations

import time
from typing import Any


def to_epoch_seconds(ts: int | float) -> float:
    """Normalize a s / ms / µs / ns epoch value to seconds."""
    ts = float(ts)
    if ts > 1e17:
        return ts / 1e9      # nanoseconds  (lastTrade.t, lastQuote.t, updated)
    if ts > 1e14:
        return ts / 1e6      # microseconds
    if ts > 1e11:
        return ts / 1e3      # milliseconds (min.t, aggregates)
    return ts                # seconds


def _attr(obj: Any, name: str) -> Any:
    return getattr(obj, name, None) if obj is not None else None


def extract_price(snap: Any) -> tuple[float, float] | None:
    """Return (price, timestamp_seconds) from a TickerSnapshot, or None if unpriceable.

    Preference order (MASSIVE_API.md §7):
        lastTrade.p → min.c → day.c → prevDay.c
    A zero/None price is treated as missing (day bar is zeroed early in the session).
    """
    now = time.time()

    trade = _attr(snap, "last_trade")
    if (p := _attr(trade, "price")) and p > 0:
        ts = _attr(trade, "timestamp") or _attr(snap, "updated")
        return float(p), to_epoch_seconds(ts) if ts else now

    for bar_name in ("min", "day", "prev_day"):
        bar = _attr(snap, bar_name)
        if (c := _attr(bar, "close")) and c > 0:
            return float(c), now           # bar timestamps are window-based, so use wall clock
    return None
```

Attribute names (`last_trade.price`, `min.close`, `day.close`, `prev_day.close`, `updated`) follow the `massive` package's generated models; confirm with `dir(snap)` on first run with a real key since the model layer has changed between versions.

Using `now` for bar fallbacks is intentional: the cache timestamp means "when we learned this price", and the stale-quote case stays visible through `previous_price`/`direction` staying flat.

### 8.3 `MassiveDataSource` **[CHANGE: fallbacks, backoff, fast add]**

```python
from __future__ import annotations

import asyncio
import logging
import time

from massive import RESTClient
from massive.rest.models import SnapshotMarketType

from .cache import PriceCache
from .interface import MarketDataSource, normalize_ticker

logger = logging.getLogger(__name__)

MIN_POLL_GAP = 12.0           # free tier: 5 req/min → never closer than 12 s
MAX_BACKOFF = 120.0


class MassiveDataSource(MarketDataSource):
    def __init__(self, api_key: str, price_cache: PriceCache,
                 poll_interval: float = 15.0) -> None:
        self._api_key = api_key
        self._cache = price_cache
        self._base_interval = poll_interval
        self._interval = poll_interval          # current (grows on 429, resets on success)
        self._tickers: list[str] = []
        self._task: asyncio.Task | None = None
        self._client: RESTClient | None = None
        self._last_poll = 0.0                   # monotonic time of last request
        self._poll_lock = asyncio.Lock()        # one poll at a time (loop vs. add_ticker)

    # ---------- lifecycle ----------
    async def start(self, tickers: list[str]) -> None:
        self._client = RESTClient(api_key=self._api_key)
        self._tickers = list(dict.fromkeys(normalize_ticker(t) for t in tickers))
        await self._poll_once()                           # populate the cache before serving
        self._task = asyncio.create_task(self._poll_loop(), name="massive-poller")

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        self._client = None

    async def add_ticker(self, ticker: str) -> None:
        ticker = normalize_ticker(ticker)
        if ticker in self._tickers:
            return
        self._tickers.append(ticker)
        # Fast path: fetch now instead of waiting up to 15 s, but only if the
        # rate-limit budget allows. Otherwise it appears on the next scheduled poll.
        if self._client and time.monotonic() - self._last_poll >= MIN_POLL_GAP:
            await self._poll_once()

    async def remove_ticker(self, ticker: str) -> None:
        ticker = normalize_ticker(ticker)
        self._tickers = [t for t in self._tickers if t != ticker]
        self._cache.remove(ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    # ---------- polling ----------
    async def _poll_loop(self) -> None:
        while True:
            await asyncio.sleep(self._interval)
            await self._poll_once()

    async def _poll_once(self) -> None:
        if not self._tickers or not self._client:
            return
        async with self._poll_lock:
            tickers = list(self._tickers)            # copy: the worker thread must not see mutation
            self._last_poll = time.monotonic()
            try:
                snapshots = await asyncio.to_thread(self._fetch_snapshots, tickers)
            except Exception as e:
                self._on_failure(e)
                return

            self._interval = self._base_interval      # success → recover to base cadence
            updated = 0
            for snap in snapshots:
                try:
                    found = extract_price(snap)
                    if found is None:
                        logger.warning("Massive: no usable price for %s", snap.ticker)
                        continue
                    price, ts = found
                    # Ignore symbols removed while the request was in flight.
                    if snap.ticker in self._tickers:
                        self._cache.update(ticker=snap.ticker, price=price, timestamp=ts)
                        updated += 1
                except (AttributeError, TypeError, ValueError) as e:
                    logger.warning("Massive: skipping %s: %s", getattr(snap, "ticker", "???"), e)

            missing = set(tickers) - {getattr(s, "ticker", None) for s in snapshots}
            if missing:
                logger.warning("Massive: no snapshot returned for %s (invalid symbol or not on plan)",
                               sorted(missing))
            logger.debug("Massive poll: %d/%d updated", updated, len(tickers))

    def _fetch_snapshots(self, tickers: list[str]) -> list:
        """Synchronous; runs in a worker thread."""
        return self._client.get_snapshot_all(
            market_type=SnapshotMarketType.STOCKS, tickers=tickers
        )

    def _on_failure(self, exc: Exception) -> None:
        text = str(exc)
        if "429" in text or "rate" in text.lower():     # verify the client's exception type with a real key
            self._interval = min(self._interval * 2, MAX_BACKOFF)
            logger.error("Massive rate-limited; backing off to %.0fs", self._interval)
        else:
            logger.error("Massive poll failed: %s", exc)
        # Never re-raise: the cache keeps serving last-known prices; the loop retries.
```

What changed vs. the current file and why:

| Change | Reason |
|---|---|
| `extract_price()` with `min/day/prevDay` fallbacks | Tickers no longer silently vanish pre-market or on plans without trades |
| `to_epoch_seconds()` | Fixes the ns-vs-ms timestamp bug |
| Poll takes a copied ticker list | `add/remove_ticker` run on the event loop while the fetch runs in a thread |
| `_poll_lock` | `add_ticker`'s immediate poll cannot overlap the scheduled poll (would double-spend the 5/min budget) |
| `MIN_POLL_GAP` guard in `add_ticker` | Immediate poll only when it cannot breach the free-tier limit |
| Exponential backoff on 429, reset on success | Avoids hammering a throttled key |
| Missing-symbol warning | Surfaces typos/unsupported symbols instead of silent absence |
| Drop updates for removed tickers | Prevents a late snapshot from resurrecting a ticker removed mid-flight |

### 8.4 Request flow

```
start()
  └─ _poll_once()  ── to_thread ──▶ GET snapshot?tickers=A,B,C ──▶ [TickerSnapshot…]
        └─ extract_price(snap) → cache.update(ticker, price, ts)       (cache fully warm)
  └─ create_task(_poll_loop)
        └─ every 15 s:  _poll_once()      (on 429: 30 s, 60 s, … ≤ 120 s; on success: back to 15 s)

add_ticker("PYPL")  ─▶ append; if ≥12 s since last request → _poll_once() now; else wait for next tick
```

### 8.5 Error handling matrix

| Condition | Behavior |
|---|---|
| 401 bad key | Logged each poll; cache stays empty; SSE connected but empty. Fix `.env`, restart. |
| 403 plan lacks endpoint | Same as 401. |
| 429 rate limit | Interval doubles up to 120 s; recovers after a success. |
| Network error / 5xx | Logged; retried next interval. |
| Snapshot lacks `lastTrade` | Falls back to `min.c → day.c → prevDay.c`. |
| Snapshot has no usable price at all | That ticker is skipped with a warning; others proceed. |
| Symbol not returned | Warning listing missing symbols; no cache entry. |
| Empty ticker list | `_poll_once` returns without a request. |

### 8.6 Raw HTTP equivalent (debugging)

Useful to check a key and see real field names without the SDK:

```python
import os, httpx

r = httpx.get(
    "https://api.massive.com/v2/snapshot/locale/us/markets/stocks/tickers",
    params={"tickers": "AAPL,MSFT,TSLA"},
    headers={"Authorization": f"Bearer {os.environ['MASSIVE_API_KEY']}"},
    timeout=10,
)
r.raise_for_status()
for t in r.json()["tickers"]:
    lt = t.get("lastTrade") or {}
    print(t["ticker"], lt.get("p"), lt.get("t"), t["prevDay"]["c"])
```

### 8.7 Optional extension: previous close / daily change %

`PriceUpdate.change` is tick-to-tick. If the UI's "daily change %" column needs a prior-close basis, add a small, separate helper (not part of `MarketDataSource`) that reads `prevDay.c` from the **same snapshot** the poller already fetched, at zero extra API cost:

```python
# in extract_price's sibling
def extract_prev_close(snap) -> float | None:
    c = _attr(_attr(snap, "prev_day"), "close")
    return float(c) if c else None
```

Store it in a `dict[str, float]` on the source and expose `get_prev_close(ticker)`. For the simulator, the equivalent is the seed price captured at `start()`. Because this is not required by `PLAN.md`'s current API (the watchlist's "daily change %" can initially use the cache's `change_percent`), treat it as a follow-up; if added, put `prev_close` behind an optional second accessor on the interface rather than widening `PriceUpdate`.

---

## 9. SSE streaming

`stream.py` serves `GET /api/stream/prices`.

Wire format — one event carrying **all** tickers, sent only when `cache.version` changed:

```
retry: 1000

data: {"AAPL":{"ticker":"AAPL","price":190.52,"previous_price":190.5,"timestamp":1730000000.5,"change":0.02,"change_percent":0.0105,"direction":"up"},"GOOGL":{...}}

```

### 9.1 Implementation **[CHANGE: router built per call, heartbeat]**

The current module keeps a module-level `router` that `create_stream_router` mutates; calling the factory twice (tests) registers the route twice (review item 3.6). Build the router inside the factory. Also emit an SSE comment heartbeat so proxies and the browser see traffic during Massive's quiet 15 s gaps.

```python
from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import AsyncGenerator

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from .cache import PriceCache

logger = logging.getLogger(__name__)

HEARTBEAT_SECONDS = 15.0


def create_stream_router(price_cache: PriceCache, interval: float = 0.5) -> APIRouter:
    router = APIRouter(prefix="/api/stream", tags=["streaming"])   # [CHANGE] local, not module-level

    @router.get("/prices")
    async def stream_prices(request: Request) -> StreamingResponse:
        return StreamingResponse(
            _generate_events(price_cache, request, interval),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    return router


async def _generate_events(
    price_cache: PriceCache, request: Request, interval: float = 0.5
) -> AsyncGenerator[str, None]:
    yield "retry: 1000\n\n"                        # browser reconnect delay

    last_version = -1
    last_sent = time.monotonic()
    client = request.client.host if request.client else "unknown"
    logger.info("SSE client connected: %s", client)
    try:
        while True:
            if await request.is_disconnected():
                logger.info("SSE client disconnected: %s", client)
                break

            version = price_cache.version
            if version != last_version:
                last_version = version
                prices = price_cache.get_all()
                if prices:
                    payload = json.dumps({t: u.to_dict() for t, u in prices.items()})
                    yield f"data: {payload}\n\n"
                    last_sent = time.monotonic()

            if time.monotonic() - last_sent >= HEARTBEAT_SECONDS:   # [CHANGE]
                yield ": keep-alive\n\n"                              # SSE comment; ignored by EventSource
                last_sent = time.monotonic()

            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        logger.info("SSE stream cancelled: %s", client)
        raise                                                          # [CHANGE] don't swallow cancellation
```

Why poll-and-push (not event-driven): the cache is polled on a fixed cadence, so frontend sparklines get evenly spaced points regardless of the source. It also keeps each SSE connection independent of the data-source implementation.

### 9.2 Client contract

```ts
const es = new EventSource('/api/stream/prices');
es.onopen = () => setStatus('connected');
es.onerror = () => setStatus('reconnecting');   // EventSource retries automatically (retry: 1000)
es.onmessage = (e) => {
  const prices: Record<string, PriceUpdate> = JSON.parse(e.data);
  // flash green/red from prices[t].direction, append prices[t].price to the sparkline buffer
};
```

- With Massive at 15 s, `onmessage` fires only about every 15 s, so the UI **must not** treat message silence as a disconnect; use `onopen`/`onerror` for the status dot.
- A ticker removed from the watchlist disappears from subsequent payloads. The client should drop it from its local state on the next event.

---

## 10. FastAPI integration

### 10.1 Lifespan

`backend/app/main.py`

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.market import PriceCache, create_market_data_source, create_stream_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    cache = PriceCache()
    source = create_market_data_source(cache)
    app.state.price_cache = cache
    app.state.market_source = source

    # Track the union of the watchlist and any held positions (see §11).
    tickers = await db.tracked_tickers()
    await source.start(tickers)

    yield

    await source.stop()


app = FastAPI(title="FinAlly", lifespan=lifespan)
```

Include the stream router at app construction time, not inside `lifespan`. It needs the cache object, so create the cache first (module scope) and share it:

```python
price_cache = PriceCache()                       # module scope: shared by lifespan and routers
app = FastAPI(title="FinAlly", lifespan=lifespan)
app.include_router(create_stream_router(price_cache))
```

(The earlier archived design registered the router inside `lifespan`. Both work, but module-scope registration avoids a window where `/api/stream/prices` 404s during startup and plays well with test clients. `lifespan` then uses the same `price_cache` instead of creating a new one.)

### 10.2 Dependencies

```python
from fastapi import Depends, Request

def get_price_cache(request: Request) -> PriceCache:
    return request.app.state.price_cache

def get_market_source(request: Request) -> MarketDataSource:
    return request.app.state.market_source
```

---

## 11. Watchlist and trade coordination

### 11.1 Tracked set = watchlist ∪ open positions

The data source must price everything the portfolio might need, not just what is visible in the watchlist. Define one helper in the DB layer and use it everywhere:

```python
async def tracked_tickers() -> list[str]:
    """Watchlist tickers plus any ticker with quantity > 0."""
```

### 11.2 Add to watchlist

```python
@router.post("/api/watchlist")
async def add_ticker(body: WatchlistAdd,
                     source: MarketDataSource = Depends(get_market_source),
                     cache: PriceCache = Depends(get_price_cache)):
    ticker = normalize_ticker(body.ticker)
    await db.insert_watchlist(ticker)           # idempotent (UNIQUE user_id, ticker)
    await source.add_ticker(ticker)
    return {"ticker": ticker, "price": cache.get_price(ticker)}   # may be None for Massive (priced on next poll)
```

Symbol validation policy:

- **Simulator:** any `^[A-Z]{1,5}$` symbol is accepted and priced. Validate format only.
- **Massive:** after `add_ticker`, if `cache.get_price(ticker)` is still `None`, return the ticker with `price: null` and status `pending`. A later poll that returns no snapshot for it logs the warning from §8.3. The simplest robust UX: the watchlist API reports `price: null`, and the UI shows "—" until prices arrive. An optional stricter mode is to reject the add (HTTP 404) when an immediate poll completed and the ticker is still unpriced.

### 11.3 Remove from watchlist

```python
@router.delete("/api/watchlist/{ticker}")
async def remove_ticker(ticker: str, source: MarketDataSource = Depends(get_market_source)):
    ticker = normalize_ticker(ticker)
    await db.delete_watchlist(ticker)
    position = await db.get_position(ticker)
    if position is None or position.quantity == 0:      # keep pricing held tickers
        await source.remove_ticker(ticker)
    return {"status": "ok"}
```

Consequence: a held-but-unwatched ticker remains in the cache and therefore in the SSE payload. The frontend should render only watchlist tickers in the watchlist panel and use the rest only for portfolio valuation.

The reverse also applies: when a position is fully sold, if the ticker is no longer on the watchlist, call `source.remove_ticker(ticker)`.

### 11.4 Trade execution

```python
price = cache.get_price(ticker)
if price is None:
    raise HTTPException(400, f"Price not yet available for {ticker}. Try again shortly.")
# execute at `price`; record trade; trigger a portfolio snapshot
```

Do not fabricate a fallback price. Trading at an unknown price corrupts P&L. For a *buy* of a ticker not on the watchlist, add it to the source first (`await source.add_ticker(t)`), then re-check the cache.

### 11.5 Portfolio valuation

```python
prices = cache.get_all()
market_value = sum(p.quantity * (prices[p.ticker].price if p.ticker in prices else p.avg_cost)
                   for p in positions)
```

Falling back to `avg_cost` for an unpriced position (shows zero unrealized P&L) is safer than raising or valuing at 0 during a transient Massive gap.

### 11.6 Chat (LLM) flow

`watchlist_changes` from the LLM go through the same functions as the REST handlers (11.2/11.3), and `trades` go through the same trade function (11.4). The market layer needs no LLM-specific code. With `LLM_MOCK=true` and the simulator, behavior is deterministic enough for E2E except prices; use `seed` (§7.3) when exact prices matter.

---

## 12. Testing

Tests live in `backend/tests/market/`. Existing: `test_models.py`, `test_cache.py`, `test_simulator.py`, `test_simulator_source.py`, `test_factory.py`, `test_massive.py` (73 tests, 84% coverage). Run with `cd backend && uv run --extra dev pytest -v`.

### 12.1 Pure helpers (new)

```python
# tests/market/test_massive_parsing.py
from types import SimpleNamespace as NS
import pytest
from app.market.massive_client import extract_price, to_epoch_seconds


@pytest.mark.parametrize("raw,expected", [
    (1_605_195_918_306_274_000, 1_605_195_918.306274),   # ns
    (1_605_195_918_306_274,     1_605_195_918.306274),   # µs
    (1_684_428_720_000,         1_684_428_720.0),        # ms
    (1_684_428_720,             1_684_428_720.0),        # s
])
def test_to_epoch_seconds(raw, expected):
    assert to_epoch_seconds(raw) == pytest.approx(expected)


def snap(**kw):
    base = dict(ticker="AAPL", last_trade=None, min=None, day=None, prev_day=None, updated=None)
    base.update(kw)
    return NS(**base)


def test_prefers_last_trade_with_ns_timestamp():
    s = snap(last_trade=NS(price=190.1, timestamp=1_605_195_918_306_274_000))
    price, ts = extract_price(s)
    assert price == 190.1
    assert ts == pytest.approx(1_605_195_918.306, abs=1e-3)


def test_falls_back_through_bars():
    assert extract_price(snap(min=NS(close=190.0)))[0] == 190.0
    assert extract_price(snap(day=NS(close=0), prev_day=NS(close=191.35)))[0] == 191.35  # zero day bar skipped


def test_unpriceable_returns_none():
    assert extract_price(snap()) is None
```

### 12.2 Massive source (mock the thread boundary)

```python
# tests/market/test_massive.py (additions)
from unittest.mock import patch
import pytest
from app.market.cache import PriceCache
from app.market.massive_client import MassiveDataSource


def mk(ticker, price, ts=1_707_580_800_000_000_000):
    from types import SimpleNamespace as NS
    return NS(ticker=ticker, last_trade=NS(price=price, timestamp=ts),
              min=None, day=None, prev_day=None, updated=None)


async def make_source(tickers=("AAPL",)):
    cache = PriceCache()
    src = MassiveDataSource("k", cache, poll_interval=60)
    src._client = object()                          # bypass real client construction
    src._tickers = list(tickers)
    return src, cache


async def test_poll_updates_cache_and_timestamp_in_seconds():
    src, cache = await make_source(["AAPL", "GOOGL"])
    with patch.object(src, "_fetch_snapshots", return_value=[mk("AAPL", 190.5), mk("GOOGL", 175.25)]):
        await src._poll_once()
    assert cache.get_price("AAPL") == 190.5
    assert cache.get("AAPL").timestamp == pytest.approx(1_707_580_800.0)


async def test_backoff_doubles_on_429_and_recovers():
    src, cache = await make_source()
    with patch.object(src, "_fetch_snapshots", side_effect=Exception("429 Too Many Requests")):
        await src._poll_once()
        assert src._interval == 120.0                  # 60 s doubled, at the 120 s cap
    with patch.object(src, "_fetch_snapshots", return_value=[mk("AAPL", 190.0)]):
        await src._poll_once()
    assert src._interval == 60


async def test_removed_ticker_not_resurrected():
    src, cache = await make_source(["AAPL", "TSLA"])
    def fetch(tickers):
        src._tickers.remove("TSLA")                  # simulate removal while in flight
        return [mk("AAPL", 190.0), mk("TSLA", 250.0)]
    with patch.object(src, "_fetch_snapshots", side_effect=fetch):
        await src._poll_once()
    assert cache.get("TSLA") is None


async def test_add_ticker_respects_rate_gap():
    src, cache = await make_source()
    src._last_poll = time.monotonic()                 # just polled (add `import time` at top)
    with patch.object(src, "_poll_once") as poll:
        await src.add_ticker("pypl")
    poll.assert_not_called()
    assert "PYPL" in src.get_tickers()
```

Mock `_fetch_snapshots`, not `RESTClient`; `massive` is a core dependency, so the module imports it at top level (review item 3.2 resolved).

### 12.3 Simulator statistics (new)

```python
# tests/market/test_simulator_statistics.py
import numpy as np
import pytest
from app.market.simulator import GBMSimulator


def test_log_return_stddev_matches_sigma_sqrt_dt():
    # Use the unrounded internal state, not the cent-rounded output (moves are sub-cent).
    sim = GBMSimulator(["AAPL"], event_probability=0.0, seed=42)
    logs = []
    prev = sim.get_price("AAPL")
    for _ in range(50_000):
        sim.step()
        cur = sim._prices["AAPL"]
        logs.append(np.log(cur / prev))
        prev = cur
    expected = 0.22 * np.sqrt(GBMSimulator.DEFAULT_DT)
    assert np.std(logs) == pytest.approx(expected, rel=0.03)


def test_tech_pair_more_correlated_than_cross_sector():
    sim = GBMSimulator(["AAPL", "MSFT", "JPM"], event_probability=0.0, seed=7)
    rets = {t: [] for t in ("AAPL", "MSFT", "JPM")}
    prev = dict(sim._prices)
    for _ in range(20_000):
        sim.step()
        for t in rets:
            rets[t].append(np.log(sim._prices[t] / prev[t]))
            prev[t] = sim._prices[t]
    c = np.corrcoef([rets["AAPL"], rets["MSFT"], rets["JPM"]])
    assert c[0, 1] > c[0, 2] + 0.15


def test_full_default_set_cholesky_succeeds():
    from app.market.seed_prices import SEED_PRICES
    sim = GBMSimulator(list(SEED_PRICES))
    assert sim._cholesky is not None and len(sim.step()) == 10


def test_same_seed_same_path():
    a, b = GBMSimulator(["AAPL", "MSFT"], seed=1), GBMSimulator(["AAPL", "MSFT"], seed=1)
    assert [a.step() for _ in range(100)] == [b.step() for _ in range(100)]
```

### 12.4 SSE endpoint (new; closes the 31%-coverage gap)

Use `httpx.ASGITransport`; stream a few lines then close.

```python
# tests/market/test_stream.py
import json
import httpx
from fastapi import FastAPI
from app.market import PriceCache, create_stream_router


async def test_stream_emits_retry_then_prices():
    cache = PriceCache()
    cache.update("AAPL", 190.0)
    app = FastAPI()
    app.include_router(create_stream_router(cache, interval=0.01))

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as c:
        async with c.stream("GET", "/api/stream/prices") as r:
            assert r.headers["content-type"].startswith("text/event-stream")
            lines = []
            async for line in r.aiter_lines():
                lines.append(line)
                if line.startswith("data:"):
                    break
    assert lines[0] == "retry: 1000"
    payload = json.loads(lines[-1].removeprefix("data: "))
    assert payload["AAPL"]["price"] == 190.0


def test_factory_called_twice_does_not_duplicate_routes():
    cache = PriceCache()
    a, b = create_stream_router(cache), create_stream_router(cache)
    assert len(a.routes) == len(b.routes) == 1
```

> `ASGITransport` buffers the full response in some httpx versions, which would hang on an infinite stream. If so, drive `_generate_events` directly with a stub request exposing `is_disconnected()` and `client`, and `aclose()` the generator after the first `data:` event. Treat that as the fallback implementation.

### 12.5 Shared contract test (new)

Run the same behavioral contract against both sources (Massive with `_fetch_snapshots` patched):

```python
@pytest.fixture(params=["sim", "massive"])
async def source_and_cache(request):
    cache = PriceCache()
    if request.param == "sim":
        src = SimulatorDataSource(cache, update_interval=0.05)
    else:
        src = MassiveDataSource("k", cache, poll_interval=60)
        src._fetch_snapshots = lambda tickers: [mk(t, 100.0) for t in tickers]
        src._client = object()
    yield src, cache
    await src.stop()


async def test_contract(source_and_cache):
    src, cache = source_and_cache
    await src.start(["AAPL"])
    assert cache.get_price("AAPL") is not None          # rule 1
    await src.add_ticker(" msft ")
    assert "MSFT" in src.get_tickers()                  # rule 3 (normalized)
    await src.add_ticker("MSFT")
    assert src.get_tickers().count("MSFT") == 1         # idempotent
    await src.remove_ticker("MSFT")
    assert cache.get("MSFT") is None                    # rule 4
    await src.stop(); await src.stop()                  # rule 2
```

### 12.6 Cache

Add a concurrency test: 8 threads × 1,000 `update()` calls on distinct and shared tickers; assert `version == 8000` and no exceptions.

### 12.7 Factory

Always `monkeypatch.delenv("MASSIVE_API_KEY", raising=False)` (or `setenv("MASSIVE_API_KEY", "  ")` for the blank case) so tests don't depend on the developer's `.env`.

### 12.8 Live smoke test (manual, optional)

Not for CI. Run with a real key to confirm field names and the timestamp unit:

```bash
cd backend
MASSIVE_API_KEY=... uv run python - <<'EOF'
from massive import RESTClient
from massive.rest.models import SnapshotMarketType
s = RESTClient().get_snapshot_all(SnapshotMarketType.STOCKS, tickers=["AAPL"])[0]
print(s.last_trade.price, s.last_trade.timestamp, s.prev_day.close, s.updated)
EOF
```

A 19-digit `timestamp` confirms nanoseconds.

---

## 13. Error handling and edge cases

| Case | Behavior |
|---|---|
| Empty watchlist at startup | `start([])` is fine. The simulator produces nothing; Massive skips the request. SSE emits nothing until a ticker is added. |
| First SSE client connects before first Massive poll completes | Cannot happen: `start()` awaits the first poll before the app serves. If that poll failed, the stream stays empty until a later success. |
| Trade on an unpriced ticker | HTTP 400 "price not yet available". Never invent a price. |
| Bad / expired Massive key | Errors logged each interval; no prices; header dot stays green (SSE is up). Consider a `GET /api/health` field `market_data: {source, last_update_age_s}` so the UI can show "no data". |
| Stale data (Massive outage) | Cache keeps last prices. `health` can expose `age = now - max(timestamp)`; UI may dim stale rows. |
| Market closed / weekend with Massive | Snapshot returns last trade / prior close; prices are flat; flash animations naturally stop. |
| Market closed with simulator | Simulator ignores the calendar and keeps ticking (intentional for demos). |
| Unknown symbol (simulator) | Accepted; random $50–$300 seed. Validate format only. |
| Unknown symbol (Massive) | No snapshot returned → warning logged → `price: null`. |
| Duplicate / mixed-case add | Normalized to one key; idempotent. |
| Ticker removed while Massive request in flight | Result dropped (§8.3). |
| Task crashes | Both loops wrap their body in `try/except Exception` and continue; `CancelledError` (a `BaseException`) still propagates so shutdown works. |
| Rounding | Cache rounds to 2 dp. The simulator keeps full precision internally, so quiet tickers may show `flat` for several ticks. This is normal. |
| Float precision / negatives / NaN | Not possible in the simulator: exp of finite values is positive and finite. |

Health endpoint addition (small, useful for the Docker healthcheck and the UI):

```python
@app.get("/api/health")
async def health(request: Request):
    cache: PriceCache = request.app.state.price_cache
    prices = cache.get_all()
    newest = max((u.timestamp for u in prices.values()), default=None)
    return {
        "status": "ok",
        "market_source": type(request.app.state.market_source).__name__,
        "tickers": len(prices),
        "last_price_age_s": None if newest is None else round(time.time() - newest, 1),
    }
```

---

## 14. Configuration reference

| Parameter | Where | Default | Notes |
|---|---|---|---|
| `MASSIVE_API_KEY` | env | `""` | Non-blank → Massive; blank/unset → simulator |
| `MASSIVE_POLL_INTERVAL` | env **[CHANGE]** | `15` | Seconds; 12 is the free-tier floor, 2–5 for paid plans |
| `update_interval` | `SimulatorDataSource` | `0.5` s | Simulator tick |
| `event_probability` | `GBMSimulator` | `0.001` | Per ticker per tick; `0` to disable shocks |
| `dt` | `GBMSimulator` | `≈8.48e-8` | Fraction of a trading year per tick; raise for livelier motion |
| `seed` | `GBMSimulator` / `SimulatorDataSource` **[CHANGE]** | `None` | Reproducible paths for tests/E2E |
| `poll_interval` | `MassiveDataSource` | `15.0` s | Base interval; grows on 429 |
| `MIN_POLL_GAP` | `massive_client.py` | `12.0` s | Minimum spacing for out-of-band polls |
| `MAX_BACKOFF` | `massive_client.py` | `120.0` s | Backoff cap |
| SSE `interval` | `create_stream_router` | `0.5` s | Cache check cadence |
| SSE `HEARTBEAT_SECONDS` | `stream.py` | `15.0` s | Comment line during quiet periods |
| SSE `retry` | `stream.py` | `1000` ms | Browser reconnect delay |

Dependencies (`backend/pyproject.toml`, already present): `fastapi`, `uvicorn[standard]`, `numpy`, `massive`; dev: `pytest`, `pytest-asyncio` (`asyncio_mode = "auto"`), `pytest-cov`, `ruff`. Add `httpx` to the dev extras for the SSE and raw-HTTP tests.

Package exports `backend/app/market/__init__.py` (unchanged):

```python
from .cache import PriceCache
from .factory import create_market_data_source
from .interface import MarketDataSource
from .models import PriceUpdate
from .stream import create_stream_router

__all__ = ["PriceUpdate", "PriceCache", "MarketDataSource",
           "create_market_data_source", "create_stream_router"]
```

---

## 15. Change list vs. current code

Ordered by value. Nothing here changes the public API signatures except additive optional parameters.

| # | File | Change | Severity |
|---|---|---|---|
| 1 | `massive_client.py` | Replace `/1000.0` with `to_epoch_seconds()` (timestamp unit bug) | **Bug** |
| 2 | `massive_client.py` | `extract_price()` with `min → day → prevDay` fallbacks; unpriceable tickers warn, not vanish | **Bug** |
| 3 | `massive_client.py` | Pass a copied ticker list to the worker thread; drop results for tickers removed mid-flight | Correctness |
| 4 | `massive_client.py` | `_poll_lock`, `MIN_POLL_GAP`, immediate poll in `add_ticker` | UX |
| 5 | `massive_client.py` | 429 backoff (double up to 120 s, reset on success); missing-symbol warning | Resilience |
| 6 | `stream.py` | Build `APIRouter` inside `create_stream_router` (no module-level router) | Footgun |
| 7 | `stream.py` | Heartbeat comment; re-raise `CancelledError` | Robustness |
| 8 | `cache.py` | Read `version` under the lock | Hygiene |
| 9 | `interface.py` | Add `normalize_ticker()`; use in both sources | Consistency |
| 10 | `simulator.py` | Private RNGs plus optional `seed` | Testability |
| 11 | `factory.py` | Optional `MASSIVE_POLL_INTERVAL` | Config |
| 12 | `main.py` (new) | Lifespan, DI, `/api/health` with market-source info | Integration |
| 13 | tests | Parsing, backoff, stats, SSE, contract, cache concurrency (§12) | Coverage |

Suggested implementation order: 1 → 2 (small, fixes real bugs, testable with pure functions) → 3–5 → 6–7 → 8–11 → tests → 12 when the rest of the backend (DB, portfolio, watchlist routes) lands.
