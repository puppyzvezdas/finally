# Market Data Interface

The unified Python API for stock prices in FinAlly. One abstract interface, two implementations: the **Massive REST poller** (when `MASSIVE_API_KEY` is set and non-empty) and the **GBM simulator** (otherwise). Everything downstream (SSE, portfolio valuation, trade execution) reads from one shared `PriceCache` and never knows which source is live.

This documents the implemented code in `backend/app/market/`. Data source details: `MASSIVE_API.md`. Simulator details: `MARKET_SIMULATOR.md`.

## Design principles

1. **Push, not pull.** Sources run a background task and *write* into `PriceCache`. Consumers never call a source for a price; they read the cache. This decouples a 500ms simulator from a 15s poller.
2. **One data type.** `PriceUpdate` is the only object leaving the layer.
3. **Selection by environment, once, at startup** via a factory.
4. **Failure isolation.** A failed poll or step is logged and retried on the next tick; the cache keeps serving the last known price.

## Architecture

```
MASSIVE_API_KEY set? ──yes──▶ MassiveDataSource ─┐  (poll /v2/snapshot… every 15s)
        │                                         ├──▶ PriceCache ──▶ SSE /api/stream/prices
        └──no───────────▶ SimulatorDataSource ───┘     (in-memory,    ├▶ portfolio valuation
                           (GBM step every 500ms)       thread-safe)   └▶ trade execution
```

## Module layout

```
backend/app/market/
  __init__.py       # public exports
  models.py         # PriceUpdate (frozen dataclass)
  cache.py          # PriceCache
  interface.py      # MarketDataSource ABC
  factory.py        # create_market_data_source()
  massive_client.py # MassiveDataSource
  simulator.py      # GBMSimulator + SimulatorDataSource
  seed_prices.py    # seed prices, per-ticker params, correlations
  stream.py         # create_stream_router(): SSE endpoint
```

Public API (`from app.market import ...`): `PriceUpdate`, `PriceCache`, `MarketDataSource`, `create_market_data_source`, `create_stream_router`.

## Data model

```python
@dataclass(frozen=True, slots=True)
class PriceUpdate:
    ticker: str
    price: float
    previous_price: float
    timestamp: float                      # Unix seconds
    reference_price: float | None = None  # daily-change baseline (see below)

    # derived properties
    change: float                         # round(price - previous_price, 4)
    change_percent: float                 # % vs previous_price (0.0 if previous is 0)
    direction: str                        # "up" | "down" | "flat"
    day_change_percent: float | None      # % vs reference_price; None if no baseline

    def to_dict(self) -> dict: ...        # JSON/SSE shape, includes derived fields
```

`previous_price` is the **previous update's price** (not the prior close). It drives the green/red flash, so it behaves identically for both sources. First update for a ticker has `previous_price == price` (direction `flat`).

`reference_price` is the baseline for the watchlist's "daily change %": the **previous session close** (`prevDay.c`) for Massive, and the **price the simulator started from** (its seed price, or the price at the time a ticker was added) for the simulator. It is carried forward across updates by the cache, and is `None` if the source cannot supply one.

## PriceCache

Thread-safe (`threading.Lock`), because the Massive client runs in a worker thread.

| Method | Description |
|---|---|
| `update(ticker, price, timestamp=None, reference_price=None) -> PriceUpdate` | Record a price (rounded to 2dp); computes `previous_price`; carries the previous `reference_price` forward if none is given; bumps `version`. **Raises `ValueError` for NaN, infinite, zero or negative prices** (they would reach trades and valuation). A `timestamp` of `0.0` is honoured; only `None` means "now". |
| `get(ticker) -> PriceUpdate \| None` | Latest update |
| `get_price(ticker) -> float \| None` | Just the price |
| `get_all() -> dict[str, PriceUpdate]` | Shallow copy of everything |
| `remove(ticker)` | Drop a ticker (watchlist removal); bumps `version` if something was removed so SSE clients see it |
| `version` (property) | Monotonic counter (read under the lock); SSE sends only when it changes |
| `len(cache)`, `ticker in cache` | Convenience |

## MarketDataSource (abstract)

```python
class MarketDataSource(ABC):
    async def start(self, tickers: list[str]) -> None: ...   # once; spawns background task
    async def stop(self) -> None: ...                         # idempotent; cancels task
    async def add_ticker(self, ticker: str) -> None: ...      # no-op if present; normalizes; ValueError if malformed
    async def remove_ticker(self, ticker: str) -> None: ...   # no-op if absent; also clears cache
    def get_tickers(self) -> list[str]: ...
```

### Ticker helpers (`interface.py`)

- `normalize_ticker(t)`: strip + upper-case. Both sources use it, so `" tsla "` and `"TSLA"` are one ticker.
- `validate_ticker(t)`: normalizes and checks the format `^[A-Z]{1,5}([.-][A-Z]{1,2})?$` (`BRK.B`, `BF-B`); raises `ValueError` otherwise. `add_ticker()` calls it. The watchlist API should map this `ValueError` to HTTP 400.
- `UnknownTickerError(ValueError)`: raised by `MassiveDataSource.add_ticker()` when a poll completed and Massive returned no price for the symbol (the ticker is then not tracked). Map to HTTP 404. The simulator prices any well-formed symbol and never raises it.
- `clean_tickers(list)`: normalize, de-duplicate and drop (with a warning) malformed tickers; used by `start()`.

## Factory

```python
def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()
    if api_key:
        return MassiveDataSource(api_key=api_key, price_cache=price_cache)
    return SimulatorDataSource(price_cache=price_cache)
```

Returns an *unstarted* source. Whitespace-only keys count as unset.

## Implementations

### SimulatorDataSource
- `start()` builds a `GBMSimulator`, **seeds the cache immediately** with seed prices (so SSE has data before the first tick), then loops: `step()` → `cache.update()` for each ticker → sleep 0.5s.
- `add_ticker()` adds to the simulator and seeds the cache right away. The seed price is stored as the cache's `reference_price` (daily-change baseline).
- Step exceptions are logged and the loop continues.
- `GBMSimulator` and `SimulatorDataSource` accept an optional `seed` for reproducible paths (E2E tests); each simulator owns its RNGs instead of using the global `random`/`numpy` state.

### MassiveDataSource
- `start()` creates the `RESTClient`, does an **immediate first poll** (cache populated before the app serves), then polls every `poll_interval` (default **15s**, safe for the free tier; use 2-5s on paid plans). The first poll is bounded by `FIRST_POLL_TIMEOUT` (10s): if the network is slow, startup continues and the loop retries immediately.
- Each poll is **one** `get_snapshot_all(STOCKS, tickers=[...])` call run through `asyncio.to_thread` (the client is synchronous), given a **copy** of the ticker list.
- `parse_snapshot()` maps a `TickerSnapshot` to a price using the fallback chain `last_trade.price` → `min.close` → `day.close` → `prev_day.close` (zero/missing values are skipped). The timestamp is `last_trade.sip_timestamp` (nanoseconds; `participant_timestamp`, `trf_timestamp` and `updated` are fallbacks) normalized by `to_epoch_seconds()`; bar-based prices use the time of the poll. `prev_day.close` becomes the `reference_price`.
- Tickers removed while a request is in flight are not written back to the cache.
- Per-snapshot errors (`AttributeError`/`TypeError`/`ValueError`) skip that ticker only; whole-poll errors (401/403/network) are logged and retried next interval. A **429 / rate-limit error doubles the interval** (capped at `MAX_BACKOFF` = 120s) and the next successful poll restores it.
- `add_ticker()` validates the symbol, then triggers an **out-of-band poll** so the ticker prices immediately, but only if the last request was at least `MIN_POLL_GAP` (12s) ago (free tier: 5 calls/min). If the poll ran and Massive returned nothing for the symbol, it raises `UnknownTickerError` and does not track it. If no poll could be made (too soon, network failure), the ticker is accepted and prices on the next scheduled poll, so callers should still handle `cache.get_price(t) is None`.
- `remove_ticker()` also removes the cache entry.

## SSE integration

`create_stream_router(cache)` serves `GET /api/stream/prices`:
- Emits `retry: 1000` once, then every 0.5s checks `cache.version`; if changed, sends one event containing **all** tickers: `data: {"AAPL": {ticker, price, previous_price, timestamp, change, change_percent, direction, reference_price, day_change_percent}, ...}`.
- When nothing has been sent for 10s it emits a `: ping` comment (ignored by `EventSource`) so proxies do not drop the idle connection.
- `create_stream_router(cache, interval=0.5, heartbeat=10.0)` builds a **new** `APIRouter` on every call, so it is safe to call from app factories and tests.
- Stops when the client disconnects. Sends `X-Accel-Buffering: no` and `Cache-Control: no-cache`.
- With Massive at 15s polling the version only changes every ~15s, so data events are ~15s apart (heartbeats fill the gaps). The frontend should use `EventSource` `onopen`/`onerror` for the connection dot and not treat a lack of data events as a disconnect.

## Lifecycle (FastAPI)

```python
cache = PriceCache()
source = create_market_data_source(cache)

@asynccontextmanager
async def lifespan(app):
    await source.start(load_watchlist_tickers())     # from SQLite
    yield
    await source.stop()

app.include_router(create_stream_router(cache))
```

- Watchlist POST/DELETE → `source.add_ticker()` / `source.remove_ticker()` (in addition to the DB write).
- Trade execution → `cache.get_price(ticker)`; reject if `None` (unknown/unpriced ticker).
- Portfolio valuation → `cache.get_all()`.

## Testing

`backend/tests/market/`: models, cache (validation, baseline, thread safety), interface helpers, simulator (statistics, correlation, seeding), simulator source, factory, Massive (snapshots built with the real `TickerSnapshot.from_dict`, not mocks; covers fallbacks, nanosecond timestamps, the remove-during-poll race, backoff, unknown tickers, startup timeout), SSE (`test_stream.py`), and a shared contract test run against both sources. Run `cd backend && uv run --extra dev pytest`. Factory tests clear `MASSIVE_API_KEY` with `patch.dict(os.environ, ..., clear=True)` so they are independent of a developer's environment. One optional live test is skipped unless `MASSIVE_API_KEY` is set.

## Resolved issues and remaining follow-ups

The 2026-11-04 review (`MARKET_DATA_REVIEW.md`) found that the original Massive path dropped every snapshot against the real `massive` client. Resolved: wrong attribute name (`sip_timestamp`), nanosecond timestamps, missing-trade fallbacks, removed-ticker race, shared-list mutation, startup blocking, unknown-ticker feedback, 429 backoff, fast add, SSE router duplication and heartbeat, ticker normalization/validation, price validation, previous-close baseline, seedable simulator.

Remaining for the integration work (not market-layer code):

1. **Protect held tickers.** The watchlist route must not call `source.remove_ticker()` for a ticker with an open position (and should call it when a position is closed and the ticker is no longer watched), otherwise portfolio valuation loses its price.
2. **HTTP mapping.** `ValueError` (malformed) → 400, `UnknownTickerError` → 404, `cache.get_price(t) is None` on a trade → 400 "price not yet available".
3. **Live verification.** Run the optional live smoke test with a real key (`MASSIVE_API_KEY=... uv run --extra dev pytest tests/market/test_massive.py -k live`) to confirm real payloads; the unit tests use the real model classes but not real network data.
4. **End-of-day helper (optional).** If daily history beyond `prevDay.c` is ever needed, add a separate helper using `get_grouped_daily_aggs` (see `MASSIVE_API.md` §4a).
