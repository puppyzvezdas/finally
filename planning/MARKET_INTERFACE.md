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

    # derived properties
    change: float                         # round(price - previous_price, 4)
    change_percent: float                 # % vs previous_price (0.0 if previous is 0)
    direction: str                        # "up" | "down" | "flat"

    def to_dict(self) -> dict: ...        # JSON/SSE shape, includes derived fields
```

`previous_price` is the **previous update's price** (not the prior close). It drives the green/red flash, so it behaves identically for both sources. First update for a ticker has `previous_price == price` (direction `flat`).

## PriceCache

Thread-safe (`threading.Lock`), because the Massive client runs in a worker thread.

| Method | Description |
|---|---|
| `update(ticker, price, timestamp=None) -> PriceUpdate` | Record a price (rounded to 2dp); computes `previous_price`; bumps `version` |
| `get(ticker) -> PriceUpdate \| None` | Latest update |
| `get_price(ticker) -> float \| None` | Just the price |
| `get_all() -> dict[str, PriceUpdate]` | Shallow copy of everything |
| `remove(ticker)` | Drop a ticker (watchlist removal) |
| `version` (property) | Monotonic counter; SSE sends only when it changes |
| `len(cache)`, `ticker in cache` | Convenience |

## MarketDataSource (abstract)

```python
class MarketDataSource(ABC):
    async def start(self, tickers: list[str]) -> None: ...   # once; spawns background task
    async def stop(self) -> None: ...                         # idempotent; cancels task
    async def add_ticker(self, ticker: str) -> None: ...      # no-op if present
    async def remove_ticker(self, ticker: str) -> None: ...   # no-op if absent; also clears cache
    def get_tickers(self) -> list[str]: ...
```

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
- `add_ticker()` adds to the simulator and seeds the cache right away.
- Step exceptions are logged and the loop continues.

### MassiveDataSource
- `start()` creates the `RESTClient`, does an **immediate first poll** (cache populated before the app serves), then polls every `poll_interval` (default **15s**, safe for the free tier; use 2-5s on paid plans).
- Each poll is **one** `get_snapshot_all(STOCKS, tickers=[...])` call run through `asyncio.to_thread` (the client is synchronous).
- Per-snapshot errors (`AttributeError`/`TypeError`, e.g. a missing `last_trade`) skip that ticker only; whole-poll errors (401/403/429/network) are logged and retried next interval.
- `add_ticker()` appears in the cache on the **next poll** (up to 15s). If immediate feedback matters, trigger an out-of-band `_poll_once()` (see follow-ups).
- `remove_ticker()` also removes the cache entry.

## SSE integration

`create_stream_router(cache)` serves `GET /api/stream/prices`:
- Emits `retry: 1000` once, then every 0.5s checks `cache.version`; if changed, sends one event containing **all** tickers: `data: {"AAPL": {ticker, price, previous_price, timestamp, change, change_percent, direction}, ...}`.
- Stops when the client disconnects. Sends `X-Accel-Buffering: no` and `Cache-Control: no-cache`.
- With Massive at 15s polling the version only changes every ~15s, so the stream is effectively quiet between polls. The frontend should not treat silence as a disconnect.

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

`backend/tests/market/`: models, cache, simulator, simulator source, factory, and Massive (with the client mocked). Run `cd backend && uv run pytest`. The factory test should clear `MASSIVE_API_KEY` via `monkeypatch` so it is independent of the developer's `.env`.

## Known issues and recommended follow-ups

Found while researching the API for this document:

1. **Timestamp unit bug (Massive).** `massive_client.py` does `snap.last_trade.timestamp / 1000.0`, but snapshot `lastTrade.t` is in **nanoseconds** (per the API's sample payloads), so stored timestamps would be ~1,000,000× too large. Fix by normalizing with `to_epoch_seconds()` (defined in `MASSIVE_API.md` §6) and confirm with a real key.
2. **Missing-trade fallback.** On plans without trade data, or pre-market, `last_trade` is absent and the ticker is skipped forever. Fall back to `min.close` → `day.close` → `prev_day.close`.
3. **Unknown-ticker feedback.** Massive silently omits invalid symbols. The watchlist API should validate (e.g. wait for the next poll or call the single-ticker snapshot) instead of accepting a ticker that will never price.
4. **Faster add.** Trigger a poll immediately in `MassiveDataSource.add_ticker()` so newly added tickers show a price without waiting up to 15s (mind the free-tier 5/min budget).
5. **Backoff on 429.** Currently it just waits for the next interval; consider doubling the interval after a rate-limit error and recovering after a success.
6. **End-of-day helper (optional).** If daily history or previous close is needed for "daily change %", add a small `EodPrices` helper using `get_grouped_daily_aggs` (see `MASSIVE_API.md` §4a), separate from the streaming interface, as the poller's `previous_price` is tick-to-tick, not prior-close.
