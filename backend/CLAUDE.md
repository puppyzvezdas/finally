# Backend — Developer Guide

## Project Setup

```bash
cd backend
uv sync --extra dev   # Install all dependencies including test/lint tools
```

## Market Data API

The market data subsystem lives in `app/market/`. Use these imports:

```python
from app.market import PriceCache, PriceUpdate, MarketDataSource, create_market_data_source
```

### Core Types

- **`PriceUpdate`** — Immutable dataclass: `ticker`, `price`, `previous_price`, `timestamp`, optional `reference_price` (previous close / simulator start price), plus properties `change`, `change_percent`, `day_change_percent` (None without a baseline), `direction` ("up"/"down"/"flat"), and `to_dict()` for JSON serialization.

- **`PriceCache`** — Thread-safe in-memory store. Key methods:
  - `update(ticker, price, timestamp=None, reference_price=None) -> PriceUpdate` (raises `ValueError` for NaN/non-positive prices)
  - `get(ticker) -> PriceUpdate | None`
  - `get_price(ticker) -> float | None`
  - `get_all() -> dict[str, PriceUpdate]`
  - `remove(ticker)`
  - `version` property — monotonic counter, increments on every update (for SSE change detection)

- **`MarketDataSource`** — Abstract interface implemented by `SimulatorDataSource` and `MassiveDataSource`. Lifecycle: `start(tickers)` -> `add_ticker()` / `remove_ticker()` -> `stop()`.

- **Ticker helpers** — `normalize_ticker`, `validate_ticker` (raises `ValueError`), `UnknownTickerError` (raised by `MassiveDataSource.add_ticker` for unknown symbols). Map to HTTP 400 / 404 in the API layer.

- **`create_market_data_source(cache)`** — Factory. Returns `MassiveDataSource` if `MASSIVE_API_KEY` is set, otherwise `SimulatorDataSource`.

### SSE Streaming

```python
from app.market import create_stream_router

router = create_stream_router(price_cache)  # Returns FastAPI APIRouter
# Endpoint: GET /api/stream/prices (text/event-stream)
```

### Rules for the watchlist/portfolio code

- Never call `source.remove_ticker()` for a ticker with an open position; the price is needed for valuation. Track `watchlist ∪ open positions`.
- Trades: `cache.get_price(t)` can be `None` (Massive prices a new ticker on a later poll); reject with a clear 400, never invent a price.
- Pass the user's text through `validate_ticker()` once at the API boundary.

### Seed Data

Default tickers: AAPL, GOOGL, MSFT, AMZN, TSLA, NVDA, META, JPM, V, NFLX. Seed prices and per-ticker volatility/drift params are in `app/market/seed_prices.py`.

## Running Tests

```bash
uv run --extra dev pytest -v              # All tests
uv run --extra dev pytest --cov=app       # With coverage
uv run --extra dev ruff check app/ tests/ # Lint
uv run --extra dev ruff format app/ tests/ # Format
```

## Demo

```bash
uv run market_data_demo.py   # Live terminal dashboard with simulated prices
```
