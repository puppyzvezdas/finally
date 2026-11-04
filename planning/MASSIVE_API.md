# Massive API Reference (formerly Polygon.io)

Research notes and code examples for the Massive REST API, covering real-time (snapshot) and end-of-day (aggregate) prices for **multiple tickers**. This is the reference for `backend/app/market/massive_client.py`.

> Polygon.io rebranded to Massive.com on 2025-10-30. Existing keys and the legacy host `api.polygon.io` keep working. Sources: massive.com/docs/rest/*, github.com/massive-com/client-python. Items marked **(verify)** could not be confirmed from official docs and should be checked against a real key.

## 1. Basics

| Item | Value |
|---|---|
| Base URL | `https://api.massive.com` |
| Auth | `Authorization: Bearer <KEY>` header, or `?apiKey=<KEY>` query param |
| Python package | `massive` (`uv add massive`), imports as `from massive import RESTClient` |
| Python version | 3.10+ |
| Response envelope | JSON with `status`, `request_id`, `count`/`resultsCount`, `results` (or `tickers` for snapshots), optional `next_url` |
| Timestamps | Mostly Unix **milliseconds**, but snapshot `lastTrade.t` / `lastQuote.t` / `updated` are Unix **nanoseconds** (see §6) |

### Rate limits and data recency

| Plan | Limit | Data recency |
|---|---|---|
| Free ("Basic") | ~5 requests/minute | End-of-day / delayed **(verify)** |
| Paid (Starter, Developer) | Effectively unlimited (fair use; stay under ~100 req/s) | 15-minute delayed |
| Paid (Advanced, Business) | Effectively unlimited | Real-time |

Plan names and prices change; the official page did not expose limits when fetched. Treat the table as guidance and confirm at massive.com/pricing. Some fields are plan-gated: `lastTrade`/`lastQuote` appear only if the plan includes trades/quotes, and `fmv` only on Business.

**Design consequence:** on the free tier the only workable pattern is *one batched call per poll* at ≥12s intervals (we default to 15s). Never loop one request per ticker.

## 2. Client setup

```python
from massive import RESTClient

client = RESTClient(api_key="...")        # or reads MASSIVE_API_KEY from the environment
```

Client notes:
- The client is **synchronous** (urllib3). Inside asyncio, wrap calls in `asyncio.to_thread(...)`.
- `list_*` methods return an iterator that **auto-paginates** by following `next_url`. Pass `RESTClient(pagination=False)` to get only the first page. `limit` is the page size.
- Retries on transient errors are built in. `maxsize` / `connection_pool_kw` tune the connection pool.

## 3. Real-time prices for many tickers: Snapshot (primary endpoint)

One call returns the latest state for every requested ticker. This is what the poller uses.

```
GET /v2/snapshot/locale/us/markets/stocks/tickers?tickers=AAPL,MSFT,TSLA
```

| Param | Notes |
|---|---|
| `tickers` | Comma-separated, **case-sensitive** (upper-case). Omit to receive *every* US stock (huge payload; avoid). |
| `include_otc` | Default `false`. |

### Raw JSON response

```json
{
  "status": "OK",
  "count": 2,
  "tickers": [
    {
      "ticker": "AAPL",
      "todaysChange": -1.25,
      "todaysChangePerc": -0.65,
      "updated": 1605195918306274000,
      "day":     { "o": 191.2, "h": 192.8, "l": 189.9, "c": 190.1, "v": 41200000, "vw": 191.0 },
      "min":     { "o": 190.0, "h": 190.2, "l": 189.9, "c": 190.1, "v": 52000, "vw": 190.05,
                   "av": 41200000, "n": 310, "t": 1684428720000 },
      "prevDay": { "o": 190.5, "h": 192.0, "l": 189.0, "c": 191.35, "v": 55000000, "vw": 190.7 },
      "lastTrade": { "p": 190.1, "s": 100, "x": 4, "i": "52983", "c": [14, 41], "t": 1605195918306274000 },
      "lastQuote": { "p": 190.09, "s": 3, "P": 190.11, "S": 2, "t": 1605195918507251700 }
    }
  ]
}
```

Field key (short names in the raw JSON):

| Object | Field | Meaning |
|---|---|---|
| `day` | `o h l c v vw` | Today's bar so far: open/high/low/close/volume/VWAP |
| `prevDay` | `o h l c v vw` | Previous session's bar. `prevDay.c` = previous close |
| `lastTrade` | `p` price, `s` size, `x` exchange id, `t` timestamp (**ns**) | Most recent trade |
| `lastQuote` | `p` bid, `s` bid size, `P` ask, `S` ask size, `t` (**ns**) | Most recent NBBO quote |
| `min` | `o h l c v vw av n t` | Latest minute bar; `av` = accumulated volume, `t` in **ms** |
| top level | `todaysChange`, `todaysChangePerc`, `updated` (**ns**) | Change vs previous close |

Snapshot data is cleared at ~3:30 AM ET and repopulates from ~4:00 AM ET, so `day` can be empty/zero pre-market. Always fall back (see §7).

### Python client

The client maps the short JSON names to attribute names:

```python
from massive import RESTClient
from massive.rest.models import SnapshotMarketType

client = RESTClient()
snaps = client.get_snapshot_all(
    market_type=SnapshotMarketType.STOCKS,
    tickers=["AAPL", "GOOGL", "MSFT"],
)
for s in snaps:                                   # list of TickerSnapshot
    print(s.ticker,
          s.last_trade.price,                     # lastTrade.p
          s.last_trade.timestamp,                 # lastTrade.t  (ns! see §6)
          s.prev_day.close,                       # prevDay.c
          s.todays_change_percent)                # todaysChangePerc
```

> Attribute names (`last_trade.price`, `prev_day.close`, `todays_change_percent`) follow the client's model classes; confirm with `dir(snap)` on first run, as the model layer is generated from the OpenAPI spec and has changed between versions.

### Raw HTTP equivalent (no client; useful for debugging)

```python
import httpx, os

r = httpx.get(
    "https://api.massive.com/v2/snapshot/locale/us/markets/stocks/tickers",
    params={"tickers": "AAPL,MSFT,TSLA"},
    headers={"Authorization": f"Bearer {os.environ['MASSIVE_API_KEY']}"},
    timeout=10,
)
r.raise_for_status()
for t in r.json()["tickers"]:
    print(t["ticker"], t["lastTrade"]["p"], t["prevDay"]["c"])
```

### Single-ticker snapshot

`GET /v2/snapshot/locale/us/markets/stocks/tickers/{ticker}`; same object shape wrapped as `{"ticker": {...}}`.

```python
snap = client.get_snapshot_ticker(SnapshotMarketType.STOCKS, "AAPL")
```

## 4. End-of-day prices

### 4a. Grouped daily bars: EOD for the whole market in one call

Best way to get EOD closes for many tickers: one request returns every US stock for a date; filter locally.

```
GET /v2/aggs/grouped/locale/us/market/stocks/{date}?adjusted=true
```

Response `results[]`: `T` ticker, `o h l c`, `v`, `vw`, `n` (trade count), `t` (ms, **end** of window).

```python
grouped = client.get_grouped_daily_aggs("2025-11-03", adjusted=True)   # list of bars
wanted = {"AAPL", "MSFT", "NVDA"}
closes = {b.ticker: b.close for b in grouped if b.ticker in wanted}
```

Returns an empty `results` on weekends/holidays; walk back to the previous trading day.

### 4b. Previous day bar (one ticker)

```
GET /v2/aggs/ticker/{ticker}/prev?adjusted=true
```

```python
prev = client.get_previous_close_agg("AAPL")      # list with one bar
print(prev[0].close, prev[0].open, prev[0].volume)
```

Raw `results[0]`: `{"T":"AAPL","o":..,"h":..,"l":..,"c":..,"v":..,"vw":..,"n":..,"t":..}`.

### 4c. Custom aggregates (bars over a range, one ticker)

Use for history/daily charts. Loop over tickers yourself (one call each), so mind the free-tier limit.

```
GET /v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{from}/{to}
    ?adjusted=true&sort=asc&limit=50000
```

`timespan`: `minute|hour|day|week|month|quarter|year`. `from`/`to`: `YYYY-MM-DD` or ms timestamps. `limit` max 50000 (default 5000). Bar fields: `o h l c v vw t n`; `t` is the **start** of the window in ms.

```python
bars = list(client.list_aggs(
    ticker="AAPL", multiplier=1, timespan="day",
    from_="2025-10-01", to="2025-10-31", limit=50000,
))
for b in bars:
    print(b.timestamp, b.open, b.high, b.low, b.close, b.volume)
```

## 5. Other price endpoints

| Purpose | Endpoint | Client call |
|---|---|---|
| Last trade (1 ticker) | `GET /v2/last/trade/{ticker}` | `client.get_last_trade(ticker="AAPL")` |
| Last NBBO quote (1 ticker) | `GET /v2/last/nbbo/{ticker}` | `client.get_last_quote(ticker="AAPL")` |

Both are per-ticker, so they are not suitable for polling a watchlist. Use the batched snapshot.

## 6. Gotchas

1. **Timestamp units differ per field.** `lastTrade.t`, `lastQuote.t` and `updated` are **nanoseconds** (19 digits); `min.t` and aggregate `t` are **milliseconds** (13 digits). Normalize defensively:

   ```python
   def to_epoch_seconds(ts: int | float) -> float:
       """Accept s / ms / us / ns epoch values and return seconds."""
       ts = float(ts)
       if ts > 1e17: return ts / 1e9     # ns
       if ts > 1e14: return ts / 1e6     # us
       if ts > 1e11: return ts / 1e3     # ms
       return ts                          # s
   ```

   `backend/app/market/massive_client.py` currently does `timestamp / 1000.0`, which is only right for ms; see the follow-up note in MARKET_INTERFACE.md.
2. **`lastTrade` may be missing** (plan doesn't include trades, or pre-market with no prints). Fall back to `min.c`, then `day.c`, then `prevDay.c`.
3. **Tickers are case-sensitive**; upper-case before sending. Unknown tickers are simply absent from the response, so diff the request against the response to detect bad symbols.
4. **Prices outside regular hours** are reflected in `lastTrade` (pre/after-market) but `day` resets and may be zero early in the day.
5. **Adjusted by default** for splits; pass `adjusted=false` for raw prices.
6. **Errors:** 401 bad key, 403 plan does not include the endpoint, 429 rate-limited, 5xx server. The client raises an exception for non-2xx. The poller must catch, log and retry on the next tick, never crash the loop.

## 7. How FinAlly maps Massive data to `PriceUpdate`

| FinAlly field | Source (preferred → fallback) |
|---|---|
| `price` | `lastTrade.p` → `min.c` → `day.c` → `prevDay.c` |
| `timestamp` (s) | `to_epoch_seconds(lastTrade.t)` → `updated` → `time.time()` |
| `previous_price` | Cache-derived: the previous poll's price (keeps SSE flash semantics identical to the simulator) |
| Day change % (optional, future) | `todaysChangePerc`, or `(price - prevDay.c) / prevDay.c` |

The poller (one batched snapshot call per interval, run via `asyncio.to_thread`) is documented in MARKET_INTERFACE.md.

## 8. Minimal end-to-end example

```python
import asyncio
from massive import RESTClient
from massive.rest.models import SnapshotMarketType

TICKERS = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "NVDA", "META", "JPM", "V", "NFLX"]

async def main() -> None:
    client = RESTClient()                                  # MASSIVE_API_KEY from env
    while True:
        snaps = await asyncio.to_thread(
            client.get_snapshot_all, SnapshotMarketType.STOCKS, TICKERS
        )
        for s in snaps:
            print(f"{s.ticker:5} {s.last_trade.price:>9.2f}")
        await asyncio.sleep(15)                            # free tier: ≥12s

asyncio.run(main())
```
