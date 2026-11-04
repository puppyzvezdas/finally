# Market Data Backend: Code Review

**Date:** 2026-11-04
**Scope:** `backend/app/market/` (9 modules) and `backend/tests/market/` (6 test modules), checked against `planning/PLAN.md`, `MARKET_DATA_SUMMARY.md`, `MARKET_INTERFACE.md`, `MARKET_SIMULATOR.md` and `MASSIVE_API.md`.

## 1. Verdict

The simulator path is solid. The **Massive path does not work against the real `massive` client**: every snapshot is dropped, so the cache stays empty when `MASSIVE_API_KEY` is set. The 73 passing tests hide this because they mock the snapshot objects. This must be fixed before the market layer is wired into the app.

## 2. Test and lint results

| Check | Result |
|---|---|
| `uv run --extra dev pytest -v --cov=app` | **73 passed**, 0 failed, 8.0s |
| `ruff check app/ tests/` | All checks passed |
| Coverage | **91%** overall |

Coverage by module: `cache`, `factory`, `interface`, `models`, `seed_prices`, `__init__` 100%; `simulator` 98% (missing L149, 268-269); `massive_client` 94% (missing L85-87, 125); **`stream` 33%** (the SSE generator and route are untested).

Warnings: 73 `DeprecationWarning`s from `tests/conftest.py:11` (`asyncio.DefaultEventLoopPolicy`, removal in Python 3.16).

Environment note: tests ran on Python 3.14.6, but the Dockerfile target in PLAN.md is Python 3.12. `pyproject.toml` allows `>=3.12`. Run the suite on 3.12 before relying on it.

## 3. Findings

Severity: **Critical** breaks a feature, **High** wrong behavior in realistic use, **Medium** latent or edge-case, **Low** hygiene.

### C1. Critical: Massive client reads a field that does not exist (`massive_client.py:103`)

The code does `snap.last_trade.timestamp / 1000.0`. The real `LastTrade` model has no `timestamp` attribute. Its fields are `price`, `size`, `exchange`, `sip_timestamp`, `participant_timestamp`, `trf_timestamp`, and so on. The resulting `AttributeError` is caught by the per-snapshot `except (AttributeError, TypeError)` and logged as a warning, so the failure is quiet.

Reproduced by building a real `TickerSnapshot.from_dict(...)` and running `_poll_once()` on it:

```
Skipping snapshot for AAPL: 'LastTrade' object has no attribute 'timestamp'
cache after real-model poll: None
```

Impact: with a Massive key, the watchlist, trades and portfolio valuation all see no prices. `test_timestamp_conversion` and the other Massive tests pass only because `_make_snapshot` uses `MagicMock`, which accepts any attribute.

Fix: read `sip_timestamp` (nanoseconds), and build test snapshots with the real model classes (`TickerSnapshot.from_dict`) so a wrong attribute name fails the test.

### C2. High: timestamp unit is wrong even once the attribute is fixed (`massive_client.py:102-103`)

The code and `test_timestamp_conversion` assume milliseconds. Snapshot trade timestamps are nanoseconds. Running a nanosecond value through the current logic stores `1605195918306274.0` (about 50 million years). `MASSIVE_API.md` §6 already specifies the fix (`to_epoch_seconds`). Use it, and change the test to use a nanosecond value.

### H1. High: tickers with no `lastTrade` are skipped forever (`massive_client.py:101`)

With `lastTrade` absent (plan without trades data, pre-market, illiquid names), `TickerSnapshot.last_trade` is `None`, and the ticker is skipped on every poll. `MASSIVE_API.md` §7 specifies the fallback chain `lastTrade.p` → `min.c` → `day.c` → `prevDay.c`. It is not implemented. Verified: a snapshot with only `day.c` gives `last_trade is None` while `day.close == 190.0` is available.

### H2. High: removed tickers can reappear in the cache (`massive_client.py:72-76, 97-109`)

`_poll_once` awaits a thread, then writes every returned snapshot. If `remove_ticker()` runs while a poll is in flight, the poll still writes the removed ticker back. Reproduced:

```
after remove, cache has AAPL: 1.0 | tickers: []
```

The stale entry then shows in SSE and in `get_all()` until restart. Fix: after the fetch, drop snapshots whose ticker is no longer in `self._tickers`.

### H3. High: first poll blocks startup (`massive_client.py:46`)

`start()` awaits `_poll_once()` before returning. `_poll_once` catches errors, so it will not crash, but a slow or unreachable network keeps FastAPI's lifespan startup waiting for the client's timeouts and retries. Consider a bounded `asyncio.wait_for` or run the first poll as a task. The simulator is unaffected.

### H4. High: SSE route registration is not idempotent (`stream.py:17, 26`)

`router` is created at module level and `create_stream_router` adds a route to it each call. Reproduced: two calls give two `/api/stream/prices` routes on the shared router, and the second one's closure holds a different cache. Tests or app factories that call it twice will misbehave. Create the `APIRouter` inside the function. (Previously reported as low; it is more likely to bite once an app factory and tests both call it.)

### M1. Medium: ticker input is not normalized or validated consistently

`MassiveDataSource` upper-cases and strips; `SimulatorDataSource` does not. Verified: adding `"aapl"` and `" tsla "` to the simulator yields tickers `['AAPL', 'aapl', ' tsla ']`, creating a duplicate `aapl` with a random seed price and a `" tsla "` entry. No validation exists for format or length. The watchlist API will pass user and LLM text through, so normalize once in the base interface or the API layer, and validate (for example `^[A-Z]{1,5}$`, allowing `.`/`-` classes if needed).

### M2. Medium: `PriceCache.update` accepts invalid prices (`cache.py:23-42`)

Verified: `NaN` and negative prices are stored (`nan flat`, `-5.0`). A bad provider value would then reach trade execution and portfolio valuation. Reject non-finite or non-positive prices. Also `timestamp or time.time()` treats `0.0` as missing; use `if timestamp is None`.

### M3. Medium: unknown Massive tickers are accepted silently

Massive omits invalid symbols from the response. `add_ticker()` returns success, the ticker never prices, and a later trade fails with a "price not available" error. Validate on add (single-ticker snapshot or wait for the next poll). Also tracked in `MARKET_INTERFACE.md` follow-up 3.

### M4. Medium: no rate-limit handling

A 429 is logged and retried at the normal interval. On the free tier (5 calls/min) every `add_ticker` plus polling can exceed the budget. Add exponential backoff on 429 and reset on success. A newly added ticker also waits up to 15s for a price; triggering a poll on add helps, but must be rate-limited.

### M5. Medium: SSE stream has no keep-alive and goes silent with Massive (`stream.py:62-85`)

With a 15s poll the cache version changes every ~15s, so the stream sends nothing in between. Proxies and some browsers can drop idle connections, and the frontend cannot tell silence from a dead link. Send a comment heartbeat (`: ping\n\n`) every ~10-15s, or resend the current snapshot periodically. Each event also carries every ticker, so adding the data volume to the poll cadence is worth watching for large watchlists.

### M6. Medium: shared list mutated while a worker thread reads it (`massive_client.py:67-76, 123-128`)

`_fetch_snapshots` passes `self._tickers` (the live list) to the client in a worker thread, while `add_ticker` appends to it on the event loop. Pass a copy: `tickers=list(self._tickers)`.

### M7. Medium: `previous_price` is tick-to-tick, not previous close

This is documented and correct for the flash effect. But PLAN.md §10 asks the watchlist for "daily change %". Nothing in the layer provides a previous-close baseline. The Massive snapshot has `prevDay.c` and `todaysChangePerc`; the simulator would use its seed price. Decide who supplies it (extend `PriceUpdate` or a separate helper) before building the watchlist UI. Treat as a design gap.

### M8. Medium: open-position tickers are not protected from removal

`MARKET_DATA_DESIGN` (archived) specified keeping a ticker tracked while a position exists. The source API has no such guard, and it must live in the watchlist route. Without it, removing a held ticker deletes its price and breaks portfolio valuation. Keep this on the checklist for the watchlist/portfolio work.

### L1. Low: `PriceCache.version` read without the lock (`cache.py:64-67`)
Safe under the GIL, inconsistent with the rest of the class and fragile on free-threaded builds. Take the lock.

### L2. Low: every event resends all tickers
Fine for 10 tickers at 2 Hz. Revisit if the watchlist grows.

### L3. Low: deprecated event-loop-policy fixture (`tests/conftest.py`)
pytest-asyncio with `asyncio_mode = "auto"` does not need this fixture. Remove it to clear 73 warnings and avoid a break on Python 3.16.

### L4. Low: `massive` is imported eagerly by the factory
`factory.py` imports `massive_client`, which imports `massive` at module level. The simulator-only path therefore needs `massive` installed. It is a declared core dependency, so this is acceptable. The older docs describing a lazy import are stale (`MARKET_INTERFACE.md` and the archived design).

### L5. Low: random events can use more than the simulator's own RNG
`GBMSimulator` uses global `np.random` and `random`, so runs cannot be seeded per instance. Accept an optional seed or `numpy.random.Generator` for deterministic E2E runs (also suggested in `MARKET_SIMULATOR.md`).

## 4. What is good

- Clean strategy pattern; `PriceCache` as the single hand-off point keeps consumers source-agnostic.
- Immutable `PriceUpdate` with derived fields cannot go inconsistent.
- GBM math is correct (Itô-corrected drift, `exp` keeps prices positive). Cholesky of a correlation matrix with off-diagonals in [0.3, 0.6] is always positive definite; the ten default tickers factor cleanly.
- Simulator seeds the cache in `start()` and `add_ticker()`, so there is no blank screen and no trade-price gap.
- Per-step and per-poll exception handling keeps background loops alive; `stop()` is idempotent.
- Test suite is well organized, and the earlier review's items (hatch config, return annotation, public `get_tickers`, unused imports) are all fixed.

## 5. Test gaps

1. **Real-model Massive tests** (blocks C1/C2/H1): build snapshots with `TickerSnapshot.from_dict` using raw API JSON (ns timestamps, missing `lastTrade`). Do not use `MagicMock` for snapshot shape.
2. **SSE endpoint** (stream.py at 33%): test with `httpx.AsyncClient`/ASGI transport that the first chunk is `retry: 1000`, a data event follows a cache update, no event is sent when the version is unchanged, and the generator exits on disconnect.
3. **Race test** for H2 (remove during in-flight poll).
4. **Cache thread-safety**: multiple writer threads, final version equals total updates.
5. **Cache validation** (M2) and **ticker normalization** (M1).
6. **Statistical simulator tests**: log-return std ≈ σ√dt, and AAPL/MSFT correlation exceeds AAPL/JPM.
7. **All ten default tickers** construct a simulator (Cholesky succeeds) and stay positive.
8. **Factory test isolation**: confirm `MASSIVE_API_KEY` is cleared via `monkeypatch` in every factory test so a developer's `.env` cannot change the result.
9. **Live smoke test** (optional, skipped without a key) that runs one real poll and asserts a priced ticker.

## 6. Recommended order of work

1. C1 + C2 + H1: fix attribute name, nanosecond handling, price fallback chain; rewrite Massive tests on real models.
2. H2 + M6: filter stale snapshots after fetch; pass a copy of the ticker list.
3. H4 + M5: build the router inside the factory; add SSE heartbeat; add SSE tests.
4. M1 + M2: normalize/validate tickers; validate cache prices.
5. H3, M3, M4: startup timeout, unknown-ticker feedback, 429 backoff.
6. M7 + M8: decide previous-close source and guard held tickers before the watchlist and portfolio work.
7. L1, L3, L5: small cleanups.

## 7. Housekeeping

- `git status` shows `planning/archive/*` deleted in the working tree. Confirm that is intended; this review replaces the archived `MARKET_DATA_REVIEW.md`, whose open items are listed above where they still apply.
- `MARKET_DATA_SUMMARY.md` says "complete, tested, reviewed, all issues resolved". Given C1, update it once the Massive fixes land.

## 8. Resolution (2026-11-04)

All findings were addressed on branch `claude/market-data-review-fixes`. Result: **185 passed, 1 skipped, 98% coverage, `ruff check` clean, no deprecation warnings**, on Python 3.13 and 3.12.

| Finding | Resolution |
|---|---|
| C1 wrong attribute | `parse_snapshot()` reads `last_trade.sip_timestamp` (then `participant_timestamp`, `trf_timestamp`, `updated`). Tests use `TickerSnapshot.from_dict`; a regression test asserts `LastTrade` has no `timestamp`. |
| C2 timestamp unit | `to_epoch_seconds()` normalizes ns/µs/ms/s; tested with a real nanosecond payload. |
| H1 no `lastTrade` | Fallback chain `last_trade.price` → `min.close` → `day.close` → `prev_day.close`; zero values skipped. |
| H2 removed ticker resurrected | Tracked set is re-read after the fetch; stale snapshots are dropped. Race test with a blocked worker thread. |
| H3 startup blocks | First poll bounded by `FIRST_POLL_TIMEOUT` (10s); on timeout the loop retries immediately. |
| H4 router not idempotent | `APIRouter` is built inside `create_stream_router`. |
| M1 ticker normalization | `normalize_ticker` / `validate_ticker` / `clean_tickers` in `interface.py`, used by both sources. |
| M2 invalid prices | `PriceCache.update` raises `ValueError` for non-finite or non-positive prices; `timestamp is None` check. |
| M3 unknown Massive ticker | `add_ticker` polls (rate-limited) and raises `UnknownTickerError` if Massive returns no price; otherwise accepted as pending. |
| M4 rate limits | Interval doubles on 429 up to 120s and resets on success; immediate poll on add respects a 12s minimum gap. |
| M5 SSE silence | `: ping` heartbeat after 10s without data; tests cover it. |
| M6 shared list | Poll passes a copy of the ticker list to the worker thread. |
| M7 previous close | `PriceUpdate.reference_price` / `day_change_percent`: `prevDay.c` for Massive, start price for the simulator; included in the SSE payload. |
| M8 held tickers | Not a source concern (no watchlist route exists yet). Recorded as a requirement in `MARKET_INTERFACE.md` and `backend/CLAUDE.md`. |
| L1 `version` lock | Read under the lock; `remove()` now also bumps it. |
| L2 resend all tickers | Left as is (fine at this scale). |
| L3 conftest fixture | Removed; deprecation warnings are gone. |
| L4 stale lazy-import docs | Docs updated. |
| L5 seedable RNG | Optional `seed`, per-instance RNGs. |

Test gaps 1-8 are covered by `test_massive.py`, `test_stream.py`, `test_cache.py`, `test_interface.py`, `test_simulator.py`, `test_source_contract.py` and the existing factory tests (which already clear `MASSIVE_API_KEY`). Gap 9 is an optional live test in `test_massive.py`, skipped without a key; it has not been run against the real API.
