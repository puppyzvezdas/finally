"""Massive (Polygon.io) API client for real market data."""

from __future__ import annotations

import asyncio
import logging
import math
import time
from dataclasses import dataclass
from typing import Any

from massive import RESTClient
from massive.rest.models import SnapshotMarketType

from .cache import PriceCache
from .interface import (
    MarketDataSource,
    UnknownTickerError,
    clean_tickers,
    normalize_ticker,
    validate_ticker,
)

logger = logging.getLogger(__name__)

# Free tier allows ~5 requests/minute, so never poll closer together than this.
MIN_POLL_GAP = 12.0
# Upper bound for the poll interval after repeated rate-limit errors.
MAX_BACKOFF = 120.0
# start() waits at most this long for the first poll, so a slow network cannot hold up startup.
FIRST_POLL_TIMEOUT = 10.0


def to_epoch_seconds(ts: int | float) -> float:
    """Normalize an epoch value in s / ms / µs / ns to seconds.

    Snapshot trade timestamps (``sip_timestamp``, ``updated``) are nanoseconds; minute and
    aggregate timestamps are milliseconds.
    """
    ts = float(ts)
    if ts > 1e17:
        return ts / 1e9  # ns
    if ts > 1e14:
        return ts / 1e6  # µs
    if ts > 1e11:
        return ts / 1e3  # ms
    return ts  # s


@dataclass(frozen=True, slots=True)
class ParsedSnapshot:
    """What FinAlly needs from a Massive ``TickerSnapshot``."""

    price: float
    timestamp: float  # Unix seconds
    reference_price: float | None  # previous session close, if present


def _positive(value: Any) -> float | None:
    """Return value as a float if it is a finite number > 0, else None."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value) if math.isfinite(value) and value > 0 else None


def parse_snapshot(snap: Any, now: float | None = None) -> ParsedSnapshot | None:
    """Extract a price from a ``massive.rest.models.TickerSnapshot``.

    Price preference: lastTrade.p -> min.c -> day.c -> prevDay.c. Zero or missing values are
    skipped (the day bar is empty early in the session; lastTrade is absent on plans without
    trade data). Returns None if no usable price exists.
    """
    now = time.time() if now is None else now
    prev_close = _positive(getattr(getattr(snap, "prev_day", None), "close", None))

    trade = getattr(snap, "last_trade", None)
    price = _positive(getattr(trade, "price", None))
    if price is not None:
        raw_ts = (
            getattr(trade, "sip_timestamp", None)
            or getattr(trade, "participant_timestamp", None)
            or getattr(trade, "trf_timestamp", None)
            or getattr(snap, "updated", None)
        )
        ts = to_epoch_seconds(raw_ts) if _positive(raw_ts) else now
        return ParsedSnapshot(price, ts, prev_close)

    for bar_name in ("min", "day", "prev_day"):
        price = _positive(getattr(getattr(snap, bar_name, None), "close", None))
        if price is not None:
            # Bar timestamps describe the bar window, not when we learned the price.
            return ParsedSnapshot(price, now, prev_close)
    return None


class MassiveDataSource(MarketDataSource):
    """MarketDataSource backed by the Massive (Polygon.io) REST API.

    Polls GET /v2/snapshot/locale/us/markets/stocks/tickers for all watched
    tickers in a single API call, then writes results to the PriceCache.

    Rate limits:
      - Free tier: 5 req/min → poll every 15s (default)
      - Paid tiers: higher limits → poll every 2-5s
    On a rate-limit error the interval doubles (up to MAX_BACKOFF) and returns to normal
    after the next successful poll.
    """

    def __init__(
        self,
        api_key: str,
        price_cache: PriceCache,
        poll_interval: float = 15.0,
    ) -> None:
        self._api_key = api_key
        self._cache = price_cache
        self._base_interval = poll_interval
        self._interval = poll_interval  # current interval; grows on rate limiting
        self._tickers: list[str] = []
        self._task: asyncio.Task | None = None
        self._client: RESTClient | None = None
        self._last_poll: float | None = None  # time.monotonic() of the last request
        self._poll_lock = asyncio.Lock()  # one request at a time (loop vs. add_ticker)

    async def start(self, tickers: list[str]) -> None:
        self._client = RESTClient(api_key=self._api_key)
        self._tickers = clean_tickers(tickers)

        # Populate the cache before the app serves, but never wait on a slow network for long.
        first_delay = self._interval
        try:
            await asyncio.wait_for(self._poll_once(), timeout=FIRST_POLL_TIMEOUT)
        except TimeoutError:
            logger.warning(
                "Massive: first poll exceeded %.0fs; continuing startup and retrying now",
                FIRST_POLL_TIMEOUT,
            )
            first_delay = 0.0

        self._task = asyncio.create_task(self._poll_loop(first_delay), name="massive-poller")
        logger.info(
            "Massive poller started: %d tickers, %.1fs interval",
            len(self._tickers),
            self._interval,
        )

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        self._client = None
        logger.info("Massive poller stopped")

    async def add_ticker(self, ticker: str) -> None:
        """Track a ticker; fetch its price now if the rate-limit budget allows.

        Raises UnknownTickerError if Massive answered a poll and did not know the symbol.
        If no poll could be made (rate-limit gap, network error) the ticker is accepted and
        priced by a later poll.
        """
        ticker = validate_ticker(ticker)
        if ticker in self._tickers:
            return
        self._tickers.append(ticker)
        logger.info("Massive: added ticker %s", ticker)

        if self._client is None:
            return
        unpriced = await self._poll_once(min_gap=MIN_POLL_GAP)
        if unpriced is not None and ticker in unpriced:
            self._tickers = [t for t in self._tickers if t != ticker]
            self._cache.remove(ticker)
            raise UnknownTickerError(f"Massive has no price data for {ticker}")

    async def remove_ticker(self, ticker: str) -> None:
        ticker = normalize_ticker(ticker)
        self._tickers = [t for t in self._tickers if t != ticker]
        self._cache.remove(ticker)
        logger.info("Massive: removed ticker %s", ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    # --- Internal ---

    async def _poll_loop(self, first_delay: float) -> None:
        """Poll on interval; the interval is re-read each cycle so backoff takes effect."""
        await asyncio.sleep(first_delay)
        while True:
            await self._poll_once()
            await asyncio.sleep(self._interval)

    async def _poll_once(self, min_gap: float = 0.0) -> set[str] | None:
        """Execute one poll cycle: fetch snapshots, update cache.

        Returns the set of requested tickers that came back without a usable price, or None
        if no poll happened (nothing to poll, too soon after the last request, or failure).
        """
        async with self._poll_lock:
            if not self._tickers or not self._client:
                return None
            if (
                min_gap
                and self._last_poll is not None
                and time.monotonic() - self._last_poll < min_gap
            ):
                return None

            requested = list(self._tickers)  # copy: the worker thread must not see mutation
            self._last_poll = time.monotonic()
            try:
                # The Massive RESTClient is synchronous — run it in a thread.
                snapshots = await asyncio.to_thread(self._fetch_snapshots, requested)
            except Exception as e:
                self._on_failure(e)
                return None

            self._interval = self._base_interval  # success: back to the normal cadence
            tracked = set(self._tickers)  # re-read: tickers may have been removed in flight
            priced: set[str] = set()
            for snap in snapshots:
                ticker = normalize_ticker(getattr(snap, "ticker", None) or "")
                try:
                    parsed = parse_snapshot(snap)
                    if parsed is None:
                        logger.warning("Massive: no usable price for %s", ticker or "???")
                        continue
                    if ticker not in tracked:
                        continue  # removed (or never requested): do not resurrect it
                    self._cache.update(
                        ticker=ticker,
                        price=parsed.price,
                        timestamp=parsed.timestamp,
                        reference_price=parsed.reference_price,
                    )
                    priced.add(ticker)
                except (AttributeError, TypeError, ValueError) as e:
                    logger.warning("Massive: skipping snapshot for %s: %s", ticker or "???", e)

            unpriced = set(requested) - priced
            if unpriced:
                logger.warning(
                    "Massive: no price for %s (unknown symbol or not on this plan)",
                    sorted(unpriced),
                )
            logger.debug("Massive poll: updated %d/%d tickers", len(priced), len(requested))
            return unpriced

    def _fetch_snapshots(self, tickers: list[str]) -> list:
        """Synchronous call to the Massive REST API. Runs in a thread."""
        return self._client.get_snapshot_all(
            market_type=SnapshotMarketType.STOCKS,
            tickers=tickers,
        )

    def _on_failure(self, exc: Exception) -> None:
        """Log a failed poll; back off if we were rate limited. Never raises."""
        text = str(exc).lower()
        if "429" in text or "too many" in text or "rate limit" in text or "exceeded" in text:
            ceiling = max(MAX_BACKOFF, self._base_interval)
            self._interval = min(self._interval * 2, ceiling)
            logger.error("Massive rate limited; next poll in %.0fs", self._interval)
        else:
            # 401/403 (bad key / plan), network errors, 5xx: retry on the normal interval.
            logger.error("Massive poll failed: %s", exc)
