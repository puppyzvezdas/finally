"""Thread-safe in-memory price cache."""

from __future__ import annotations

import math
import time
from threading import Lock

from .models import PriceUpdate


def _valid_price(value: float) -> bool:
    return isinstance(value, (int, float)) and math.isfinite(value) and value > 0


class PriceCache:
    """Thread-safe in-memory cache of the latest price for each ticker.

    Writers: SimulatorDataSource or MassiveDataSource (one at a time).
    Readers: SSE streaming endpoint, portfolio valuation, trade execution.
    """

    def __init__(self) -> None:
        self._prices: dict[str, PriceUpdate] = {}
        self._lock = Lock()
        self._version: int = 0  # Monotonically increasing; bumped on every update

    def update(
        self,
        ticker: str,
        price: float,
        timestamp: float | None = None,
        reference_price: float | None = None,
    ) -> PriceUpdate:
        """Record a new price for a ticker. Returns the created PriceUpdate.

        Automatically computes direction and change from the previous price.
        If this is the first update for the ticker, previous_price == price (direction='flat').

        ``reference_price`` is the daily-change baseline. When omitted, the baseline from the
        ticker's previous update (if any) is carried forward.

        Raises ValueError for a non-finite or non-positive price, so a bad provider value can
        never reach trade execution or portfolio valuation.
        """
        if not _valid_price(price):
            raise ValueError(f"Invalid price for {ticker}: {price!r}")
        if reference_price is not None and not _valid_price(reference_price):
            raise ValueError(f"Invalid reference price for {ticker}: {reference_price!r}")

        with self._lock:
            ts = time.time() if timestamp is None else timestamp
            prev = self._prices.get(ticker)
            previous_price = prev.price if prev else price
            if reference_price is None and prev is not None:
                reference_price = prev.reference_price

            update = PriceUpdate(
                ticker=ticker,
                price=round(price, 2),
                previous_price=round(previous_price, 2),
                timestamp=ts,
                reference_price=None if reference_price is None else round(reference_price, 2),
            )
            self._prices[ticker] = update
            self._version += 1
            return update

    def get(self, ticker: str) -> PriceUpdate | None:
        """Get the latest price for a single ticker, or None if unknown."""
        with self._lock:
            return self._prices.get(ticker)

    def get_all(self) -> dict[str, PriceUpdate]:
        """Snapshot of all current prices. Returns a shallow copy."""
        with self._lock:
            return dict(self._prices)

    def get_price(self, ticker: str) -> float | None:
        """Convenience: get just the price float, or None."""
        update = self.get(ticker)
        return update.price if update else None

    def remove(self, ticker: str) -> None:
        """Remove a ticker from the cache (e.g., when removed from watchlist).

        Bumps the version when something was removed so SSE clients see the removal.
        """
        with self._lock:
            if self._prices.pop(ticker, None) is not None:
                self._version += 1

    @property
    def version(self) -> int:
        """Current version counter. Useful for SSE change detection."""
        with self._lock:
            return self._version

    def __len__(self) -> int:
        with self._lock:
            return len(self._prices)

    def __contains__(self, ticker: str) -> bool:
        with self._lock:
            return ticker in self._prices
