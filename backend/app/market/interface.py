"""Abstract interface for market data sources."""

from __future__ import annotations

import logging
import re
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)

# 1-5 letters, optionally followed by a class suffix (BRK.B, BF-B).
_TICKER_RE = re.compile(r"^[A-Z]{1,5}(?:[.\-][A-Z]{1,2})?$")


class UnknownTickerError(ValueError):
    """Raised by add_ticker() when the provider definitively does not know the symbol."""


def normalize_ticker(ticker: str) -> str:
    """Canonical cache/watchlist key: stripped and upper-cased."""
    return ticker.strip().upper()


def validate_ticker(ticker: str) -> str:
    """Normalize a ticker and check its format. Raises ValueError if it is malformed."""
    normalized = normalize_ticker(ticker)
    if not _TICKER_RE.match(normalized):
        raise ValueError(f"Invalid ticker symbol: {ticker!r}")
    return normalized


def clean_tickers(tickers: list[str]) -> list[str]:
    """Normalize a list of tickers, dropping duplicates and (with a warning) malformed ones."""
    cleaned: dict[str, None] = {}
    for raw in tickers:
        try:
            cleaned[validate_ticker(raw)] = None
        except ValueError:
            logger.warning("Ignoring invalid ticker %r", raw)
    return list(cleaned)


class MarketDataSource(ABC):
    """Contract for market data providers.

    Implementations push price updates into a shared PriceCache on their own
    schedule. Downstream code never calls the data source directly for prices —
    it reads from the cache.

    Lifecycle:
        source = create_market_data_source(cache)
        await source.start(["AAPL", "GOOGL", ...])
        # ... app runs ...
        await source.add_ticker("TSLA")
        await source.remove_ticker("GOOGL")
        # ... app shutting down ...
        await source.stop()
    """

    @abstractmethod
    async def start(self, tickers: list[str]) -> None:
        """Begin producing price updates for the given tickers.

        Starts a background task that periodically writes to the PriceCache.
        Must be called exactly once. Calling start() twice is undefined behavior.
        """

    @abstractmethod
    async def stop(self) -> None:
        """Stop the background task and release resources.

        Safe to call multiple times. After stop(), the source will not write
        to the cache again.
        """

    @abstractmethod
    async def add_ticker(self, ticker: str) -> None:
        """Add a ticker to the active set. No-op if already present.

        The ticker is normalized (stripped, upper-cased). Raises ValueError for a malformed
        symbol and UnknownTickerError if the provider definitively rejects it. A provider that
        cannot tell yet (rate-limited, request failed) accepts the ticker and prices it on a
        later update, so callers should still handle ``cache.get_price(t) is None``.
        """

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker from the active set. No-op if not present.

        Also removes the ticker from the PriceCache.
        """

    @abstractmethod
    def get_tickers(self) -> list[str]:
        """Return the current list of actively tracked tickers."""
