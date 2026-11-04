"""Behavioral contract every MarketDataSource implementation must satisfy.

The same tests run against the simulator and (with its network call patched) the Massive
source, which is what keeps downstream code source-agnostic.
"""

import asyncio
from contextlib import ExitStack
from unittest.mock import patch

import pytest

from app.market.cache import PriceCache
from app.market.massive_client import MassiveDataSource
from app.market.simulator import SimulatorDataSource

from .helpers import NS, snap


def _massive_fetch(tickers):
    return [snap(t, lastTrade={"p": 100.0, "t": NS}) for t in tickers]


@pytest.fixture(params=["simulator", "massive"])
async def started_source(request):
    """Yield (source, cache) with ["AAPL"] already started; always stopped afterwards."""
    cache = PriceCache()
    with ExitStack() as stack:
        if request.param == "simulator":
            source = SimulatorDataSource(price_cache=cache, update_interval=0.05)
        else:
            source = MassiveDataSource(api_key="k", price_cache=cache, poll_interval=60.0)
            stack.enter_context(patch("app.market.massive_client.RESTClient"))
            stack.enter_context(
                patch.object(source, "_fetch_snapshots", side_effect=_massive_fetch)
            )
        await source.start(["AAPL"])
        try:
            yield source, cache
        finally:
            await source.stop()


async def test_start_populates_cache(started_source):
    source, cache = started_source
    assert cache.get_price("AAPL") is not None
    assert source.get_tickers() == ["AAPL"]


async def test_add_ticker_normalizes_and_is_idempotent(started_source):
    source, cache = started_source
    await source.add_ticker(" msft ")
    await source.add_ticker("MSFT")
    assert source.get_tickers() == ["AAPL", "MSFT"]
    # Pricing may be pending: Massive defers the fetch if the rate-limit budget is spent.
    assert cache.get(" msft ") is None and cache.get("msft") is None


async def test_remove_ticker_clears_cache_and_is_idempotent(started_source):
    source, cache = started_source
    await source.add_ticker("MSFT")
    await source.remove_ticker("msft")
    await source.remove_ticker("MSFT")
    assert source.get_tickers() == ["AAPL"]
    assert cache.get("MSFT") is None


async def test_malformed_ticker_rejected(started_source):
    source, _ = started_source
    with pytest.raises(ValueError):
        await source.add_ticker("not a ticker")
    assert source.get_tickers() == ["AAPL"]


async def test_stop_is_idempotent_and_silences_the_source(started_source):
    source, cache = started_source
    await source.stop()
    await source.stop()
    version = cache.version
    await asyncio.sleep(0.15)
    assert cache.version == version  # nothing writes after stop()


async def test_updates_carry_valid_prices(started_source):
    _, cache = started_source
    update = cache.get("AAPL")
    assert update.price > 0 and update.timestamp > 0
    assert update.direction in {"up", "down", "flat"}
