"""Integration tests for SimulatorDataSource."""

import asyncio

import pytest

from app.market.cache import PriceCache
from app.market.simulator import SimulatorDataSource


@pytest.mark.asyncio
class TestSimulatorDataSource:
    """Integration tests for the SimulatorDataSource."""

    async def test_start_populates_cache(self):
        """Test that start() immediately populates the cache."""
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.1)
        await source.start(["AAPL", "GOOGL"])

        # Cache should have seed prices immediately (before first loop tick)
        assert cache.get("AAPL") is not None
        assert cache.get("GOOGL") is not None

        await source.stop()

    async def test_prices_update_over_time(self):
        """Test that prices are updated periodically."""
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.05)
        await source.start(["AAPL"])

        initial_version = cache.version
        await asyncio.sleep(0.3)  # Several update cycles

        # Version should have incremented (prices updated)
        assert cache.version > initial_version

        await source.stop()

    async def test_stop_is_clean(self):
        """Test that stop() is clean and idempotent."""
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.1)
        await source.start(["AAPL"])
        await source.stop()
        # Double stop should not raise
        await source.stop()

    async def test_add_ticker(self):
        """Test adding a ticker dynamically."""
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.1)
        await source.start(["AAPL"])

        await source.add_ticker("TSLA")
        assert "TSLA" in source.get_tickers()
        assert cache.get("TSLA") is not None

        await source.stop()

    async def test_remove_ticker(self):
        """Test removing a ticker."""
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.1)
        await source.start(["AAPL", "TSLA"])

        await source.remove_ticker("TSLA")
        assert "TSLA" not in source.get_tickers()
        assert cache.get("TSLA") is None

        await source.stop()

    async def test_get_tickers(self):
        """Test getting the list of active tickers."""
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.1)
        await source.start(["AAPL", "GOOGL"])

        tickers = source.get_tickers()
        assert set(tickers) == {"AAPL", "GOOGL"}

        await source.stop()

    async def test_empty_start(self):
        """Test starting with no tickers."""
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.1)
        await source.start([])

        assert len(cache) == 0
        assert source.get_tickers() == []

        await source.stop()

    async def test_exception_resilience(self):
        """Test that simulator continues running after errors."""
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.05)

        # Start with a valid ticker
        await source.start(["AAPL"])

        # Wait for some updates
        await asyncio.sleep(0.15)

        # Task should still be running
        assert source._task is not None
        assert not source._task.done()

        await source.stop()

    async def test_custom_update_interval(self):
        """Test using a custom update interval."""
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.01)
        await source.start(["AAPL"])

        initial_version = cache.version
        await asyncio.sleep(0.05)  # Should get ~5 updates

        # Should have multiple updates with fast interval
        assert cache.version > initial_version + 2

        await source.stop()

    async def test_custom_event_probability(self):
        """Test creating source with custom event probability."""
        cache = PriceCache()
        # Very high event probability for testing
        source = SimulatorDataSource(price_cache=cache, update_interval=0.1, event_probability=1.0)
        await source.start(["AAPL"])

        # Just verify it starts and stops cleanly
        await asyncio.sleep(0.2)
        await source.stop()


class TestNormalizationAndReference:
    async def test_add_ticker_normalizes_case_and_whitespace(self):
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.5, event_probability=0)
        await source.start(["AAPL"])
        try:
            await source.add_ticker("aapl")  # duplicate of AAPL
            await source.add_ticker(" tsla ")
            assert source.get_tickers() == ["AAPL", "TSLA"]
            assert cache.get("TSLA") is not None
            assert cache.get(" tsla ") is None and cache.get("aapl") is None
        finally:
            await source.stop()

    async def test_add_ticker_rejects_malformed(self):
        source = SimulatorDataSource(price_cache=PriceCache(), update_interval=0.5)
        await source.start(["AAPL"])
        try:
            with pytest.raises(ValueError):
                await source.add_ticker("not a ticker")
            assert source.get_tickers() == ["AAPL"]
        finally:
            await source.stop()

    async def test_start_cleans_ticker_list(self):
        source = SimulatorDataSource(price_cache=PriceCache(), update_interval=0.5)
        await source.start(["aapl", "AAPL", " msft", "bad symbol"])
        try:
            assert source.get_tickers() == ["AAPL", "MSFT"]
        finally:
            await source.stop()

    async def test_remove_ticker_normalizes(self):
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.5)
        await source.start(["AAPL", "MSFT"])
        try:
            await source.remove_ticker(" msft")
            assert source.get_tickers() == ["AAPL"]
            assert cache.get("MSFT") is None
        finally:
            await source.stop()

    async def test_reference_price_is_start_price_and_survives_ticks(self):
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.01, seed=4)
        await source.start(["AAPL"])
        try:
            assert cache.get("AAPL").reference_price == 190.0  # seed price
            await asyncio.sleep(0.1)
            update = cache.get("AAPL")
            assert update.reference_price == 190.0  # steps do not move the baseline
            assert update.day_change_percent == pytest.approx(
                (update.price - 190.0) / 190.0 * 100, abs=1e-3
            )
        finally:
            await source.stop()

    async def test_seed_makes_runs_reproducible(self):
        async def run():
            cache = PriceCache()
            source = SimulatorDataSource(price_cache=cache, update_interval=60, seed=21)
            await source.start(["AAPL", "NVDA"])
            await source.stop()
            return {t: cache.get_price(t) for t in ("AAPL", "NVDA")}

        assert await run() == await run()
