"""Tests for PriceCache."""

import threading
import time

import pytest

from app.market.cache import PriceCache


class TestPriceCache:
    """Unit tests for the PriceCache."""

    def test_update_and_get(self):
        """Test updating and getting a price."""
        cache = PriceCache()
        update = cache.update("AAPL", 190.50)
        assert update.ticker == "AAPL"
        assert update.price == 190.50
        assert cache.get("AAPL") == update

    def test_first_update_is_flat(self):
        """Test that the first update has flat direction."""
        cache = PriceCache()
        update = cache.update("AAPL", 190.50)
        assert update.direction == "flat"
        assert update.previous_price == 190.50

    def test_direction_up(self):
        """Test price update with upward direction."""
        cache = PriceCache()
        cache.update("AAPL", 190.00)
        update = cache.update("AAPL", 191.00)
        assert update.direction == "up"
        assert update.change == 1.00

    def test_direction_down(self):
        """Test price update with downward direction."""
        cache = PriceCache()
        cache.update("AAPL", 190.00)
        update = cache.update("AAPL", 189.00)
        assert update.direction == "down"
        assert update.change == -1.00

    def test_remove(self):
        """Test removing a ticker from cache."""
        cache = PriceCache()
        cache.update("AAPL", 190.00)
        cache.remove("AAPL")
        assert cache.get("AAPL") is None

    def test_remove_nonexistent(self):
        """Test removing a ticker that doesn't exist."""
        cache = PriceCache()
        cache.remove("AAPL")  # Should not raise

    def test_get_all(self):
        """Test getting all prices."""
        cache = PriceCache()
        cache.update("AAPL", 190.00)
        cache.update("GOOGL", 175.00)
        all_prices = cache.get_all()
        assert set(all_prices.keys()) == {"AAPL", "GOOGL"}

    def test_version_increments(self):
        """Test that version counter increments."""
        cache = PriceCache()
        v0 = cache.version
        cache.update("AAPL", 190.00)
        assert cache.version == v0 + 1
        cache.update("AAPL", 191.00)
        assert cache.version == v0 + 2

    def test_get_price_convenience(self):
        """Test the convenience get_price method."""
        cache = PriceCache()
        cache.update("AAPL", 190.50)
        assert cache.get_price("AAPL") == 190.50
        assert cache.get_price("NOPE") is None

    def test_len(self):
        """Test __len__ method."""
        cache = PriceCache()
        assert len(cache) == 0
        cache.update("AAPL", 190.00)
        assert len(cache) == 1
        cache.update("GOOGL", 175.00)
        assert len(cache) == 2

    def test_contains(self):
        """Test __contains__ method."""
        cache = PriceCache()
        cache.update("AAPL", 190.00)
        assert "AAPL" in cache
        assert "GOOGL" not in cache

    def test_custom_timestamp(self):
        """Test updating with a custom timestamp."""
        cache = PriceCache()
        custom_ts = 1234567890.0
        update = cache.update("AAPL", 190.50, timestamp=custom_ts)
        assert update.timestamp == custom_ts

    def test_price_rounding(self):
        """Test that prices are rounded to 2 decimal places."""
        cache = PriceCache()
        update = cache.update("AAPL", 190.12345)
        assert update.price == 190.12


class TestPriceValidation:
    """Bad provider values must never be stored (they would reach trades and valuation)."""

    @pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf"), 0, 0.0, -5.0])
    def test_rejects_invalid_price(self, bad):
        cache = PriceCache()
        with pytest.raises(ValueError):
            cache.update("AAPL", bad)
        assert cache.get("AAPL") is None
        assert cache.version == 0

    def test_rejected_update_keeps_previous_price(self):
        cache = PriceCache()
        cache.update("AAPL", 190.0)
        with pytest.raises(ValueError):
            cache.update("AAPL", float("nan"))
        assert cache.get_price("AAPL") == 190.0

    def test_rejects_invalid_reference_price(self):
        cache = PriceCache()
        with pytest.raises(ValueError):
            cache.update("AAPL", 190.0, reference_price=-1.0)

    def test_zero_timestamp_is_not_treated_as_missing(self):
        assert PriceCache().update("AAPL", 190.0, timestamp=0.0).timestamp == 0.0

    def test_missing_timestamp_defaults_to_now(self):
        before = time.time()
        assert PriceCache().update("AAPL", 190.0).timestamp >= before


class TestReferencePrice:
    def test_defaults_to_none(self):
        assert PriceCache().update("AAPL", 190.0).reference_price is None

    def test_set_on_update_and_carried_forward(self):
        cache = PriceCache()
        cache.update("AAPL", 190.0, reference_price=188.0)
        update = cache.update("AAPL", 191.0)  # no reference given: keep the baseline
        assert update.reference_price == 188.0

    def test_can_be_replaced(self):
        cache = PriceCache()
        cache.update("AAPL", 190.0, reference_price=188.0)
        assert cache.update("AAPL", 191.0, reference_price=190.5).reference_price == 190.5

    def test_removed_ticker_loses_baseline(self):
        cache = PriceCache()
        cache.update("AAPL", 190.0, reference_price=188.0)
        cache.remove("AAPL")
        assert cache.update("AAPL", 190.0).reference_price is None


class TestVersionAndConcurrency:
    def test_remove_bumps_version_only_when_something_removed(self):
        cache = PriceCache()
        cache.update("AAPL", 190.0)
        before = cache.version
        cache.remove("NOPE")
        assert cache.version == before
        cache.remove("AAPL")
        assert cache.version == before + 1

    def test_concurrent_writers_do_not_lose_updates(self):
        cache = PriceCache()
        threads_n, per_thread = 8, 500

        def writer(i: int) -> None:
            for n in range(per_thread):
                cache.update(f"T{i % 4}", 100.0 + n)  # shared and distinct tickers
                cache.get_all()

        threads = [threading.Thread(target=writer, args=(i,)) for i in range(threads_n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert cache.version == threads_n * per_thread
        assert len(cache) == 4
