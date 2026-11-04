"""Tests for GBMSimulator."""

from app.market.seed_prices import SEED_PRICES
from app.market.simulator import GBMSimulator


class TestGBMSimulator:
    """Unit tests for the GBM price simulator."""

    def test_step_returns_all_tickers(self):
        """Test that step() returns prices for all tickers."""
        sim = GBMSimulator(tickers=["AAPL", "GOOGL"])
        result = sim.step()
        assert set(result.keys()) == {"AAPL", "GOOGL"}

    def test_prices_are_positive(self):
        """GBM prices can never go negative (exp() is always positive)."""
        sim = GBMSimulator(tickers=["AAPL"])
        for _ in range(10_000):
            prices = sim.step()
            assert prices["AAPL"] > 0

    def test_initial_prices_match_seeds(self):
        """Test that initial prices match seed prices."""
        sim = GBMSimulator(tickers=["AAPL"])
        # Before any step, price should be the seed price
        assert sim.get_price("AAPL") == SEED_PRICES["AAPL"]

    def test_add_ticker(self):
        """Test adding a ticker dynamically."""
        sim = GBMSimulator(tickers=["AAPL"])
        sim.add_ticker("TSLA")
        result = sim.step()
        assert "TSLA" in result

    def test_remove_ticker(self):
        """Test removing a ticker."""
        sim = GBMSimulator(tickers=["AAPL", "GOOGL"])
        sim.remove_ticker("GOOGL")
        result = sim.step()
        assert "GOOGL" not in result
        assert "AAPL" in result

    def test_add_duplicate_is_noop(self):
        """Test that adding a duplicate ticker is a no-op."""
        sim = GBMSimulator(tickers=["AAPL"])
        sim.add_ticker("AAPL")
        assert len(sim._tickers) == 1

    def test_remove_nonexistent_is_noop(self):
        """Test that removing a non-existent ticker is a no-op."""
        sim = GBMSimulator(tickers=["AAPL"])
        sim.remove_ticker("NOPE")  # Should not raise

    def test_unknown_ticker_gets_random_seed_price(self):
        """Test that unknown tickers get random seed prices."""
        sim = GBMSimulator(tickers=["ZZZZ"])
        price = sim.get_price("ZZZZ")
        assert price is not None
        assert 50.0 <= price <= 300.0

    def test_empty_step(self):
        """Test stepping with no tickers."""
        sim = GBMSimulator(tickers=[])
        result = sim.step()
        assert result == {}

    def test_prices_change_over_time(self):
        """After many steps, prices should have drifted from their seeds."""
        sim = GBMSimulator(tickers=["AAPL"])
        initial_price = sim.get_price("AAPL")

        for _ in range(1000):
            sim.step()

        final_price = sim.get_price("AAPL")
        # Price should have changed (extremely unlikely to be exactly the seed)
        assert final_price != initial_price

    def test_cholesky_rebuilds_on_add(self):
        """Test that Cholesky matrix is rebuilt when tickers are added."""
        sim = GBMSimulator(tickers=["AAPL"])
        assert sim._cholesky is None  # Only 1 ticker, no correlation matrix
        sim.add_ticker("GOOGL")
        assert sim._cholesky is not None  # Now 2 tickers, matrix exists

    def test_cholesky_none_with_one_ticker(self):
        """Test that Cholesky is None with only one ticker."""
        sim = GBMSimulator(tickers=["AAPL"])
        assert sim._cholesky is None

    def test_get_price_returns_none_for_unknown(self):
        """Test that get_price returns None for unknown ticker."""
        sim = GBMSimulator(tickers=["AAPL"])
        assert sim.get_price("UNKNOWN") is None

    def test_pairwise_correlation_tech_stocks(self):
        """Test that tech stocks have high correlation."""
        corr = GBMSimulator._pairwise_correlation("AAPL", "GOOGL")
        assert corr == 0.6

    def test_pairwise_correlation_finance_stocks(self):
        """Test that finance stocks have moderate correlation."""
        corr = GBMSimulator._pairwise_correlation("JPM", "V")
        assert corr == 0.5

    def test_pairwise_correlation_tsla(self):
        """Test that TSLA has lower correlation with everything."""
        corr = GBMSimulator._pairwise_correlation("TSLA", "AAPL")
        assert corr == 0.3
        corr = GBMSimulator._pairwise_correlation("TSLA", "JPM")
        assert corr == 0.3

    def test_pairwise_correlation_cross_sector(self):
        """Test cross-sector correlation."""
        corr = GBMSimulator._pairwise_correlation("AAPL", "JPM")
        assert corr == 0.3

    def test_default_dt_is_reasonable(self):
        """Test that default dt is a reasonable small value."""
        assert 0 < GBMSimulator.DEFAULT_DT < 0.0001

    def test_prices_rounded_to_two_decimals(self):
        """Test that prices are rounded to 2 decimal places."""
        sim = GBMSimulator(tickers=["AAPL"])
        result = sim.step()
        price_str = str(result["AAPL"])
        # Check that we have at most 2 decimal places
        if "." in price_str:
            decimal_part = price_str.split(".")[1]
            assert len(decimal_part) <= 2


class TestStatistics:
    """Statistical properties of the GBM output (seeded, so deterministic)."""

    @staticmethod
    def _log_returns(sim, tickers, steps):
        """Log returns from unrounded internal state (per-tick moves are sub-cent)."""
        import numpy as np

        prev = {t: sim._prices[t] for t in tickers}
        out = {t: [] for t in tickers}
        for _ in range(steps):
            sim.step()
            for t in tickers:
                out[t].append(np.log(sim._prices[t] / prev[t]))
                prev[t] = sim._prices[t]
        return {t: np.array(v) for t, v in out.items()}

    def test_log_return_stddev_matches_sigma_sqrt_dt(self):
        import numpy as np
        import pytest

        from app.market.seed_prices import TICKER_PARAMS

        sim = GBMSimulator(["AAPL"], event_probability=0.0, seed=42)
        returns = self._log_returns(sim, ["AAPL"], 50_000)["AAPL"]
        expected = TICKER_PARAMS["AAPL"]["sigma"] * np.sqrt(GBMSimulator.DEFAULT_DT)
        assert np.std(returns) == pytest.approx(expected, rel=0.03)

    def test_higher_sigma_means_more_movement(self):
        import numpy as np

        sim = GBMSimulator(["TSLA", "V"], event_probability=0.0, seed=1)
        returns = self._log_returns(sim, ["TSLA", "V"], 20_000)
        assert np.std(returns["TSLA"]) > 2 * np.std(returns["V"])  # sigma 0.50 vs 0.17

    def test_same_sector_more_correlated_than_cross_sector(self):
        import numpy as np

        sim = GBMSimulator(["AAPL", "MSFT", "JPM"], event_probability=0.0, seed=7)
        r = self._log_returns(sim, ["AAPL", "MSFT", "JPM"], 30_000)
        tech = np.corrcoef(r["AAPL"], r["MSFT"])[0, 1]
        cross = np.corrcoef(r["AAPL"], r["JPM"])[0, 1]
        assert tech > cross + 0.15
        assert 0.5 < tech < 0.7  # configured rho = 0.6
        assert 0.2 < cross < 0.4  # configured rho = 0.3

    def test_all_default_tickers_construct_and_stay_positive(self):
        from app.market.seed_prices import SEED_PRICES

        sim = GBMSimulator(list(SEED_PRICES), seed=3)
        assert sim._cholesky is not None and sim._cholesky.shape == (10, 10)
        for _ in range(2_000):
            prices = sim.step()
            assert len(prices) == 10 and all(p > 0 for p in prices.values())

    def test_shock_events_move_price_two_to_five_percent(self):
        sim = GBMSimulator(["AAPL"], event_probability=1.0, seed=5)
        before = sim.get_price("AAPL")
        after = sim.step()["AAPL"]
        assert 0.019 <= abs(after / before - 1) <= 0.051


class TestSeeding:
    def test_same_seed_same_path(self):
        a = GBMSimulator(["AAPL", "MSFT", "ZZZZ"], seed=11)
        b = GBMSimulator(["AAPL", "MSFT", "ZZZZ"], seed=11)
        assert a.get_price("ZZZZ") == b.get_price("ZZZZ")  # unknown-ticker seed price too
        assert [a.step() for _ in range(200)] == [b.step() for _ in range(200)]

    def test_different_seeds_diverge(self):
        a, b = GBMSimulator(["AAPL"], seed=1), GBMSimulator(["AAPL"], seed=2)
        assert [a.step() for _ in range(50)] != [b.step() for _ in range(50)]

    def test_does_not_touch_global_rng_state(self):
        import random

        import numpy as np

        random.seed(123)
        np.random.seed(123)
        expected = (random.random(), np.random.random())
        random.seed(123)
        np.random.seed(123)
        GBMSimulator(["AAPL", "MSFT"], seed=9).step()
        assert (random.random(), np.random.random()) == expected
