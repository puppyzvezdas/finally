"""Tests for MassiveDataSource.

Snapshots are built with the real ``massive`` model classes from raw API JSON, never
MagicMock: a mock accepts any attribute name and hid a wrong field name in the past.
"""

import asyncio
import os
import threading
import time
from unittest.mock import MagicMock, patch

import pytest

from app.market import massive_client
from app.market.cache import PriceCache
from app.market.interface import UnknownTickerError
from app.market.massive_client import (
    MassiveDataSource,
    parse_snapshot,
    to_epoch_seconds,
)

from .helpers import NS, NS_SECONDS, snap


def make_source(tickers=("AAPL",), poll_interval=60.0) -> tuple[MassiveDataSource, PriceCache]:
    cache = PriceCache()
    source = MassiveDataSource(api_key="test-key", price_cache=cache, poll_interval=poll_interval)
    source._tickers = list(tickers)
    source._client = MagicMock()  # satisfy the "started" guard; fetches are patched
    return source, cache


class TestToEpochSeconds:
    @pytest.mark.parametrize(
        "raw, expected",
        [
            (NS, NS_SECONDS),  # nanoseconds
            (1_605_195_918_306_274, NS_SECONDS),  # microseconds
            (1_684_428_720_000, 1_684_428_720.0),  # milliseconds
            (1_684_428_720, 1_684_428_720.0),  # seconds
        ],
    )
    def test_units(self, raw, expected):
        assert to_epoch_seconds(raw) == pytest.approx(expected)


class TestParseSnapshot:
    def test_prefers_last_trade_and_converts_ns(self):
        parsed = parse_snapshot(snap(), now=1.0)
        assert parsed.price == 190.1
        assert parsed.timestamp == pytest.approx(NS_SECONDS)
        assert parsed.reference_price == 191.35  # prevDay.c

    def test_real_model_has_no_timestamp_attribute(self):
        """Regression: the client once read last_trade.timestamp, which does not exist."""
        assert not hasattr(snap().last_trade, "timestamp")
        assert snap().last_trade.sip_timestamp == NS

    def test_falls_back_to_participant_timestamp_then_updated(self):
        no_sip = {"p": 190.1, "y": NS + 1_000_000_000}  # participant_timestamp only
        assert parse_snapshot(snap(lastTrade=no_sip)).timestamp == pytest.approx(NS_SECONDS + 1)
        no_ts = {"p": 190.1}
        assert parse_snapshot(snap(lastTrade=no_ts)).timestamp == pytest.approx(NS_SECONDS)

    def test_no_last_trade_falls_back_to_minute_close(self):
        parsed = parse_snapshot(snap(lastTrade=None), now=42.0)
        assert parsed.price == 190.15
        assert parsed.timestamp == 42.0  # bar fallbacks use the time we learned the price

    def test_falls_back_to_day_then_prev_day_close(self):
        assert parse_snapshot(snap(lastTrade=None, min=None)).price == 190.2
        assert parse_snapshot(snap(lastTrade=None, min=None, day=None)).price == 191.35

    def test_zero_prices_are_skipped(self):
        zeroed = {"o": 0, "h": 0, "l": 0, "c": 0, "v": 0}
        parsed = parse_snapshot(snap(lastTrade={"p": 0, "t": NS}, min=zeroed, day=zeroed))
        assert parsed.price == 191.35

    def test_no_usable_price_returns_none(self):
        assert parse_snapshot(snap(lastTrade=None, min=None, day=None, prevDay=None)) is None

    def test_reference_price_optional(self):
        assert parse_snapshot(snap(prevDay=None)).reference_price is None


class TestPolling:
    async def test_poll_updates_cache_from_real_models(self):
        source, cache = make_source(["AAPL", "GOOGL"])
        snaps = [snap("AAPL"), snap("GOOGL", lastTrade={"p": 175.25, "t": NS})]
        with patch.object(source, "_fetch_snapshots", return_value=snaps):
            unpriced = await source._poll_once()

        assert unpriced == set()
        assert cache.get_price("AAPL") == 190.1
        assert cache.get_price("GOOGL") == 175.25
        update = cache.get("AAPL")
        assert update.timestamp == pytest.approx(NS_SECONDS)  # seconds, not 1.6e15
        assert update.reference_price == 191.35

    async def test_ticker_without_last_trade_is_priced(self):
        source, cache = make_source()
        with patch.object(source, "_fetch_snapshots", return_value=[snap(lastTrade=None)]):
            await source._poll_once()
        assert cache.get_price("AAPL") == 190.15

    async def test_unpriceable_snapshot_skipped_others_processed(self):
        source, cache = make_source(["AAPL", "BAD"])
        bad = snap("BAD", lastTrade=None, min=None, day=None, prevDay=None)
        with patch.object(source, "_fetch_snapshots", return_value=[snap("AAPL"), bad]):
            unpriced = await source._poll_once()
        assert cache.get_price("AAPL") == 190.1
        assert cache.get_price("BAD") is None
        assert unpriced == {"BAD"}

    async def test_symbol_missing_from_response_is_reported_unpriced(self):
        source, cache = make_source(["AAPL", "ZZZZ"])
        with patch.object(source, "_fetch_snapshots", return_value=[snap("AAPL")]):
            assert await source._poll_once() == {"ZZZZ"}

    async def test_api_error_does_not_crash(self):
        source, cache = make_source()
        with patch.object(source, "_fetch_snapshots", side_effect=Exception("network error")):
            assert await source._poll_once() is None
        assert cache.get_price("AAPL") is None

    async def test_empty_tickers_skips_poll(self):
        source, _ = make_source([])
        with patch.object(source, "_fetch_snapshots") as fetch:
            assert await source._poll_once() is None
            fetch.assert_not_called()

    async def test_fetch_receives_a_copy_of_the_ticker_list(self):
        """The worker thread must not see add_ticker() mutating the live list."""
        source, _ = make_source(["AAPL"])
        received = []

        def fetch(tickers):
            received.append(tickers)
            return [snap()]

        with patch.object(source, "_fetch_snapshots", side_effect=fetch):
            await source._poll_once()
        assert received[0] == ["AAPL"]
        assert received[0] is not source._tickers

    async def test_ticker_removed_during_poll_is_not_resurrected(self):
        source, cache = make_source(["AAPL", "TSLA"])
        in_flight = threading.Event()
        release = threading.Event()

        def slow_fetch(tickers):
            in_flight.set()
            release.wait(timeout=5)
            return [snap("AAPL"), snap("TSLA", lastTrade={"p": 250.0, "t": NS})]

        with patch.object(source, "_fetch_snapshots", side_effect=slow_fetch):
            poll = asyncio.create_task(source._poll_once())
            await asyncio.to_thread(in_flight.wait, 5)
            await source.remove_ticker("TSLA")  # while the request is in flight
            release.set()
            await poll

        assert cache.get_price("AAPL") == 190.1
        assert cache.get("TSLA") is None
        assert "TSLA" not in source.get_tickers()


class TestRateLimiting:
    async def test_429_doubles_interval_up_to_cap_and_success_resets(self, monkeypatch):
        monkeypatch.setattr(massive_client, "MAX_BACKOFF", 100.0)
        source, _ = make_source(poll_interval=30.0)

        with patch.object(source, "_fetch_snapshots", side_effect=Exception("429 Too Many")):
            await source._poll_once()
            assert source._interval == 60.0
            await source._poll_once()
            assert source._interval == 100.0  # capped
            await source._poll_once()
            assert source._interval == 100.0

        with patch.object(source, "_fetch_snapshots", return_value=[snap()]):
            await source._poll_once()
        assert source._interval == 30.0

    async def test_other_errors_do_not_back_off(self):
        source, _ = make_source(poll_interval=15.0)
        with patch.object(source, "_fetch_snapshots", side_effect=Exception("401 Unauthorized")):
            await source._poll_once()
        assert source._interval == 15.0

    async def test_min_gap_skips_poll(self):
        source, _ = make_source()
        source._last_poll = time.monotonic()
        with patch.object(source, "_fetch_snapshots") as fetch:
            assert await source._poll_once(min_gap=12.0) is None
            fetch.assert_not_called()


class TestTickerManagement:
    async def test_add_ticker_normalizes(self):
        source, _ = make_source([])
        source._client = None  # not started: no immediate poll
        await source.add_ticker("  aapl ")
        assert source.get_tickers() == ["AAPL"]

    async def test_add_ticker_is_idempotent(self):
        source, _ = make_source(["AAPL"])
        await source.add_ticker("aapl")
        assert source.get_tickers() == ["AAPL"]

    @pytest.mark.parametrize("bad", ["", "TOOLONGSYMBOL", "AA PL", "123", "AAPL;DROP"])
    async def test_add_ticker_rejects_malformed(self, bad):
        source, _ = make_source([])
        with pytest.raises(ValueError):
            await source.add_ticker(bad)
        assert source.get_tickers() == []

    async def test_add_ticker_prices_immediately(self):
        source, cache = make_source(["AAPL"])
        pypl = snap("PYPL", lastTrade={"p": 70.5, "t": NS})
        with patch.object(source, "_fetch_snapshots", return_value=[snap(), pypl]):
            await source.add_ticker("pypl")
        assert cache.get_price("PYPL") == 70.5

    async def test_add_unknown_ticker_raises_and_is_not_tracked(self):
        source, cache = make_source(["AAPL"])
        with patch.object(source, "_fetch_snapshots", return_value=[snap()]):
            with pytest.raises(UnknownTickerError):
                await source.add_ticker("ZZZZ")
        assert source.get_tickers() == ["AAPL"]
        assert cache.get("ZZZZ") is None

    async def test_add_within_rate_limit_gap_is_accepted_pending(self):
        source, cache = make_source(["AAPL"])
        source._last_poll = time.monotonic()
        with patch.object(source, "_fetch_snapshots") as fetch:
            await source.add_ticker("PYPL")
            fetch.assert_not_called()
        assert "PYPL" in source.get_tickers()
        assert cache.get("PYPL") is None

    async def test_add_when_poll_fails_is_accepted_pending(self):
        source, _ = make_source(["AAPL"])
        with patch.object(source, "_fetch_snapshots", side_effect=Exception("boom")):
            await source.add_ticker("PYPL")
        assert "PYPL" in source.get_tickers()

    async def test_remove_ticker_clears_cache(self):
        source, cache = make_source(["AAPL", "GOOGL"])
        cache.update("AAPL", 190.0)
        await source.remove_ticker(" aapl")
        assert source.get_tickers() == ["GOOGL"]
        assert cache.get("AAPL") is None

    async def test_remove_missing_ticker_is_noop(self):
        source, _ = make_source(["AAPL"])
        await source.remove_ticker("NOPE")
        assert source.get_tickers() == ["AAPL"]

    async def test_get_tickers_returns_copy(self):
        source, _ = make_source(["AAPL"])
        source.get_tickers().append("X")
        assert source.get_tickers() == ["AAPL"]


class TestLifecycle:
    async def test_start_polls_immediately_and_stop_cancels(self):
        cache = PriceCache()
        source = MassiveDataSource(api_key="k", price_cache=cache, poll_interval=60.0)
        with patch("app.market.massive_client.RESTClient"):
            with patch.object(source, "_fetch_snapshots", return_value=[snap()]):
                await source.start(["aapl", "aapl", "bad symbol"])

        assert source.get_tickers() == ["AAPL"]  # normalized, de-duplicated, invalid dropped
        assert cache.get_price("AAPL") == 190.1  # populated before start() returned
        assert source._task is not None and not source._task.done()

        await source.stop()
        assert source._task is None

    async def test_stop_is_idempotent(self):
        source, _ = make_source()
        await source.stop()
        await source.stop()

    async def test_slow_first_poll_does_not_block_startup(self, monkeypatch):
        monkeypatch.setattr(massive_client, "FIRST_POLL_TIMEOUT", 0.05)
        release = threading.Event()

        def hanging_fetch(tickers):
            release.wait(timeout=5)
            return [snap()]

        cache = PriceCache()
        source = MassiveDataSource(api_key="k", price_cache=cache, poll_interval=60.0)
        try:
            with patch("app.market.massive_client.RESTClient"):
                with patch.object(source, "_fetch_snapshots", side_effect=hanging_fetch):
                    started = time.monotonic()
                    await source.start(["AAPL"])
                    assert time.monotonic() - started < 2.0
                    assert source._task is not None  # the loop retries in the background
        finally:
            release.set()
            await source.stop()


@pytest.mark.skipif(not os.environ.get("MASSIVE_API_KEY"), reason="needs a real MASSIVE_API_KEY")
async def test_live_poll_prices_a_real_ticker():
    """Optional smoke test: one real poll against the Massive API."""
    cache = PriceCache()
    source = MassiveDataSource(api_key=os.environ["MASSIVE_API_KEY"], price_cache=cache)
    try:
        await source.start(["AAPL"])
        update = cache.get("AAPL")
        assert update is not None and update.price > 0
        assert abs(update.timestamp - time.time()) < 10 * 365 * 86400  # seconds, not ns
    finally:
        await source.stop()
