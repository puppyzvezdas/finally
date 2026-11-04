"""Tests for the shared ticker helpers in app.market.interface."""

import pytest

from app.market.interface import (
    UnknownTickerError,
    clean_tickers,
    normalize_ticker,
    validate_ticker,
)


class TestNormalize:
    @pytest.mark.parametrize("raw, expected", [("aapl", "AAPL"), (" tsla ", "TSLA"), ("V", "V")])
    def test_strips_and_uppercases(self, raw, expected):
        assert normalize_ticker(raw) == expected


class TestValidate:
    @pytest.mark.parametrize("ok", ["AAPL", "v", " nvda ", "BRK.B", "BF-B", "GOOGL"])
    def test_accepts_valid(self, ok):
        assert validate_ticker(ok) == ok.strip().upper()

    @pytest.mark.parametrize(
        "bad", ["", "  ", "TOOLONG", "AA PL", "A1", "123", "AAPL;DROP", "BRK.", ".B", "A.BCD"]
    )
    def test_rejects_invalid(self, bad):
        with pytest.raises(ValueError):
            validate_ticker(bad)

    def test_unknown_ticker_error_is_a_value_error(self):
        assert issubclass(UnknownTickerError, ValueError)


class TestCleanTickers:
    def test_normalizes_dedupes_and_drops_invalid(self):
        assert clean_tickers(["aapl", " AAPL", "msft", "bad symbol", "", "TSLA"]) == [
            "AAPL",
            "MSFT",
            "TSLA",
        ]

    def test_preserves_order(self):
        assert clean_tickers(["V", "AAPL", "JPM"]) == ["V", "AAPL", "JPM"]

    def test_empty(self):
        assert clean_tickers([]) == []
