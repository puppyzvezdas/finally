"""Shared test helpers: real-model Massive snapshots built from raw API JSON."""

from massive.rest.models import TickerSnapshot

NS = 1_605_195_918_306_274_000  # lastTrade.t is nanoseconds
NS_SECONDS = 1_605_195_918.306274


def raw_snapshot(ticker: str = "AAPL", **sections) -> dict:
    """Raw JSON for one entry of the snapshot endpoint's ``tickers`` array.

    Pass ``name=None`` to drop a section (e.g. ``lastTrade=None``).
    """
    raw = {
        "ticker": ticker,
        "todaysChange": -1.25,
        "todaysChangePerc": -0.65,
        "updated": NS,
        "day": {"o": 191.2, "h": 192.8, "l": 189.9, "c": 190.2, "v": 41200000, "vw": 191.0},
        "min": {"o": 190.0, "h": 190.2, "l": 189.9, "c": 190.15, "v": 52000, "av": 1, "t": 1},
        "prevDay": {"o": 190.5, "h": 192.0, "l": 189.0, "c": 191.35, "v": 55000000},
        "lastTrade": {"p": 190.1, "s": 100, "x": 4, "i": "52983", "c": [14, 41], "t": NS},
        "lastQuote": {"p": 190.09, "s": 3, "P": 190.11, "S": 2, "t": NS},
    }
    raw.update(sections)
    return {k: v for k, v in raw.items() if v is not None}


def snap(ticker: str = "AAPL", **sections) -> TickerSnapshot:
    return TickerSnapshot.from_dict(raw_snapshot(ticker, **sections))
