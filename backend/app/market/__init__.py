"""Market data subsystem for FinAlly.

Public API:
    PriceUpdate         - Immutable price snapshot dataclass
    PriceCache          - Thread-safe in-memory price store
    MarketDataSource    - Abstract interface for data providers
    UnknownTickerError  - Raised by add_ticker() for symbols the provider rejects
    normalize_ticker, validate_ticker - Shared ticker helpers
    create_market_data_source - Factory that selects simulator or Massive
    create_stream_router - FastAPI router factory for SSE endpoint
"""

from .cache import PriceCache
from .factory import create_market_data_source
from .interface import MarketDataSource, UnknownTickerError, normalize_ticker, validate_ticker
from .models import PriceUpdate
from .stream import create_stream_router

__all__ = [
    "PriceUpdate",
    "PriceCache",
    "MarketDataSource",
    "UnknownTickerError",
    "normalize_ticker",
    "validate_ticker",
    "create_market_data_source",
    "create_stream_router",
]
