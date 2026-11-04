"""SSE streaming endpoint for live price updates."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import AsyncGenerator

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from .cache import PriceCache

logger = logging.getLogger(__name__)

# With a slow source (Massive polls every 15s) the stream would otherwise go quiet. A comment
# line keeps proxies from dropping the connection; EventSource ignores it.
HEARTBEAT_SECONDS = 10.0


def create_stream_router(
    price_cache: PriceCache,
    interval: float = 0.5,
    heartbeat: float = HEARTBEAT_SECONDS,
) -> APIRouter:
    """Create the SSE streaming router with a reference to the price cache.

    A new APIRouter is built on every call, so calling this more than once (app factories,
    tests) never registers duplicate routes or mixes up caches.
    """
    router = APIRouter(prefix="/api/stream", tags=["streaming"])

    @router.get("/prices")
    async def stream_prices(request: Request) -> StreamingResponse:
        """SSE endpoint for live price updates.

        Streams all tracked ticker prices whenever the cache changes (checked every
        ``interval`` seconds). The client connects with EventSource and receives events in
        the format:

            data: {"AAPL": {"ticker": "AAPL", "price": 190.50, ...}, ...}

        Includes a retry directive so the browser auto-reconnects on
        disconnection (EventSource built-in behavior).
        """
        return StreamingResponse(
            _generate_events(price_cache, request, interval, heartbeat),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",  # Disable nginx buffering if proxied
            },
        )

    return router


async def _generate_events(
    price_cache: PriceCache,
    request: Request,
    interval: float = 0.5,
    heartbeat: float = HEARTBEAT_SECONDS,
) -> AsyncGenerator[str, None]:
    """Async generator that yields SSE-formatted price events.

    Sends all prices when the cache version changes, and a comment heartbeat when nothing
    has been sent for ``heartbeat`` seconds. Stops when the client disconnects (detected via
    request.is_disconnected()).
    """
    # Tell the client to retry after 1 second if the connection drops
    yield "retry: 1000\n\n"

    last_version = -1
    last_sent = time.monotonic()
    client_ip = request.client.host if request.client else "unknown"
    logger.info("SSE client connected: %s", client_ip)

    try:
        while True:
            # Check for client disconnect
            if await request.is_disconnected():
                logger.info("SSE client disconnected: %s", client_ip)
                break

            current_version = price_cache.version
            if current_version != last_version:
                last_version = current_version
                prices = price_cache.get_all()

                if prices:
                    data = {ticker: update.to_dict() for ticker, update in prices.items()}
                    yield f"data: {json.dumps(data)}\n\n"
                    last_sent = time.monotonic()

            if time.monotonic() - last_sent >= heartbeat:
                yield ": ping\n\n"
                last_sent = time.monotonic()

            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        logger.info("SSE stream cancelled for: %s", client_ip)
        raise
