"""Tests for the SSE endpoint (app.market.stream)."""

import asyncio
import json
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.responses import StreamingResponse

from app.market.cache import PriceCache
from app.market.stream import _generate_events, create_stream_router


class FakeRequest:
    """Stand-in for starlette's Request: disconnects after N polls, with optional hooks."""

    def __init__(self, disconnect_after: int, hooks: dict | None = None):
        self.client = SimpleNamespace(host="test-client")
        self._calls = 0
        self._disconnect_after = disconnect_after
        self._hooks = hooks or {}

    async def is_disconnected(self) -> bool:
        self._calls += 1
        if hook := self._hooks.get(self._calls):
            hook()
        return self._calls > self._disconnect_after


async def collect(cache, request, interval=0.001, heartbeat=3600.0) -> list[str]:
    return [chunk async for chunk in _generate_events(cache, request, interval, heartbeat)]


def data_events(chunks: list[str]) -> list[dict]:
    return [json.loads(c.removeprefix("data: ")) for c in chunks if c.startswith("data: ")]


class TestEventGenerator:
    async def test_first_chunk_is_retry_directive(self):
        chunks = await collect(PriceCache(), FakeRequest(disconnect_after=1))
        assert chunks[0] == "retry: 1000\n\n"

    async def test_sends_current_prices_as_one_event(self):
        cache = PriceCache()
        cache.update("AAPL", 190.0)
        cache.update("MSFT", 420.0)
        chunks = await collect(cache, FakeRequest(disconnect_after=2))

        assert chunks[1].startswith("data: ") and chunks[1].endswith("\n\n")
        (event,) = data_events(chunks)
        assert set(event) == {"AAPL", "MSFT"}
        assert event["AAPL"]["price"] == 190.0
        assert event["AAPL"]["direction"] == "flat"
        assert {"previous_price", "change", "change_percent", "timestamp"} <= set(event["AAPL"])

    async def test_no_repeat_event_when_version_unchanged(self):
        cache = PriceCache()
        cache.update("AAPL", 190.0)
        chunks = await collect(cache, FakeRequest(disconnect_after=6))
        assert len(data_events(chunks)) == 1

    async def test_new_event_after_cache_update(self):
        cache = PriceCache()
        cache.update("AAPL", 190.0)
        request = FakeRequest(disconnect_after=6, hooks={3: lambda: cache.update("AAPL", 191.0)})
        events = data_events(await collect(cache, request))

        assert [e["AAPL"]["price"] for e in events] == [190.0, 191.0]
        assert events[1]["AAPL"]["direction"] == "up"

    async def test_removal_is_streamed(self):
        cache = PriceCache()
        cache.update("AAPL", 190.0)
        cache.update("MSFT", 420.0)
        request = FakeRequest(disconnect_after=6, hooks={3: lambda: cache.remove("MSFT")})
        events = data_events(await collect(cache, request))
        assert set(events[-1]) == {"AAPL"}

    async def test_empty_cache_sends_no_data_event(self):
        chunks = await collect(PriceCache(), FakeRequest(disconnect_after=3))
        assert data_events(chunks) == []

    async def test_heartbeat_when_quiet(self):
        chunks = await collect(
            PriceCache(), FakeRequest(disconnect_after=30), interval=0.01, heartbeat=0.05
        )
        assert ": ping\n\n" in chunks

    async def test_no_heartbeat_right_after_data(self):
        cache = PriceCache()
        cache.update("AAPL", 190.0)
        chunks = await collect(cache, FakeRequest(disconnect_after=3), heartbeat=3600.0)
        assert ": ping\n\n" not in chunks

    async def test_stops_when_client_disconnects(self):
        # Completing without hanging is the assertion: the generator must exit on disconnect.
        chunks = await asyncio.wait_for(
            collect(PriceCache(), FakeRequest(disconnect_after=0)), timeout=2
        )
        assert chunks == ["retry: 1000\n\n"]

    async def test_cancellation_propagates(self):
        gen = _generate_events(PriceCache(), FakeRequest(disconnect_after=10_000), 0.01, 3600.0)

        async def consume():
            async for _ in gen:
                pass

        task = asyncio.create_task(consume())
        await asyncio.sleep(0.05)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


class TestRouter:
    def _route(self, router):
        (route,) = [r for r in router.routes if r.path == "/api/stream/prices"]
        return route

    async def test_route_returns_event_stream_with_proxy_safe_headers(self):
        router = create_stream_router(PriceCache())
        response = await self._route(router).endpoint(FakeRequest(disconnect_after=0))

        assert isinstance(response, StreamingResponse)
        assert response.media_type == "text/event-stream"
        assert response.headers["cache-control"] == "no-cache"
        assert response.headers["x-accel-buffering"] == "no"

    def test_calling_factory_twice_does_not_duplicate_routes(self):
        first = create_stream_router(PriceCache())
        second = create_stream_router(PriceCache())
        assert first is not second
        assert len(first.routes) == 1
        assert len(second.routes) == 1

    async def test_each_router_streams_its_own_cache(self):
        cache_a, cache_b = PriceCache(), PriceCache()
        cache_a.update("AAPL", 1.0)
        cache_b.update("MSFT", 2.0)
        resp_a = await self._route(create_stream_router(cache_a, 0.001)).endpoint(
            FakeRequest(disconnect_after=2)
        )
        resp_b = await self._route(create_stream_router(cache_b, 0.001)).endpoint(
            FakeRequest(disconnect_after=2)
        )

        async def first_event(resp):
            return data_events([c async for c in resp.body_iterator])[0]

        assert set(await first_event(resp_a)) == {"AAPL"}
        assert set(await first_event(resp_b)) == {"MSFT"}

    def test_router_mounts_on_an_app(self):
        app = FastAPI()
        app.include_router(create_stream_router(PriceCache()))
        assert "/api/stream/prices" in [r.path for r in app.routes]
