"""Async runners for the SDK tests in this directory."""

import asyncio

import pytest


@pytest.fixture
def run_sdk_async():
    """Bound each owned test with the native asyncio event loop, without a selector patch."""
    def run(coroutine, timeout=30):
        async def bounded():
            return await asyncio.wait_for(coroutine, timeout=timeout)
        return asyncio.run(bounded())
    return run
