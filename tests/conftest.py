"""Async runners for the SDK tests in this directory."""

import asyncio

import pytest


@pytest.fixture
def run_sdk_async():
    """Run a coroutine with the caller's timeout using standard asyncio."""
    def run(coroutine, timeout=10):
        async def bounded():
            return await asyncio.wait_for(coroutine, timeout=timeout)
        return asyncio.run(bounded())
    return run
