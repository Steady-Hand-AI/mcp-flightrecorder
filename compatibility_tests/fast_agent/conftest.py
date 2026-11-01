"""Async runners for the SDK tests in this directory."""

import asyncio

import pytest


@pytest.fixture
def run_sdk_async():
    """Use native asyncio with a test-owned completion deadline."""
    def run(coroutine, timeout=45):
        async def bounded():
            return await asyncio.wait_for(coroutine, timeout=timeout)
        return asyncio.run(bounded())
    return run
