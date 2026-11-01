"""Shared temporary output and async runners for compatibility tests."""

import asyncio

import pytest


@pytest.fixture
def evidence_path(tmp_path):
    """Keep each test's recorder and peer outputs in its pytest temporary directory."""
    return tmp_path


@pytest.fixture
def run_sdk_async():
    """Run a coroutine with the caller's timeout using standard asyncio."""
    def run(coroutine, timeout=30):
        async def bounded():
            return await asyncio.wait_for(coroutine, timeout=timeout)
        return asyncio.run(bounded())
    return run
