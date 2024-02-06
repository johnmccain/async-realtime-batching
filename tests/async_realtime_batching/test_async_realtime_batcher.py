import asyncio
import time
from unittest.mock import AsyncMock

import pytest

from async_realtime_batching import AsyncRealtimeBatcher
from async_realtime_batching.async_realtime_batcher import SubmittedCall


@pytest.mark.asyncio
async def test_async_realtime_batcher_raises_on_reuse():
    async_realtime_batcher = AsyncRealtimeBatcher(
        batch_size=1, max_wait_time_seconds=0.0
    )
    async_realtime_batcher(lambda x: x)
    with pytest.raises(ValueError):
        async_realtime_batcher(lambda x: x)


@pytest.mark.asyncio
async def test_async_realtime_batcher_wraps_function():
    async_realtime_batcher = AsyncRealtimeBatcher(
        batch_size=1, max_wait_time_seconds=0.0
    )

    async def func(x: list[int]) -> list[int]:
        return x

    wrapped_func = async_realtime_batcher(func)
    assert callable(wrapped_func)

    result = await wrapped_func([1, 2, 3])
    assert result == [1, 2, 3]


@pytest.mark.asyncio
async def test_async_realtime_batcher_batches_calls():
    async_realtime_batcher = AsyncRealtimeBatcher(
        batch_size=6, max_wait_time_seconds=1.0
    )

    async def func(x: list[int]) -> list[int]:
        return x

    mock_func = AsyncMock(side_effect=func)
    wrapped_func = async_realtime_batcher(mock_func)

    awaitable1 = wrapped_func([1, 2, 3])
    awaitable2 = wrapped_func([4, 5, 6])

    result1, result2 = await asyncio.gather(
        awaitable1,
        awaitable2,
    )

    assert result1 == [1, 2, 3]
    assert result2 == [4, 5, 6]
    mock_func.assert_called_once_with([1, 2, 3, 4, 5, 6])


@pytest.mark.asyncio
async def test_buffer_monitor_calls_process_batch_when_buffer_full():
    async_realtime_batcher = AsyncRealtimeBatcher(
        batch_size=2, max_wait_time_seconds=1000.0
    )
    async_realtime_batcher._process_batch = AsyncMock()
    async_realtime_batcher.buffer = [
        SubmittedCall(
            requests=[1, 2],
            submission_time=time.monotonic(),
            future=asyncio.get_event_loop().create_future(),
        ),
        SubmittedCall(
            requests=[3, 4],
            submission_time=time.monotonic(),
            future=asyncio.get_event_loop().create_future(),
        ),
    ]

    await asyncio.sleep(0.1)

    async_realtime_batcher._process_batch.assert_called()


@pytest.mark.asyncio
async def test_buffer_monitor_calls_process_batch_when_oldest_item_too_old():
    async_realtime_batcher = AsyncRealtimeBatcher(
        batch_size=1000, max_wait_time_seconds=0.01
    )
    async_realtime_batcher._process_batch = AsyncMock()
    async_realtime_batcher.buffer = [
        SubmittedCall(
            requests=[1, 2],
            submission_time=time.monotonic() - 0.2,
            future=asyncio.get_event_loop().create_future(),
        ),
    ]

    await asyncio.sleep(0.1)

    async_realtime_batcher._process_batch.assert_called()


@pytest.mark.asyncio
async def test_process_batch_processes_batch():
    async_realtime_batcher = AsyncRealtimeBatcher(
        batch_size=2, max_wait_time_seconds=1000.0
    )
    # cancel the buffer monitor task so it doesn't interfere with this test
    async_realtime_batcher.buffer_monitor_task.cancel()
    async_realtime_batcher.func = AsyncMock(side_effect=lambda x: x)
    async_realtime_batcher.buffer = [
        SubmittedCall(
            requests=[1, 2],
            submission_time=time.monotonic(),
            future=asyncio.get_event_loop().create_future(),
        ),
        SubmittedCall(
            requests=[3, 4],
            submission_time=time.monotonic(),
            future=asyncio.get_event_loop().create_future(),
        ),
    ]

    await async_realtime_batcher._process_batch()

    async_realtime_batcher.func.assert_called_once_with([1, 2, 3, 4])
    assert async_realtime_batcher.buffer == []
