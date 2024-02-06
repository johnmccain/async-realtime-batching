import asyncio
import functools
import itertools
import time
from typing import Awaitable, Callable, Generic, NamedTuple, TypeVar

RequestT = TypeVar("RequestT")
ResponseT = TypeVar("ResponseT")


class SubmittedCall(NamedTuple, Generic[RequestT, ResponseT]):
    requests: list[RequestT]
    submission_time: float
    future: asyncio.Future[ResponseT]


class AsyncRealtimeBatcher(Generic[RequestT, ResponseT]):
    def __init__(
        self,
        batch_size: int,
        max_wait_time_seconds: float,
        check_interval_seconds: float = 0.001,
    ) -> None:
        """
        :param batch_size: The maximum number of items to batch together. If the buffer reaches this size, it will be
            processed immediately.
        :param max_wait_time_seconds: The maximum amount of time a submitted call will wait before being processed,
            even if the buffer is not full.
        :check_interval_seconds: The interval at which the buffer will be checked for processing. Defaults to 0.0, which
            means the buffer will be checked as often as possible while surrendering control of the event loop between
            checks.
        """
        self.batch_size = batch_size
        self.max_wait_time_seconds = max_wait_time_seconds
        self.check_interval_seconds = check_interval_seconds
        self.func: Callable[[list[RequestT]], Awaitable[list[ResponseT]]] | None = None
        self.buffer: list[SubmittedCall] = []
        self.buffer_monitor_task: asyncio.Task[None] | None = None

    async def _start_monitor(self):
        """
        Create and start the monitor task if it is not already running. This should be called from an async context.
        """
        if (
            self.buffer_monitor_task is None
        ):  # Ensure we do not start multiple monitor tasks.
            self.buffer_monitor_task = asyncio.create_task(self._buffer_monitor())

    def __call__(
        self,
        func: Callable[[list[RequestT]], Awaitable[list[ResponseT]]],
    ) -> Callable[[list[RequestT]], Awaitable[list[ResponseT]]]:
        """
        Decorator to batch async function calls.
        :param func: The function to batch. Should be a Callable that accepts a list of items and returns a
            corresponding list of results.
        :return: A wrapped function that will batch calls to the original function.
        """
        if self.func is not None:
            raise ValueError(
                "This batcher is already in use. Create a new batcher for each function."
            )
        self.func = func

        @functools.wraps(func)
        async def wrapper(requests: list[RequestT]) -> list[ResponseT]:
            await self._start_monitor()
            submitted_call = SubmittedCall(
                requests=requests,
                submission_time=time.monotonic(),
                future=asyncio.get_event_loop().create_future(),
            )
            self.buffer.append(submitted_call)
            return await submitted_call.future

        return wrapper

    async def _process_batch(self):
        """
        Process the oldest batch_size items from the buffer and set the results on the corresponding futures.
        """
        if not self.func:
            raise ValueError("No function has been set for this batcher.")

        batch_to_process = self.buffer[: self.batch_size]
        if not batch_to_process:
            return

        combined_requests = itertools.chain(
            *[call.requests for call in batch_to_process]
        )
        combined_responses = await self.func(list(combined_requests))
        separated_responses = []
        i = 0
        for call in batch_to_process:
            separated_responses.append(combined_responses[i : i + len(call.requests)])
            i += len(call.requests)

        for call, responses in zip(batch_to_process, separated_responses):
            call.future.set_result(responses)

        self.buffer = self.buffer[len(batch_to_process) :]

    async def _buffer_monitor(self):
        """
        Loop forever, checking the buffer for items to process. If the buffer is full or the oldest item has been
        waiting too long, process the batch.
        """
        while True:
            current_time = time.monotonic()
            # each item in the buffer can contain multiple requests, so we need to account for that when checking the
            # buffer size
            buffer_size = sum(len(call.requests) for call in self.buffer)
            if buffer_size >= self.batch_size:
                await self._process_batch()
            elif (
                self.buffer
                and current_time - self.buffer[0].submission_time
                >= self.max_wait_time_seconds
            ):
                await self._process_batch()

            await asyncio.sleep(self.check_interval_seconds)
