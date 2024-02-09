import asyncio
import functools
import itertools
import threading
import time
from typing import Awaitable, Callable, Generic, NamedTuple, TypeVar

RequestT = TypeVar("RequestT")
ResponseT = TypeVar("ResponseT")


class SubmittedCall(NamedTuple, Generic[RequestT, ResponseT]):
    requests: list[RequestT]
    submission_time: float
    future: asyncio.Future[ResponseT]
    callback_event_loop: asyncio.AbstractEventLoop


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

        self.buffer_lock = threading.Lock()
        self.buffer: list[SubmittedCall] = []

        self.stop_event = threading.Event()
        self.buffer_monitor_thread = threading.Thread(
            target=self._start_monitor, daemon=True
        )
        self.buffer_monitor_thread.start()

    def stop(self, join: bool = True):
        """
        Stop the buffer monitor thread. Calling this method is optional.
        :param join: Whether to join the buffer monitor thread. Defaults to True. The buffer monitor thread is a daemon
            thread, so it will not prevent the program from exiting if it is not joined.
        """
        self.stop_event.set()
        if join:
            self.buffer_monitor_thread.join()

    def _start_monitor(self):
        """
        Start the buffer monitor loop. This method should be called in a separate thread. This method is required to
        start the new event loop for the buffer monitor.
        """
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self._buffer_monitor())
        loop.close()

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
            submitted_call = SubmittedCall(
                requests=requests,
                submission_time=time.monotonic(),
                future=asyncio.get_event_loop().create_future(),
                callback_event_loop=asyncio.get_event_loop(),
            )
            with self.buffer_lock:
                self.buffer.append(submitted_call)
            return await submitted_call.future

        return wrapper

    async def _process_batch(self):
        """
        Process the oldest batch_size items from the buffer and set the results on the corresponding futures.
        """
        if not self.func:
            raise ValueError("No function has been set for this batcher.")

        with self.buffer_lock:
            batch_to_process = self.buffer[: self.batch_size]
            if not batch_to_process:
                return
            self.buffer = self.buffer[len(batch_to_process) :]

        combined_requests = itertools.chain(
            *[call.requests for call in batch_to_process]
        )
        try:
            combined_responses = await self.func(list(combined_requests))
        except Exception as e:  # pylint: disable=broad-except
            # If an exception is raised, set the exception on all the futures in the batch
            for call in batch_to_process:
                call.callback_event_loop.call_soon_threadsafe(
                    call.future.set_exception,
                    e,
                )
            return
        separated_responses = []
        i = 0
        for call in batch_to_process:
            separated_responses.append(combined_responses[i : i + len(call.requests)])
            i += len(call.requests)

        for call, responses in zip(batch_to_process, separated_responses):
            # Schedule the setting of the result on the event loop that the call originated from
            call.callback_event_loop.call_soon_threadsafe(
                call.future.set_result,
                responses,
            )

    async def _buffer_monitor(self):
        """
        Loop until stop event is set, checking the buffer for items to process. If the buffer is full or the oldest
        item has been waiting too long, process the batch.
        """
        while not self.stop_event.is_set():
            current_time = time.monotonic()
            # each item in the buffer can contain multiple requests, so we need to account for that when checking the
            # buffer size
            with self.buffer_lock:
                buffer_size = sum(len(call.requests) for call in self.buffer)
                buffer_size_over_limit = buffer_size >= self.batch_size
                oldest_call = self.buffer[0] if self.buffer else None
                oldest_call_over_time_limit = (
                    oldest_call
                    and current_time - oldest_call.submission_time
                    >= self.max_wait_time_seconds
                )
                should_process = buffer_size_over_limit or oldest_call_over_time_limit
            if should_process:
                await self._process_batch()
            else:
                # Sleep if the buffer wasn't processed. Don't sleep if the buffer was processed, so we can check
                # the buffer again immediately.
                await asyncio.sleep(self.check_interval_seconds)
