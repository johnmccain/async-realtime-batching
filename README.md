# Async Realtime Batching

The `async_realtime_batching` package provides a utility for batching asynchronous function calls in real-time. It is designed to aggregate multiple requests into a single batch request, optimizing processing and reducing overhead when the underlying function has efficiency gains from batching. Specifically, this package was built with batched ML inference in mind. When properly configured, realtime batching can improve average processing times and throughput in exchange for a small increase in minimum processing time.

## Usage

```python
from async_realtime_batching import AsyncRealtimeBatcher

@AsyncRealtimeBatcher(batch_size=10, max_wait_time_seconds=2.0)
async def batched_function(requests: list[int]) -> list[int]:
    return requests

# Use the wrapped function
results = await batched_function([1, 2, 3])
```

A single `AsyncRealtimeBatcher` is intended to be used to wrap only one function, and attempting to reuse a batcher will raise an error.

## Configuration

When creating an instance of AsyncRealtimeBatcher, you can configure the following parameters:

`batch_size`: The maximum number of items to batch together. The batch will be processed once this size is reached. The optimal batch size is highly task specific.

`max_wait_time_seconds`: The maximum amount of time a submitted call will wait before being processed, even if the batch size has not been reached. The appropriate value for this depends on your workload, though very small values tend to work well (in the 1-5ms range).

`check_interval_seconds`: The interval at which the buffer will be checked for processing. A smaller interval allows more frequent checks but might increase CPU usage. This defaults to 0.0 for very frequent checks while surrendering control of the event loop between checks.
