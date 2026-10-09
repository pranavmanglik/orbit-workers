# Orbit Workers: architecture and boundaries

## Responsibility

`orbit-workers` is a provider-neutral runner for queue-backed async jobs. It is distinct from
`orbit-scheduler`, which starts process-local jobs on intervals, and from Core's supervisor for
application-owned background tasks. This package supports Python 3.11 through 3.14 and is pre-alpha.

```bash
python -m pip install orbit-workers
```

## Declared dependencies

The following dependency declarations come from the checked-in manifests. Optional groups and development dependencies are called out separately.

### `pyproject.toml`
- No dependencies declared.
- Optional `dev` group: `pytest>=8,<10`, `pytest-asyncio>=0.24,<2`, `ruff>=0.8,<1`, `mypy>=1.13,<2`.

Declared dependencies do not mean that optional providers or services are bundled with this package.

## Implementation layout

Representative implementation files in this checkout:

- `src/orbit_workers/__init__.py`
- `src/orbit_workers/worker.py`

## Public contract and scope

## Queue contract and worker runner

`WorkPublisher.publish()` is the producer-side contract. An implementation must return only after
its backend confirms acceptance and raise on failed or ambiguous publication. That lets an HTTP
application return success only after the configured transport accepts a message. Delivery remains
at least once, so consumers must be idempotent. `orbit-workers` defines this contract but includes
no broker client or durable storage; provider packages such as `orbit-workers-rabbitmq` own those.

An adapter implements `WorkQueue.receive()` and returns a leased `WorkDelivery`. The runner invokes
the handler with an immutable `WorkMessage`, then acknowledges success, retries handler failures
using bounded exponential backoff, or rejects a delivery after the configured attempt limit:

```python
from orbit_workers import QueueWorker, RetryPolicy, WorkMessage


async def handle_invoice(message: WorkMessage) -> None:
    await process_invoice_bytes(message.payload)


worker = QueueWorker(
    broker_queue,
    handle_invoice,
    workers=8,
    retry_policy=RetryPolicy(max_attempts=5, initial_delay=0.5, max_delay=20),
)

# Supervise this coroutine with the application lifecycle; cancellation stops its worker tasks.
await worker.run()
```

Concurrency and message payload/header sizes are bounded. Failure callbacks receive only the queue,
message ID, attempt, exception type, and timestamp; the message payload and provider exception text
are never passed to the observer. Task cancellation leaves a delivery unsettled so the broker can
redeliver it after its visibility/lease timeout. Queue adapters must propagate cancellation, bound
prefetch, normalize transport failures, implement each delivery settlement once, and own client
credentials and shutdown. Core-aware adapters register the `WorkQueue` contract under
`WORK_QUEUE_DEPENDENCY_KEY`. The runner does not close the queue. A receive or settlement failure
raises a sanitized `WorkerQueueError` and stops the worker group. Handlers and adapters must
cooperate with cancellation; a callback that suppresses cancellation can delay worker shutdown.

## Boundary rules

Keep provider SDKs, credentials, transports, and provider-specific error translation in provider adapters. Keep reusable capability contracts in the matching capability package and lifecycle orchestration in Core. Apply the relevant layer for this repository and preserve the dependency direction shown above.
