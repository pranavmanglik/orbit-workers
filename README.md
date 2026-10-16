# Orbit Workers

`orbit-workers` defines provider-neutral `WorkQueue`/`WorkPublisher` contracts and a runner for
queue-backed async jobs. It is distinct from `orbit-scheduler`, which starts process-local jobs on intervals, and from Core's supervisor for
application-owned background tasks. This package supports Python 3.11 through 3.14 and is an
unpublished v0.1.0-alpha review candidate. Its API may change before a stable release. The local
CPython 3.11.14–3.14.7 matrix passes 26 tests per version, Ruff lint/format, strict mypy, dependency
audit, and wheel/source builds. Hosted CI and broker-backed deployment validation remain open.

```bash
python -m pip install orbit-workers
```

## Queue contract and worker runner

`WorkPublisher.publish()` is the producer-side contract. Adapters return only after backend
confirmation and raise on failed or ambiguous publication; messages may be delivered more than once,
so consumers must be idempotent. This package defines the contract but includes no broker or
durable-storage implementation.

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
For graceful shutdown, the application host sets an `asyncio.Event` passed to `run(stop_event=...)`
on SIGTERM, then enforces its deployment termination deadline. The library does not install signal
handlers.

## Delivery limits

Handlers are at-least-once: they can run again after a timeout, process crash, or lost acknowledgement.
Make side effects idempotent. The runner does not provide exactly-once processing, durable storage,
dead-letter storage, cross-worker ordering, or a broker. A queue failure stops the worker group so
the host can mark it unhealthy and apply an explicit restart policy. No queue or provider adapter is
included; without a separately installed adapter this package cannot publish or receive jobs.

## Documentation

The package-specific guides cover [architecture](docs/architecture/overview.md), [operations and security](docs/operations/README.md), and [development](docs/development/README.md), with [security guidance](docs/security/overview.md). The [documentation index](docs/README.md) links to the full package overview and project policies.

## Development

```bash
python -m pip install -e '.[dev]'
pytest
ruff check .
mypy
```

Licensed under Apache-2.0.
