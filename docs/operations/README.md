# Orbit Workers: operations guide

`orbit-workers` runs queue consumers but does not provide the broker, durable storage, queue
adapter, or process supervisor. The application owns the queue adapter and should supervise
`QueueWorker.run()` through its lifecycle. A worker instance is single-use; construct a new one
when starting a new supervised run.

## Capacity and polling

The runner allows 1–256 concurrent worker tasks (default 4), each with one active delivery at a
time. This bounds handler concurrency for one `QueueWorker` instance only. Multiply by the number
of instances and replicas to estimate total application concurrency; separately configure the
broker adapter's prefetch, connection-pool size, and provider quotas. The default receive poll
timeout is one second and may be configured from 1 ms to 60 seconds.

Messages are capped at 1 MiB of payload. Up to 32 string headers are accepted, each at most 256
characters, with a 4 KiB combined encoded size. These limits apply when constructing `WorkMessage`;
they do not constrain bytes buffered by the broker client before construction. Set provider-side
message, prefetch, and request limits too.

## Acknowledgement and retry behavior

The runner acknowledges after the handler returns successfully. When the handler raises an
ordinary exception, it reports payload-free metadata and asks the queue to retry or reject:

- Attempts are one-based and supplied by the queue adapter.
- The default policy allows five attempts, beginning with a 250 ms delay and capping at 30 seconds.
- Backoff doubles per attempt and applies downward jitter by default. The queue adapter decides
  whether it can honor the requested delay and must document any rounding or retry policy of its
  own.
- Once the configured attempt count is reached, the delivery is rejected; whether that means
  dead-lettering, deletion, or another broker action is adapter-specific.
- Cancellation propagates and leaves the current delivery unsettled for lease/visibility timeout
  redelivery. The broker's lease must outlast normal processing or be renewed by the adapter.

Processing is at least once. Crashes can happen after side effects but before acknowledgement, and
acknowledgement can be ambiguous if the connection fails. Make handlers idempotent or use an
application-level deduplication strategy. The package provides no exactly-once processing,
distributed transaction, durable retry schedule, ordering, or dead-letter storage.

## Failure and shutdown handling

A receive, metadata, acknowledgement, retry, or reject failure raises a generic `WorkerQueueError`
and stops the worker group. The host should mark the service unhealthy and apply an explicit
restart/backoff policy; restarting cannot determine whether an ambiguous delivery was already
settled. The worker does not close the queue adapter.

On SIGTERM, the application host should stop admitting new work and set an `asyncio.Event` passed
to `QueueWorker.run(stop_event=...)`. Workers finish and settle active handlers, then stop polling.
A delivery returned at the same time as shutdown is left unsettled for broker redelivery. The
worker exits after any in-progress receive returns, bounded by the configured poll timeout. Apply
the deployment termination deadline around `run()`; if it expires, cancel the task and allow the
broker lease to recover unsettled work. Cancellation cannot force-stop a handler that suppresses
cancellation. After the worker task finishes, close adapter-owned clients and pools through the
host lifecycle. Adapters should propagate cancellation and make each delivery settlement
operation single-use. The library does not install process signal handlers.

`on_failure` receives queue name, message ID, attempt count, sanitized exception type, and a Unix
timestamp. It receives neither payload nor exception message. Observer failures are logged and do
not replace the handler outcome. Keep callback work fast and bounded; a slow asynchronous observer
delays retry/settlement. The package does not export built-in metrics or health endpoints, so expose
worker state through the host's observability integrations.

## Production validation

Validate the concrete broker adapter and deployment for visibility timeout/lease renewal,
prefetch, connection recovery, ambiguous settlements, retry scheduling, dead-letter policy,
credential rotation, TLS verification, shutdown deadlines, and duplicate handling. Exercise broker
outages and handler cancellation in an isolated environment. The package's fake queue tests do not
prove broker durability or hosted release readiness.
