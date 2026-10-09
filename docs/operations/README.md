# Orbit Workers: operations and security

This guide organizes runtime behavior documented by the package. It does not certify production readiness. Verify provider/client versions, permissions, transport security, limits, and failure behavior in the target environment before release.

## Configuration surface

Environment names found in the package README:

The package README does not name `ORBIT_*` variables. Use its typed constructors and application configuration, and confirm exact runtime inputs in the implementation before deployment.

Use the package README's constructor and deployment examples. Store credentials in a secret manager and avoid logging credentials, raw provider errors, request data, or opaque cursors.

## Lifecycle, failure behavior, and limits

`orbit-workers` is a provider-neutral runner for queue-backed async jobs. It is distinct from
`orbit-scheduler`, which starts process-local jobs on intervals, and from Core's supervisor for
application-owned background tasks. This package supports Python 3.11 through 3.14 and is pre-alpha.

```bash
python -m pip install orbit-workers
```

## Delivery limits

Handlers are at-least-once: they can run again after a timeout, process crash, or lost acknowledgement.
Make side effects idempotent. The runner does not provide exactly-once processing, durable storage,
dead-letter storage, cross-worker ordering, or a broker. `WorkPublisher` defines a confirmed
publication contract but does not itself store messages. A queue failure stops the worker group so
the host can mark it unhealthy and apply an explicit restart policy. No queue or provider adapter is
included; without a separately installed adapter this package cannot publish or receive jobs.

## Production validation

Validate startup/shutdown cleanup, timeout and cancellation behavior, concurrency and payload bounds where applicable, secret rotation and least-privilege access, data durability, backup/restore, and failover against the selected provider. Do not infer distributed or durable guarantees from an in-process API or fake-client tests.
