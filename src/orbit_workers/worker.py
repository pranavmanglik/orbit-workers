# Copyright 2026-present Orbit Contributors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Provider-neutral queue work contracts and bounded async execution."""

from __future__ import annotations

import asyncio
import inspect
import logging
import math
import random
import re
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Literal, Protocol, runtime_checkable

_LOG = logging.getLogger("orbit.workers")
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
_MAX_PAYLOAD_BYTES = 1_048_576
_MAX_HEADERS = 32
_MAX_HEADER_VALUE_LENGTH = 256
_MAX_HEADER_BYTES = 4_096
_MAX_WORKERS = 256
_MAX_ATTEMPTS = 1_000_000_000
_MAX_POLL_TIMEOUT = 60.0
WORK_QUEUE_DEPENDENCY_KEY = "orbit.work_queue"


class WorkerQueueError(RuntimeError):
    """Safe error indicating a work queue receive or settlement operation failed."""


@dataclass(frozen=True, slots=True)
class WorkMessage:
    """Bounded queue message; payload and headers are omitted from its representation."""

    queue: str
    message_id: str
    payload: bytes = field(repr=False)
    headers: Mapping[str, str] = field(default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        """Validate identifiers, copy header values, and cap retained payload size."""
        object.__setattr__(self, "queue", _validate_identifier(self.queue, "queue"))
        object.__setattr__(self, "message_id", _validate_identifier(self.message_id, "message_id"))
        if not isinstance(self.payload, bytes) or len(self.payload) > _MAX_PAYLOAD_BYTES:
            raise ValueError("payload must be bytes no larger than 1 MiB.")
        object.__setattr__(self, "headers", _freeze_headers(self.headers))


WorkHandler = Callable[[WorkMessage], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class WorkFailure:
    """Safe handler failure metadata without message payload or exception contents."""

    queue: str
    message_id: str
    attempt: int
    error_type: str
    occurred_at: float


WorkFailureHandler = Callable[[WorkFailure], None | Awaitable[None]]


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Bounded retry count and capped exponential delay with optional downward jitter."""

    max_attempts: int = 5
    initial_delay: float = 0.25
    max_delay: float = 30.0
    jitter: float = 0.2

    def __post_init__(self) -> None:
        """Reject invalid attempt and delay bounds before a worker starts."""
        if (
            isinstance(self.max_attempts, bool)
            or not isinstance(self.max_attempts, int)
            or not 1 <= self.max_attempts <= _MAX_ATTEMPTS
        ):
            raise ValueError(f"max_attempts must be an integer from 1 to {_MAX_ATTEMPTS}.")
        if not _finite_range(self.initial_delay, 0.001, 86_400):
            raise ValueError("initial_delay must be finite and between 0.001 and 86400 seconds.")
        if not _finite_range(self.max_delay, self.initial_delay, 86_400):
            raise ValueError(
                "max_delay must be finite and between initial_delay and 86400 seconds."
            )
        if not _finite_range(self.jitter, 0, 1):
            raise ValueError("jitter must be finite and between 0 and 1.")

    def delay_for(self, attempt: int, *, random_value: float | None = None) -> float:
        """Return a capped exponential retry delay with bounded downward jitter.

        ``attempt`` is the 1-based attempt that just failed. Supplying ``random_value`` supports
        deterministic tests; production calls use the standard library's system-seeded generator.
        """
        if (
            isinstance(attempt, bool)
            or not isinstance(attempt, int)
            or not 1 <= attempt <= _MAX_ATTEMPTS
        ):
            raise ValueError("attempt must be a positive bounded integer.")
        delay = self.initial_delay
        remaining = min(attempt - 1, 64)
        while remaining > 0 and delay < self.max_delay:
            delay = min(self.max_delay, delay * 2)
            remaining -= 1
        if self.jitter == 0:
            return delay
        sample = random.random() if random_value is None else random_value
        if not _finite_range(sample, 0, 1):
            raise ValueError("random_value must be finite and between 0 and 1.")
        return delay * (1 - self.jitter * sample)


@runtime_checkable
class WorkDelivery(Protocol):
    """One queue-owned message lease that the worker must settle after processing."""

    @property
    def message(self) -> WorkMessage:
        """Return the immutable work message for this delivery."""

    @property
    def attempt(self) -> int:
        """Return the 1-based current delivery attempt."""

    async def ack(self) -> None:
        """Acknowledge successful handling exactly once."""

    async def retry(self, delay: float) -> None:
        """Release the message for redelivery after a provider-supported delay."""

    async def reject(self) -> None:
        """Permanently reject the message according to provider policy."""


@runtime_checkable
class WorkQueue(Protocol):
    """Queue contract consumed by :class:`QueueWorker`; adapters own their transport lifecycle.

    Core-aware adapters register this contract under ``WORK_QUEUE_DEPENDENCY_KEY``.
    """

    async def receive(self, *, timeout: float) -> WorkDelivery | None:
        """Wait no longer than ``timeout`` and return a delivery, or ``None`` when idle."""


@runtime_checkable
class WorkPublisher(Protocol):
    """Durable publication contract for producers that require broker confirmation.

    Implementations return only after the backend confirms acceptance. A failed or ambiguous
    publication raises so callers can return a retryable response; messages may be delivered more
    than once and consumers must remain idempotent.
    """

    async def publish(self, message: WorkMessage) -> None:
        """Publish one message and wait for durable backend confirmation."""


class QueueWorker:
    """Run bounded queue-backed handlers with explicit at-least-once settlement semantics.

    The queue adapter owns sockets, credentials, visibility leases, durable storage, and shutdown.
    This runner owns only its worker tasks and never closes the queue. Handler success is followed
    by one acknowledgement. Handler failure schedules bounded retries or rejects after the attempt
    limit. Cancellation leaves an in-flight delivery unsettled so the adapter can redeliver it.
    """

    def __init__(
        self,
        queue: WorkQueue,
        handler: WorkHandler,
        *,
        workers: int = 4,
        poll_timeout: float = 1.0,
        retry_policy: RetryPolicy | None = None,
        on_failure: WorkFailureHandler | None = None,
    ) -> None:
        """Validate worker bounds and bind caller-owned queue and handler contracts."""
        if not callable(getattr(queue, "receive", None)):
            raise TypeError("queue must provide an async receive method.")
        if not callable(handler) or not (
            inspect.iscoroutinefunction(handler)
            or inspect.iscoroutinefunction(type(handler).__call__)
        ):
            raise TypeError("handler must be an async callable.")
        if (
            isinstance(workers, bool)
            or not isinstance(workers, int)
            or not 1 <= workers <= _MAX_WORKERS
        ):
            raise ValueError(f"workers must be an integer from 1 to {_MAX_WORKERS}.")
        if not _finite_range(poll_timeout, 0.001, _MAX_POLL_TIMEOUT):
            raise ValueError(
                f"poll_timeout must be finite and between 0.001 and {_MAX_POLL_TIMEOUT}."
            )
        if retry_policy is not None and not isinstance(retry_policy, RetryPolicy):
            raise TypeError("retry_policy must be a RetryPolicy.")
        if on_failure is not None and not callable(on_failure):
            raise TypeError("on_failure must be callable.")
        self._queue = queue
        self._handler = handler
        self._workers = workers
        self._poll_timeout = float(poll_timeout)
        self._retry_policy = retry_policy or RetryPolicy()
        self._on_failure = on_failure
        self._has_run = False

    async def run(self) -> None:
        """Run fixed workers until caller cancellation or a queue operation fails.

        A worker queue failure cancels sibling workers and fails the pool. Supervise this coroutine
        with Core's application lifecycle or another host task manager. A pool instance is single
        use because message state may be uncertain after cancellation or transport failure.
        """
        if self._has_run:
            raise RuntimeError("QueueWorker instances can only be run once.")
        self._has_run = True
        try:
            async with asyncio.TaskGroup() as group:
                for index in range(self._workers):
                    group.create_task(self._worker(index), name=f"orbit-worker-{index}")
        except* WorkerQueueError:
            raise WorkerQueueError(
                "Queue worker group stopped after a queue operation failed."
            ) from None

    async def _worker(self, index: int) -> None:
        """Receive, execute, and settle one delivery at a time for this worker slot."""
        del index  # Task names aid host diagnostics; worker identity is not part of queue data.
        while True:
            try:
                delivery = await self._queue.receive(timeout=self._poll_timeout)
            except asyncio.CancelledError:
                raise
            except Exception:
                raise WorkerQueueError("Queue receive failed.") from None
            if delivery is None:
                # The protocol asks adapters to block for ``timeout``; this guard also prevents
                # a broken adapter from spinning continuously while it has no messages.
                await asyncio.sleep(min(self._poll_timeout, 0.05))
                continue
            if not isinstance(delivery, WorkDelivery):
                raise WorkerQueueError("Queue returned an invalid work delivery.")
            try:
                message = delivery.message
                attempt = delivery.attempt
            except asyncio.CancelledError:
                raise
            except Exception:
                raise WorkerQueueError("Queue returned unreadable delivery metadata.") from None
            if (
                not isinstance(message, WorkMessage)
                or isinstance(attempt, bool)
                or not isinstance(attempt, int)
                or not 1 <= attempt <= _MAX_ATTEMPTS
            ):
                raise WorkerQueueError("Queue returned invalid delivery metadata.")
            try:
                await self._handler(message)
            except asyncio.CancelledError:
                # Leave the lease unsettled; the adapter controls visibility and redelivery.
                raise
            except Exception as exc:
                failure = WorkFailure(
                    queue=message.queue,
                    message_id=message.message_id,
                    attempt=attempt,
                    error_type=type(exc).__name__[:128],
                    occurred_at=time.time(),
                )
                await self._report_failure(failure)
                if attempt >= self._retry_policy.max_attempts:
                    await self._settle(delivery, "reject")
                else:
                    delay = self._retry_policy.delay_for(attempt)
                    await self._settle(delivery, "retry", delay=delay)
            else:
                await self._settle(delivery, "ack")

    async def _settle(
        self,
        delivery: WorkDelivery,
        action: Literal["ack", "retry", "reject"],
        *,
        delay: float | None = None,
    ) -> None:
        """Normalize queue settlement failures without returning backend exception details."""
        try:
            if action == "ack":
                await delivery.ack()
            elif action == "reject":
                await delivery.reject()
            elif delay is not None:
                await delivery.retry(delay)
            else:
                raise WorkerQueueError("Queue retry delay is missing.")
        except asyncio.CancelledError:
            raise
        except Exception:
            message = {
                "ack": "Queue acknowledgement failed.",
                "retry": "Queue retry failed.",
                "reject": "Queue reject failed.",
            }[action]
            raise WorkerQueueError(message) from None

    async def _report_failure(self, failure: WorkFailure) -> None:
        """Notify with payload-free metadata while isolating observer errors."""
        if self._on_failure is None:
            _LOG.warning(
                "Work handler failed: queue=%s attempt=%s error_type=%s",
                failure.queue,
                failure.attempt,
                failure.error_type,
            )
            return
        try:
            outcome = self._on_failure(failure)
            if inspect.isawaitable(outcome):
                await outcome
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _LOG.error(
                "Work failure observer failed: queue=%s error_type=%s",
                failure.queue,
                type(exc).__name__[:128],
            )


def _validate_identifier(value: object, field_name: str) -> str:
    """Validate a bounded identifier safe for diagnostics and provider key composition."""
    if not isinstance(value, str) or _IDENTIFIER.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be a bounded safe ASCII identifier.")
    return value


def _freeze_headers(value: object) -> Mapping[str, str]:
    """Copy bounded string headers into an immutable mapping with a cumulative byte limit."""
    if not isinstance(value, Mapping):
        raise TypeError("headers must be a mapping of string keys and values.")
    result: dict[str, str] = {}
    size = 0
    for key, item in value.items():
        if len(result) >= _MAX_HEADERS:
            raise ValueError("headers exceed the entry limit.")
        if not isinstance(key, str) or _IDENTIFIER.fullmatch(key) is None:
            raise ValueError("header names must be bounded safe ASCII identifiers.")
        if (
            not isinstance(item, str)
            or len(item) > _MAX_HEADER_VALUE_LENGTH
            or any(ord(character) < 32 or ord(character) == 127 for character in item)
        ):
            raise ValueError("header values must be bounded control-free strings.")
        size += len(key.encode("ascii")) + len(item.encode("utf-8"))
        if size > _MAX_HEADER_BYTES:
            raise ValueError("headers exceed the encoded size limit.")
        result[key] = item
    return MappingProxyType(result)


def _finite_range(value: object, minimum: float, maximum: float) -> bool:
    """Check a finite numeric range while rejecting booleans and coercible strings."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    try:
        return math.isfinite(float(value)) and minimum <= value <= maximum
    except (OverflowError, ValueError):
        return False


__all__ = [
    "QueueWorker",
    "RetryPolicy",
    "WORK_QUEUE_DEPENDENCY_KEY",
    "WorkDelivery",
    "WorkFailure",
    "WorkFailureHandler",
    "WorkHandler",
    "WorkMessage",
    "WorkPublisher",
    "WorkQueue",
    "WorkerQueueError",
]
