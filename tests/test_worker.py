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
"""Queue worker settlement, retry, bounds, and cancellation semantics."""

from __future__ import annotations

import asyncio

import pytest

from orbit_workers import (
    WORK_QUEUE_DEPENDENCY_KEY,
    QueueWorker,
    RetryPolicy,
    WorkerQueueError,
    WorkFailure,
    WorkMessage,
)


class FakeDelivery:
    """Record one fake queue lease's explicit settlement call."""

    def __init__(self, *, attempt: int = 1, failure: Exception | None = None) -> None:
        self.message = WorkMessage(
            queue="invoices",
            message_id=f"message-{attempt}",
            payload=b"payload",
            headers={"content-type": "application/octet-stream"},
        )
        self.attempt = attempt
        self.failure = failure
        self.settlement: tuple[str, float | None] | None = None
        self.settled = asyncio.Event()

    async def ack(self) -> None:
        await self._settle("ack", None)

    async def retry(self, delay: float) -> None:
        await self._settle("retry", delay)

    async def reject(self) -> None:
        await self._settle("reject", None)

    async def _settle(self, action: str, delay: float | None) -> None:
        if self.failure is not None:
            raise self.failure
        if self.settlement is not None:
            raise RuntimeError("delivery was settled more than once")
        self.settlement = action, delay
        self.settled.set()


class FakeQueue:
    """Cancellable bounded-wait queue fake with optional transport failure."""

    def __init__(self) -> None:
        self.items: asyncio.Queue[FakeDelivery] = asyncio.Queue()
        self.failure: Exception | None = None

    async def receive(self, *, timeout: float) -> FakeDelivery | None:
        if self.failure is not None:
            raise self.failure
        try:
            return await asyncio.wait_for(self.items.get(), timeout=timeout)
        except TimeoutError:
            return None


async def stop_pool(task: asyncio.Task[None]) -> None:
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.parametrize("workers", [0, -1, 257, True, 1.5])
def test_worker_count_is_bounded(workers: object) -> None:
    async def handler(message: WorkMessage) -> None:
        return None

    with pytest.raises(ValueError, match="workers"):
        QueueWorker(FakeQueue(), handler, workers=workers)  # type: ignore[arg-type]


@pytest.mark.parametrize("delay", [0, -1, float("inf"), float("nan"), True])
def test_retry_delays_must_be_finite_and_positive(delay: object) -> None:
    with pytest.raises(ValueError, match="initial_delay"):
        RetryPolicy(initial_delay=delay)  # type: ignore[arg-type]


def test_retry_policy_caps_backoff_and_applies_bounded_jitter() -> None:
    policy = RetryPolicy(max_attempts=5, initial_delay=1, max_delay=4, jitter=0.25)

    assert policy.delay_for(1, random_value=1) == pytest.approx(0.75)
    assert policy.delay_for(3, random_value=0) == 4
    assert policy.delay_for(10, random_value=1) == pytest.approx(3)
    with pytest.raises(ValueError, match="random_value"):
        policy.delay_for(1, random_value=2)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"queue": "bad queue", "message_id": "id", "payload": b""},
        {"queue": "queue", "message_id": "id", "payload": "not-bytes"},
        {"queue": "queue", "message_id": "id", "payload": b"x" * (1_048_576 + 1)},
        {"queue": "queue", "message_id": "id", "payload": b"", "headers": {"bad key": "v"}},
        {"queue": "queue", "message_id": "id", "payload": b"", "headers": {"x": "a\nb"}},
    ],
)
def test_message_rejects_invalid_identifiers_payloads_and_headers(
    kwargs: dict[str, object],
) -> None:
    with pytest.raises(ValueError):
        WorkMessage(**kwargs)  # type: ignore[arg-type]


def test_message_repr_omits_payload_and_headers() -> None:
    message = WorkMessage(
        queue="invoices",
        message_id="message-1",
        payload=b"secret-payload",
        headers={"authorization": "secret-header"},
    )

    assert "secret-payload" not in repr(message)
    assert "secret-header" not in repr(message)
    assert WORK_QUEUE_DEPENDENCY_KEY == "orbit.work_queue"


async def test_worker_acks_success_and_obeys_concurrency_bound() -> None:
    queue = FakeQueue()
    deliveries = [FakeDelivery(attempt=index + 1) for index in range(6)]
    for delivery in deliveries:
        await queue.items.put(delivery)
    active = 0
    peak = 0
    completed = 0
    all_done = asyncio.Event()

    async def handler(message: WorkMessage) -> None:
        nonlocal active, peak, completed
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.005)
        active -= 1
        completed += 1
        if completed == len(deliveries):
            all_done.set()

    pool = QueueWorker(queue, handler, workers=2)
    task = asyncio.create_task(pool.run())
    await asyncio.wait_for(all_done.wait(), timeout=1)
    await stop_pool(task)

    assert peak == 2
    assert all(delivery.settlement == ("ack", None) for delivery in deliveries)


async def test_handler_failure_reports_safe_metadata_and_retries() -> None:
    queue = FakeQueue()
    delivery = FakeDelivery(attempt=1)
    await queue.items.put(delivery)
    reports: list[WorkFailure] = []
    failed = asyncio.Event()

    async def handler(message: WorkMessage) -> None:
        raise ValueError("database credential=secret-value")

    def report(failure: WorkFailure) -> None:
        reports.append(failure)
        failed.set()

    pool = QueueWorker(
        queue,
        handler,
        retry_policy=RetryPolicy(initial_delay=0.1, max_delay=1, jitter=0),
        on_failure=report,
    )
    task = asyncio.create_task(pool.run())
    await asyncio.wait_for(failed.wait(), timeout=1)
    await asyncio.wait_for(delivery.settled.wait(), timeout=1)
    await stop_pool(task)

    assert delivery.settlement == ("retry", 0.1)
    assert reports[0].error_type == "ValueError"
    assert "secret-value" not in repr(reports[0])


async def test_handler_exception_type_is_sanitized_before_reporting() -> None:
    queue = FakeQueue()
    delivery = FakeDelivery()
    await queue.items.put(delivery)
    reports: list[WorkFailure] = []
    failed = asyncio.Event()
    unsafe_error_type = type("Failure\nforged", (RuntimeError,), {})

    async def handler(message: WorkMessage) -> None:
        raise unsafe_error_type("private handler details")

    def report(failure: WorkFailure) -> None:
        reports.append(failure)
        failed.set()

    task = asyncio.create_task(
        QueueWorker(
            queue,
            handler,
            on_failure=report,
            retry_policy=RetryPolicy(jitter=0),
        ).run()
    )
    await asyncio.wait_for(failed.wait(), timeout=1)
    await asyncio.wait_for(delivery.settled.wait(), timeout=1)
    await stop_pool(task)

    assert reports[0].error_type == "Exception"
    assert "\n" not in reports[0].error_type
    assert "private handler details" not in repr(reports[0])


async def test_attempt_limit_rejects_without_retrying() -> None:
    queue = FakeQueue()
    delivery = FakeDelivery(attempt=2)
    await queue.items.put(delivery)
    entered = asyncio.Event()

    async def handler(message: WorkMessage) -> None:
        entered.set()
        raise RuntimeError("handler failed")

    task = asyncio.create_task(
        QueueWorker(queue, handler, retry_policy=RetryPolicy(max_attempts=2)).run()
    )
    await asyncio.wait_for(entered.wait(), timeout=1)
    await asyncio.wait_for(delivery.settled.wait(), timeout=1)
    await stop_pool(task)
    assert delivery.settlement == ("reject", None)


async def test_handler_cancellation_leaves_delivery_unsettled() -> None:
    queue = FakeQueue()
    delivery = FakeDelivery()
    await queue.items.put(delivery)
    entered = asyncio.Event()

    async def handler(message: WorkMessage) -> None:
        entered.set()
        await asyncio.Event().wait()

    task = asyncio.create_task(QueueWorker(queue, handler).run())
    await asyncio.wait_for(entered.wait(), timeout=1)
    await stop_pool(task)

    assert delivery.settlement is None


async def test_graceful_stop_finishes_active_work_without_receiving_more() -> None:
    queue = FakeQueue()
    active = FakeDelivery(attempt=1)
    pending = FakeDelivery(attempt=2)
    await queue.items.put(active)
    await queue.items.put(pending)
    entered = asyncio.Event()
    finish = asyncio.Event()
    stop = asyncio.Event()

    async def handler(message: WorkMessage) -> None:
        entered.set()
        await finish.wait()

    task = asyncio.create_task(
        QueueWorker(queue, handler, workers=1, poll_timeout=0.01).run(stop_event=stop)
    )
    await asyncio.wait_for(entered.wait(), timeout=1)
    stop.set()
    finish.set()
    await asyncio.wait_for(task, timeout=1)

    assert active.settlement == ("ack", None)
    assert pending.settlement is None
    assert queue.items.qsize() == 1


@pytest.mark.parametrize(
    ("queue_failure", "settlement"),
    [
        (RuntimeError("redis://user:secret@host"), None),
        (None, "ack"),
    ],
)
async def test_queue_failures_are_sanitized(
    queue_failure: Exception | None, settlement: str | None
) -> None:
    queue = FakeQueue()
    queue.failure = queue_failure
    if settlement is not None:
        delivery = FakeDelivery(failure=RuntimeError("password=secret-value"))
        await queue.items.put(delivery)

    async def handler(message: WorkMessage) -> None:
        return None

    with pytest.raises(WorkerQueueError) as caught:
        await QueueWorker(queue, handler, poll_timeout=0.01).run()
    assert "secret" not in str(caught.value)


async def test_failure_observer_error_does_not_prevent_retry() -> None:
    queue = FakeQueue()
    delivery = FakeDelivery()
    await queue.items.put(delivery)
    failed = asyncio.Event()

    async def handler(message: WorkMessage) -> None:
        failed.set()
        raise ValueError("failure")

    def observer(failure: WorkFailure) -> None:
        raise RuntimeError("observer failure")

    task = asyncio.create_task(
        QueueWorker(queue, handler, on_failure=observer, retry_policy=RetryPolicy(jitter=0)).run()
    )
    await asyncio.wait_for(failed.wait(), timeout=1)
    await asyncio.wait_for(delivery.settled.wait(), timeout=1)
    await stop_pool(task)
    assert delivery.settlement == ("retry", 0.25)
