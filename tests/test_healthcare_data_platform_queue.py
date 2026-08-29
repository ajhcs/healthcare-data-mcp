from datetime import datetime, timedelta, timezone
import sqlite3
import threading

import pytest

from shared.queue import (
    DurableQueue,
    QueueCollisionError,
    QueuePolicy,
    QueueStateError,
    SourceBudget,
)


UTC = timezone.utc
T0 = datetime(2026, 1, 1, tzinfo=UTC)
HASH = "a" * 64


def add(queue: DurableQueue, name: str, *, source: str = "source:test", size: int = 10):
    return queue.enqueue(source, name, HASH, size, f"ref/{name}", now=T0)


def test_restart_and_idempotent_admission(tmp_path):
    database = tmp_path / "queue.sqlite"
    with DurableQueue(database) as first:
        receipt = add(first, "work-1")
        duplicate = add(first, "work-1")
        assert receipt.action == "enqueued"
        assert duplicate.action == "duplicate"
        with pytest.raises(QueueCollisionError):
            first.enqueue("source:test", "work-1", "b" * 64, 10, "ref/work-1", now=T0)
    with DurableQueue(database) as second:
        assert second.get("work-1").state == "queued"
        lease = second.claim("worker", now=T0)
        assert lease is not None and lease.work_id == "work-1"


def test_owner_bound_heartbeat_and_completion():
    queue = DurableQueue(":memory:", lease_ttl_seconds=10, heartbeat_extension_seconds=20)
    add(queue, "work-1")
    lease = queue.claim("worker", now=T0)
    assert lease is not None
    with pytest.raises(QueueStateError):
        queue.heartbeat("work-1", "other", lease.lease_token, now=T0 + timedelta(seconds=1))
    renewed = queue.heartbeat("work-1", "worker", lease.lease_token, now=T0 + timedelta(seconds=1))
    assert renewed.lease_expires_at == T0 + timedelta(seconds=21)
    with pytest.raises(QueueStateError):
        queue.complete("work-1", "worker", "wrong-token", now=T0 + timedelta(seconds=2))
    assert queue.complete("work-1", "worker", lease.lease_token, now=T0 + timedelta(seconds=2)).state == "completed"


def test_expired_recovery_is_idempotent_and_retries_then_poison():
    queue = DurableQueue(":memory:", lease_ttl_seconds=5, max_attempts=2)
    add(queue, "work-1")
    first = queue.claim("worker", now=T0)
    assert first is not None
    recovered = queue.recover_expired(now=T0 + timedelta(seconds=6))
    assert recovered[0].state == "requeued"
    assert queue.recover_expired(now=T0 + timedelta(seconds=6)) == ()
    second = queue.claim("worker", now=T0 + timedelta(seconds=6))
    assert second is not None and second.attempt == 2
    recovered_again = queue.recover_expired(now=T0 + timedelta(seconds=12))
    assert recovered_again[0].state == "poisoned"
    assert queue.get("work-1").state == "poison"
    assert queue.claim("worker", now=T0 + timedelta(seconds=12)) is None


def test_retryable_and_explicit_poison_isolate_work():
    queue = DurableQueue(":memory:", retry_delay_seconds=5)
    add(queue, "retry")
    add(queue, "poison")
    leases = queue.claim_many("worker", now=T0, limit=2).leases
    retry_lease = next(lease for lease in leases if lease.work_id == "retry")
    poison_lease = next(lease for lease in leases if lease.work_id == "poison")
    assert queue.fail("retry", "worker", retry_lease.lease_token, "temporary", now=T0).state == "retry_wait"
    assert queue.claim("worker", now=T0 + timedelta(seconds=4)) is None
    retry_again = queue.claim("worker", now=T0 + timedelta(seconds=5))
    assert retry_again is not None and retry_again.work_id == "retry"
    assert (
        queue.fail(
            "retry", "worker", retry_again.lease_token, "bad", retryable=False, now=T0 + timedelta(seconds=5)
        ).state
        == "poison"
    )
    assert (
        queue.mark_poison("poison", "malformed", owner="worker", lease_token=poison_lease.lease_token, now=T0).state
        == "poison"
    )


def test_global_and_source_backpressure_remain_explicit():
    policy = QueuePolicy(max_active_items=4, max_active_bytes=100, source_budgets={"source:a": SourceBudget(1, 10)})
    queue = DurableQueue(":memory:", policy=policy)
    add(queue, "a-1", source="source:a", size=10)
    add(queue, "a-2", source="source:a", size=10)
    first = queue.claim_many("worker", now=T0, limit=2)
    assert len(first.leases) == 1
    assert first.decisions[0].reason == "source_item_budget"
    queue.complete(first.leases[0].work_id, "worker", first.leases[0].lease_token, now=T0)
    lease = queue.claim("worker", now=T0)
    assert lease is not None and lease.work_id == "a-2"


def test_concurrent_claims_do_not_duplicate(tmp_path):
    database = tmp_path / "queue.sqlite"
    with DurableQueue(database) as queue:
        add(queue, "work-1")
    results = []
    barrier = threading.Barrier(2)

    def claim():
        with DurableQueue(database) as queue:
            barrier.wait()
            results.append(queue.claim("worker", now=T0))

    threads = [threading.Thread(target=claim) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert sum(result is not None for result in results) == 1


def test_queue_does_not_store_payload_bytes():
    connection = sqlite3.connect(":memory:")
    queue = DurableQueue(connection)
    add(queue, "work-1")
    columns = {row[1] for row in connection.execute("PRAGMA table_info(queue_work_items)")}
    assert "payload" not in columns
    assert "payload_ref" in columns
