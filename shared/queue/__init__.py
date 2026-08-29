"""Durable source-plane queue contracts.

The queue stores only bounded operational metadata.  Source payloads stay in
the custody plane; workers receive an owner-bound, one-time lease token when a
work item is claimed.
"""

from shared.queue.durable import (
    BackpressureDecision,
    ClaimResult,
    DurableQueue,
    EnqueueReceipt,
    LeaseExpiredError,
    LeaseReceipt,
    QueueCollisionError,
    QueueError,
    QueuePolicy,
    QueueStateError,
    RecoveryReceipt,
    SourceBudget,
    WorkItem,
    WorkItemState,
)

__all__ = [
    "BackpressureDecision",
    "ClaimResult",
    "DurableQueue",
    "EnqueueReceipt",
    "LeaseExpiredError",
    "LeaseReceipt",
    "QueueCollisionError",
    "QueueError",
    "QueuePolicy",
    "QueueStateError",
    "RecoveryReceipt",
    "SourceBudget",
    "WorkItem",
    "WorkItemState",
]
