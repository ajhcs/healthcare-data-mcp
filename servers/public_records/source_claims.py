"""Source-claim helpers for public-record responses."""

from typing import Any


def public_source_claim(
    *,
    collection: str,
    dataset_id: str = "",
    match_policy: str,
    identity_paths: list[str] | None = None,
    row_evidence_paths: list[str] | None = None,
) -> dict[str, Any]:
    claim: dict[str, Any] = {
        "collection": collection,
        "identity_paths": identity_paths or ["evidence.query"],
        "evidence_path": "evidence",
        "source_metadata_path": "source_metadata",
        "match_policy": match_policy,
    }
    if dataset_id:
        claim["dataset_id"] = dataset_id
    if row_evidence_paths:
        claim["row_evidence_paths"] = row_evidence_paths
    return claim
