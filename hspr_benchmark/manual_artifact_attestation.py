"""Exact, fail-closed validation for scorer-side manual artifact reviews."""

from __future__ import annotations

import hashlib
import json
import stat
from collections.abc import Callable
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "hspr.manual-artifact-attestation.v1"
DISPOSITIONS = {"cleared", "unresolved"}
RECORD_KEYS = {
    "blob_sha1",
    "blob_sha256",
    "path",
    "raw_reason",
    "review_method",
    "review_method_version",
    "disposition",
    "findings",
}
BINDING_KEYS = {
    "audit_implementation_sha256",
    "questions_sha256",
    "identity_sha256",
    "public_ref_shas",
    "raw_uninspectables_sha256",
}


def implementation_sha256() -> str:
    return sha256_bytes(Path(__file__).read_bytes())


def sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def canonical_sha256(value: object) -> str:
    return sha256_bytes(json.dumps(value, sort_keys=True, separators=(",", ":")).encode())


def protected_attestation_bytes(path: Path) -> bytes:
    if path.is_symlink():
        raise ValueError("manual artifact attestation may not be a symlink")
    resolved = path.resolve(strict=True)
    if path.absolute() != resolved or not resolved.is_file():
        raise ValueError("manual artifact attestation must be a direct regular file")
    if stat.S_IMODE(resolved.stat().st_mode) & 0o077:
        raise ValueError("manual artifact attestation permits group or other access")
    return resolved.read_bytes()


def _raw_key(item: dict[str, Any]) -> tuple[str, str, str]:
    if set(item) != {"blob", "path", "reason"}:
        raise ValueError("raw uninspectable artifact has an invalid shape")
    values = (str(item["blob"]), str(item["path"]), str(item["reason"]))
    if not all(values):
        raise ValueError("raw uninspectable artifact has an empty identity field")
    return values


def _record_key(item: dict[str, Any]) -> tuple[str, str, str]:
    return (str(item["blob_sha1"]), str(item["path"]), str(item["raw_reason"]))


def validate_attestation(
    document: dict[str, Any],
    *,
    raw_uninspectables: list[dict[str, Any]],
    audit_implementation_sha256: str,
    questions_sha256: str,
    identity_sha256: str | None,
    public_ref_shas: dict[str, str],
    blob_sha256: Callable[[str], str],
) -> list[dict[str, Any]]:
    """Return residual uninspectables after exact-record review validation."""
    if not isinstance(document, dict) or set(document) != {
        "schema_version",
        "publication_status",
        "bindings",
        "records",
    }:
        raise ValueError("manual artifact attestation has an invalid top-level shape")
    if document.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("manual artifact attestation schema is unsupported")
    if document.get("publication_status") != "protected_scorer_only_exact_record_review":
        raise ValueError("manual artifact attestation has an invalid publication status")
    bindings = document.get("bindings")
    if not isinstance(bindings, dict) or set(bindings) != BINDING_KEYS:
        raise ValueError("manual artifact attestation bindings have an invalid shape")
    expected_bindings = {
        "audit_implementation_sha256": audit_implementation_sha256,
        "questions_sha256": questions_sha256,
        "identity_sha256": identity_sha256,
        "public_ref_shas": public_ref_shas,
        "raw_uninspectables_sha256": canonical_sha256(raw_uninspectables),
    }
    if bindings != expected_bindings:
        raise ValueError("manual artifact attestation is stale or bound to different inputs")

    raw_by_key: dict[tuple[str, str, str], dict[str, Any]] = {}
    for item in raw_uninspectables:
        key = _raw_key(item)
        if key in raw_by_key:
            raise ValueError("raw uninspectable artifact set contains a duplicate record")
        raw_by_key[key] = item
    records = document.get("records")
    if not isinstance(records, list):
        raise TypeError("manual artifact attestation records must be a list")
    reviewed_by_key: dict[tuple[str, str, str], dict[str, Any]] = {}
    for record in records:
        if not isinstance(record, dict) or set(record) != RECORD_KEYS:
            raise ValueError("manual artifact attestation record has an invalid shape")
        key = _record_key(record)
        if key in reviewed_by_key:
            raise ValueError("manual artifact attestation reuses a record")
        if key not in raw_by_key:
            raise ValueError("manual artifact attestation contains an extra record")
        if record.get("disposition") not in DISPOSITIONS:
            raise ValueError("manual artifact attestation disposition is invalid")
        if not isinstance(record.get("review_method"), str) or not record["review_method"].strip():
            raise ValueError("manual artifact attestation review method is empty")
        if not isinstance(record.get("review_method_version"), str) or not record["review_method_version"].strip():
            raise ValueError("manual artifact attestation review method version is empty")
        findings = record.get("findings")
        if (
            not isinstance(findings, list)
            or not findings
            or not all(isinstance(item, str) and item for item in findings)
        ):
            raise ValueError("manual artifact attestation findings are invalid")
        expected_blob_sha256 = blob_sha256(str(record["blob_sha1"]))
        if record.get("blob_sha256") != expected_blob_sha256:
            raise ValueError("manual artifact attestation blob digest is stale")
        reviewed_by_key[key] = record
    if set(reviewed_by_key) != set(raw_by_key):
        raise ValueError("manual artifact attestation is missing records")
    return [raw_by_key[key] for key, record in reviewed_by_key.items() if record["disposition"] != "cleared"]
