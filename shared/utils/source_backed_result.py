"""Traceability contracts for source-backed healthcare results."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
import re
from typing import Any

from shared.utils.mcp_response import (
    ReportIngestContractError,
    to_structured,
    validate_evidence_receipt,
)


@dataclass(frozen=True, slots=True)
class SourceClaimPathIssue:
    """One source claim path defect found in a result payload."""

    path: str
    reason: str
    detail: str = ""

    def to_dict(self) -> dict[str, str]:
        return asdict(self)


class SourceClaimPathError(ValueError):
    """Raised when source claim paths do not satisfy the requested contract."""


def source_claim(
    *,
    collection: str,
    source_name: str = "",
    source_url: str = "",
    evidence_path: str = "evidence",
    source_metadata_path: str = "source_metadata",
    identity_paths: Iterable[str] = (),
    row_evidence_paths: Iterable[str] = (),
    match_policy: str = "",
    **fields: Any,
) -> dict[str, Any]:
    """Build the canonical traceability link between a result claim and evidence."""

    claim = {
        "collection": collection,
        "source_name": source_name,
        "source_url": source_url,
        "evidence_path": evidence_path,
        "source_metadata_path": source_metadata_path,
        "identity_paths": list(identity_paths),
        "row_evidence_paths": list(row_evidence_paths),
        "match_policy": match_policy,
    }
    for key, value in fields.items():
        if value is not None:
            claim[key] = to_structured(value)
    return claim


def validate_source_claim_paths(
    payload: Any,
    *,
    require_boundary_traceability: bool = False,
    raise_on_error: bool = False,
) -> dict[str, Any]:
    """Validate source claim paths for exploratory or boundary-bound results.

    Compatibility mode validates only declared paths. Boundary mode additionally
    requires source claims, evidence paths, source metadata paths, and contentful
    evidence receipts suitable for live-gateway provenance checks.
    """

    structured = to_structured(payload)
    issues: list[SourceClaimPathIssue] = []
    if not isinstance(structured, Mapping):
        issues.append(SourceClaimPathIssue("result", "non_object_result", "Source-backed result must be an object."))
        return _summary(issues, require_boundary_traceability, raise_on_error)

    identity_map = structured.get("identity_map")
    if not isinstance(identity_map, Mapping):
        if require_boundary_traceability:
            issues.append(SourceClaimPathIssue("identity_map", "missing_identity_map"))
        return _summary(issues, require_boundary_traceability, raise_on_error)

    source_claims = identity_map.get("source_claims")
    if not isinstance(source_claims, Sequence) or isinstance(source_claims, str | bytes | bytearray):
        if require_boundary_traceability:
            issues.append(SourceClaimPathIssue("identity_map.source_claims", "missing_source_claims"))
        return _summary(issues, require_boundary_traceability, raise_on_error)
    if require_boundary_traceability and not source_claims:
        issues.append(SourceClaimPathIssue("identity_map.source_claims", "missing_source_claims"))
        return _summary(issues, require_boundary_traceability, raise_on_error)

    for index, raw_claim in enumerate(source_claims):
        claim_path = f"identity_map.source_claims[{index}]"
        if not isinstance(raw_claim, Mapping):
            issues.append(SourceClaimPathIssue(claim_path, "non_object_source_claim"))
            continue
        _validate_claim(structured, raw_claim, claim_path, issues, require_boundary_traceability=require_boundary_traceability)

    return _summary(issues, require_boundary_traceability, raise_on_error)


def values_at_path(payload: Any, path: str) -> list[Any]:
    """Resolve a dotted source claim path with ``[]`` list wildcards."""

    if not path:
        return []
    structured = to_structured(payload)
    parts = [part for part in _normalize_path(path).split(".") if part]
    values: list[Any] = [structured]
    for part in parts:
        next_values: list[Any] = []
        key, selector = _parse_part(part)
        for value in values:
            if not isinstance(value, Mapping) or key not in value:
                continue
            child = value[key]
            if selector == "all":
                if isinstance(child, Sequence) and not isinstance(child, str | bytes | bytearray):
                    next_values.extend(child)
            elif isinstance(selector, int):
                if isinstance(child, Sequence) and not isinstance(child, str | bytes | bytearray):
                    if 0 <= selector < len(child):
                        next_values.append(child[selector])
            else:
                next_values.append(child)
        values = next_values
        if not values:
            return []
    return values


def _validate_claim(
    payload: Mapping[str, Any],
    claim: Mapping[str, Any],
    claim_path: str,
    issues: list[SourceClaimPathIssue],
    *,
    require_boundary_traceability: bool,
) -> None:
    evidence_path = str(claim.get("evidence_path") or "")
    source_metadata_path = str(claim.get("source_metadata_path") or "")

    if require_boundary_traceability and not evidence_path:
        issues.append(SourceClaimPathIssue(f"{claim_path}.evidence_path", "missing_evidence_path"))
    if evidence_path:
        evidence_values = _require_path(payload, evidence_path, f"{claim_path}.evidence_path", issues)
        if require_boundary_traceability:
            for value_index, value in enumerate(evidence_values):
                try:
                    validate_evidence_receipt(value, require_content=True)
                except ReportIngestContractError as exc:
                    issues.append(
                        SourceClaimPathIssue(
                            f"{claim_path}.evidence_path[{value_index}]",
                            "invalid_evidence_receipt",
                            str(exc),
                        )
                    )

    if require_boundary_traceability and not source_metadata_path:
        issues.append(SourceClaimPathIssue(f"{claim_path}.source_metadata_path", "missing_source_metadata_path"))
    if source_metadata_path:
        source_metadata_values = _require_path(
            payload,
            source_metadata_path,
            f"{claim_path}.source_metadata_path",
            issues,
        )
        for value_index, value in enumerate(source_metadata_values):
            if not isinstance(value, Mapping):
                issues.append(
                    SourceClaimPathIssue(
                        f"{claim_path}.source_metadata_path[{value_index}]",
                        "source_metadata_not_object",
                    )
                )

    for field_name in ("identity_path", "identity_paths"):
        raw_paths = claim.get(field_name)
        for path_index, path in enumerate(_as_paths(raw_paths)):
            _require_path(payload, path, f"{claim_path}.{field_name}[{path_index}]", issues)

    legacy_row_paths = _as_paths(claim.get("row_evidence_path"))
    if require_boundary_traceability and legacy_row_paths:
        issues.append(
            SourceClaimPathIssue(
                f"{claim_path}.row_evidence_path",
                "legacy_row_evidence_path",
                "Use row_evidence_paths[] for boundary traceability.",
            )
        )

    row_paths = _as_paths(claim.get("row_evidence_paths"))
    if not require_boundary_traceability:
        row_paths = [*legacy_row_paths, *row_paths]
    for path_index, path in enumerate(row_paths):
        row_values = _require_path(payload, path, f"{claim_path}.row_evidence_paths[{path_index}]", issues)
        if require_boundary_traceability:
            for value_index, value in enumerate(row_values):
                try:
                    validate_evidence_receipt(value, require_content=True)
                except ReportIngestContractError as exc:
                    issues.append(
                        SourceClaimPathIssue(
                            f"{claim_path}.row_evidence_paths[{path_index}][{value_index}]",
                            "invalid_row_evidence_receipt",
                            str(exc),
                        )
                    )


def _require_path(
    payload: Mapping[str, Any],
    path: str,
    issue_path: str,
    issues: list[SourceClaimPathIssue],
) -> list[Any]:
    values = values_at_path(payload, path)
    if not values:
        issues.append(SourceClaimPathIssue(issue_path, "path_not_found", path))
    return values


def _summary(
    issues: list[SourceClaimPathIssue],
    require_boundary_traceability: bool,
    raise_on_error: bool,
) -> dict[str, Any]:
    ok = not issues
    status = "source_claim_paths_valid" if ok else "source_claim_paths_invalid"
    payload = {
        "status": status,
        "valid": ok,
        "boundary_traceability_required": require_boundary_traceability,
        "issue_count": len(issues),
        "issues": [issue.to_dict() for issue in issues],
    }
    if issues and raise_on_error:
        raise SourceClaimPathError("; ".join(f"{issue.path}: {issue.reason}" for issue in issues))
    return payload


def _normalize_path(path: str) -> str:
    stripped = str(path or "").strip()
    if stripped.startswith("result."):
        return stripped[len("result.") :]
    return stripped


_PART_RE = re.compile(r"^(?P<key>[^\[\]]+)(?:\[(?P<selector>\d*)\])?$")


def _parse_part(part: str) -> tuple[str, str | int | None]:
    match = _PART_RE.match(part)
    if not match:
        return part, None
    selector = match.group("selector")
    if selector is None:
        return match.group("key"), None
    if selector == "":
        return match.group("key"), "all"
    return match.group("key"), int(selector)


def _as_paths(value: Any) -> list[str]:
    if value in ("", None):
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, Iterable):
        return [str(item) for item in value if str(item or "").strip()]
    return [str(value)]


__all__ = [
    "SourceClaimPathError",
    "SourceClaimPathIssue",
    "source_claim",
    "validate_source_claim_paths",
    "values_at_path",
]
