"""Canonical healthcare entity identity map primitives."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from collections.abc import Mapping
from typing import Any

from shared.utils.identity import (
    normalize_address,
    normalize_ccn,
    normalize_enrollment_id,
    normalize_name,
    normalize_npi,
    normalize_zip,
)
from shared.utils.mcp_response import to_structured


@dataclass(frozen=True, slots=True)
class SourceAlias:
    """A source-specific name or identifier observed for an entity."""

    source_name: str
    source_url: str = ""
    name: str = ""
    identifier: str = ""
    identifier_type: str = ""
    retrieved_at: str = ""


@dataclass(frozen=True, slots=True)
class MatchDecision:
    """Auditable match decision joining or rejecting source records."""

    basis: str
    confidence: str
    decided_at: str = ""
    notes: str = ""


@dataclass(slots=True)
class HealthcareIdentity:
    """Canonical public-data identity spine for facilities and organizations."""

    canonical_name: str = ""
    entity_type: str = ""
    ccn: str = ""
    npi: str = ""
    pecos_enrollment_id: str = ""
    ahrq_system_id: str = ""
    owner_id: str = ""
    address: str = ""
    zip_code: str = ""
    aliases: list[SourceAlias] = field(default_factory=list)
    match_decisions: list[MatchDecision] = field(default_factory=list)
    conflicts: list[dict[str, str]] = field(default_factory=list)
    unresolved_identifiers: list[dict[str, str]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return to_structured(asdict(self))  # type: ignore[return-value]


IDENTITY_EXACT_FIELDS: tuple[str, ...] = (
    "ccn",
    "npi",
    "pecos_enrollment_id",
    "ahrq_system_id",
    "owner_id",
)
IDENTITY_CANDIDATE_FIELDS: tuple[str, ...] = (
    "canonical_name",
    "address",
    "zip_code",
)


def identity_from_public_record(
    *,
    name: Any = "",
    entity_type: str = "",
    ccn: Any = "",
    npi: Any = "",
    pecos_enrollment_id: Any = "",
    ahrq_system_id: Any = "",
    owner_id: Any = "",
    address: Any = "",
    zip_code: Any = "",
    source_name: str = "",
    source_url: str = "",
) -> HealthcareIdentity:
    """Normalize one public record into the canonical identity-map shape."""

    identity = HealthcareIdentity(
        canonical_name=normalize_name(name, remove_legal_suffixes=True),
        entity_type=str(entity_type or ""),
        ccn=normalize_ccn(ccn) or "",
        npi=normalize_npi(npi) or "",
        pecos_enrollment_id=normalize_enrollment_id(pecos_enrollment_id) or "",
        ahrq_system_id=str(ahrq_system_id or "").strip(),
        owner_id=str(owner_id or "").strip(),
        address=normalize_address(address),
        zip_code=normalize_zip(zip_code) or "",
    )
    alias_identifier = identity.ccn or identity.npi or identity.pecos_enrollment_id or identity.ahrq_system_id or identity.owner_id
    if name or alias_identifier:
        identity.aliases.append(
            SourceAlias(
                source_name=source_name,
                source_url=source_url,
                name=str(name or ""),
                identifier=alias_identifier,
                identifier_type=_alias_identifier_type(identity),
            )
        )
    for identifier_type, raw, normalized in (
        ("ccn", ccn, identity.ccn),
        ("npi", npi, identity.npi),
        ("pecos_enrollment_id", pecos_enrollment_id, identity.pecos_enrollment_id),
    ):
        if raw not in ("", None) and not normalized:
            identity.unresolved_identifiers.append({"type": identifier_type, "value": str(raw)})
    return identity


def _alias_identifier_type(identity: HealthcareIdentity) -> str:
    if identity.ccn:
        return "ccn"
    if identity.npi:
        return "npi"
    if identity.pecos_enrollment_id:
        return "pecos_enrollment_id"
    if identity.ahrq_system_id:
        return "ahrq_system_id"
    if identity.owner_id:
        return "owner_id"
    return ""


def record_identity_conflict(
    identity: HealthcareIdentity,
    *,
    field: str,
    left: Any,
    right: Any,
    source: str,
) -> HealthcareIdentity:
    """Attach a conflict without mutating unrelated identity fields."""

    identity.conflicts.append(
        {
            "field": field,
            "left": str(left),
            "right": str(right),
            "source": source,
        }
    )
    return identity


def coerce_healthcare_identity(value: HealthcareIdentity | Mapping[str, Any]) -> HealthcareIdentity:
    """Coerce a serialized identity map back into a HealthcareIdentity."""

    if isinstance(value, HealthcareIdentity):
        return coerce_healthcare_identity(value.to_dict())
    identity = HealthcareIdentity(
        canonical_name=str(value.get("canonical_name") or ""),
        entity_type=str(value.get("entity_type") or ""),
        ccn=str(value.get("ccn") or ""),
        npi=str(value.get("npi") or ""),
        pecos_enrollment_id=str(value.get("pecos_enrollment_id") or ""),
        ahrq_system_id=str(value.get("ahrq_system_id") or ""),
        owner_id=str(value.get("owner_id") or ""),
        address=str(value.get("address") or ""),
        zip_code=str(value.get("zip_code") or ""),
    )
    identity.aliases.extend(_coerce_aliases(value.get("aliases") or []))
    identity.match_decisions.extend(_coerce_match_decisions(value.get("match_decisions") or []))
    identity.conflicts.extend(
        {
            "field": str(conflict.get("field") or ""),
            "left": str(conflict.get("left") or ""),
            "right": str(conflict.get("right") or ""),
            "source": str(conflict.get("source") or ""),
        }
        for conflict in value.get("conflicts") or []
        if isinstance(conflict, Mapping)
    )
    identity.unresolved_identifiers.extend(
        {"type": str(item.get("type") or ""), "value": str(item.get("value") or "")}
        for item in value.get("unresolved_identifiers") or []
        if isinstance(item, Mapping)
    )
    return identity


def merge_healthcare_identities(
    seed: HealthcareIdentity | Mapping[str, Any],
    *candidates: HealthcareIdentity | Mapping[str, Any],
    basis: str = "conservative_public_identifier_merge",
    confidence: str = "review_required",
) -> HealthcareIdentity:
    """Merge public-data identities without overwriting conflicting identifiers.

    Exact identifier fields merge only when they agree or the base identity is
    missing the value. Candidate fields such as names and addresses fill blanks
    but otherwise become conflicts for human review.
    """

    merged = coerce_healthcare_identity(seed)
    for candidate_value in candidates:
        candidate = coerce_healthcare_identity(candidate_value)
        _merge_scalar_fields(merged, candidate)
        _merge_aliases(merged, candidate.aliases)
        _merge_match_decisions(merged, candidate.match_decisions)
        _merge_conflicts(merged, candidate.conflicts)
        _merge_unresolved_identifiers(merged, candidate.unresolved_identifiers)

    if candidates:
        decision = MatchDecision(
            basis=basis,
            confidence=confidence,
            notes=(
                "Exact public identifiers were merged only when non-conflicting; "
                "name, address, and ZIP disagreements were preserved as conflicts."
            ),
        )
        _merge_match_decisions(merged, [decision])
    return merged


def _merge_scalar_fields(target: HealthcareIdentity, candidate: HealthcareIdentity) -> None:
    for field_name in ("entity_type", *IDENTITY_EXACT_FIELDS, *IDENTITY_CANDIDATE_FIELDS):
        current = str(getattr(target, field_name) or "")
        incoming = str(getattr(candidate, field_name) or "")
        if not incoming:
            continue
        if not current:
            setattr(target, field_name, incoming)
            continue
        if current == incoming:
            continue
        record_identity_conflict(
            target,
            field=field_name,
            left=current,
            right=incoming,
            source=_identity_source_label(candidate),
        )


def _identity_source_label(identity: HealthcareIdentity) -> str:
    for alias in identity.aliases:
        if alias.source_name:
            return alias.source_name
    return "source_identity"


def _coerce_aliases(values: Any) -> list[SourceAlias]:
    aliases: list[SourceAlias] = []
    for value in values:
        if isinstance(value, SourceAlias):
            aliases.append(value)
        elif isinstance(value, Mapping):
            aliases.append(
                SourceAlias(
                    source_name=str(value.get("source_name") or ""),
                    source_url=str(value.get("source_url") or ""),
                    name=str(value.get("name") or ""),
                    identifier=str(value.get("identifier") or ""),
                    identifier_type=str(value.get("identifier_type") or ""),
                    retrieved_at=str(value.get("retrieved_at") or ""),
                )
            )
    return aliases


def _coerce_match_decisions(values: Any) -> list[MatchDecision]:
    decisions: list[MatchDecision] = []
    for value in values:
        if isinstance(value, MatchDecision):
            decisions.append(value)
        elif isinstance(value, Mapping):
            decisions.append(
                MatchDecision(
                    basis=str(value.get("basis") or ""),
                    confidence=str(value.get("confidence") or ""),
                    decided_at=str(value.get("decided_at") or ""),
                    notes=str(value.get("notes") or ""),
                )
            )
    return decisions


def _merge_aliases(target: HealthcareIdentity, aliases: list[SourceAlias]) -> None:
    seen = {
        (alias.source_name, alias.source_url, alias.name, alias.identifier, alias.identifier_type)
        for alias in target.aliases
    }
    for alias in aliases:
        key = (alias.source_name, alias.source_url, alias.name, alias.identifier, alias.identifier_type)
        if key not in seen:
            target.aliases.append(alias)
            seen.add(key)


def _merge_match_decisions(target: HealthcareIdentity, decisions: list[MatchDecision]) -> None:
    seen = {(decision.basis, decision.confidence, decision.decided_at, decision.notes) for decision in target.match_decisions}
    for decision in decisions:
        key = (decision.basis, decision.confidence, decision.decided_at, decision.notes)
        if key not in seen:
            target.match_decisions.append(decision)
            seen.add(key)


def _merge_conflicts(target: HealthcareIdentity, conflicts: list[dict[str, str]]) -> None:
    seen = {(conflict.get("field"), conflict.get("left"), conflict.get("right"), conflict.get("source")) for conflict in target.conflicts}
    for conflict in conflicts:
        normalized = {
            "field": str(conflict.get("field") or ""),
            "left": str(conflict.get("left") or ""),
            "right": str(conflict.get("right") or ""),
            "source": str(conflict.get("source") or ""),
        }
        key = (normalized["field"], normalized["left"], normalized["right"], normalized["source"])
        if key not in seen:
            target.conflicts.append(normalized)
            seen.add(key)


def _merge_unresolved_identifiers(target: HealthcareIdentity, unresolved: list[dict[str, str]]) -> None:
    seen = {(item.get("type"), item.get("value")) for item in target.unresolved_identifiers}
    for item in unresolved:
        normalized = {"type": str(item.get("type") or ""), "value": str(item.get("value") or "")}
        key = (normalized["type"], normalized["value"])
        if key not in seen:
            target.unresolved_identifiers.append(normalized)
            seen.add(key)


__all__ = [
    "HealthcareIdentity",
    "IDENTITY_CANDIDATE_FIELDS",
    "IDENTITY_EXACT_FIELDS",
    "MatchDecision",
    "SourceAlias",
    "coerce_healthcare_identity",
    "identity_from_public_record",
    "merge_healthcare_identities",
    "record_identity_conflict",
]
