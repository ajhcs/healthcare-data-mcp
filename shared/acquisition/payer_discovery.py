"""Source-bound payer TOC candidates for the Data MCP acquisition plane."""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from typing import Literal, Mapping

from shared.contracts.healthcare_data_platform import validate_observation_envelope

PayerType = Literal["medicare_advantage", "medicare_part_d", "marketplace"]
Missingness = Literal["not_yet_researched", "unavailable_public", "not_applicable", "blocked_source_conflict"]

SOURCE_CATALOG = {
    "cms_ma_state_county_enrollment": {"source_id": "source:cms:ma-state-county-enrollment", "payer_type": "medicare_advantage", "source_url": "https://data.cms.gov/summary-statistics-on-beneficiary-enrollment/medicare-advantage-enrollment", "release_locator": "https://data.cms.gov/summary-statistics-on-beneficiary-enrollment/medicare-advantage-enrollment"},
    "cms_part_d_state_county_enrollment": {"source_id": "source:cms:part-d-state-county-enrollment", "payer_type": "medicare_part_d", "source_url": "https://data.cms.gov/summary-statistics-on-beneficiary-enrollment/part-d-enrollment", "release_locator": "https://data.cms.gov/summary-statistics-on-beneficiary-enrollment/part-d-enrollment"},
    "cms_marketplace_effectuated_enrollment": {"source_id": "source:cms:marketplace-effectuated-enrollment", "payer_type": "marketplace", "source_url": "https://www.cms.gov/data-research/statistics-trends-and-reports/marketplace-products/marketplace-enrollment", "release_locator": "https://www.cms.gov/data-research/statistics-trends-and-reports/marketplace-products/marketplace-enrollment"},
}
REJECTED_FAMILIES = frozenset({"census_population", "acs_population", "census_insurance", "modeled_population"})


@dataclass(frozen=True, slots=True)
class PayerCandidate:
    payer_type: PayerType | None
    source_family: str
    source_period: str
    geography: str
    plan_or_contract_id: str
    numerator: int | None
    denominator: int | None
    denominator_scope: str
    source_url: str
    artifact_id: str
    content_sha256: str
    rights_status: Literal["approved_public", "pending_review", "blocked"]
    custody_state: Literal["frozen_verified_external", "unverified_external", "rejected"]
    missingness: Missingness | None = None

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> "PayerCandidate":
        family = str(value.get("source_family") or "")
        configured = SOURCE_CATALOG.get(family)
        payer = str(value.get("payer_type") or value.get("type_of_coverage") or "")
        if family in REJECTED_FAMILIES or "census" in family or "acs" in family:
            raise ValueError("census/ACS sources cannot provide payer denominators")
        if configured is None or payer != configured["payer_type"]:
            raise ValueError("payer type is not registered for source family")
        if not str(value.get("source_period") or "") or not str(value.get("geography") or value.get("state") or ""):
            raise ValueError("source period and geography are required")
        denominator = value.get("denominator")
        if denominator is None and not value.get("missingness"):
            raise ValueError("denominator is required for a supported payer candidate")
        if denominator is not None and (not isinstance(denominator, int) or denominator < 0):
            raise ValueError("denominator must be a non-negative integer")
        return cls(payer, family, str(value["source_period"]), str(value.get("geography") or value["state"]), str(value.get("plan_or_contract_id") or value.get("plan_id") or value.get("contract_id") or ""), value.get("numerator") if isinstance(value.get("numerator"), int) else None, denominator if isinstance(denominator, int) else None, str(value.get("denominator_scope") or configured["payer_type"] + " enrollment"), str(value.get("source_url") or configured["source_url"]), str(value.get("artifact_id") or ""), str(value.get("content_sha256") or ""), "approved_public", "frozen_verified_external" if value.get("content_sha256") else "unverified_external", value.get("missingness") if value.get("missingness") in {"not_yet_researched", "unavailable_public", "not_applicable", "blocked_source_conflict"} else None)  # type: ignore[arg-type]


def build_payer_observation_envelope(*, tracking_bead: str, source_family: str, source_period: str, artifact_bytes: bytes, candidates: list[Mapping[str, object]], retrieved_at: str, custody_metadata: Mapping[str, object] | None = None) -> dict[str, object]:
    """Build a secret-free source-bound observation envelope for candidates."""
    source = SOURCE_CATALOG.get(source_family)
    if source is None:
        raise ValueError("unregistered payer source family")
    custody = dict(custody_metadata or {})
    custody_locator = custody.get("custody_locator") or custody.get("locator")
    if not isinstance(custody_locator, str) or not custody_locator.startswith(("object://", "parquet://")):
        raise ValueError("caller custody metadata must provide an object:// or parquet:// locator")
    if not isinstance(custody.get("artifact_id"), str) or not isinstance(custody.get("verified"), bool):
        raise ValueError("caller custody metadata must provide artifact_id and verified")
    if custody["verified"] is not True:
        raise ValueError("payer observations require verified caller custody")
    parsed = [PayerCandidate.from_mapping({**row, "source_family": source_family, "source_period": source_period, "content_sha256": "sha256:" + sha256(artifact_bytes).hexdigest()}) for row in candidates]
    digest = "sha256:" + sha256(artifact_bytes).hexdigest()
    release_id = "release:" + source_family + ":" + source_period
    artifact_id = str(custody["artifact_id"])
    activity_id = "activity:payer:normalize:" + source_period
    receipt_id = "receipt:payer:" + source_period
    observation_ids = ["observation:payer:" + str(index) for index in range(1, len(parsed) + 1)]
    envelope = {"schema_version": "hdp.observation-envelope.v1", "record_type": "observation_envelope", "record_id": "hdp:observation-envelope:payer:" + source_period, "packet_id": "p0-09-observation-provenance-delta-v1", "tracking_bead": "healthcare-toolkit-rrna.9", "frozen_dispatch_base": "11d16f8303226619161f9bef03cb312f693b2d49", "source_release": {"source_id": source["source_id"], "release_id": release_id, "release_label": source_period, "source_kind": "official_dataset", "release_sha256": digest, "evidence_locator": source["release_locator"], "coverage_state": "present"}, "artifact": {"artifact_id": artifact_id, "release_ref": release_id, "artifact_kind": "raw_source", "media_type": "application/octet-stream", "content_sha256": digest, "byte_length": len(artifact_bytes), "custody": {"locator": custody_locator, "storage_plane": str(custody.get("storage_plane") or "object_storage"), "immutable": bool(custody.get("immutable", False)), "retention": str(custody.get("retention") or "append_only")}}, "receipt": {"receipt_id": receipt_id, "producer": "healthcare-data-mcp:payer-observation-producer", "receipt_schema": "hdp.receipt.v1", "source_release_ref": release_id, "artifact_ref": artifact_id, "source_release_sha256": digest, "artifact_sha256": digest, "receipt_sha256": digest, "state": "succeeded", "recorded_at": retrieved_at, "evidence_locator": source["source_url"]}, "activity": {"activity_id": activity_id, "run_id": "run:payer:" + source_period, "activity_type": "normalization", "actor": {"actor_type": "deterministic_transform", "actor_id": "healthcare-data-mcp:payer-observation-producer"}, "started_at": retrieved_at, "ended_at": retrieved_at, "status": "succeeded", "input_artifact_refs": [artifact_id], "output_artifact_refs": [artifact_id]}, "observations": [], "lineage": {"lineage_id": "lineage:payer:" + source_period, "source_release_ref": release_id, "artifact_ref": artifact_id, "receipt_ref": receipt_id, "activity_ref": activity_id, "observation_ids": observation_ids, "deterministic_order": observation_ids, "replay": {"idempotency_key": "idempotency:payer:" + source_period, "state": "first_seen", "replay_of": None, "deterministic": True}}, "authority_limits": {"acquisition_allowed": False, "mutation_allowed": False, "deletion_allowed": False, "publication_allowed": False, "release_allowed": False, "production_allowed": False, "runtime_allowed": False, "current_projection_allowed": False, "identity_promotion_allowed": False}}
    for index, candidate in enumerate(parsed, 1):
        state = "observed" if candidate.missingness is None else candidate.missingness
        subject_key = candidate.geography.lower().replace(" ", "-")
        envelope["observations"].append({"observation_id": observation_ids[index - 1], "identity_key": "identity:payer:" + subject_key, "subject_ref": "entity:payer:" + subject_key, "attribute_term_ref": "term:payer-coverage", "value": {"type_of_coverage": candidate.payer_type, "plan_or_contract_id": candidate.plan_or_contract_id, "numerator": candidate.numerator, "denominator": candidate.denominator, "denominator_scope": candidate.denominator_scope} if state == "observed" else None, "value_state": state, "source_scope": {"scope_id": "scope:payer:" + source_period, "source_id": source["source_id"], "release_ref": release_id, "artifact_ref": artifact_id, "custody_locator": custody_locator, "selector": "record:" + str(index), "authority_state": "source_scoped" if state == "observed" else "abstained"}, "activity_ref": activity_id, "receipt_ref": receipt_id, "valid_time": {"precision": "year", "as_of": source_period + "-12-31", "valid_from": source_period + "-01-01", "valid_to": source_period + "-12-31"}, "transaction_time": {"recorded_from": retrieved_at, "recorded_to": None}, "conflict": {"state": "none" if state == "observed" else "missingness", "reason": "Official payer enrollment candidate." if state == "observed" else "Candidate is explicitly missing or unresolved.", "resolution": "not_required" if state == "observed" else "abstained", "competing_observation_refs": []}, "promotion_state": "unpromoted_observation"})
    return validate_observation_envelope(envelope)
