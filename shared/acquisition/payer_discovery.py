"""Source-bound payer TOC candidates for the Data MCP acquisition plane."""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from typing import Literal, Mapping

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


def build_payer_observation_envelope(*, tracking_bead: str, source_family: str, source_period: str, artifact_bytes: bytes, candidates: list[Mapping[str, object]], retrieved_at: str) -> dict[str, object]:
    """Build a secret-free source-bound observation envelope for candidates."""
    source = SOURCE_CATALOG.get(source_family)
    if source is None:
        raise ValueError("unregistered payer source family")
    parsed = [PayerCandidate.from_mapping({**row, "source_family": source_family, "source_period": source_period, "content_sha256": "sha256:" + sha256(artifact_bytes).hexdigest()}) for row in candidates]
    digest = "sha256:" + sha256(artifact_bytes).hexdigest()
    return {"schema_version": "hdp.payer-observation.v1", "record_type": "payer_observation_envelope", "tracking_bead": tracking_bead, "source_release": {"source_id": source["source_id"], "release_id": "release:" + source_family, "release_label": source_period, "source_url": source["source_url"], "release_sha256": digest, "rights_status": "approved_public"}, "artifact": {"artifact_id": "artifact:" + source_family + ":" + source_period, "source_url": source["source_url"], "content_sha256": digest, "content_length": len(artifact_bytes), "custody_state": "frozen_verified_external"}, "receipt": {"retrieved_at": retrieved_at, "source_url": source["source_url"], "content_sha256": digest}, "observations": [{"observation_id": "observation:payer:" + str(index), "type_of_coverage": candidate.payer_type, "geography": candidate.geography, "plan_or_contract_id": candidate.plan_or_contract_id, "numerator": candidate.numerator, "denominator": candidate.denominator, "denominator_scope": candidate.denominator_scope, "missingness": candidate.missingness} for index, candidate in enumerate(parsed, 1)], "authority_limits": {"read_only": True, "promotion_allowed": False, "projection_write_allowed": False}}
