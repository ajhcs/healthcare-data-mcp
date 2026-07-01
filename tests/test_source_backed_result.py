"""Tests for source-backed result traceability contracts."""

from __future__ import annotations

import pytest

from shared.utils.mcp_response import evidence_receipt
from shared.utils.source_backed_result import (
    SourceClaimPathError,
    source_claim,
    validate_source_claim_paths,
    values_at_path,
)


def _receipt(match_basis: str = "ccn_exact") -> dict:
    return evidence_receipt(
        source_name="CMS Provider of Services",
        source_url="https://data.cms.gov/provider-of-services",
        dataset_id="cms_provider_of_services",
        source_period="Q4 2025 public file",
        retrieved_at="2026-07-01T00:00:00Z",
        cache_status="ready",
        match_basis=match_basis,
        confidence="high",
        caveat="Public CMS row; preserve source period before citing.",
        next_step="Verify the cited field against the source row.",
    )


def test_source_claim_helper_builds_canonical_traceability_shape() -> None:
    claim = source_claim(
        collection="cms_provider_of_services",
        source_name="CMS Provider of Services",
        evidence_path="evidence",
        source_metadata_path="source_metadata",
        identity_paths=("facility.identity",),
        row_evidence_paths=("rows[].evidence",),
        match_policy="ccn_exact",
    )

    assert claim["collection"] == "cms_provider_of_services"
    assert claim["evidence_path"] == "evidence"
    assert claim["source_metadata_path"] == "source_metadata"
    assert claim["identity_paths"] == ["facility.identity"]
    assert claim["row_evidence_paths"] == ["rows[].evidence"]
    assert claim["match_policy"] == "ccn_exact"


def test_values_at_path_resolves_result_prefix_and_list_wildcards() -> None:
    payload = {"rows": [{"evidence": {"id": "a"}}, {"evidence": {"id": "b"}}]}

    assert values_at_path(payload, "result.rows[].evidence") == [{"id": "a"}, {"id": "b"}]
    assert values_at_path(payload, "rows[1].evidence") == [{"id": "b"}]
    assert values_at_path(payload, "rows[].missing") == []


def test_compatibility_mode_accepts_legacy_source_claims_when_declared_paths_exist() -> None:
    payload = {
        "evidence": _receipt(),
        "results": [{"identity": {"ccn": "390223"}, "evidence": _receipt("result_row")}],
        "identity_map": {
            "source_claims": [
                {
                    "collection": "cms_provider_of_services",
                    "identity_paths": ["results[].identity"],
                    "evidence_path": "evidence",
                    "row_evidence_path": "results[].evidence",
                }
            ]
        },
    }

    summary = validate_source_claim_paths(payload)

    assert summary["valid"] is True
    assert summary["issues"] == []


def test_boundary_mode_requires_source_metadata_path_and_contentful_receipts() -> None:
    payload = {
        "evidence": _receipt(),
        "identity_map": {
            "source_claims": [
                {
                    "collection": "cms_provider_of_services",
                    "evidence_path": "evidence",
                }
            ]
        },
    }

    summary = validate_source_claim_paths(payload, require_boundary_traceability=True)

    assert summary["valid"] is False
    assert {
        issue["reason"]
        for issue in summary["issues"]
    } == {"missing_source_metadata_path"}


def test_boundary_mode_rejects_empty_source_claims() -> None:
    payload = {"identity_map": {"source_claims": []}}

    summary = validate_source_claim_paths(payload, require_boundary_traceability=True)

    assert summary["valid"] is False
    assert summary["issues"] == [
        {
            "path": "identity_map.source_claims",
            "reason": "missing_source_claims",
            "detail": "",
        }
    ]


def test_boundary_mode_rejects_legacy_singular_row_evidence_path() -> None:
    payload = {
        "evidence": _receipt(),
        "source_metadata": {"dataset_id": "cms_provider_of_services"},
        "rows": [{"evidence": _receipt("row_exact")}],
        "identity_map": {
            "source_claims": [
                {
                    "collection": "cms_provider_of_services",
                    "evidence_path": "evidence",
                    "source_metadata_path": "source_metadata",
                    "row_evidence_path": "rows[].evidence",
                }
            ]
        },
    }

    summary = validate_source_claim_paths(payload, require_boundary_traceability=True)

    assert summary["valid"] is False
    assert {
        issue["reason"]
        for issue in summary["issues"]
    } == {"legacy_row_evidence_path"}


def test_boundary_mode_validates_top_level_and_row_level_claim_paths() -> None:
    payload = {
        "evidence": _receipt(),
        "source_metadata": {
            "source_name": "CMS Provider of Services",
            "dataset_id": "cms_provider_of_services",
        },
        "rows": [{"evidence": _receipt("row_exact")}],
        "identity_map": {
            "source_claims": [
                source_claim(
                    collection="cms_provider_of_services",
                    evidence_path="evidence",
                    source_metadata_path="source_metadata",
                    row_evidence_paths=("rows[].evidence",),
                )
            ]
        },
    }

    summary = validate_source_claim_paths(payload, require_boundary_traceability=True)

    assert summary["valid"] is True
    assert summary["status"] == "source_claim_paths_valid"


def test_boundary_mode_can_raise_for_agent_friendly_fast_failure() -> None:
    payload = {
        "identity_map": {
            "source_claims": [
                source_claim(
                    collection="cms_provider_of_services",
                    evidence_path="missing.evidence",
                    source_metadata_path="source_metadata",
                )
            ]
        }
    }

    with pytest.raises(SourceClaimPathError, match="path_not_found"):
        validate_source_claim_paths(payload, require_boundary_traceability=True, raise_on_error=True)
