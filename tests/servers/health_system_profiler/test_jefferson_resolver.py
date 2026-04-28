"""Acceptance coverage for Jefferson Health post-LVHN deterministic reconciliation."""

from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from servers.health_system_profiler import server
from servers.health_system_profiler.jefferson_resolver import (
    JEFFERSON_SLUG,
    reconcile_system_facilities,
    resolve_combined_system_slug,
)


def test_alias_ledger_resolves_legacy_systems_to_jefferson():
    assert resolve_combined_system_slug(system_name="Jefferson Health") == JEFFERSON_SLUG
    assert resolve_combined_system_slug(system_name="Einstein Healthcare Network") == JEFFERSON_SLUG
    assert resolve_combined_system_slug(system_name="LVHN") == JEFFERSON_SLUG


def test_reconcile_system_facilities_closes_32_vs_33_discrepancy():
    ledger = reconcile_system_facilities(JEFFERSON_SLUG, as_of_date="2026-04-28")

    assert ledger["facility_count"] == 32
    assert ledger["discrepancy_closure"]["official_count"] == 32
    assert ledger["discrepancy_closure"]["candidate_count"] == 33
    assert ledger["discrepancy_closure"]["excluded_candidate"] == "Lehigh Valley Reilly Children's Hospital"
    assert "does not count Reilly Children's as a separate canonical hospital" in ledger["discrepancy_closure"]["resolution"]

    names = {facility["name"] for facility in ledger["facilities"]}
    assert "Jefferson Einstein Philadelphia Hospital" in names
    assert "Jefferson Einstein Montgomery Hospital" in names
    assert "Lehigh Valley Hospital - Cedar Crest" in names
    assert "Lehigh Valley Hospital - Gilbertsville" in names
    assert "Lehigh Valley Reilly Children's Hospital" not in names
    assert not any("Geisinger" in name or "Penn " in name for name in names)

    for facility in ledger["facilities"]:
        assert set(facility) >= {
            "ccn",
            "npi",
            "subsystem",
            "legacy_system",
            "source_refs",
            "confidence",
            "active_status",
        }
        assert facility["active_status"] == "active"
        assert "cms_hgi" in facility["source_refs"]
        assert "cms_provider_enrollment" in facility["source_refs"]


@pytest.mark.asyncio
async def test_get_system_profile_uses_combined_jefferson_resolver_without_ahrq_2023():
    with (
        patch.object(server, "_load_ahrq_systems", new_callable=AsyncMock) as load_systems,
        patch.object(server, "_load_ahrq_hospitals", new_callable=AsyncMock) as load_hospitals,
        patch.object(server, "_load_pos", new_callable=AsyncMock) as load_pos,
    ):
        result = await server.get_system_profile(system_name="Jefferson Health", edition_date="2026-04-28")

    load_systems.assert_not_awaited()
    load_hospitals.assert_not_awaited()
    load_pos.assert_not_awaited()
    assert result["system"]["name"] == "Jefferson Health"
    assert result["system"]["system_id"] == JEFFERSON_SLUG
    assert result["system"]["hospital_count"] == 32
    assert result["facility_reconciliation"]["facility_count"] == 32
    assert result["legacy_system_counts"]["Einstein Healthcare Network"] == 3
    assert result["legacy_system_counts"]["Lehigh Valley Health Network"] == 15
    assert len(result["facility_reconciliation"]["merger_evidence"]) >= 4
    assert {evidence["date"] for evidence in result["facility_reconciliation"]["merger_evidence"]} >= {
        "2024-05-15",
        "2024-08-01",
        "2025-07-30",
    }


@pytest.mark.asyncio
async def test_reconcile_system_facilities_tool_returns_jefferson_ledger():
    result = await server.reconcile_system_facilities(system_slug=JEFFERSON_SLUG, as_of_date="2026-04-28")

    assert result["system_slug"] == JEFFERSON_SLUG
    assert result["facility_count"] == 32


def test_reconcile_system_facilities_can_mark_external_source_matches():
    ahrq = pd.DataFrame(
        [{"ccn": "390142", "hospital_name": "Jefferson Einstein Philadelphia Hospital"}]
    )
    hgi = pd.DataFrame(
        [{"PRVDR_NUM": "390133", "FAC_NAME": "Lehigh Valley Hospital - Cedar Crest"}]
    )
    enrollment = pd.DataFrame(
        [{"Facility Name": "Jefferson Einstein Montgomery Hospital"}]
    )

    ledger = reconcile_system_facilities(
        JEFFERSON_SLUG,
        as_of_date="2026-04-28",
        ahrq_hospitals=ahrq,
        cms_hgi=hgi,
        provider_enrollment=enrollment,
    )

    by_name = {facility["name"]: facility for facility in ledger["facilities"]}
    assert "ahrq_row" in by_name["Jefferson Einstein Philadelphia Hospital"]["source_refs"]
    assert "cms_hgi_row" in by_name["Lehigh Valley Hospital - Cedar Crest"]["source_refs"]
    assert "provider_enrollment_row" in by_name["Jefferson Einstein Montgomery Hospital"]["source_refs"]
