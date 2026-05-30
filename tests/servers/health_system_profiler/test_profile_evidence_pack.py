"""Evidence-pack workflow tests for Healthcare Toolkit profile population."""

from __future__ import annotations

import importlib

from unittest.mock import AsyncMock

import pandas as pd
import pytest

from servers.health_system_profiler import server
from shared.utils.mcp_response import validate_evidence_receipt


def test_health_system_profiler_imports_profile_evidence_pack_module() -> None:
    imported = importlib.import_module("servers.health_system_profiler.server")

    assert hasattr(imported, "assemble_profile_evidence_pack")


@pytest.fixture
def pack_ahrq_systems() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "health_sys_id": "SYS_PA_001",
                "health_sys_name": "Example Health",
                "health_sys_city": "Philadelphia",
                "health_sys_state": "PA",
                "hosp_count": 3,
            }
        ]
    )


@pytest.fixture
def pack_ahrq_hospitals() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "health_sys_id": "SYS_PA_001",
                "ccn": "390001",
                "hospital_name": "Example Main Hospital",
                "hosp_city": "Philadelphia",
                "hosp_state": "PA",
                "hosp_zip": "19107",
                "hos_beds": 880,
            },
            {
                "health_sys_id": "SYS_PA_001",
                "ccn": "390002",
                "hospital_name": "Example East Campus",
                "hosp_city": "Philadelphia",
                "hosp_state": "PA",
                "hosp_zip": "19107",
                "hos_beds": 100,
            },
            {
                "health_sys_id": "SYS_PA_001",
                "ccn": "390003",
                "hospital_name": "Example West Hospital",
                "hosp_city": "Pittsburgh",
                "hosp_state": "PA",
                "hosp_zip": "15213",
                "hos_beds": 350,
            },
            {
                "health_sys_id": "SYS_PA_001",
                "ccn": "390004",
                "hospital_name": "Example Coordinate Hospital",
                "hosp_city": "Harrisburg",
                "hosp_state": "PA",
                "hosp_zip": "17101",
                "hos_beds": 120,
            },
        ]
    )


@pytest.fixture
def pack_pos() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "PRVDR_NUM": "390001",
                "FAC_NAME": "Example Main Hospital",
                "ST_ADR": "111 S 11th St",
                "CITY_NAME": "Philadelphia",
                "STATE_CD": "PA",
                "ZIP_CD": "19107",
                "COUNTY_NAME": "Philadelphia",
                "BED_CNT": "900",
                "CRTFD_BED_CNT": "880",
            },
            {
                "PRVDR_NUM": "390002",
                "FAC_NAME": "Example East Campus",
                "ST_ADR": "111 S 11th St",
                "CITY_NAME": "Philadelphia",
                "STATE_CD": "PA",
                "ZIP_CD": "19107",
                "COUNTY_NAME": "Philadelphia",
                "BED_CNT": "100",
                "CRTFD_BED_CNT": "100",
            },
            {
                "PRVDR_NUM": "390003",
                "FAC_NAME": "Example West Hospital",
                "ST_ADR": "Approximate Road",
                "CITY_NAME": "Pittsburgh",
                "STATE_CD": "PA",
                "ZIP_CD": "15213",
                "COUNTY_NAME": "Allegheny",
                "BED_CNT": "350",
                "CRTFD_BED_CNT": "340",
            },
            {
                "PRVDR_NUM": "390004",
                "FAC_NAME": "Example Coordinate Hospital",
                "ST_ADR": "",
                "CITY_NAME": "Harrisburg",
                "STATE_CD": "PA",
                "ZIP_CD": "17101",
                "COUNTY_NAME": "Dauphin",
                "BED_CNT": "",
                "CRTFD_BED_CNT": "",
                "latitude": "40.2732",
                "longitude": "-76.8867",
            },
        ]
    )


@pytest.fixture
def geography_pos(pack_pos: pd.DataFrame) -> pd.DataFrame:
    frame = pack_pos.copy()
    frame.loc[frame["PRVDR_NUM"] == "390002", "ST_ADR"] = "222 Fallback Ave"
    return frame


@pytest.mark.asyncio
async def test_profile_evidence_pack_pa_fixture_contract_and_cache_preflight(
    monkeypatch: pytest.MonkeyPatch,
    pack_ahrq_systems: pd.DataFrame,
    pack_ahrq_hospitals: pd.DataFrame,
    pack_pos: pd.DataFrame,
) -> None:
    _patch_pack_sources(monkeypatch, pack_ahrq_systems, pack_ahrq_hospitals, pack_pos)
    monkeypatch.setattr(server, "_census_geocode_address", AsyncMock(return_value=_census_match()))
    monkeypatch.setattr(server, "_osm_geocode_address", AsyncMock(return_value=None))
    monkeypatch.setattr(server, "_reverse_geocode_coordinates", AsyncMock(return_value=_reverse_match()))

    result = await server.build_profile_evidence_pack(
        state="PA",
        system_name="Example Health",
        system_slug="example-health",
        required_fields=["county_geoid", "facility_site_count"],
    )

    assert result["workflow_id"] == "profile_evidence_pack"
    assert result["metadata"]["read_only"] is True
    assert result["source_precedence"][0]["source_family"] == "cms_pos_hgi"
    assert result["cache_preflight"]["sources"]
    assert {row["field"] for row in result["system_identity_aliases"]} == {"system_identity"}
    assert len(result["current_hospital_roster"]) == 4
    assert result["current_hospital_roster"][0]["metadata"]["mcp_tool"] == "build_profile_evidence_pack"
    assert result["identity_map"]["source_claims"][0]["row_evidence_paths"]
    assert any(row["field"] == "facility_site_count" for row in result["unavailable_public_findings"])
    assert any(call["tool"] == "scrape_system_profile" for call in result["suggested_next_calls"])
    _assert_candidate_contracts(result)


@pytest.mark.asyncio
async def test_profile_evidence_pack_loads_ccn_scoped_sources_after_roster_resolution(
    monkeypatch: pytest.MonkeyPatch,
    pack_ahrq_systems: pd.DataFrame,
    pack_ahrq_hospitals: pd.DataFrame,
    pack_pos: pd.DataFrame,
) -> None:
    _patch_pack_sources(monkeypatch, pack_ahrq_systems, pack_ahrq_hospitals, pack_pos)
    monkeypatch.setattr(server, "_census_geocode_address", AsyncMock(return_value=_census_match()))
    provider_calls: list[list[str]] = []
    hcris_calls: list[tuple[str, list[str]]] = []
    state_calls: list[tuple[str, list[str]]] = []

    monkeypatch.setattr(server, "_load_profile_provider_rows", lambda ccns: provider_calls.append(list(ccns)) or [])
    monkeypatch.setattr(server, "_load_hcris_bed_rows", lambda state, ccns: hcris_calls.append((state, list(ccns))) or [])
    monkeypatch.setattr(server, "_load_state_bed_rows", lambda state, ccns: state_calls.append((state, list(ccns))) or [])

    result = await server.build_profile_evidence_pack(state="PA", system_name="Example Health")

    assert result["resolved_identifiers"]["ccns"] == ["390001", "390002", "390003", "390004"]
    assert provider_calls == [["390001", "390002", "390003", "390004"]]
    assert hcris_calls == [("PA", ["390001", "390002", "390003", "390004"])]
    assert state_calls == [("PA", ["390001", "390002", "390003", "390004"])]


@pytest.mark.asyncio
async def test_profile_evidence_pack_bed_resolver_conflict_duplicate_and_source_families(
    monkeypatch: pytest.MonkeyPatch,
    pack_ahrq_systems: pd.DataFrame,
    pack_ahrq_hospitals: pd.DataFrame,
    pack_pos: pd.DataFrame,
) -> None:
    _patch_pack_sources(monkeypatch, pack_ahrq_systems, pack_ahrq_hospitals, pack_pos)
    monkeypatch.setattr(server, "_census_geocode_address", AsyncMock(return_value=_census_match()))
    monkeypatch.setattr(
        server,
        "_load_hcris_bed_rows",
        lambda state, ccns: [{"ccn": "390001", "beds": "1200", "fiscal_year": "2023"}],
    )
    monkeypatch.setattr(
        server,
        "_load_state_bed_rows",
        lambda state, ccns: [
            {
                "ccn": "390001",
                "state": "PA",
                "metric_name": "licensed_beds",
                "metric_value": "910",
                "source": "Pennsylvania Department of Health Hospital Reports",
                "row_scope": "ccn",
                "report_year": "2023",
            },
            {
                "ccn": "390003",
                "state": "PA",
                "metric_name": "licensed_beds",
                "metric_value": "360",
                "source": "Official system annual report",
                "row_scope": "ccn",
                "report_year": "2023",
                "source_artifact": "https://example.org/annual-report",
            },
        ],
    )

    result = await server.build_profile_evidence_pack(state="PA", system_name="Example Health")

    main_beds = next(row for row in result["hospital_bed_counts"] if row["value"]["ccn"] == "390001")
    candidate_sources = {row["source_family"] for row in main_beds["value"]["resolution"]["candidates"]}
    assert {"cms_pos_hgi", "hcris_state_official_beds", "ahrq_compendium"} <= candidate_sources
    assert any(row["status"] == "source_conflict" and row["field"] == "hospital_bed_count" for row in result["conflicts"])
    assert any(row["field"] == "duplicate_campus" for row in result["conflicts"])
    assert result["bed_rollup_guidance"][0]["value"]["duplicate_campus_conflict"] is True


@pytest.mark.asyncio
async def test_profile_evidence_pack_geography_census_osm_reverse_and_rejected_match(
    monkeypatch: pytest.MonkeyPatch,
    pack_ahrq_systems: pd.DataFrame,
    pack_ahrq_hospitals: pd.DataFrame,
    geography_pos: pd.DataFrame,
) -> None:
    _patch_pack_sources(monkeypatch, pack_ahrq_systems, pack_ahrq_hospitals, geography_pos)

    async def census(address: str) -> dict | None:
        if "111 S 11th" in address:
            return _census_match()
        return None

    async def osm(address: str) -> dict | None:
        if "Approximate" in address:
            return {
                "source_family": "osm_nominatim",
                "status": "rejected",
                "match_quality": "approximate_rejected",
                "source_url": "https://nominatim.openstreetmap.org/search",
                "source_period": "Nominatim live lookup",
            }
        if "Fallback" not in address:
            return None
        return {
            "source_family": "osm_nominatim",
            "status": "matched",
            "match_quality": "fallback_acceptable",
            "latitude": "40.4406",
            "longitude": "-79.9959",
            "county": "Allegheny County",
            "source_url": "https://nominatim.openstreetmap.org/search",
            "source_period": "Nominatim live lookup",
        }

    monkeypatch.setattr(server, "_census_geocode_address", census)
    monkeypatch.setattr(server, "_osm_geocode_address", osm)
    monkeypatch.setattr(server, "_reverse_geocode_coordinates", AsyncMock(return_value=_reverse_match()))

    result = await server.build_profile_evidence_pack(state="PA", system_name="Example Health")

    assert any(row["source_family"] == "census_geocoder" for row in result["geography_candidates"])
    assert any(row["source_family"] == "osm_nominatim" for row in result["geography_candidates"])
    assert any(row["value"]["match_quality"] == "approximate_rejected" for row in result["unavailable_public_findings"])
    assert any(row["value"]["county_geoid"] == "42043" for row in result["geography_candidates"])


@pytest.mark.asyncio
async def test_profile_evidence_pack_affiliation_mismatch_and_count_claim_review(
    monkeypatch: pytest.MonkeyPatch,
    pack_ahrq_systems: pd.DataFrame,
    pack_ahrq_hospitals: pd.DataFrame,
    pack_pos: pd.DataFrame,
) -> None:
    _patch_pack_sources(monkeypatch, pack_ahrq_systems, pack_ahrq_hospitals, pack_pos)
    monkeypatch.setattr(server, "_census_geocode_address", AsyncMock(return_value=_census_match()))
    monkeypatch.setattr(
        server,
        "_load_official_profile_evidence",
        lambda system_name, state: [
            {
                "ccn": "390001",
                "current_operator": "Different Health",
                "source_name": "Official facility page",
                "source_url": "https://example.org/main",
                "source_period": "retrieved 2026-05-30",
            },
            {
                "count_value": 4,
                "claim_text": "Example Health operates 4 hospitals.",
                "claim_precision": "exact",
                "source_name": "Official system fact sheet",
                "source_url": "https://example.org/facts",
            },
            {
                "claim_text": "Example Health operates more than 40 care sites.",
                "source_name": "Official system about page",
                "source_url": "https://example.org/about",
            },
        ],
    )

    result = await server.build_profile_evidence_pack(state="PA", system_name="Example Health")

    assert any(row["field"] == "affiliation" and row["status"] == "source_conflict" for row in result["conflicts"])
    count_rows = result["facility_site_count_evidence"]
    exact = next(row for row in count_rows if row["value"].get("claim_text") == "Example Health operates 4 hospitals.")
    vague = next(row for row in count_rows if "more than 40" in row["value"].get("claim_text", ""))
    assert exact["status"] == "supported"
    assert vague["status"] == "needs_review"
    assert vague["confidence"] == "needs_review_vague_count_claim"


@pytest.mark.asyncio
async def test_hcris_bed_loader_reads_cms_cost_report_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    from servers.hospital_quality import data_loaders as hospital_quality_loaders

    async def load_cost_report() -> pd.DataFrame:
        return pd.DataFrame(
            [
                {"provider_ccn": "390001", "beds": "1200", "fy_end_dt": "2023-06-30"},
                {"provider_ccn": "390999", "beds": "50", "fy_end_dt": "2023-06-30"},
            ]
        )

    monkeypatch.setattr(hospital_quality_loaders, "load_cost_report", load_cost_report)

    rows = await server._load_hcris_bed_rows("PA", ["390001"])

    assert rows[0]["ccn"] == "390001"
    assert rows[0]["beds"] == "1200"
    assert rows[0]["dataset_id"] == "cms_cost_report"


def test_pa_state_bed_loader_reads_normalized_state_health_cache(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from shared import state_health_data

    cache = tmp_path / "state-health-data" / "pa-doh-hospital-extract"
    cache.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "ccn": "390001",
                "state": "PA",
                "facility_name": "Example Main Hospital",
                "metric_name": "beds",
                "metric_value": "910",
                "report_year": "2023",
            }
        ]
    ).to_csv(cache / "normalized.csv", index=False)
    monkeypatch.setattr(state_health_data, "DEFAULT_CACHE_ROOT", tmp_path)

    rows = server._load_state_bed_rows("PA", ["390001"])

    assert rows[0]["ccn"] == "390001"
    assert rows[0]["metric_value"] == "910"
    assert rows[0]["dataset_id"] == "pa_hospital_reports"


def test_official_profile_evidence_loader_reads_reviewed_cache(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache = tmp_path / "profile-evidence"
    cache.mkdir(parents=True)
    (cache / "official_profile_evidence.json").write_text(
        """
        {
          "rows": [
            {
              "system_name": "Example Health",
              "state": "PA",
              "claim_text": "Example Health operates 4 hospitals.",
              "count_value": 4,
              "claim_precision": "exact",
              "source_url": "https://example.org/facts"
            }
          ]
        }
        """,
        encoding="utf-8",
    )
    monkeypatch.setenv("HC_MCP_CACHE_ROOT", str(tmp_path))

    rows = server._load_official_profile_evidence("Example Health", "PA")

    assert rows[0]["count_value"] == 4
    assert rows[0]["dataset_id"] == "official_system_page"
    assert rows[0]["cache_status"] == "reviewed_local_cache"


def _patch_pack_sources(
    monkeypatch: pytest.MonkeyPatch,
    systems: pd.DataFrame,
    hospitals: pd.DataFrame,
    pos: pd.DataFrame,
) -> None:
    monkeypatch.setattr(server, "_load_ahrq_systems", AsyncMock(return_value=systems))
    monkeypatch.setattr(server, "_load_ahrq_hospitals", AsyncMock(return_value=hospitals))
    monkeypatch.setattr(server, "_load_pos", AsyncMock(return_value=pos))
    monkeypatch.setattr(server, "_load_profile_provider_rows", lambda ccns: [])
    monkeypatch.setattr(server, "_load_hcris_bed_rows", lambda state, ccns: [])
    monkeypatch.setattr(server, "_load_state_bed_rows", lambda state, ccns: [])
    monkeypatch.setattr(server, "_load_official_profile_evidence", lambda system_name, state: [])
    monkeypatch.setattr(server, "_osm_geocode_address", AsyncMock(return_value=None))
    monkeypatch.setattr(server, "_reverse_geocode_coordinates", AsyncMock(return_value=None))


def _assert_candidate_contracts(payload: dict) -> None:
    validate_evidence_receipt(payload["evidence"], require_content=True)
    for section in (
        "system_identity_aliases",
        "current_hospital_roster",
        "source_identifiers",
        "addresses",
        "geography_candidates",
        "hospital_bed_counts",
        "bed_rollup_guidance",
        "facility_site_count_evidence",
        "conflicts",
        "unavailable_public_findings",
    ):
        for row in payload[section]:
            validate_evidence_receipt(row["evidence"], require_content=True)
            assert row["source_metadata"]["dataset_id"] == row["evidence"]["dataset_id"]
            assert row["metadata"]["mcp_server"] == "health-system-profiler"
            assert row["metadata"]["mcp_tool"] == "build_profile_evidence_pack"
            assert row["retrieval_access_date"]
            assert row["confidence"]
            assert row["match_basis"]


def _census_match() -> dict:
    return {
        "source_family": "census_geocoder",
        "status": "matched",
        "match_quality": "census_exact_address",
        "matched_address": "111 S 11th St, Philadelphia, PA, 19107",
        "latitude": 39.949,
        "longitude": -75.158,
        "county": "Philadelphia County",
        "county_geoid": "42101",
        "source_url": "https://geocoding.geo.census.gov/geocoder/geographies/onelineaddress",
        "source_period": "Census Geocoder current benchmark/vintage",
    }


def _reverse_match() -> dict:
    return {
        "source_family": "census_geocoder",
        "status": "matched",
        "match_quality": "reverse_geocode",
        "latitude": 40.2732,
        "longitude": -76.8867,
        "county": "Dauphin County",
        "county_geoid": "42043",
        "source_url": "https://geocoding.geo.census.gov/geocoder/geographies/coordinates",
        "source_period": "Census Geocoder current benchmark/vintage",
    }
