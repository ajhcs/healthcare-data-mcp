"""Tests for the health-system-profiler MCP server tools.

Uses monkeypatching to avoid real data downloads.
"""

from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from servers.health_system_profiler import server
from shared.utils.mcp_response import validate_evidence_receipt


@pytest.fixture
def mock_ahrq_systems():
    return pd.DataFrame([
        {"health_sys_id": "SYS_001", "health_sys_name": "Jefferson Health",
         "health_sys_city": "Philadelphia", "health_sys_state": "PA", "hosp_count": 2, "phys_grp_count": 10},
    ])


@pytest.fixture
def mock_ahrq_hospitals():
    return pd.DataFrame([
        {"health_sys_id": "SYS_001", "ccn": "390001", "hospital_name": "Jefferson Main",
         "hosp_city": "Philadelphia", "hosp_state": "PA", "hosp_zip": "19107", "hos_beds": 900, "hos_dsch": 40000},
        {"health_sys_id": "SYS_001", "ccn": "390149", "hospital_name": "Jefferson Einstein",
         "hosp_city": "Philadelphia", "hosp_state": "PA", "hosp_zip": "19141", "hos_beds": 500, "hos_dsch": 20000},
    ])


@pytest.fixture
def mock_pos():
    return pd.DataFrame([
        {"PRVDR_NUM": "390001", "FAC_NAME": "Jefferson Main", "ST_ADR": "111 S 11th St",
         "CITY_NAME": "Philadelphia", "STATE_CD": "PA", "ZIP_CD": "19107", "COUNTY_NAME": "Philadelphia",
         "PHNE_NUM": "2155551234", "BED_CNT": "900", "CRTFD_BED_CNT": "880",
         "PSYCH_UNIT_BED_CNT": "50", "REHAB_UNIT_BED_CNT": "30", "HOSPC_BED_CNT": "0",
         "VNTLTR_BED_CNT": "10", "AIDS_BED_CNT": "0", "ALZHMR_BED_CNT": "0", "DLYS_BED_CNT": "0",
         "CRDC_CTHRTZTN_LAB_SRVC_CD": "1", "OPEN_HRT_SRGRY_SRVC_CD": "1", "MGNTC_RSNC_IMG_SRVC_CD": "1",
         "CT_SCAN_SRVC_CD": "1", "PET_SCAN_SRVC_CD": "0", "NUCLR_MDCN_SRVC_CD": "1",
         "SHCK_TRMA_SRVC_CD": "1", "BURN_CARE_UNIT_SRVC_CD": "0", "NEONTL_ICU_SRVC_CD": "1",
         "OB_SRVC_CD": "1", "ORGN_TRNSPLNT_SRVC_CD": "0", "DCTD_ER_SRVC_CD": "1",
         "RN_CNT": "2000", "LPN_CNT": "150", "PHYSN_CNT": "500",
         "REG_PHRMCST_CNT": "50", "OCPTNL_THRPST_CNT": "30", "PHYS_THRPST_CNT": "40",
         "INHLTN_THRPST_CNT": "30", "EMPLEE_CNT": "4500",
         "OPRTG_ROOM_CNT": "30", "ENDSCPY_PRCDR_ROOMS_CNT": "8", "CRDC_CTHRTZTN_PRCDR_ROOMS_CNT": "4",
         "TOT_OFSITE_EMER_DEPT_CNT": "2", "TOT_OFSITE_URGNT_CARE_CNTR_CNT": "5",
         "TOT_OFSITE_PSYCH_UNIT_CNT": "1", "TOT_OFSITE_REHAB_HOSP_CNT": "1",
         "RELATED_PROVIDER_NUMBER": "", "PRVDR_CTGRY_CD": "01", "PRVDR_CTGRY_SBTYP_CD": "01",
         "GNRL_CNTL_TYPE_CD": "04"},
        {"PRVDR_NUM": "390149", "FAC_NAME": "Jefferson Einstein", "ST_ADR": "5501 Old York Rd",
         "CITY_NAME": "Philadelphia", "STATE_CD": "PA", "ZIP_CD": "19141", "COUNTY_NAME": "Philadelphia",
         "PHNE_NUM": "2155555678", "BED_CNT": "500", "CRTFD_BED_CNT": "490",
         "PSYCH_UNIT_BED_CNT": "20", "REHAB_UNIT_BED_CNT": "10", "HOSPC_BED_CNT": "0",
         "VNTLTR_BED_CNT": "5", "AIDS_BED_CNT": "0", "ALZHMR_BED_CNT": "0", "DLYS_BED_CNT": "0",
         "CRDC_CTHRTZTN_LAB_SRVC_CD": "1", "OPEN_HRT_SRGRY_SRVC_CD": "0", "MGNTC_RSNC_IMG_SRVC_CD": "1",
         "CT_SCAN_SRVC_CD": "1", "PET_SCAN_SRVC_CD": "0", "NUCLR_MDCN_SRVC_CD": "0",
         "SHCK_TRMA_SRVC_CD": "0", "BURN_CARE_UNIT_SRVC_CD": "0", "NEONTL_ICU_SRVC_CD": "0",
         "OB_SRVC_CD": "1", "ORGN_TRNSPLNT_SRVC_CD": "0", "DCTD_ER_SRVC_CD": "1",
         "RN_CNT": "1000", "LPN_CNT": "80", "PHYSN_CNT": "200",
         "REG_PHRMCST_CNT": "25", "OCPTNL_THRPST_CNT": "15", "PHYS_THRPST_CNT": "20",
         "INHLTN_THRPST_CNT": "15", "EMPLEE_CNT": "2200",
         "OPRTG_ROOM_CNT": "15", "ENDSCPY_PRCDR_ROOMS_CNT": "4", "CRDC_CTHRTZTN_PRCDR_ROOMS_CNT": "2",
         "TOT_OFSITE_EMER_DEPT_CNT": "1", "TOT_OFSITE_URGNT_CARE_CNTR_CNT": "3",
         "TOT_OFSITE_PSYCH_UNIT_CNT": "0", "TOT_OFSITE_REHAB_HOSP_CNT": "0",
         "RELATED_PROVIDER_NUMBER": "", "PRVDR_CTGRY_CD": "01", "PRVDR_CTGRY_SBTYP_CD": "01",
         "GNRL_CNTL_TYPE_CD": "04"},
    ])


@pytest.mark.asyncio
async def test_search_health_systems(mock_ahrq_systems):
    with patch.object(server, "_load_ahrq_systems", new_callable=AsyncMock, return_value=mock_ahrq_systems):
        result = await server.search_health_systems("Jefferson")
    assert "results" in result
    assert len(result["results"]) >= 1
    assert result["results"][0]["name"] == "Jefferson Health"
    _assert_system_evidence(result["evidence"])
    _assert_system_source_metadata(result)
    _assert_system_row_evidence(
        result["results"][0]["evidence"],
        dataset_id="ahrq_health_system_compendium",
        match_basis="ahrq_system_search_result",
    )
    _assert_system_identity_map(result["identity_map"], expected_system_id="SYS_001")
    claim = _system_source_claim(result["identity_map"], "system")
    assert claim["row_evidence_paths"] == ["results[].evidence"]


@pytest.mark.asyncio
async def test_list_health_system_metrics_missing_ahrq_cache_returns_structured_recovery(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(server, "AHRQ_SYSTEM_CACHE", tmp_path / "missing_system.csv")
    monkeypatch.setattr(server, "AHRQ_HOSPITAL_LINKAGE_CACHE", tmp_path / "missing_linkage.csv")

    result = await server.list_health_system_metrics(page_size=1)

    assert result["ok"] is False
    assert result["error_code"] == "AHRQ_CACHE_REQUIRED"
    assert result["status"] == "blocked_missing_required_cache"
    assert result["required_files"] == ["missing_system.csv", "missing_linkage.csv"]
    assert "recovery_steps" in result


@pytest.mark.asyncio
async def test_get_system_profile(mock_ahrq_systems, mock_ahrq_hospitals, mock_pos):
    with (
        patch.object(server, "_load_ahrq_systems", new_callable=AsyncMock, return_value=mock_ahrq_systems),
        patch.object(server, "_load_ahrq_hospitals", new_callable=AsyncMock, return_value=mock_ahrq_hospitals),
        patch.object(server, "_load_pos", new_callable=AsyncMock, return_value=mock_pos),
        patch.object(server, "_search_nppes", new_callable=AsyncMock, return_value=[]),
    ):
        result = await server.get_system_profile(system_id="SYS_001")
    assert result["system"]["name"] == "Jefferson Health"
    assert result["system"]["hospital_count"] == 2
    assert len(result["inpatient_facilities"]) == 2
    # Check enrichment worked
    main = next(f for f in result["inpatient_facilities"] if f["ccn"] == "390001")
    assert main["beds"]["total"] == 900
    assert main["services"]["cardiac_catheterization"] is True
    _assert_system_evidence(result["evidence"])
    _assert_system_source_metadata(result)
    _assert_system_row_evidence(
        main["evidence"],
        dataset_id="ahrq_health_system_compendium",
        match_basis="inpatient_facility_source_row",
    )
    _assert_system_row_evidence(
        result["facility_reconciliation"]["facilities"][0]["evidence"],
        dataset_id="ahrq_health_system_compendium",
        match_basis="health_system_facility_reconciliation_row",
    )
    assert result["identity"]["ahrq_system_id"] == "SYS_001"
    _assert_system_identity_map(result["identity_map"], expected_system_id="SYS_001", expected_ccns={"390001", "390149"})
    claim = _system_source_claim(result["identity_map"], "facilities")
    assert "inpatient_facilities[].evidence" in claim["row_evidence_paths"]
    assert "facility_reconciliation.facilities[].evidence" in claim["row_evidence_paths"]


@pytest.mark.asyncio
async def test_get_system_profile_handles_nullable_ahrq_legacy_counts(mock_pos):
    systems = pd.DataFrame(
        [
            {
                "health_sys_id": "HSI00000715",
                "health_sys_name": "Munson Healthcare",
                "health_sys_city": "Traverse City",
                "health_sys_state": "MI",
                "hosp_count": 1,
                "phys_grp_count": pd.NA,
            }
        ]
    )
    systems["phys_grp_count"] = systems["phys_grp_count"].astype("Int64")
    hospitals = pd.DataFrame(
        [
            {
                "health_sys_id": "HSI00000715",
                "ccn": "230097",
                "hospital_name": "Munson Medical Center",
                "hosp_city": "Traverse City",
                "hosp_state": "MI",
                "hosp_zip": "49684",
                "hos_beds": pd.NA,
                "hos_dsch": pd.NA,
            }
        ]
    )
    hospitals["hos_beds"] = hospitals["hos_beds"].astype("Int64")
    hospitals["hos_dsch"] = hospitals["hos_dsch"].astype("Int64")

    with (
        patch.object(server, "_load_ahrq_systems", new_callable=AsyncMock, return_value=systems),
        patch.object(server, "_load_ahrq_hospitals", new_callable=AsyncMock, return_value=hospitals),
        patch.object(server, "_load_pos", new_callable=AsyncMock, return_value=mock_pos.iloc[0:0]),
        patch.object(server, "_search_nppes", new_callable=AsyncMock, return_value=[]),
    ):
        result = await server.get_system_profile(system_id="HSI00000715", include_outpatient=False)

    assert result["system"]["system_id"] == "HSI00000715"
    assert result["system"]["physician_group_count"] == 0
    assert result["system"]["total_beds"] == 0
    assert result["system"]["total_discharges"] == 0
    assert result["inpatient_facilities"][0]["beds"]["total"] == 0


@pytest.mark.asyncio
async def test_get_system_profile_outpatient_sites_have_nppes_row_evidence(
    mock_ahrq_systems,
    mock_ahrq_hospitals,
    mock_pos,
):
    nppes_rows = [
        {
            "number": "1234567890",
            "basic": {
                "organization_name": "Jefferson Family Medicine",
                "status": "A",
            },
            "addresses": [
                {
                    "address_purpose": "LOCATION",
                    "address_1": "123 Main St",
                    "city": "Philadelphia",
                    "state": "PA",
                    "postal_code": "191070000",
                    "telephone_number": "215-555-1234",
                }
            ],
            "taxonomies": [
                {
                    "code": "207Q00000X",
                    "desc": "Family Medicine",
                    "primary": True,
                }
            ],
        }
    ]
    with (
        patch.object(server, "_load_ahrq_systems", new_callable=AsyncMock, return_value=mock_ahrq_systems),
        patch.object(server, "_load_ahrq_hospitals", new_callable=AsyncMock, return_value=mock_ahrq_hospitals),
        patch.object(server, "_load_pos", new_callable=AsyncMock, return_value=mock_pos),
        patch.object(server, "_search_nppes", new_callable=AsyncMock, return_value=nppes_rows),
    ):
        result = await server.get_system_profile(system_id="SYS_001", include_outpatient=True)

    site = result["outpatient_sites"][0]
    _assert_system_row_evidence(
        site["evidence"],
        dataset_id="nppes_npi_registry",
        match_basis="outpatient_site_source_row",
    )
    assert site["evidence"]["source_name"] == "NPPES NPI Registry"
    assert site["evidence"]["source_url"].startswith("https://npiregistry.cms.hhs.gov/api/?number=1234567890")
    assert site["evidence"]["cache_status"] == "live_api"
    assert site["evidence"]["landing_page"] == "https://npiregistry.cms.hhs.gov/search"
    assert site["evidence"]["query"]["npi"] == "1234567890"
    assert site["evidence"]["query"]["taxonomy_code"] == "207Q00000X"
    assert site["evidence"]["query"]["category"] == "Family Medicine"
    assert "system affiliation remains candidate context" in site["evidence"]["caveat"]
    claim = _system_source_claim(result["identity_map"], "facilities")
    assert "outpatient_sites[].evidence" in claim["row_evidence_paths"]


@pytest.mark.asyncio
async def test_get_system_profile_not_found(mock_ahrq_systems, mock_ahrq_hospitals, mock_pos):
    with (
        patch.object(server, "_load_ahrq_systems", new_callable=AsyncMock, return_value=mock_ahrq_systems),
        patch.object(server, "_load_ahrq_hospitals", new_callable=AsyncMock, return_value=mock_ahrq_hospitals),
        patch.object(server, "_load_pos", new_callable=AsyncMock, return_value=mock_pos),
    ):
        result = await server.get_system_profile(system_name="Mayo Clinic")
    assert "error" in result
    assert result["error"]["code"] == "not_found"
    _assert_system_evidence(result["evidence"])
    _assert_system_source_metadata(result)
    assert result["evidence"]["match_basis"] == "system_name_search_no_ahrq_match"
    assert result["evidence"]["confidence"] == "no_candidate_match_in_loaded_ahrq_compendium"
    assert "exact AHRQ system_id" in result["evidence"]["next_step"]
    assert result["identity"]["canonical_name"] == "MAYO CLINIC"
    _assert_system_identity_map(result["identity_map"], expected_name="MAYO CLINIC")


@pytest.mark.asyncio
async def test_get_system_facilities(mock_ahrq_hospitals, mock_pos):
    with (
        patch.object(server, "_load_ahrq_hospitals", new_callable=AsyncMock, return_value=mock_ahrq_hospitals),
        patch.object(server, "_load_pos", new_callable=AsyncMock, return_value=mock_pos),
    ):
        result = await server.get_system_facilities(system_id="SYS_001")
    assert result["system_id"] == "SYS_001"
    assert len(result["inpatient_facilities"]) == 2
    _assert_system_evidence(result["evidence"])
    _assert_system_source_metadata(result)
    _assert_system_row_evidence(
        result["inpatient_facilities"][0]["evidence"],
        dataset_id="ahrq_health_system_compendium",
        match_basis="inpatient_facility_source_row",
    )
    _assert_system_identity_map(result["identity_map"], expected_system_id="SYS_001", expected_ccns={"390001", "390149"})
    claim = _system_source_claim(result["identity_map"], "facilities")
    assert "inpatient_facilities[].evidence" in claim["row_evidence_paths"]


@pytest.mark.asyncio
async def test_get_system_facilities_no_linked_hospitals_has_evidence(mock_pos):
    with (
        patch.object(server, "_load_ahrq_hospitals", new_callable=AsyncMock, return_value=pd.DataFrame(columns=["health_sys_id", "ccn"])),
        patch.object(server, "_load_pos", new_callable=AsyncMock, return_value=mock_pos),
    ):
        result = await server.get_system_facilities(system_id="SYS_MISSING")

    assert result["error"]["code"] == "not_found"
    _assert_system_evidence(result["evidence"])
    _assert_system_source_metadata(result)
    assert result["evidence"]["match_basis"] == "ahrq_system_id_no_linked_hospitals"
    assert result["identity"]["ahrq_system_id"] == "SYS_MISSING"
    _assert_system_identity_map(result["identity_map"], expected_system_id="SYS_MISSING")


def _assert_system_evidence(evidence: dict) -> None:
    validate_evidence_receipt(evidence, require_content=True)
    assert evidence["source_name"] == "AHRQ Compendium, CMS Provider of Services, and NPPES public registry"
    assert evidence["dataset_id"] == "ahrq_health_system_compendium"
    assert evidence["source_period"]
    assert evidence["landing_page"].startswith("https://www.ahrq.gov/")
    assert evidence["cache_status"] == "mixed_public_cache"
    assert evidence["cache_freshness"]
    assert evidence["entity_scope"] == "health_system_facility_identity"
    assert evidence["caveat"]
    assert evidence["next_step"]


def _assert_system_row_evidence(evidence: dict, *, dataset_id: str, match_basis: str) -> None:
    validate_evidence_receipt(evidence, require_content=True)
    assert evidence["dataset_id"] == dataset_id
    assert evidence["match_basis"] == match_basis
    assert evidence["entity_scope"] == "health_system_facility_identity"
    assert evidence["query"]["row_kind"]
    assert evidence["confidence"]
    assert evidence["caveat"]
    assert evidence["next_step"]


def _assert_system_source_metadata(result: dict) -> None:
    metadata = result["source_metadata"]
    evidence = result["evidence"]

    assert metadata["source_name"] == evidence["source_name"]
    assert metadata["source_url"] == evidence["source_url"]
    assert metadata["dataset_id"] == evidence["dataset_id"]
    assert metadata["source_period"] == evidence["source_period"]
    assert metadata["landing_page"] == evidence["landing_page"]
    assert metadata["retrieved_at"] == evidence["retrieved_at"]
    assert metadata["source_modified"] == evidence["source_modified"]
    assert metadata["cache_status"] == evidence["cache_status"]
    assert metadata["cache_freshness"] == evidence["cache_freshness"]
    assert metadata["entity_scope"] == evidence["entity_scope"]
    assert metadata["query"] == evidence["query"]
    assert metadata["cache_key"] == evidence["cache_key"]
    assert metadata["source_type"] == "ahrq_cms_nppes_health_system_public_sources"


def _assert_system_identity_map(
    identity_map: dict,
    *,
    expected_system_id: str = "",
    expected_ccns: set[str] | None = None,
    expected_name: str = "",
) -> None:
    by_field = {entry["field"]: entry for entry in identity_map["join_keys"]}

    assert identity_map["entity_scope"] == "health_system_facility_identity"
    assert identity_map["source_claims"]
    assert identity_map["conflict_policy"]
    assert identity_map["missing_data_policy"].startswith("No-match system-profiler responses")
    if expected_system_id:
        assert expected_system_id in by_field["ahrq_system_id"]["values"]
        assert by_field["ahrq_system_id"]["status"] == "provided"
    if expected_ccns:
        assert set(by_field["ccn"]["values"]) >= expected_ccns
        assert "facilities" in by_field["ccn"]["used_by"] or "inpatient_facilities" in by_field["ccn"]["used_by"]
    if expected_name:
        assert expected_name in by_field["canonical_name"]["values"]


def _system_source_claim(identity_map: dict, collection: str) -> dict:
    claims = {claim["collection"]: claim for claim in identity_map["source_claims"]}
    return claims[collection]
