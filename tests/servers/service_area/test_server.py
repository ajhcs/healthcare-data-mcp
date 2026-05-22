"""Tests for service-area MCP tool wrappers."""

from __future__ import annotations

import pandas as pd
import pytest

from servers.service_area import server
from shared.utils.mcp_response import validate_evidence_receipt
from tests.helpers import parse_tool_result


@pytest.fixture
def hsaf_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"ccn": "390001", "facility_name": "Example Hospital", "zip_code": "19107", "discharges": 70},
            {"ccn": "390001", "facility_name": "Example Hospital", "zip_code": "19104", "discharges": 20},
            {"ccn": "390001", "facility_name": "Example Hospital", "zip_code": "19103", "discharges": 10},
            {"ccn": "390002", "facility_name": "Other Hospital", "zip_code": "19107", "discharges": 30},
        ]
    )


@pytest.fixture
def dartmouth_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "zip_code": "19107",
                "hsanum": "101",
                "hsacity": "Philadelphia",
                "hsastate": "PA",
                "hrrnum": "201",
                "hrrcity": "Philadelphia",
                "hrrstate": "PA",
            },
            {
                "zip_code": "19104",
                "hsanum": "101",
                "hsacity": "Philadelphia",
                "hsastate": "PA",
                "hrrnum": "201",
                "hrrcity": "Philadelphia",
                "hrrstate": "PA",
            },
        ]
    )


def _assert_receipt(result: dict, *, dataset_id: str, match_basis: str) -> None:
    assert result["evidence"]["dataset_id"] == dataset_id
    assert result["evidence"]["match_basis"] == match_basis
    validate_evidence_receipt(result["evidence"], require_content=True)


@pytest.mark.asyncio
async def test_compute_service_area_returns_evidence_and_identity(monkeypatch, hsaf_frame: pd.DataFrame) -> None:
    async def fake_hsaf() -> pd.DataFrame:
        return hsaf_frame

    monkeypatch.setattr(server, "download_hsaf", fake_hsaf)

    result = parse_tool_result(await server.compute_service_area("390001", psa_threshold=0.75, ssa_threshold=0.95))

    assert result["facility_ccn"] == "390001"
    assert result["psa_zips"] == ["19107", "19104"]
    _assert_receipt(result, dataset_id="cms_hospital_service_area_file", match_basis="ccn_exact_hsaf_zip_discharge_rows")
    assert result["identity"]["ccn"] == "390001"
    assert {entity["entity_type"] for entity in result["identity_map"]["entities"]} == {"facility", "zip_geography"}


@pytest.mark.asyncio
async def test_get_market_share_returns_hospital_identity_map(monkeypatch, hsaf_frame: pd.DataFrame) -> None:
    async def fake_hsaf() -> pd.DataFrame:
        return hsaf_frame

    async def fake_names() -> dict[str, str]:
        return {"390001": "Example Hospital", "390002": "Other Hospital"}

    monkeypatch.setattr(server, "download_hsaf", fake_hsaf)
    monkeypatch.setattr(server, "load_hospital_names", fake_names)

    result = parse_tool_result(await server.get_market_share("19107"))

    assert result["zip_code"] == "19107"
    assert result["hospitals"][0]["ccn"] == "390001"
    validate_evidence_receipt(result["hospitals"][0]["evidence"], require_content=True)
    assert result["hospitals"][0]["evidence"]["dataset_id"] == "cms_hospital_service_area_file"
    assert result["hospitals"][0]["evidence"]["match_basis"] == "hsaf_zip_hospital_market_share_row"
    assert result["hospitals"][0]["evidence"]["query"]["ccn"] == "390001"
    _assert_receipt(result, dataset_id="cms_hospital_service_area_file", match_basis="zip_exact_hsaf_discharge_rows")
    assert result["identity"]["zip_code"] == "19107"
    assert {entity["ccn"] for entity in result["identity_map"]["entities"]} == {"390001", "390002"}


@pytest.mark.asyncio
async def test_get_hsa_hrr_mapping_returns_dartmouth_evidence(monkeypatch, dartmouth_frame: pd.DataFrame) -> None:
    async def fake_crosswalk() -> pd.DataFrame:
        return dartmouth_frame

    monkeypatch.setattr(server, "download_dartmouth_crosswalk", fake_crosswalk)

    result = parse_tool_result(await server.get_hsa_hrr_mapping("19107"))

    assert result["hsa_number"] == 101
    assert result["hrr_number"] == 201
    _assert_receipt(result, dataset_id="dartmouth_atlas_zip_hsa_hrr_crosswalk", match_basis="zip_exact_dartmouth_crosswalk_row")
    assert result["identity_map"]["entities"][0]["hsa_number"] == "101"


@pytest.mark.asyncio
async def test_compare_to_dartmouth_returns_combined_evidence(
    monkeypatch,
    hsaf_frame: pd.DataFrame,
    dartmouth_frame: pd.DataFrame,
) -> None:
    async def fake_hsaf() -> pd.DataFrame:
        return hsaf_frame

    async def fake_crosswalk() -> pd.DataFrame:
        return dartmouth_frame

    monkeypatch.setattr(server, "download_hsaf", fake_hsaf)
    monkeypatch.setattr(server, "download_dartmouth_crosswalk", fake_crosswalk)

    result = parse_tool_result(await server.compare_to_dartmouth("390001"))

    assert result["facility_ccn"] == "390001"
    assert result["hsa_number"] == 101
    _assert_receipt(result, dataset_id="service_area_dartmouth_overlap", match_basis="ccn_exact_hsaf_rows_plus_top_zip_dartmouth_crosswalk")
    assert result["source_metadata"]["sources"][0]["dataset_id"] == "cms_hospital_service_area_file"
    assert result["identity"]["ccn"] == "390001"
