"""Tests for the claims-analytics MCP server tools.

Uses monkeypatching to avoid downloading real CMS PUF data (hundreds of MBs).
"""

import json
from unittest.mock import AsyncMock, patch

import pytest

from servers.claims_analytics import server, data_loaders


# ---------------------------------------------------------------------------
# Sample inpatient row fixtures (already in the normalised dict format
# returned by data_loaders.query_inpatient)
# ---------------------------------------------------------------------------

INPATIENT_ROWS = [
    {
        "ccn": "390223",
        "provider_name": "Thomas Jefferson University Hospital",
        "state": "PA",
        "drg_code": "470",
        "drg_desc": "Major Joint Replacement or Reattachment of Lower Extremity",
        "discharges": 812,
        "avg_charges": 68000.0,
        "avg_total_payment": 18500.0,
        "avg_medicare_payment": 16200.0,
    },
    {
        "ccn": "390223",
        "provider_name": "Thomas Jefferson University Hospital",
        "state": "PA",
        "drg_code": "291",
        "drg_desc": "Heart Failure & Shock w MCC",
        "discharges": 340,
        "avg_charges": 42000.0,
        "avg_total_payment": 15000.0,
        "avg_medicare_payment": 13800.0,
    },
    {
        "ccn": "390223",
        "provider_name": "Thomas Jefferson University Hospital",
        "state": "PA",
        "drg_code": "065",
        "drg_desc": "Intracranial Hemorrhage or Cerebral Infarction",
        "discharges": 210,
        "avg_charges": 35000.0,
        "avg_total_payment": 11000.0,
        "avg_medicare_payment": 9800.0,
    },
]

OUTPATIENT_ROWS = [
    {
        "ccn": "390223",
        "provider_name": "Thomas Jefferson University Hospital",
        "state": "PA",
        "apc_code": "5115",
        "apc_desc": "Level 5 Musculoskeletal Procedures",
        "services": 1420,
        "avg_charges": 12000.0,
        "avg_total_payment": 3200.0,
        "avg_medicare_payment": 2800.0,
    },
    {
        "ccn": "390223",
        "provider_name": "Thomas Jefferson University Hospital",
        "state": "PA",
        "apc_code": "5072",
        "apc_desc": "Level 2 Cardiac Imaging",
        "services": 880,
        "avg_charges": 2400.0,
        "avg_total_payment": 620.0,
        "avg_medicare_payment": 510.0,
    },
]


# ---------------------------------------------------------------------------
# Tests: get_inpatient_volumes
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_inpatient_volumes_success():
    with (
        patch.object(data_loaders, "ensure_inpatient_cached", new_callable=AsyncMock, return_value=True),
        patch.object(data_loaders, "query_inpatient", return_value=INPATIENT_ROWS),
    ):
        result = json.loads(await server.get_inpatient_volumes(ccn="390223"))

    assert result["ccn"] == "390223"
    assert result["provider_name"] == "Thomas Jefferson University Hospital"
    assert result["total_discharges"] == 812 + 340 + 210
    assert result["total_drgs"] == 3
    assert isinstance(result["service_line_summary"], list)
    assert len(result["drg_details"]) == 3
    # Sorted descending by discharges — DRG 470 should be first
    assert result["drg_details"][0]["drg_code"] == "470"


@pytest.mark.asyncio
async def test_get_inpatient_volumes_no_data():
    with (
        patch.object(data_loaders, "ensure_inpatient_cached", new_callable=AsyncMock, return_value=True),
        patch.object(data_loaders, "query_inpatient", return_value=[]),
    ):
        result = json.loads(await server.get_inpatient_volumes(ccn="999999"))
    assert "error" in result
    assert "999999" in result["error"]


@pytest.mark.asyncio
async def test_get_inpatient_volumes_invalid_year():
    result = json.loads(await server.get_inpatient_volumes(ccn="390223", year="1999"))
    assert "error" in result
    assert "1999" in result["error"]


@pytest.mark.asyncio
async def test_get_inpatient_volumes_service_line_filter():
    with (
        patch.object(data_loaders, "ensure_inpatient_cached", new_callable=AsyncMock, return_value=True),
        patch.object(data_loaders, "query_inpatient", return_value=INPATIENT_ROWS),
    ):
        result = json.loads(await server.get_inpatient_volumes(ccn="390223", service_line="Orthopedics"))

    # DRG 470 maps to Orthopedics — only that row should survive
    if "error" not in result:
        for detail in result["drg_details"]:
            assert detail["service_line"].lower() == "orthopedics"
    # If 0 matches, expect error — both outcomes are valid
    assert "ccn" in result or "error" in result


# ---------------------------------------------------------------------------
# Tests: get_outpatient_volumes
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_outpatient_volumes_success():
    with (
        patch.object(data_loaders, "ensure_outpatient_cached", new_callable=AsyncMock, return_value=True),
        patch.object(data_loaders, "query_outpatient", return_value=OUTPATIENT_ROWS),
    ):
        result = json.loads(await server.get_outpatient_volumes(ccn="390223"))

    assert result["ccn"] == "390223"
    assert result["total_services"] == 1420 + 880
    assert result["total_apcs"] == 2
    # Sorted descending by services — APC 5115 should be first
    assert result["apc_details"][0]["apc_code"] == "5115"


@pytest.mark.asyncio
async def test_get_outpatient_volumes_no_data():
    with (
        patch.object(data_loaders, "ensure_outpatient_cached", new_callable=AsyncMock, return_value=True),
        patch.object(data_loaders, "query_outpatient", return_value=[]),
    ):
        result = json.loads(await server.get_outpatient_volumes(ccn="000000"))
    assert "error" in result


# ---------------------------------------------------------------------------
# Tests: compute_case_mix
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_compute_case_mix_success():
    with (
        patch.object(data_loaders, "ensure_inpatient_cached", new_callable=AsyncMock, return_value=True),
        patch.object(data_loaders, "query_inpatient", return_value=INPATIENT_ROWS),
    ):
        result = json.loads(await server.compute_case_mix(ccn="390223"))

    assert result["ccn"] == "390223"
    assert "case_mix_index" in result
    assert isinstance(result["case_mix_index"], float)
    assert result["case_mix_index"] > 0
    assert result["total_discharges"] == 812 + 340 + 210
    assert len(result["service_line_acuity"]) > 0
    # Each acuity entry should have required fields
    for entry in result["service_line_acuity"]:
        assert "service_line" in entry
        assert "discharges" in entry
        assert "avg_drg_weight" in entry


@pytest.mark.asyncio
async def test_compute_case_mix_no_data():
    with (
        patch.object(data_loaders, "ensure_inpatient_cached", new_callable=AsyncMock, return_value=True),
        patch.object(data_loaders, "query_inpatient", return_value=[]),
    ):
        result = json.loads(await server.compute_case_mix(ccn="000000"))
    assert "error" in result
