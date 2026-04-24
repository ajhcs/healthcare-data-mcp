from __future__ import annotations

import pytest

from servers.provider_enrollment import data_loaders, server


@pytest.fixture
def provider_cache(tmp_path, monkeypatch):
    monkeypatch.setattr(data_loaders, "_CACHE_DIR", tmp_path)
    data_loaders.cache_records(
        "medicare_ffs_public_provider_enrollment",
        [
            {
                "NPI": "1234567893",
                "PAC ID": "PAC-77",
                "Enrollment ID": "ENR-1",
                "State": "PA",
                "Provider Name": "Jefferson Medical Group",
                "Provider Type": "Group Practice",
            }
        ],
    )
    data_loaders.cache_records(
        "hospital_enrollments",
        [
            {
                "Enrollment ID": "ENR-1",
                "CCN": "390001",
                "Facility Name": "Jefferson Hospital",
                "State": "PA",
                "Provider Type": "Hospital",
            }
        ],
    )
    data_loaders.cache_records(
        "hospital_all_owners",
        [
            {
                "Enrollment ID": "ENR-1",
                "CCN": "390001",
                "Facility Name": "Jefferson Hospital",
                "State": "PA",
                "Owner Organization Name": "Jefferson Parent LLC",
                "Owner Associate ID": "OWN-1",
                "Owner PAC ID": "PAC-OWNER",
                "Association Role Text": "Direct owner",
                "Ownership Percentage": "100",
            }
        ],
    )
    data_loaders.cache_records(
        "hospital_chow",
        [
            {
                "Enrollment ID": "ENR-1",
                "CCN": "390001",
                "Facility Name": "Jefferson Hospital",
                "State": "PA",
                "Transaction Date": "2026-04-01",
                "Change Type": "Change of ownership",
            }
        ],
    )
    return tmp_path


@pytest.mark.asyncio
async def test_search_provider_enrollment(provider_cache) -> None:
    result = await server.search_provider_enrollment(npi="1234567893")

    assert result["total_results"] == 1
    assert result["enrollments"][0]["npi"] == "1234567893"
    assert result["enrollments"][0]["raw"]["PAC ID"] == "PAC-77"
    assert result["metadata"]


@pytest.mark.asyncio
async def test_get_provider_enrollment_detail_links_owners_and_chow(provider_cache) -> None:
    result = await server.get_provider_enrollment_detail(npi="1234567893")

    assert result["enrollments"][0]["enrollment_id"] == "ENR1"
    assert result["ownership"][0]["owner_name"] == "Jefferson Parent LLC"
    assert result["chow_history"][0]["change_type"] == "Change of ownership"


@pytest.mark.asyncio
async def test_get_facility_ownership(provider_cache) -> None:
    result = await server.get_facility_ownership(ccn="390001")

    assert result["total_results"] == 1
    assert result["owners"][0]["owner_associate_id"] == "OWN1"
    assert result["owners"][0]["is_active"] is True


@pytest.mark.asyncio
async def test_trace_owner_network(provider_cache) -> None:
    result = await server.trace_owner_network(owner_name="Jefferson Parent", depth=3)

    assert {node["kind"] for node in result["nodes"]} == {"owner", "facility"}
    assert result["edges"][0]["relationship"] == "owns_or_controls"


@pytest.mark.asyncio
async def test_trace_owner_network_defaults_invalid_depth(provider_cache) -> None:
    result = await server.trace_owner_network(owner_name="Jefferson Parent", depth="bad")  # type: ignore[arg-type]

    assert result["depth"] == 1
    assert result["nodes"]


@pytest.mark.asyncio
async def test_search_change_of_ownership(provider_cache) -> None:
    result = await server.search_change_of_ownership(ccn="390001", start_date="2026-01-01")

    assert result["total_results"] == 1
    assert result["events"][0]["transaction_date"] == "2026-04-01"


@pytest.mark.asyncio
async def test_profile_provider_control(provider_cache) -> None:
    result = await server.profile_provider_control(ccn="390001")

    assert result["ownership"][0]["owner_name"] == "Jefferson Parent LLC"
    assert result["join_keys"]["ccn"] == ["390001"]
    assert result["owner_network"]["nodes"]


@pytest.mark.asyncio
async def test_invalid_npi_returns_structured_error(provider_cache) -> None:
    result = await server.search_provider_enrollment(npi="1111111111")

    assert result["ok"] is False
    assert result["error"]["code"] == "invalid_params"
