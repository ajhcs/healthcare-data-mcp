"""Fixture-based tests for the SAM.gov Exclusions API client."""

from __future__ import annotations

from typing import Any

import httpx
import pytest

from servers.public_records import sam_exclusions_client


class _Response:
    def __init__(self, payload: dict[str, Any]) -> None:
        self._payload = payload

    def json(self) -> dict[str, Any]:
        return self._payload


def test_build_search_params_maps_public_tool_inputs() -> None:
    params = sam_exclusions_client.build_search_params(
        entity_name="Acme & Health",
        uei="abc123abc123",
        cage_code="1cm51",
        npi="1234567893",
        state="pa",
        country="usa",
        classification="Firm",
        exclusion_type="Ineligible Pending",
        excluding_agency="HHS",
        limit=25,
    )

    assert params["exclusionName"] == "Acme Health"
    assert params["ueiSAM"] == "ABC123ABC123"
    assert params["cageCode"] == "1CM51"
    assert params["npi"] == "1234567893"
    assert params["stateProvince"] == "PA"
    assert params["country"] == "USA"
    assert params["classification"] == "Firm"
    assert params["exclusionType"] == "Ineligible Pending"
    assert params["excludingAgencyCode"] == "HHS"
    assert params["recordStatus"] == "active"
    assert params["size"] == 10


@pytest.mark.asyncio
async def test_search_exclusions_missing_api_key_returns_structured_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("SAM_GOV_API_KEY", raising=False)

    response = await sam_exclusions_client.search_exclusions(entity_name="Acme Health")

    assert response["code"] == "missing_api_key"
    assert response["retryable"] is False
    assert response["source_metadata"]["source_name"] == "SAM.gov Exclusions"
    assert response["source_metadata"]["api_key_configured"] is False


@pytest.mark.asyncio
async def test_search_exclusions_paginates_json_api(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SAM_GOV_API_KEY", "fixture-key")
    calls: list[dict[str, Any]] = []

    async def fake_request(
        method: str,
        url: str,
        *,
        params: dict[str, Any],
        timeout: float,
    ) -> _Response:
        calls.append(params)
        page = params["page"]
        payloads = [
            {
                "totalRecords": 3,
                "excludedEntity": [{"exclusionIdentification": {"entityName": "A"}}],
            },
            {
                "totalRecords": 3,
                "excludedEntity": [{"exclusionIdentification": {"entityName": "B"}}],
            },
            {
                "totalRecords": 3,
                "excludedEntity": [{"exclusionIdentification": {"entityName": "C"}}],
            },
        ]
        assert method == "GET"
        assert url == sam_exclusions_client.BASE_URL
        assert timeout == sam_exclusions_client.TIMEOUT
        return _Response(payloads[page])

    monkeypatch.setattr(sam_exclusions_client, "resilient_request", fake_request)

    response = await sam_exclusions_client.search_exclusions(entity_name="Acme", limit=3)

    assert response["totalRecords"] == 3
    assert [r["exclusionIdentification"]["entityName"] for r in response["excludedEntity"]] == [
        "A",
        "B",
        "C",
    ]
    assert [call["page"] for call in calls] == [0, 1, 2]
    assert all(call["api_key"] == "fixture-key" for call in calls)
    assert response["source_metadata"]["returned_records"] == 3


@pytest.mark.asyncio
async def test_search_exclusions_accepts_single_record_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SAM_GOV_API_KEY", "fixture-key")

    async def fake_request(
        method: str,
        url: str,
        *,
        params: dict[str, Any],
        timeout: float,
    ) -> _Response:
        return _Response({
            "totalRecords": 1,
            "excludedEntity": {"exclusionIdentification": {"entityName": "A"}},
        })

    monkeypatch.setattr(sam_exclusions_client, "resilient_request", fake_request)

    response = await sam_exclusions_client.search_exclusions(entity_name="Acme", limit=1)

    assert response["source_metadata"]["returned_records"] == 1
    assert response["excludedEntity"][0]["exclusionIdentification"]["entityName"] == "A"


@pytest.mark.asyncio
async def test_search_exclusions_redacts_api_key_from_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SAM_GOV_API_KEY", "secret-key")

    async def fake_request(
        method: str,
        url: str,
        *,
        params: dict[str, Any],
        timeout: float,
    ) -> _Response:
        request = httpx.Request("GET", f"{url}?api_key=secret-key")
        response = httpx.Response(500, request=request, text="upstream error")
        raise httpx.HTTPStatusError(
            "Server error for url https://api.sam.gov/entity-information/v4/exclusions?api_key=secret-key",
            request=request,
            response=response,
        )

    monkeypatch.setattr(sam_exclusions_client, "resilient_request", fake_request)

    response = await sam_exclusions_client.search_exclusions(entity_name="Acme")

    assert response["code"] == "source_unavailable"
    assert "secret-key" not in response["source_metadata"]["last_error"]
    assert "api_key=[REDACTED]" in response["source_metadata"]["last_error"]
