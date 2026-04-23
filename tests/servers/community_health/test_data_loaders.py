from __future__ import annotations

from servers.community_health import data_loaders


def sample_places_rows() -> list[dict]:
    return [
        {
            "year": "2022",
            "stateabbr": "PA",
            "statedesc": "Pennsylvania",
            "locationname": "Allegheny",
            "datasource": "BRFSS",
            "category": "Health Outcomes",
            "measure": "High blood pressure among adults aged >=18 years",
            "data_value_unit": "%",
            "data_value_type": "Age-adjusted prevalence",
            "data_value": "33.4",
            "low_confidence_limit": "31.2",
            "high_confidence_limit": "35.6",
            "totalpop18plus": "955000",
            "totalpopulation": "1230000",
            "geolocation": {"type": "Point", "coordinates": [-79.9959, 40.4406]},
            "locationid": "42003",
            "categoryid": "HLTHOUT",
            "measureid": "BPHIGH",
            "datavaluetypeid": "AgeAdjPrv",
            "short_question_text": "High Blood Pressure",
        },
        {
            "year": "2022",
            "stateabbr": "PA",
            "statedesc": "Pennsylvania",
            "locationname": "Philadelphia",
            "datasource": "BRFSS",
            "category": "Health Risk Behaviors",
            "measure": "Current smoking among adults",
            "data_value_unit": "%",
            "data_value_type": "Crude prevalence",
            "data_value": "",
            "low_confidence_limit": "",
            "high_confidence_limit": "",
            "totalpopulation": "1600000",
            "locationid": "42101",
            "categoryid": "RISKBEH",
            "measureid": "CSMOKING",
            "datavaluetypeid": "CrdPrv",
            "short_question_text": "Current Smoking",
        },
    ]


def test_normalize_places_record_preserves_values_and_notes() -> None:
    normalized = data_loaders.normalize_places_record(sample_places_rows()[0], geography_type="county")

    assert normalized["location_id"] == "42003"
    assert normalized["measure_id"] == "BPHIGH"
    assert normalized["data_value"] == 33.4
    assert normalized["confidence_interval"] == {"low": 31.2, "high": 35.6}
    assert normalized["population"]["adult_18_plus"] == 955000
    assert normalized["geolocation"] == {"latitude": 40.4406, "longitude": -79.9959}
    assert "not patient-level facts" in normalized["notes"][0]


def test_normalize_places_record_reports_missing_source_values() -> None:
    normalized = data_loaders.normalize_places_record(sample_places_rows()[1], geography_type="county")

    assert normalized["data_value"] is None
    assert "Missing data_value in source row." in normalized["notes"]
    assert "Missing confidence_interval in source row." in normalized["notes"]


def test_filter_rows_and_measure_metadata() -> None:
    rows = data_loaders.filter_rows(sample_places_rows(), state="pa", measure_ids=["bphigh"], search="Allegheny")
    measures = data_loaders.build_measure_metadata(rows)

    assert len(rows) == 1
    assert measures == [
        {
            "measure_id": "BPHIGH",
            "measure": "High blood pressure among adults aged >=18 years",
            "short_question_text": "High Blood Pressure",
            "category": "Health Outcomes",
            "category_id": "HLTHOUT",
            "data_value_type": "Age-adjusted prevalence",
            "data_value_type_id": "AgeAdjPrv",
            "value_unit": "%",
            "source_note": data_loaders.COMMUNITY_ESTIMATE_NOTE,
        }
    ]


def test_filter_rows_bounds_invalid_limit() -> None:
    rows = data_loaders.filter_rows(sample_places_rows(), limit="bad")  # type: ignore[arg-type]

    assert len(rows) == 2


def test_parquet_cache_roundtrip(tmp_path) -> None:
    cache_path = data_loaders.write_parquet_cache(sample_places_rows(), tmp_path / "places.parquet")

    loaded = data_loaders.load_rows(cache_path)

    assert len(loaded) == 2
    assert loaded[0]["locationid"] == "42003"
