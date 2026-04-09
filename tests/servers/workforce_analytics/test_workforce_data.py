"""Tests for workforce-analytics ACGME import and query behavior."""

from pathlib import Path

import pandas as pd

from servers.workforce_analytics import workforce_data


def test_normalize_acgme_dataframe_maps_export_aliases():
    raw = pd.DataFrame(
        [
            {
                "Program ID": "1403521487",
                "Specialty Name": "Internal Medicine",
                "Sponsoring Institution": "Cleveland Clinic Foundation",
                "City": "Cleveland",
                "State": "OH",
                "Approved Positions": "180",
                "On Duty": "176",
                "Status": "Accredited",
            }
        ]
    )

    normalized = workforce_data.normalize_acgme_dataframe(raw)

    assert list(normalized.columns) == [
        "program_id",
        "specialty",
        "institution",
        "city",
        "state",
        "total_positions",
        "filled_positions",
        "accreditation_status",
    ]
    row = normalized.iloc[0]
    assert row["program_id"] == "1403521487"
    assert row["specialty"] == "Internal Medicine"
    assert row["institution"] == "Cleveland Clinic Foundation"
    assert row["state"] == "OH"
    assert row["total_positions"] == 180
    assert row["filled_positions"] == 176
    assert row["accreditation_status"] == "Accredited"


def test_query_acgme_programs_reads_env_configured_csv(tmp_path: Path, monkeypatch):
    csv_path = tmp_path / "acgme_export.csv"
    pd.DataFrame(
        [
            {
                "program_id": "1403521487",
                "specialty": "Internal Medicine",
                "institution": "Cleveland Clinic Foundation",
                "city": "Cleveland",
                "state": "OH",
                "total_positions": "180",
                "filled_positions": "176",
                "accreditation_status": "Accredited",
            },
            {
                "program_id": "4403521234",
                "specialty": "General Surgery",
                "institution": "Cleveland Clinic Foundation",
                "city": "Cleveland",
                "state": "OH",
                "total_positions": "70",
                "filled_positions": "68",
                "accreditation_status": "Accredited",
            },
        ]
    ).to_csv(csv_path, index=False)

    monkeypatch.setenv(workforce_data._ACGME_ENV_VAR, str(csv_path))
    monkeypatch.setattr(workforce_data, "_ACGME_CACHE_CSV", tmp_path / "missing-cache.csv")
    monkeypatch.setattr(workforce_data, "_ACGME_CSV", tmp_path / "missing-bundled.csv")

    results = workforce_data.query_acgme_programs(
        institution="cleveland clinic",
        specialty="internal medicine",
        state="oh",
    )

    assert len(results) == 1
    assert results[0]["program_id"] == "1403521487"
    assert results[0]["filled_positions"] == 176


def test_query_acgme_programs_returns_actionable_error_when_missing(monkeypatch, tmp_path: Path):
    monkeypatch.delenv(workforce_data._ACGME_ENV_VAR, raising=False)
    monkeypatch.setattr(workforce_data, "_ACGME_CACHE_CSV", tmp_path / "missing-cache.csv")
    monkeypatch.setattr(workforce_data, "_ACGME_CSV", tmp_path / "missing-bundled.csv")

    results = workforce_data.query_acgme_programs(state="OH")

    assert len(results) == 1
    assert "error" in results[0]
    assert "scripts/import_acgme_programs.py" in results[0]["error"]
    assert "acgmecloud.org/analytics/explore-public-data/program-search" in results[0]["error"]
