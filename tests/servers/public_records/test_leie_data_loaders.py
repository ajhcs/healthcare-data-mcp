"""Fixture-based tests for HHS OIG LEIE data loading and matching."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from servers.public_records import data_loaders


HEADER = [
    "LASTNAME",
    "FIRSTNAME",
    "MIDNAME",
    "BUSNAME",
    "GENERAL",
    "SPECIALTY",
    "UPIN",
    "NPI",
    "DOB",
    "ADDRESS",
    "CITY",
    "STATE",
    "ZIP",
    "EXCLTYPE",
    "EXCLDATE",
    "REINDATE",
    "WAIVERDATE",
    "WVRSTATE",
]


ROWS = [
    [
        "Smith",
        "Jane",
        "Q",
        "",
        "BUSOWNER",
        "NURSING",
        "",
        "1234567893",
        "19700131",
        "123 Main St",
        "Pittsburgh",
        "PA",
        "15213",
        "1128b4",
        "20240115",
        "00000000",
        "00000000",
        "",
    ],
    [
        "",
        "",
        "",
        "Acme Health LLC",
        "BUSOWNER",
        "DME",
        "",
        "0000000000",
        "00000000",
        "1 Market St",
        "Philadelphia",
        "Pennsylvania",
        "19103",
        "1128a1",
        "20240201",
        "00000000",
        "00000000",
        "PA",
    ],
]


class FakeResponse:
    def __init__(self, content: bytes = b"", headers: dict[str, str] | None = None) -> None:
        self.content = content
        self.headers = headers or {}


def write_leie_csv(path: Path, rows: list[list[str]] | None = None) -> Path:
    rows = ROWS if rows is None else rows
    path.write_text(
        ",".join(HEADER) + "\n" + "\n".join(",".join(row) for row in rows) + "\n",
        encoding="utf-8",
    )
    return path


@pytest.fixture
def leie_cache(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    csv_path = write_leie_csv(tmp_path / "leie.csv")
    parquet_path = tmp_path / "leie_current.parquet"
    meta_path = tmp_path / "leie_current.meta.json"
    cache_csv_path = tmp_path / "leie_current.csv"

    df = data_loaders.parse_leie_csv(csv_path)
    data_loaders._write_dataframe_parquet(df, parquet_path, compression="zstd")
    meta_path.write_text(
        json.dumps({
            "downloaded_at": "2026-04-10T11:00:39+00:00",
            "source_last_modified": "Fri, 10 Apr 2026 11:00:39 GMT",
            "source_etag": '"fixture"',
            "record_count": len(df),
            "cache_status": "fresh",
        }),
        encoding="utf-8",
    )
    cache_csv_path.write_text(csv_path.read_text(encoding="utf-8"), encoding="utf-8")

    monkeypatch.setattr(data_loaders, "_LEIE_PARQUET", parquet_path)
    monkeypatch.setattr(data_loaders, "_LEIE_META", meta_path)
    monkeypatch.setattr(data_loaders, "_LEIE_CSV", cache_csv_path)
    return parquet_path


def test_parse_leie_csv_normalizes_layout_columns(tmp_path: Path) -> None:
    df = data_loaders.parse_leie_csv(write_leie_csv(tmp_path / "leie.csv"))

    assert list(df["entity_type"]) == ["individual", "entity"]
    assert df.loc[0, "display_name"] == "Jane Q Smith"
    assert df.loc[1, "display_name"] == "Acme Health LLC"
    assert df.loc[0, "normalized_individual_name"] == "JANE Q SMITH"
    assert df.loc[1, "normalized_business_name"] == "ACME HEALTH"
    assert df.loc[1, "state"] == "PA"


def test_parse_leie_csv_normalizes_placeholder_values(tmp_path: Path) -> None:
    df = data_loaders.parse_leie_csv(write_leie_csv(tmp_path / "leie.csv"))

    assert df.loc[1, "npi"] == ""
    assert df.loc[1, "dob"] == ""
    assert df.loc[0, "reinstatement_date"] == ""
    assert df.loc[0, "exclusion_date"] == "2024-01-15"


def test_parse_leie_csv_rejects_missing_required_columns(tmp_path: Path) -> None:
    bad_path = tmp_path / "bad.csv"
    bad_path.write_text("LASTNAME,FIRSTNAME\nSmith,Jane\n", encoding="utf-8")

    with pytest.raises(ValueError, match="missing required columns"):
        data_loaders.parse_leie_csv(bad_path)


def test_query_leie_by_npi_exact_match(leie_cache: Path) -> None:
    rows = data_loaders.query_leie_by_npi("1234567893")

    assert len(rows) == 1
    assert rows[0]["display_name"] == "Jane Q Smith"
    assert rows[0]["match_basis"] == "npi_exact"
    assert rows[0]["verification_status"] == "strong_potential_match"


def test_query_leie_by_individual_uses_name_state_filters(leie_cache: Path) -> None:
    rows = data_loaders.query_leie_by_individual(
        last_name="Smi",
        first_name="Jane",
        state="Pennsylvania",
        dob="19700131",
    )

    assert len(rows) == 1
    assert rows[0]["display_name"] == "Jane Q Smith"
    assert rows[0]["match_score"] >= 90
    assert rows[0]["match_basis"] in {"name_state_dob", "name_dob"}


def test_query_leie_by_entity_scores_business_name_candidates(leie_cache: Path) -> None:
    rows = data_loaders.query_leie_by_entity("Acme Health", state="PA")

    assert len(rows) == 1
    assert rows[0]["display_name"] == "Acme Health LLC"
    assert rows[0]["match_score"] >= 90
    assert rows[0]["verification_status"] == "potential_match"


def test_get_leie_source_metadata_reads_meta_json(leie_cache: Path) -> None:
    metadata = data_loaders.get_leie_source_metadata()

    assert metadata["source_url"] == data_loaders.LEIE_URL
    assert metadata["source_last_modified"] == "Fri, 10 Apr 2026 11:00:39 GMT"
    assert metadata["record_count"] == 2
    assert metadata["cache_path"].endswith("leie_current.parquet")


@pytest.mark.asyncio
async def test_ensure_leie_cached_force_refresh_attempts_get_after_head_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    csv_path = write_leie_csv(tmp_path / "fixture.csv")
    parquet_path = tmp_path / "leie_current.parquet"
    meta_path = tmp_path / "leie_current.meta.json"
    cache_csv_path = tmp_path / "leie_current.csv"
    data_loaders._write_dataframe_parquet(
        data_loaders.parse_leie_csv(csv_path),
        parquet_path,
        compression="zstd",
    )
    meta_path.write_text(
        json.dumps({"downloaded_at": "2026-04-10T11:00:39+00:00", "record_count": 2}),
        encoding="utf-8",
    )

    monkeypatch.setattr(data_loaders, "_LEIE_PARQUET", parquet_path)
    monkeypatch.setattr(data_loaders, "_LEIE_META", meta_path)
    monkeypatch.setattr(data_loaders, "_LEIE_CSV", cache_csv_path)

    calls: list[str] = []

    async def fake_request(method: str, url: str, **kwargs: object) -> FakeResponse:
        calls.append(method)
        if method == "HEAD":
            raise RuntimeError("head unavailable")
        return FakeResponse(
            csv_path.read_bytes(),
            {"last-modified": "Fri, 10 Apr 2026 11:00:39 GMT", "etag": '"new"'},
        )

    monkeypatch.setattr(data_loaders, "resilient_request", fake_request)

    metadata = await data_loaders.ensure_leie_cached(force_refresh=True)

    assert calls == ["HEAD", "GET"]
    assert metadata["cache_status"] == "refreshed"
    assert metadata["source_etag"] == '"new"'


def test_leie_parquet_helpers_work_without_pandas_engine(tmp_path: Path) -> None:
    parquet_path = tmp_path / "leie_current.parquet"
    df = data_loaders.parse_leie_csv(write_leie_csv(tmp_path / "leie.csv"))

    data_loaders._write_dataframe_parquet(df, parquet_path, compression="zstd")
    reread = data_loaders._read_parquet_dataframe(parquet_path)

    assert list(reread["display_name"]) == ["Jane Q Smith", "Acme Health LLC"]


def test_screen_leie_candidates_rejects_tax_identifier_alias(
    leie_cache: Path,
) -> None:
    with pytest.raises(ValueError, match="does not accept"):
        data_loaders.screen_leie_candidates(
            [{"candidate_id": "1", "tax_identifier": "12-3456789"}],
        )
