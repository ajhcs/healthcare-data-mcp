"""Tests for shared tabular source normalization helpers."""

from __future__ import annotations

from shared.utils.tabular_normalization import (
    normalize_tabular_columns,
    normalize_tabular_key,
    read_csv_strings,
)


def test_normalize_tabular_key_is_stable_for_source_columns() -> None:
    assert normalize_tabular_key(" Provider Name / Legal-Business ") == "provider_name_legal_business"
    assert normalize_tabular_key("NPI#") == "npi"
    assert normalize_tabular_columns(["A B", "State/Province", ""]) == ["a_b", "state_province", ""]


def test_read_csv_strings_preserves_identifiers_and_normalizes_columns(tmp_path) -> None:
    csv_path = tmp_path / "source.csv"
    csv_path.write_text("CCN,Provider Name,ZIP\n000123,Example Hospital,01234\n", encoding="utf-8")

    frame = read_csv_strings(csv_path, normalize_columns=True)

    assert list(frame.columns) == ["ccn", "provider_name", "zip"]
    assert frame.to_dict(orient="records") == [
        {"ccn": "000123", "provider_name": "Example Hospital", "zip": "01234"}
    ]
