from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from shared.acquisition.ahrq_compendium_historical import (
    HISTORICAL_AHRQ_RELEASES,
    SUPPORTED_AHRQ_RELEASE_KEYS,
    AhrqHistoricalExpectations,
    AhrqHistoricalSourceReceipt,
    build_ahrq_historical_receipt,
    get_historical_ahrq_release,
    write_ahrq_historical_receipt,
)
from shared.acquisition.ahrq_compendium_receipt import AhrqResponseMetadata


SYSTEM_COLUMNS = "health_sys_id,health_sys_name,health_sys_city,health_sys_state"
HOSPITAL_COLUMNS = (
    "compendium_hospital_id,ccn,hospital_name,hospital_street,hospital_city,hospital_state,"
    "hospital_zip,acutehosp_flag,health_sys_id,health_sys_name,health_sys_city,health_sys_state"
)
REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
RECEIPT_SCHEMA = (
    REPOSITORY_ROOT
    / "contracts"
    / "source-receipts"
    / "ahrq-compendium-historical-source-receipt-v1.schema.json"
)


def _write_fixture(tmp_path: Path) -> tuple[Path, Path]:
    system_path = tmp_path / "ahrq_system_2022.csv"
    hospital_path = tmp_path / "ahrq_hospital_linkage_2022.csv"
    system_path.write_text(
        SYSTEM_COLUMNS + "\n1,Alpha,City A,AA\n2,Beta,City B,BB\n",
        encoding="cp1252",
    )
    hospital_path.write_text(
        HOSPITAL_COLUMNS
        + "\nH1,000001,Alpha Hospital,1 Main,City A,AA,00001,1,1,Alpha,City A,AA"
        + "\nH2,,Independent,2 Main,City B,BB,00002,1,,,,,"
        + "\nH3,000002,Beta Hospital,3 Main,City B,BB,00003,1,2,Beta,City B,BB\n",
        encoding="cp1252",
    )
    return system_path, hospital_path


def _rows(path: Path) -> tuple[list[dict[str, str]], tuple[str, ...]]:
    with path.open("r", encoding="cp1252", newline="") as handle:
        reader = csv.DictReader(handle)
        columns = tuple(str(column).strip() for column in (reader.fieldnames or ()))
        rows = [
            {str(key).strip(): str(value or "").strip() for key, value in row.items()}
            for row in reader
        ]
    return rows, columns


def _expectations(system_path: Path, hospital_path: Path) -> AhrqHistoricalExpectations:
    system_rows, system_columns = _rows(system_path)
    hospital_rows, hospital_columns = _rows(hospital_path)
    system_id_values = [row["health_sys_id"] for row in system_rows if row["health_sys_id"]]
    system_ids = set(system_id_values)
    hospital_id_values = [
        row["compendium_hospital_id"]
        for row in hospital_rows
        if row["compendium_hospital_id"]
    ]
    linked_rows = [row for row in hospital_rows if row["health_sys_id"]]
    linked_system_ids = {row["health_sys_id"] for row in linked_rows}
    ccns = [row["ccn"] for row in hospital_rows if row["ccn"]]
    return AhrqHistoricalExpectations(
        system_checksum_sha256=hashlib.sha256(system_path.read_bytes()).hexdigest(),
        hospital_checksum_sha256=hashlib.sha256(hospital_path.read_bytes()).hexdigest(),
        system_content_length=system_path.stat().st_size,
        hospital_content_length=hospital_path.stat().st_size,
        system_schema_fingerprint=hashlib.sha256(",".join(system_columns).encode()).hexdigest(),
        hospital_schema_fingerprint=hashlib.sha256(",".join(hospital_columns).encode()).hexdigest(),
        system_rows=len(system_rows),
        unique_system_ids=len(system_ids),
        duplicate_system_ids=len(system_id_values) - len(system_ids),
        system_jurisdictions=len(
            {row["health_sys_state"] for row in system_rows if row["health_sys_state"]}
        ),
        hospital_rows=len(hospital_rows),
        unique_hospital_ids=len(set(hospital_id_values)),
        duplicate_hospital_ids=len(hospital_id_values) - len(set(hospital_id_values)),
        linked_hospital_rows=len(linked_rows),
        unlinked_hospital_rows=len(hospital_rows) - len(linked_rows),
        nonblank_ccns=len(ccns),
        missing_ccns=sum(not row["ccn"] for row in hospital_rows),
        duplicate_nonblank_ccns=len(ccns) - len(set(ccns)),
        hospital_jurisdictions=len(
            {row["hospital_state"] for row in hospital_rows if row["hospital_state"]}
        ),
        linked_system_ids=len(linked_system_ids),
        orphan_linked_system_ids=len(linked_system_ids.difference(system_ids)),
    )


def _install_fixture_expectations(
    monkeypatch: pytest.MonkeyPatch,
    system_path: Path,
    hospital_path: Path,
) -> None:
    spec = get_historical_ahrq_release("2022")
    monkeypatch.setitem(
        HISTORICAL_AHRQ_RELEASES,
        "2022",
        replace(spec, expectations=_expectations(system_path, hospital_path)),
    )


def _responses() -> dict[str, AhrqResponseMetadata]:
    spec = get_historical_ahrq_release("2022")
    return {
        artifact.role: AhrqResponseMetadata(
            artifact.source_url,
            200,
            "text/csv; charset=windows-1252",
        )
        for artifact in spec.artifacts
    }


def _all_keys(value: object) -> set[str]:
    if isinstance(value, dict):
        return {str(key) for key in value} | {
            child_key for child in value.values() for child_key in _all_keys(child)
        }
    if isinstance(value, list):
        return {child_key for child in value for child_key in _all_keys(child)}
    return set()


def test_release_registry_resolves_exact_supported_inventory() -> None:
    assert SUPPORTED_AHRQ_RELEASE_KEYS == (
        "2016",
        "2018",
        "2020",
        "2021",
        "2022",
        "2023-revised",
    )
    assert tuple(HISTORICAL_AHRQ_RELEASES) == SUPPORTED_AHRQ_RELEASE_KEYS[:-1]
    assert get_historical_ahrq_release("2018").expectations.system_rows == 637

    with pytest.raises(ValueError, match="Unsupported historical AHRQ release"):
        get_historical_ahrq_release("2019")


def test_build_and_write_historical_receipt_is_public_safe(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    system_path, hospital_path = _write_fixture(tmp_path)
    _install_fixture_expectations(monkeypatch, system_path, hospital_path)
    receipt = build_ahrq_historical_receipt(
        "2022",
        system_path,
        hospital_path,
        retrieved_at=datetime(2026, 8, 5, tzinfo=timezone.utc),
        responses=_responses(),
    )

    destination = tmp_path / "receipt.json"
    write_ahrq_historical_receipt(destination, receipt)
    payload = json.loads(destination.read_text(encoding="utf-8"))

    assert payload["matching_retrievals"] == 2
    assert payload["byte_equality_verified"] is True
    assert payload["assertions"]["linked_hospital_rows"] == 2
    assert payload["assertions"]["unlinked_hospital_rows"] == 1
    assert "path" not in _all_keys(payload)
    assert str(tmp_path) not in json.dumps(payload)


def test_historical_receipt_rejects_byte_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    system_path, hospital_path = _write_fixture(tmp_path)
    _install_fixture_expectations(monkeypatch, system_path, hospital_path)
    system_path.write_bytes(system_path.read_bytes() + b"\n")

    with pytest.raises(ValueError, match="system checksum"):
        build_ahrq_historical_receipt(
            "2022",
            system_path,
            hospital_path,
            retrieved_at=datetime(2026, 8, 5, tzinfo=timezone.utc),
            responses=_responses(),
        )


def test_historical_receipt_rejects_missing_identity_column(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    system_path, hospital_path = _write_fixture(tmp_path)
    _install_fixture_expectations(monkeypatch, system_path, hospital_path)
    system_path.write_text(
        SYSTEM_COLUMNS.replace(",health_sys_state", "") + "\n1,Alpha,City A\n",
        encoding="cp1252",
    )

    with pytest.raises(ValueError, match="health_sys_state"):
        build_ahrq_historical_receipt(
            "2022",
            system_path,
            hospital_path,
            retrieved_at=datetime(2026, 8, 5, tzinfo=timezone.utc),
            responses=_responses(),
        )


def test_historical_receipt_rejects_duplicate_source_id(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    system_path, hospital_path = _write_fixture(tmp_path)
    system_path.write_text(
        system_path.read_text(encoding="cp1252").replace("\n2,Beta", "\n1,Beta"),
        encoding="cp1252",
    )
    hospital_path.write_text(
        hospital_path.read_text(encoding="cp1252").replace(",2,Beta,City B,BB", ",1,Beta,City B,BB"),
        encoding="cp1252",
    )
    _install_fixture_expectations(monkeypatch, system_path, hospital_path)

    with pytest.raises(ValueError, match="duplicate system IDs"):
        build_ahrq_historical_receipt(
            "2022",
            system_path,
            hospital_path,
            retrieved_at=datetime(2026, 8, 5, tzinfo=timezone.utc),
            responses=_responses(),
        )


def test_historical_receipt_rejects_orphan_link(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    system_path, hospital_path = _write_fixture(tmp_path)
    hospital_path.write_text(
        hospital_path.read_text(encoding="cp1252").replace(",2,Beta,City B,BB", ",3,Beta,City B,BB"),
        encoding="cp1252",
    )
    _install_fixture_expectations(monkeypatch, system_path, hospital_path)

    with pytest.raises(ValueError, match="linked system coverage|orphan linked system IDs"):
        build_ahrq_historical_receipt(
            "2022",
            system_path,
            hospital_path,
            retrieved_at=datetime(2026, 8, 5, tzinfo=timezone.utc),
            responses=_responses(),
        )


def test_historical_receipt_rejects_final_url_substitution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    system_path, hospital_path = _write_fixture(tmp_path)
    _install_fixture_expectations(monkeypatch, system_path, hospital_path)
    responses = _responses()
    responses["system_universe"] = replace(
        responses["system_universe"],
        final_url="https://example.com/substituted.csv",
    )

    with pytest.raises(ValueError, match="final URL"):
        build_ahrq_historical_receipt(
            "2022",
            system_path,
            hospital_path,
            retrieved_at=datetime(2026, 8, 5, tzinfo=timezone.utc),
            responses=responses,
        )


def test_committed_historical_receipts_match_typed_contract() -> None:
    for release in SUPPORTED_AHRQ_RELEASE_KEYS[:-1]:
        receipt_path = (
            REPOSITORY_ROOT
            / "contracts"
            / "source-receipts"
            / f"ahrq-compendium-{release}.json"
        )
        receipt = AhrqHistoricalSourceReceipt.model_validate_json(
            receipt_path.read_text(encoding="utf-8")
        )
        spec = get_historical_ahrq_release(release)
        assert receipt.release_id == spec.release_id
        assert receipt.assertions.system_rows == spec.expectations.system_rows
        assert receipt.assertions.orphan_linked_system_ids == 0


def test_checked_in_historical_schema_matches_runtime_model() -> None:
    expected = json.dumps(
        AhrqHistoricalSourceReceipt.model_json_schema(),
        indent=2,
        sort_keys=True,
    ) + "\n"
    assert RECEIPT_SCHEMA.read_text(encoding="utf-8") == expected
    schema = json.loads(expected)

    for release in SUPPORTED_AHRQ_RELEASE_KEYS[:-1]:
        receipt_path = (
            REPOSITORY_ROOT
            / "contracts"
            / "source-receipts"
            / f"ahrq-compendium-{release}.json"
        )
        payload = json.loads(receipt_path.read_text(encoding="utf-8"))
        assert list(Draft202012Validator(schema).iter_errors(payload)) == []
