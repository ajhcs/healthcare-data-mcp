from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import pytest

from shared.acquisition.ahrq_compendium_receipt import (
    AHRQ_HOSPITAL_LINKAGE_URL,
    AHRQ_SYSTEM_URL,
    DEFAULT_AHRQ_EXPECTATIONS,
    AhrqCompendiumExpectations,
    AhrqCompendiumSourceReceipt,
    AhrqResponseMetadata,
    build_ahrq_compendium_receipt,
    write_ahrq_compendium_receipt,
)


SYSTEM_COLUMNS = (
    "health_sys_id,health_sys_name,health_sys_city,health_sys_state,hosp_cnt,acutehosp_cnt,"
    "sys_beds,sys_ma_plan_contracts,sys_ma_plan_enroll"
)
HOSPITAL_COLUMNS = (
    "compendium_hospital_id,ccn,hospital_name,hospital_street,hospital_city,hospital_state,"
    "hospital_zip,acutehosp_flag,health_sys_id,health_sys_name,health_sys_city,health_sys_state,hos_beds"
)
REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


def _write_fixture(tmp_path: Path) -> tuple[Path, Path, AhrqCompendiumExpectations]:
    system_path = tmp_path / "ahrq_system_2023.csv"
    hospital_path = tmp_path / "ahrq_hospital_linkage_2023.csv"
    system_path.write_text(
        SYSTEM_COLUMNS
        + "\n1,Alpha,City A,AA,1,1,100,C1,1000\n2,Beta,City B,BB,1,1,200,C2,2000\n",
        encoding="cp1252",
    )
    hospital_path.write_text(
        HOSPITAL_COLUMNS
        + "\nH1,000001,Alpha Hospital,1 Main,City A,AA,00001,1,1,Alpha,City A,AA,100"
        + "\nH2,,Independent,2 Main,City B,BB,00002,1,,,,,50"
        + "\nH3,000002,Beta Hospital,3 Main,City B,BB,00003,1,2,Beta,City B,BB,200\n",
        encoding="cp1252",
    )
    expectations = AhrqCompendiumExpectations(
        system_checksum_sha256=hashlib.sha256(system_path.read_bytes()).hexdigest(),
        hospital_checksum_sha256=hashlib.sha256(hospital_path.read_bytes()).hexdigest(),
        system_content_length=system_path.stat().st_size,
        hospital_content_length=hospital_path.stat().st_size,
        system_schema_fingerprint=hashlib.sha256(SYSTEM_COLUMNS.encode()).hexdigest(),
        hospital_schema_fingerprint=hashlib.sha256(HOSPITAL_COLUMNS.encode()).hexdigest(),
        system_rows=2,
        unique_system_ids=2,
        system_jurisdictions=2,
        hospital_rows=3,
        unique_hospital_ids=3,
        linked_hospital_rows=2,
        unlinked_hospital_rows=1,
        nonblank_ccns=2,
        missing_ccns=1,
        hospital_jurisdictions=2,
    )
    return system_path, hospital_path, expectations


def _responses() -> dict[str, AhrqResponseMetadata]:
    return {
        "system_universe": AhrqResponseMetadata(AHRQ_SYSTEM_URL, 200, "text/csv; charset=windows-1252"),
        "hospital_linkage": AhrqResponseMetadata(AHRQ_HOSPITAL_LINKAGE_URL, 200, "text/csv"),
    }


def _all_keys(value: object) -> set[str]:
    if isinstance(value, dict):
        return {str(key) for key in value} | {
            child_key for child in value.values() for child_key in _all_keys(child)
        }
    if isinstance(value, list):
        return {child_key for child in value for child_key in _all_keys(child)}
    return set()


def test_build_and_write_ahrq_compendium_receipt_is_public_safe(tmp_path: Path) -> None:
    system_path, hospital_path, expectations = _write_fixture(tmp_path)
    receipt = build_ahrq_compendium_receipt(
        system_path,
        hospital_path,
        retrieved_at=datetime(2026, 8, 5, tzinfo=timezone.utc),
        responses=_responses(),
        expectations=expectations,
    )

    destination = tmp_path / "receipt.json"
    write_ahrq_compendium_receipt(destination, receipt)
    payload = json.loads(destination.read_text(encoding="utf-8"))

    assert payload["receipt_status"] == "admitted"
    assert [artifact["role"] for artifact in payload["artifacts"]] == [
        "system_universe",
        "hospital_linkage",
    ]
    assert payload["assertions"]["unlinked_hospital_rows"] == 1
    assert payload["assertions"]["missing_ccns"] == 1
    assert "path" not in _all_keys(payload)
    assert str(tmp_path) not in json.dumps(payload)


def test_receipt_rejects_byte_drift(tmp_path: Path) -> None:
    system_path, hospital_path, expectations = _write_fixture(tmp_path)
    system_path.write_bytes(system_path.read_bytes() + b"\n")

    with pytest.raises(ValueError, match="system checksum"):
        build_ahrq_compendium_receipt(
            system_path,
            hospital_path,
            retrieved_at=datetime(2026, 8, 5, tzinfo=timezone.utc),
            responses=_responses(),
            expectations=expectations,
        )


def test_receipt_rejects_missing_revised_system_columns(tmp_path: Path) -> None:
    system_path, hospital_path, expectations = _write_fixture(tmp_path)
    system_path.write_text(
        SYSTEM_COLUMNS.replace(",sys_ma_plan_enroll", "") + "\n1,Alpha,City A,AA,1,1,100,C1\n",
        encoding="cp1252",
    )

    with pytest.raises(ValueError, match="sys_ma_plan_enroll"):
        build_ahrq_compendium_receipt(
            system_path,
            hospital_path,
            retrieved_at=datetime(2026, 8, 5, tzinfo=timezone.utc),
            responses=_responses(),
            expectations=expectations,
        )


def test_receipt_rejects_orphan_system_link(tmp_path: Path) -> None:
    system_path, hospital_path, expectations = _write_fixture(tmp_path)
    hospital_path.write_text(
        hospital_path.read_text(encoding="cp1252").replace(",2,Beta,City B,BB,200", ",3,Beta,City B,BB,200"),
        encoding="cp1252",
    )
    changed = replace(
        expectations,
        hospital_checksum_sha256=hashlib.sha256(hospital_path.read_bytes()).hexdigest(),
        hospital_content_length=hospital_path.stat().st_size,
    )

    with pytest.raises(ValueError, match="orphan linked system IDs"):
        build_ahrq_compendium_receipt(
            system_path,
            hospital_path,
            retrieved_at=datetime(2026, 8, 5, tzinfo=timezone.utc),
            responses=_responses(),
            expectations=changed,
        )


def test_committed_live_receipt_matches_typed_contract() -> None:
    receipt_path = (
        REPOSITORY_ROOT
        / "contracts"
        / "source-receipts"
        / "ahrq-compendium-2023-revised.json"
    )
    receipt = AhrqCompendiumSourceReceipt.model_validate_json(
        receipt_path.read_text(encoding="utf-8")
    )

    assert receipt.receipt_status == "admitted"
    assert receipt.assertions.system_rows == 639
    assert receipt.assertions.hospital_rows == 6_800
    assert receipt.assertions.orphan_linked_system_ids == 0
    assert receipt.release_checksum_sha256 == (
        "sha256:dd2198b868fe990e3d93ae36a88f541f5b3a5c0f01703f63ef7a5e8d8519393b"
    )
    assert [artifact.role for artifact in receipt.artifacts] == [
        "system_universe",
        "hospital_linkage",
    ]
    assert receipt.artifacts[0].source_url == AHRQ_SYSTEM_URL
    assert receipt.artifacts[1].source_url == AHRQ_HOSPITAL_LINKAGE_URL
    assert receipt.artifacts[0].checksum_sha256 == (
        f"sha256:{DEFAULT_AHRQ_EXPECTATIONS.system_checksum_sha256}"
    )
    assert receipt.artifacts[1].checksum_sha256 == (
        f"sha256:{DEFAULT_AHRQ_EXPECTATIONS.hospital_checksum_sha256}"
    )
