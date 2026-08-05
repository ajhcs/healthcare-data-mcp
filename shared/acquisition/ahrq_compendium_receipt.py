"""Fail-closed source receipt for the revised 2023 AHRQ Compendium files."""

from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Literal, Mapping

from pydantic import BaseModel, ConfigDict, Field

from shared.utils.cache import write_atomic_json

AHRQ_DATASET_ID = "ahrq_health_system_compendium"
AHRQ_RELEASE_ID = "ahrq-compendium-2023-revised-2025-09"
AHRQ_LANDING_PAGE = "https://www.ahrq.gov/chsp/data-resources/compendium-2023.html"
AHRQ_SYSTEM_URL = (
    "https://www.ahrq.gov/sites/default/files/wysiwyg/chsp/compendium/"
    "chsp-compendium-2023-rev.csv"
)
AHRQ_HOSPITAL_LINKAGE_URL = (
    "https://www.ahrq.gov/sites/default/files/wysiwyg/chsp/compendium/"
    "chsp-hospital-linkage-2023.csv"
)
AHRQ_SYSTEM_TECHNICAL_DOCUMENTATION_URL = (
    "https://www.ahrq.gov/sites/default/files/wysiwyg/chsp/compendium/"
    "2023-compendium-techdoc-rev.pdf"
)
AHRQ_HOSPITAL_TECHNICAL_DOCUMENTATION_URL = (
    "https://www.ahrq.gov/sites/default/files/wysiwyg/chsp/compendium/"
    "2023-hospital-linkage-techdoc.pdf"
)

SYSTEM_RELATIVE_PATH = "ahrq_system_2023.csv"
HOSPITAL_RELATIVE_PATH = "ahrq_hospital_linkage_2023.csv"

_SYSTEM_REQUIRED_COLUMNS = frozenset(
    {
        "health_sys_id",
        "health_sys_name",
        "health_sys_city",
        "health_sys_state",
        "hosp_cnt",
        "acutehosp_cnt",
        "sys_beds",
        "sys_ma_plan_contracts",
        "sys_ma_plan_enroll",
    }
)
_HOSPITAL_REQUIRED_COLUMNS = frozenset(
    {
        "compendium_hospital_id",
        "ccn",
        "hospital_name",
        "hospital_street",
        "hospital_city",
        "hospital_state",
        "hospital_zip",
        "acutehosp_flag",
        "health_sys_id",
        "health_sys_name",
        "health_sys_city",
        "health_sys_state",
        "hos_beds",
    }
)


@dataclass(frozen=True, slots=True)
class AhrqArtifactSpec:
    role: Literal["system_universe", "hospital_linkage"]
    relative_path: str
    source_url: str
    required_columns: frozenset[str]


AHRQ_ARTIFACT_SPECS = (
    AhrqArtifactSpec(
        role="system_universe",
        relative_path=SYSTEM_RELATIVE_PATH,
        source_url=AHRQ_SYSTEM_URL,
        required_columns=_SYSTEM_REQUIRED_COLUMNS,
    ),
    AhrqArtifactSpec(
        role="hospital_linkage",
        relative_path=HOSPITAL_RELATIVE_PATH,
        source_url=AHRQ_HOSPITAL_LINKAGE_URL,
        required_columns=_HOSPITAL_REQUIRED_COLUMNS,
    ),
)


@dataclass(frozen=True, slots=True)
class AhrqResponseMetadata:
    """Public-safe response metadata captured by the browser acquisition path."""

    final_url: str
    status_code: int
    content_type: str
    etag: str = ""
    last_modified: str = ""


@dataclass(frozen=True, slots=True)
class AhrqCompendiumExpectations:
    system_checksum_sha256: str
    hospital_checksum_sha256: str
    system_content_length: int
    hospital_content_length: int
    system_schema_fingerprint: str
    hospital_schema_fingerprint: str
    system_rows: int
    unique_system_ids: int
    system_jurisdictions: int
    hospital_rows: int
    unique_hospital_ids: int
    linked_hospital_rows: int
    unlinked_hospital_rows: int
    nonblank_ccns: int
    missing_ccns: int
    hospital_jurisdictions: int


DEFAULT_AHRQ_EXPECTATIONS = AhrqCompendiumExpectations(
    system_checksum_sha256=(
        "7bd62db33d2241236c662afdbd0ff9b30032da817f5ec0a2326311f77c5371b6"  # pragma: allowlist secret
    ),
    hospital_checksum_sha256=(
        "a86146f10c8de626fea1da3a24b756e6a68165e449ae3687f1e90d6bdf129727"  # pragma: allowlist secret
    ),
    system_content_length=106_647,
    hospital_content_length=1_528_734,
    system_schema_fingerprint=(
        "65e32ad895ad8f21964650352978e59c23b3ee739268ab631a9ad487600f487f"  # pragma: allowlist secret
    ),
    hospital_schema_fingerprint=(
        "fec5c12e352c26259b8691b4fa9568f9c6e3f433b272c58e063b0dab97a4d634"  # pragma: allowlist secret
    ),
    system_rows=639,
    unique_system_ids=639,
    system_jurisdictions=51,
    hospital_rows=6_800,
    unique_hospital_ids=6_800,
    linked_hospital_rows=4_193,
    unlinked_hospital_rows=2_607,
    nonblank_ccns=6_676,
    missing_ccns=124,
    hospital_jurisdictions=51,
)


class AhrqArtifactReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    role: Literal["system_universe", "hospital_linkage"]
    relative_path: Literal["ahrq_system_2023.csv", "ahrq_hospital_linkage_2023.csv"]
    source_url: str = Field(pattern=r"^https://")
    final_url: str = Field(pattern=r"^https://")
    response_status: Literal[200]
    content_type: str
    etag: str | None = None
    last_modified: str | None = None
    retrieved_at: datetime
    checksum_sha256: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    content_length: int = Field(gt=0)
    row_count: int = Field(gt=0)
    schema_fingerprint: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    validation_status: Literal["pass"] = "pass"


class AhrqCompendiumAssertions(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    system_rows: int
    unique_system_ids: int
    system_jurisdictions: int
    hospital_rows: int
    unique_hospital_ids: int
    linked_hospital_rows: int
    unlinked_hospital_rows: int
    nonblank_ccns: int
    missing_ccns: int
    duplicate_nonblank_ccns: int
    hospital_jurisdictions: int
    linked_system_ids: int
    orphan_linked_system_ids: int


class AhrqCompendiumSourceReceipt(BaseModel):
    """Public-safe two-artifact receipt admitted for deterministic handoff."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["ushso.ahrq-compendium-source-receipt.v1"]
    dataset_id: Literal["ahrq_health_system_compendium"]
    release_id: Literal["ahrq-compendium-2023-revised-2025-09"]
    source_period: Literal["2023"]
    revision: Literal["2025-09"]
    landing_page: str = Field(pattern=r"^https://")
    parser_version: Literal["ahrq-compendium-receipt-v1"]
    retrieved_at: datetime
    release_checksum_sha256: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    receipt_status: Literal["admitted"]
    rights_classification: Literal["public_free"]
    rights_basis_url: str = Field(pattern=r"^https://")
    technical_documentation_urls: tuple[str, str]
    artifacts: tuple[AhrqArtifactReceipt, AhrqArtifactReceipt]
    assertions: AhrqCompendiumAssertions
    caveats: tuple[str, ...]


def build_ahrq_compendium_receipt(
    system_path: Path,
    hospital_path: Path,
    *,
    retrieved_at: datetime,
    responses: Mapping[str, AhrqResponseMetadata],
    expectations: AhrqCompendiumExpectations = DEFAULT_AHRQ_EXPECTATIONS,
) -> AhrqCompendiumSourceReceipt:
    """Validate both source artifacts as one release and build a safe receipt."""

    if retrieved_at.tzinfo is None or retrieved_at.utcoffset() is None:
        raise ValueError("retrieved_at must be timezone-aware")
    system_rows, system_columns = _read_rows(system_path)
    hospital_rows, hospital_columns = _read_rows(hospital_path)
    _require_columns("system", system_columns, _SYSTEM_REQUIRED_COLUMNS)
    _require_columns("hospital", hospital_columns, _HOSPITAL_REQUIRED_COLUMNS)

    system_metrics = _artifact_metrics(system_path, system_columns)
    hospital_metrics = _artifact_metrics(hospital_path, hospital_columns)
    _expect("system checksum", system_metrics["checksum"], expectations.system_checksum_sha256)
    _expect("hospital checksum", hospital_metrics["checksum"], expectations.hospital_checksum_sha256)
    _expect("system content length", system_metrics["content_length"], expectations.system_content_length)
    _expect("hospital content length", hospital_metrics["content_length"], expectations.hospital_content_length)
    _expect("system schema fingerprint", system_metrics["schema_fingerprint"], expectations.system_schema_fingerprint)
    _expect("hospital schema fingerprint", hospital_metrics["schema_fingerprint"], expectations.hospital_schema_fingerprint)

    assertions = _assertions(system_rows, hospital_rows)
    for field in (
        "system_rows",
        "unique_system_ids",
        "system_jurisdictions",
        "hospital_rows",
        "unique_hospital_ids",
        "linked_hospital_rows",
        "unlinked_hospital_rows",
        "nonblank_ccns",
        "missing_ccns",
        "hospital_jurisdictions",
    ):
        _expect(field.replace("_", " "), getattr(assertions, field), getattr(expectations, field))
    _expect("duplicate nonblank CCNs", assertions.duplicate_nonblank_ccns, 0)
    _expect("linked system IDs", assertions.linked_system_ids, assertions.unique_system_ids)
    _expect("orphan linked system IDs", assertions.orphan_linked_system_ids, 0)

    artifact_receipts = (
        _artifact_receipt(
            AHRQ_ARTIFACT_SPECS[0], system_metrics, len(system_rows), retrieved_at, responses
        ),
        _artifact_receipt(
            AHRQ_ARTIFACT_SPECS[1], hospital_metrics, len(hospital_rows), retrieved_at, responses
        ),
    )
    release_checksum = _release_checksum(artifact_receipts)
    return AhrqCompendiumSourceReceipt(
        schema_version="ushso.ahrq-compendium-source-receipt.v1",
        dataset_id=AHRQ_DATASET_ID,
        release_id=AHRQ_RELEASE_ID,
        source_period="2023",
        revision="2025-09",
        landing_page=AHRQ_LANDING_PAGE,
        parser_version="ahrq-compendium-receipt-v1",
        retrieved_at=retrieved_at,
        release_checksum_sha256=f"sha256:{release_checksum}",
        receipt_status="admitted",
        rights_classification="public_free",
        rights_basis_url=AHRQ_LANDING_PAGE,
        technical_documentation_urls=(
            AHRQ_SYSTEM_TECHNICAL_DOCUMENTATION_URL,
            AHRQ_HOSPITAL_TECHNICAL_DOCUMENTATION_URL,
        ),
        artifacts=artifact_receipts,
        assertions=assertions,
        caveats=(
            "AHRQ aggregates systems to the highest level of ownership; subsidiary systems may not appear separately.",
            "This receipt proves the revised 2023 source snapshot, not current ownership or cross-year entity continuity.",
            "Unlinked hospital rows and missing CCNs are preserved as source-declared missingness, not inferred affiliations.",
        ),
    )


def write_ahrq_compendium_receipt(path: Path, receipt: AhrqCompendiumSourceReceipt) -> None:
    """Write a stable receipt that contains no raw rows or local filesystem paths."""

    write_atomic_json(path, receipt.model_dump(mode="json"))


def _read_rows(path: Path) -> tuple[list[dict[str, str]], tuple[str, ...]]:
    if not path.is_file():
        raise ValueError(f"AHRQ artifact is missing: {path.name}")
    with path.open("r", encoding="cp1252", newline="") as handle:
        reader = csv.DictReader(handle)
        columns = tuple(str(column).strip() for column in (reader.fieldnames or ()))
        rows = [{str(key).strip(): str(value or "").strip() for key, value in row.items()} for row in reader]
    return rows, columns


def _require_columns(label: str, columns: tuple[str, ...], required: frozenset[str]) -> None:
    missing = sorted(required.difference(columns))
    if missing:
        raise ValueError(f"{label} artifact is missing required columns: {', '.join(missing)}")


def _artifact_metrics(path: Path, columns: tuple[str, ...]) -> dict[str, str | int]:
    content = path.read_bytes()
    header = ",".join(columns).encode("utf-8")
    return {
        "checksum": hashlib.sha256(content).hexdigest(),
        "content_length": len(content),
        "schema_fingerprint": hashlib.sha256(header).hexdigest(),
    }


def _assertions(
    system_rows: list[dict[str, str]], hospital_rows: list[dict[str, str]]
) -> AhrqCompendiumAssertions:
    system_ids = {row["health_sys_id"] for row in system_rows if row["health_sys_id"]}
    hospital_ids = {row["compendium_hospital_id"] for row in hospital_rows if row["compendium_hospital_id"]}
    linked_rows = [row for row in hospital_rows if row["health_sys_id"]]
    linked_system_ids = {row["health_sys_id"] for row in linked_rows}
    ccns = [row["ccn"] for row in hospital_rows if row["ccn"]]
    return AhrqCompendiumAssertions(
        system_rows=len(system_rows),
        unique_system_ids=len(system_ids),
        system_jurisdictions=len({row["health_sys_state"] for row in system_rows if row["health_sys_state"]}),
        hospital_rows=len(hospital_rows),
        unique_hospital_ids=len(hospital_ids),
        linked_hospital_rows=len(linked_rows),
        unlinked_hospital_rows=len(hospital_rows) - len(linked_rows),
        nonblank_ccns=len(ccns),
        missing_ccns=sum(not row["ccn"] for row in hospital_rows),
        duplicate_nonblank_ccns=len(ccns) - len(set(ccns)),
        hospital_jurisdictions=len({row["hospital_state"] for row in hospital_rows if row["hospital_state"]}),
        linked_system_ids=len(linked_system_ids),
        orphan_linked_system_ids=len(linked_system_ids.difference(system_ids)),
    )


def _artifact_receipt(
    spec: AhrqArtifactSpec,
    metrics: Mapping[str, str | int],
    row_count: int,
    retrieved_at: datetime,
    responses: Mapping[str, AhrqResponseMetadata],
) -> AhrqArtifactReceipt:
    try:
        response = responses[spec.role]
    except KeyError as exc:
        raise ValueError(f"Missing browser response metadata for {spec.role}") from exc
    _expect(f"{spec.role} response status", response.status_code, 200)
    _expect(f"{spec.role} final URL", response.final_url, spec.source_url)
    if "csv" not in response.content_type.casefold():
        raise ValueError(f"{spec.role} response content type is not CSV: {response.content_type}")
    return AhrqArtifactReceipt(
        role=spec.role,
        relative_path=spec.relative_path,
        source_url=spec.source_url,
        final_url=response.final_url,
        response_status=200,
        content_type=response.content_type,
        etag=response.etag or None,
        last_modified=response.last_modified or None,
        retrieved_at=retrieved_at,
        checksum_sha256=f"sha256:{metrics['checksum']}",
        content_length=int(metrics["content_length"]),
        row_count=row_count,
        schema_fingerprint=f"sha256:{metrics['schema_fingerprint']}",
    )


def _release_checksum(artifacts: tuple[AhrqArtifactReceipt, AhrqArtifactReceipt]) -> str:
    identity = {
        "release_id": AHRQ_RELEASE_ID,
        "artifacts": [
            {
                "role": artifact.role,
                "relative_path": artifact.relative_path,
                "checksum_sha256": artifact.checksum_sha256,
            }
            for artifact in artifacts
        ],
    }
    encoded = json.dumps(identity, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _expect(label: str, observed: object, expected: object) -> None:
    if observed != expected:
        raise ValueError(f"AHRQ receipt drift: {label} expected {expected!r}, observed {observed!r}")
