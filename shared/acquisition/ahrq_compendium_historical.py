"""Strict source receipts for historical AHRQ Compendium release pairs."""

from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Literal, Mapping, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from shared.acquisition.ahrq_compendium_receipt import (
    AHRQ_DATASET_ID,
    AhrqResponseMetadata,
)
from shared.utils.cache import write_atomic_json

HistoricalReleaseKey = Literal["2016", "2018", "2020", "2021", "2022"]
AhrqReleaseKey = Literal["2016", "2018", "2020", "2021", "2022", "2023-revised"]
HistoricalSourcePeriod = Literal["2016", "2018", "2020", "2021", "2022"]

DEFAULT_AHRQ_RELEASE_KEY: AhrqReleaseKey = "2023-revised"
HISTORICAL_AHRQ_RELEASE_KEYS: tuple[HistoricalReleaseKey, ...] = (
    "2016",
    "2018",
    "2020",
    "2021",
    "2022",
)
SUPPORTED_AHRQ_RELEASE_KEYS: tuple[AhrqReleaseKey, ...] = (
    *HISTORICAL_AHRQ_RELEASE_KEYS,
    DEFAULT_AHRQ_RELEASE_KEY,
)

_BASE_URL = "https://www.ahrq.gov"
_SYSTEM_REQUIRED_COLUMNS = frozenset(
    {"health_sys_id", "health_sys_name", "health_sys_city", "health_sys_state"}
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
    }
)


@dataclass(frozen=True, slots=True)
class AhrqHistoricalArtifactSpec:
    role: Literal["system_universe", "hospital_linkage"]
    relative_path: str
    source_url: str
    required_columns: frozenset[str]


@dataclass(frozen=True, slots=True)
class AhrqHistoricalExpectations:
    system_checksum_sha256: str
    hospital_checksum_sha256: str
    system_content_length: int
    hospital_content_length: int
    system_schema_fingerprint: str
    hospital_schema_fingerprint: str
    system_rows: int
    unique_system_ids: int
    duplicate_system_ids: int
    system_jurisdictions: int
    hospital_rows: int
    unique_hospital_ids: int
    duplicate_hospital_ids: int
    linked_hospital_rows: int
    unlinked_hospital_rows: int
    nonblank_ccns: int
    missing_ccns: int
    duplicate_nonblank_ccns: int
    hospital_jurisdictions: int
    linked_system_ids: int
    orphan_linked_system_ids: int


@dataclass(frozen=True, slots=True)
class AhrqHistoricalReleaseSpec:
    key: HistoricalReleaseKey
    release_id: str
    source_period: HistoricalSourcePeriod
    revision: str
    landing_page: str
    technical_documentation_urls: tuple[str, str]
    artifacts: tuple[AhrqHistoricalArtifactSpec, AhrqHistoricalArtifactSpec]
    expectations: AhrqHistoricalExpectations


def _artifact_specs(
    year: HistoricalReleaseKey,
    system_filename: str,
    hospital_filename: str,
) -> tuple[AhrqHistoricalArtifactSpec, AhrqHistoricalArtifactSpec]:
    return (
        AhrqHistoricalArtifactSpec(
            role="system_universe",
            relative_path=f"ahrq_system_{year}.csv",
            source_url=f"{_BASE_URL}/sites/default/files/wysiwyg/chsp/compendium/{system_filename}",
            required_columns=_SYSTEM_REQUIRED_COLUMNS,
        ),
        AhrqHistoricalArtifactSpec(
            role="hospital_linkage",
            relative_path=f"ahrq_hospital_linkage_{year}.csv",
            source_url=f"{_BASE_URL}/sites/default/files/wysiwyg/chsp/compendium/{hospital_filename}",
            required_columns=_HOSPITAL_REQUIRED_COLUMNS,
        ),
    )


HISTORICAL_AHRQ_RELEASES: Mapping[str, AhrqHistoricalReleaseSpec] = {
    "2016": AhrqHistoricalReleaseSpec(
        key="2016",
        release_id="ahrq-compendium-2016-updated-2019",
        source_period="2016",
        revision="2019-update",
        landing_page=f"{_BASE_URL}/chsp/data-resources/compendium-2016.html",
        technical_documentation_urls=(
            f"{_BASE_URL}/sites/default/files/wysiwyg/chsp/compendium/techdocrpt_0.pdf",
            f"{_BASE_URL}/sites/default/files/wysiwyg/chsp/compendium/chsp_linkage_file_tech_doc.pdf",
        ),
        artifacts=_artifact_specs(
            "2016",
            "chsp-compendium-2019update.csv",
            "compendium_system_hospital_linkage_file.csv",
        ),
        expectations=AhrqHistoricalExpectations(
            system_checksum_sha256="7c91da1047fd64ed3bd507c0dbb3f6deecd2eed8e8b3be46cd98902127e86dc6",
            hospital_checksum_sha256="e3aa46683e622a585cf481e0ebd797493793d5be73792a73f1defc4b377d3831",
            system_content_length=85_740,
            hospital_content_length=839_847,
            system_schema_fingerprint="8c1e366c46d7e5aacd38a2d25c9e66a77b88dfc06864b79f433a82b6d8d84b0c",
            hospital_schema_fingerprint="c68873ba7c9d96e5f1e10e6be5747027dd8b25531d6d92d910035869733b5287",
            system_rows=626,
            unique_system_ids=626,
            duplicate_system_ids=0,
            system_jurisdictions=50,
            hospital_rows=6_762,
            unique_hospital_ids=6_762,
            duplicate_hospital_ids=0,
            linked_hospital_rows=3_949,
            unlinked_hospital_rows=2_813,
            nonblank_ccns=6_214,
            missing_ccns=548,
            duplicate_nonblank_ccns=0,
            hospital_jurisdictions=51,
            linked_system_ids=626,
            orphan_linked_system_ids=0,
        ),
    ),
    "2018": AhrqHistoricalReleaseSpec(
        key="2018",
        release_id="ahrq-compendium-2018-updated-2021",
        source_period="2018",
        revision="2021-update",
        landing_page=f"{_BASE_URL}/chsp/data-resources/compendium-2018.html",
        technical_documentation_urls=(
            f"{_BASE_URL}/sites/default/files/wysiwyg/chsp/compendium/2018-Compendium-TechDoc-update.pdf",
            f"{_BASE_URL}/sites/default/files/wysiwyg/chsp/compendium/2018-hospital-linkage-techdoc-cx.pdf",
        ),
        artifacts=_artifact_specs(
            "2018",
            "chsp-compendium-2018-updated-2021.csv",
            "chsp-hospital-linkage-2018.csv",
        ),
        expectations=AhrqHistoricalExpectations(
            system_checksum_sha256="dd0431af44ba1ef4ebfbaa5efd07b672b8ee54b670d796b07b1b9a4a341a4b68",
            hospital_checksum_sha256="ee0b1befa2284d3a5904b2794585fd0f0f00409b919acb8eceabd4ede424e37d",
            system_content_length=91_842,
            hospital_content_length=808_537,
            system_schema_fingerprint="cc91b2cc8eca382fa425cce9d38c167678cc442f2def5b88e8070461cf1331b0",
            hospital_schema_fingerprint="c68873ba7c9d96e5f1e10e6be5747027dd8b25531d6d92d910035869733b5287",
            system_rows=637,
            unique_system_ids=637,
            duplicate_system_ids=0,
            system_jurisdictions=50,
            hospital_rows=6_742,
            unique_hospital_ids=6_742,
            duplicate_hospital_ids=0,
            linked_hospital_rows=3_887,
            unlinked_hospital_rows=2_855,
            nonblank_ccns=6_591,
            missing_ccns=151,
            duplicate_nonblank_ccns=1,
            hospital_jurisdictions=51,
            linked_system_ids=637,
            orphan_linked_system_ids=0,
        ),
    ),
    "2020": AhrqHistoricalReleaseSpec(
        key="2020",
        release_id="ahrq-compendium-2020-revised",
        source_period="2020",
        revision="revised",
        landing_page=f"{_BASE_URL}/chsp/data-resources/compendium-2020.html",
        technical_documentation_urls=(
            f"{_BASE_URL}/sites/default/files/wysiwyg/chsp/compendium/2020-Compendium-TechDoc-rev.pdf",
            f"{_BASE_URL}/sites/default/files/wysiwyg/2020-hospital-linkage-techdoc.pdf",
        ),
        artifacts=_artifact_specs(
            "2020",
            "chsp-compendium-2020-rev.csv",
            "chsp-hospital-linkage-2020-rev.csv",
        ),
        expectations=AhrqHistoricalExpectations(
            system_checksum_sha256="b2fc3ec3a52afe9219050fa20cb40694b89d13e9595ea9681636884d1e0db900",
            hospital_checksum_sha256="e4942069d3e0257948fc3a0b1fe0a9995e931b364ccdc5b17883b4a34094eae8",
            system_content_length=98_824,
            hospital_content_length=1_149_905,
            system_schema_fingerprint="aae898bc94ee4d530f8367b5f69c33b7bb2420e11cdd6646c6e06ae37f8d07c2",
            hospital_schema_fingerprint="b6e9e7b741a72ab89a41705b96beabb196621bda9172b6212ca47730e41ab682",
            system_rows=629,
            unique_system_ids=629,
            duplicate_system_ids=0,
            system_jurisdictions=51,
            hospital_rows=6_701,
            unique_hospital_ids=6_701,
            duplicate_hospital_ids=0,
            linked_hospital_rows=4_037,
            unlinked_hospital_rows=2_664,
            nonblank_ccns=6_574,
            missing_ccns=127,
            duplicate_nonblank_ccns=0,
            hospital_jurisdictions=51,
            linked_system_ids=629,
            orphan_linked_system_ids=0,
        ),
    ),
    "2021": AhrqHistoricalReleaseSpec(
        key="2021",
        release_id="ahrq-compendium-2021-revised",
        source_period="2021",
        revision="revised",
        landing_page=f"{_BASE_URL}/chsp/data-resources/compendium-2021.html",
        technical_documentation_urls=(
            f"{_BASE_URL}/sites/default/files/wysiwyg/chsp/compendium/2021-Compendium-TechDoc-rev.pdf",
            f"{_BASE_URL}/sites/default/files/wysiwyg/chsp/compendium/2021-hospital-linkage-techdoc-rev.pdf",
        ),
        artifacts=_artifact_specs(
            "2021",
            "chsp-compendium-2021-rev.csv",
            "chsp-hospital-linkage-2021-rev.csv",
        ),
        expectations=AhrqHistoricalExpectations(
            system_checksum_sha256="a10aeec32e004f3a97b88f249c920a89815f5ef7713967335311e92f62ac25f7",
            hospital_checksum_sha256="a8e3ce1a89b040cb528df4f705542968613ce34e6745cffb67307cd777e6c15d",
            system_content_length=99_816,
            hospital_content_length=1_155_382,
            system_schema_fingerprint="aae898bc94ee4d530f8367b5f69c33b7bb2420e11cdd6646c6e06ae37f8d07c2",
            hospital_schema_fingerprint="b6e9e7b741a72ab89a41705b96beabb196621bda9172b6212ca47730e41ab682",
            system_rows=635,
            unique_system_ids=635,
            duplicate_system_ids=0,
            system_jurisdictions=51,
            hospital_rows=6_725,
            unique_hospital_ids=6_725,
            duplicate_hospital_ids=0,
            linked_hospital_rows=4_073,
            unlinked_hospital_rows=2_652,
            nonblank_ccns=6_599,
            missing_ccns=126,
            duplicate_nonblank_ccns=0,
            hospital_jurisdictions=51,
            linked_system_ids=635,
            orphan_linked_system_ids=0,
        ),
    ),
    "2022": AhrqHistoricalReleaseSpec(
        key="2022",
        release_id="ahrq-compendium-2022-revised",
        source_period="2022",
        revision="revised",
        landing_page=f"{_BASE_URL}/chsp/data-resources/compendium-2022.html",
        technical_documentation_urls=(
            f"{_BASE_URL}/sites/default/files/wysiwyg/chsp/compendium/2022-Compendium-TechDoc-021224.pdf",
            f"{_BASE_URL}/sites/default/files/wysiwyg/chsp/compendium/2022-hospital-linkage-techdoc-021224.pdf",
        ),
        artifacts=_artifact_specs(
            "2022",
            "chsp-compendium-2022-rev.csv",
            "chsp-hospital-linkage-2022-rev.csv",
        ),
        expectations=AhrqHistoricalExpectations(
            system_checksum_sha256="9829b24d7773500762bcfd7189e8ef636c53c6338358b8132ccd0a9e61f5cb53",
            hospital_checksum_sha256="9809b2972f0db28983eb80b2d384bc75f21e66ff726dc818127ca4d99ad7bc62",
            system_content_length=100_642,
            hospital_content_length=1_162_828,
            system_schema_fingerprint="aae898bc94ee4d530f8367b5f69c33b7bb2420e11cdd6646c6e06ae37f8d07c2",
            hospital_schema_fingerprint="b6e9e7b741a72ab89a41705b96beabb196621bda9172b6212ca47730e41ab682",
            system_rows=640,
            unique_system_ids=640,
            duplicate_system_ids=0,
            system_jurisdictions=51,
            hospital_rows=6_764,
            unique_hospital_ids=6_764,
            duplicate_hospital_ids=0,
            linked_hospital_rows=4_173,
            unlinked_hospital_rows=2_591,
            nonblank_ccns=6_632,
            missing_ccns=132,
            duplicate_nonblank_ccns=0,
            hospital_jurisdictions=51,
            linked_system_ids=640,
            orphan_linked_system_ids=0,
        ),
    ),
}


def get_historical_ahrq_release(release: str) -> AhrqHistoricalReleaseSpec:
    """Resolve an approved historical release without accepting caller-supplied URLs."""

    try:
        return HISTORICAL_AHRQ_RELEASES[release]
    except KeyError as exc:
        supported = ", ".join(HISTORICAL_AHRQ_RELEASE_KEYS)
        raise ValueError(f"Unsupported historical AHRQ release {release!r}; choose one of: {supported}") from exc


class AhrqHistoricalArtifactReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    role: Literal["system_universe", "hospital_linkage"]
    relative_path: str = Field(pattern=r"^ahrq_(system|hospital_linkage)_20(16|18|20|21|22)\.csv$")
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


class AhrqHistoricalAssertions(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    system_rows: int = Field(gt=0)
    unique_system_ids: int = Field(gt=0)
    duplicate_system_ids: int = Field(ge=0)
    system_jurisdictions: int = Field(gt=0)
    hospital_rows: int = Field(gt=0)
    unique_hospital_ids: int = Field(gt=0)
    duplicate_hospital_ids: int = Field(ge=0)
    linked_hospital_rows: int = Field(ge=0)
    unlinked_hospital_rows: int = Field(ge=0)
    nonblank_ccns: int = Field(ge=0)
    missing_ccns: int = Field(ge=0)
    duplicate_nonblank_ccns: int = Field(ge=0)
    hospital_jurisdictions: int = Field(gt=0)
    linked_system_ids: int = Field(ge=0)
    orphan_linked_system_ids: int = Field(ge=0)


class AhrqHistoricalSourceReceipt(BaseModel):
    """Public-safe receipt bound to one reviewed historical AHRQ release."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["ushso.ahrq-compendium-historical-source-receipt.v1"]
    dataset_id: Literal["ahrq_health_system_compendium"]
    release_id: str
    source_period: HistoricalSourcePeriod
    revision: str
    landing_page: str = Field(pattern=r"^https://")
    parser_version: Literal["ahrq-compendium-historical-receipt-v1"]
    retrieved_at: datetime
    matching_retrievals: Literal[2]
    byte_equality_verified: Literal[True]
    release_checksum_sha256: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    receipt_status: Literal["admitted"]
    rights_classification: Literal["public_free"]
    rights_basis_url: str = Field(pattern=r"^https://")
    technical_documentation_urls: tuple[str, str]
    artifacts: tuple[AhrqHistoricalArtifactReceipt, AhrqHistoricalArtifactReceipt]
    assertions: AhrqHistoricalAssertions
    caveats: tuple[str, ...]

    @model_validator(mode="after")
    def bind_reviewed_release(self) -> Self:
        spec = get_historical_ahrq_release(self.source_period)
        _expect("release ID", self.release_id, spec.release_id)
        _expect("revision", self.revision, spec.revision)
        _expect("landing page", self.landing_page, spec.landing_page)
        _expect("rights basis URL", self.rights_basis_url, spec.landing_page)
        _expect(
            "technical documentation URLs",
            self.technical_documentation_urls,
            spec.technical_documentation_urls,
        )
        for artifact, artifact_spec in zip(self.artifacts, spec.artifacts, strict=True):
            _expect(f"{artifact.role} role", artifact.role, artifact_spec.role)
            _expect(f"{artifact.role} relative path", artifact.relative_path, artifact_spec.relative_path)
            _expect(f"{artifact.role} source URL", artifact.source_url, artifact_spec.source_url)
            _expect(f"{artifact.role} final URL", artifact.final_url, artifact_spec.source_url)
        return self


def build_ahrq_historical_receipt(
    release: str,
    system_path: Path,
    hospital_path: Path,
    *,
    retrieved_at: datetime,
    responses: Mapping[str, AhrqResponseMetadata],
) -> AhrqHistoricalSourceReceipt:
    """Validate a reviewed historical artifact pair and build its receipt."""

    if retrieved_at.tzinfo is None or retrieved_at.utcoffset() is None:
        raise ValueError("retrieved_at must be timezone-aware")
    spec = get_historical_ahrq_release(release)
    system_rows, system_columns = _read_rows(system_path)
    hospital_rows, hospital_columns = _read_rows(hospital_path)
    _require_columns("system", system_columns, spec.artifacts[0].required_columns)
    _require_columns("hospital", hospital_columns, spec.artifacts[1].required_columns)

    system_metrics = _artifact_metrics(system_path, system_columns)
    hospital_metrics = _artifact_metrics(hospital_path, hospital_columns)
    expectations = spec.expectations
    _expect("system checksum", system_metrics["checksum"], expectations.system_checksum_sha256)
    _expect("hospital checksum", hospital_metrics["checksum"], expectations.hospital_checksum_sha256)
    _expect("system content length", system_metrics["content_length"], expectations.system_content_length)
    _expect("hospital content length", hospital_metrics["content_length"], expectations.hospital_content_length)
    _expect(
        "system schema fingerprint",
        system_metrics["schema_fingerprint"],
        expectations.system_schema_fingerprint,
    )
    _expect(
        "hospital schema fingerprint",
        hospital_metrics["schema_fingerprint"],
        expectations.hospital_schema_fingerprint,
    )

    assertions = _assertions(system_rows, hospital_rows)
    for field_name in AhrqHistoricalAssertions.model_fields:
        _expect(
            field_name.replace("_", " "),
            getattr(assertions, field_name),
            getattr(expectations, field_name),
        )
    _expect("duplicate system IDs", assertions.duplicate_system_ids, 0)
    _expect("duplicate hospital IDs", assertions.duplicate_hospital_ids, 0)
    _expect("linked system coverage", assertions.linked_system_ids, assertions.unique_system_ids)
    _expect("orphan linked system IDs", assertions.orphan_linked_system_ids, 0)

    artifact_receipts = (
        _artifact_receipt(spec.artifacts[0], system_metrics, len(system_rows), retrieved_at, responses),
        _artifact_receipt(spec.artifacts[1], hospital_metrics, len(hospital_rows), retrieved_at, responses),
    )
    return AhrqHistoricalSourceReceipt(
        schema_version="ushso.ahrq-compendium-historical-source-receipt.v1",
        dataset_id=AHRQ_DATASET_ID,
        release_id=spec.release_id,
        source_period=spec.source_period,
        revision=spec.revision,
        landing_page=spec.landing_page,
        parser_version="ahrq-compendium-historical-receipt-v1",
        retrieved_at=retrieved_at,
        matching_retrievals=2,
        byte_equality_verified=True,
        release_checksum_sha256=f"sha256:{_release_checksum(spec, artifact_receipts)}",
        receipt_status="admitted",
        rights_classification="public_free",
        rights_basis_url=spec.landing_page,
        technical_documentation_urls=spec.technical_documentation_urls,
        artifacts=artifact_receipts,
        assertions=assertions,
        caveats=(
            "AHRQ aggregates systems to the highest level of ownership; subsidiary systems may not appear separately.",
            "This receipt proves one historical source snapshot, not current ownership or cross-year entity continuity.",
            "Source-year identifiers are not merged across releases; succession and continuing-enterprise claims require separate evidence.",
            "Unlinked hospital rows and missing CCNs are preserved as source-declared missingness, not inferred affiliations.",
        ),
    )


def write_ahrq_historical_receipt(path: Path, receipt: AhrqHistoricalSourceReceipt) -> None:
    """Write a stable historical receipt containing no raw rows or local paths."""

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
    system_rows: list[dict[str, str]],
    hospital_rows: list[dict[str, str]],
) -> AhrqHistoricalAssertions:
    system_id_values = [row["health_sys_id"] for row in system_rows if row["health_sys_id"]]
    system_ids = set(system_id_values)
    hospital_id_values = [
        row["compendium_hospital_id"] for row in hospital_rows if row["compendium_hospital_id"]
    ]
    linked_rows = [row for row in hospital_rows if row["health_sys_id"]]
    linked_system_ids = {row["health_sys_id"] for row in linked_rows}
    ccns = [row["ccn"] for row in hospital_rows if row["ccn"]]
    return AhrqHistoricalAssertions(
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


def _artifact_receipt(
    spec: AhrqHistoricalArtifactSpec,
    metrics: Mapping[str, str | int],
    row_count: int,
    retrieved_at: datetime,
    responses: Mapping[str, AhrqResponseMetadata],
) -> AhrqHistoricalArtifactReceipt:
    try:
        response = responses[spec.role]
    except KeyError as exc:
        raise ValueError(f"Missing browser response metadata for {spec.role}") from exc
    _expect(f"{spec.role} response status", response.status_code, 200)
    _expect(f"{spec.role} final URL", response.final_url, spec.source_url)
    if "csv" not in response.content_type.casefold():
        raise ValueError(f"{spec.role} response content type is not CSV: {response.content_type}")
    return AhrqHistoricalArtifactReceipt(
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


def _release_checksum(
    spec: AhrqHistoricalReleaseSpec,
    artifacts: tuple[AhrqHistoricalArtifactReceipt, AhrqHistoricalArtifactReceipt],
) -> str:
    identity = {
        "release_id": spec.release_id,
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
        raise ValueError(f"AHRQ historical receipt drift: {label} expected {expected!r}, observed {observed!r}")
