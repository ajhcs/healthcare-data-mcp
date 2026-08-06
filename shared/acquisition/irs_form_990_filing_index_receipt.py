"""Tamper-evident source receipt for bounded official IRS Form 990 index queries."""

from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Annotated, Literal, Mapping, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from shared.utils.cache import write_atomic_json

IRS_FORM_990_INDEX_DATASET_ID = "irs_form_990_e_file_index"
IRS_FORM_990_INDEX_SCHEMA_VERSION = "ushso.irs-form-990-filing-index-receipt.v1"
IRS_FORM_990_SCOPE_SCHEMA_VERSION = "ushso.irs-form-990-filer-query-scope.v1"
IRS_FORM_990_INDEX_BASE_URL = "https://apps.irs.gov/pub/epostcard/990/xml"
IRS_FORM_990_LANDING_PAGE = "https://www.irs.gov/charities-non-profits/form-990-series-downloads"
IRS_FORM_990_RIGHTS_BASIS_URL = "https://www.irs.gov/privacy-disclosure/tax-code-regulations-and-official-guidance"
IRS_FORM_990_TEOS_FAQ_URL = "https://www.irs.gov/charities-non-profits/tax-exempt-organization-search-teos-faqs"
IRS_FORM_990_DATA_DICTIONARY_URL = "https://www.irs.gov/pub/irs-tege/teos-data-dictionary-v2.csv"
IRS_FORM_990_RECEIPT_CAVEATS = (
    "Each row describes one exact legal filer and IRS return index entry, not a health system.",
    "An EIN or taxpayer name does not establish ownership, operation, affiliation, or enterprise continuity.",
    "Multiple same-period and amended returns are preserved without selecting an authoritative return.",
    "A not-found query means only that no supported row matched in the selected annual indexes.",
    "This receipt grants no financial comparability, Toolkit import, public projection, or release authority.",
)

INDEX_COLUMNS = (
    "RETURN_ID",
    "FILING_TYPE",
    "EIN",
    "TAX_PERIOD",
    "SUB_DATE",
    "TAXPAYER_NAME",
    "RETURN_TYPE",
    "DLN",
    "OBJECT_ID",
    "XML_BATCH_ID",
)
SUPPORTED_RETURN_TYPES = frozenset({"990", "990A"})
ObjectId = Annotated[str, Field(pattern=r"^[0-9]{18}$")]
OfficialIrsPageUrl = Annotated[str, Field(pattern=r"^https://www\.irs\.gov/[^\s]+$")]
OfficialIrsIndexUrl = Annotated[
    str,
    Field(
        pattern=(
            r"^https://apps\.irs\.gov/pub/epostcard/990/xml/"
            r"[0-9]{4}/index_[0-9]{4}\.csv$"
        )
    ),
]


def annual_index_url(filing_year: int) -> str:
    """Return the sole official annual index URL admitted by this contract."""

    return f"{IRS_FORM_990_INDEX_BASE_URL}/{filing_year}/index_{filing_year}.csv"


def annual_index_relative_path(filing_year: int) -> str:
    return f"irs_form_990_index_{filing_year}.csv"


def annual_index_artifact_id(filing_year: int) -> str:
    return f"irs-form-990-index-{filing_year}"


class IrsForm990FilerQuery(BaseModel):
    """One reviewed legal-filer query, never a health-system identity assertion."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    query_id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{7,127}$")
    ein: str = Field(pattern=r"^[0-9]{9}$")
    tax_period_year: int = Field(ge=2000, le=2100)
    filing_year: int = Field(ge=2000, le=2100)
    expected_object_id: ObjectId | None = None

    @model_validator(mode="after")
    def validate_years(self) -> Self:
        if self.filing_year < self.tax_period_year:
            raise ValueError("filing_year cannot precede tax_period_year")
        if self.filing_year > self.tax_period_year + 3:
            raise ValueError("filing_year is outside the bounded tax-period window")
        if self.expected_object_id and not self.expected_object_id.startswith(str(self.filing_year)):
            raise ValueError("expected_object_id must begin with filing_year")
        return self


class IrsForm990FilerQueryScope(BaseModel):
    """Reviewed pilot scope that grants source-query authority only."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["ushso.irs-form-990-filer-query-scope.v1"]
    scope_id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{7,127}$")
    scope_status: Literal["reviewed_source_query_scope"]
    authority: Literal["source_custody_only_not_system_identity_or_public_release"]
    reviewed_at: datetime
    source_basis: str = Field(min_length=20, max_length=500)
    queries: tuple[IrsForm990FilerQuery, ...] = Field(min_length=1)
    interpretation_limits: tuple[str, ...] = Field(min_length=3)

    @model_validator(mode="after")
    def validate_scope(self) -> Self:
        if self.reviewed_at.tzinfo is None or self.reviewed_at.utcoffset() is None:
            raise ValueError("reviewed_at must be timezone-aware")
        query_ids = [query.query_id for query in self.queries]
        if len(query_ids) != len(set(query_ids)):
            raise ValueError("query_id values must be unique")
        if query_ids != sorted(query_ids):
            raise ValueError("queries must be sorted by query_id")
        natural_keys = [(query.ein, query.tax_period_year, query.filing_year) for query in self.queries]
        if len(natural_keys) != len(set(natural_keys)):
            raise ValueError("exact EIN/tax-period/filing-year query keys must be unique")
        object_ids = [query.expected_object_id for query in self.queries if query.expected_object_id]
        if len(object_ids) != len(set(object_ids)):
            raise ValueError("expected_object_id anchors must be unique")
        return self


@dataclass(frozen=True, slots=True)
class IrsForm990ResponseMetadata:
    final_url: str
    status_code: int
    content_type: str
    retrieved_at: datetime
    etag: str = ""
    last_modified: str = ""


class IrsForm990IndexArtifactReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    artifact_id: str = Field(pattern=r"^irs-form-990-index-[0-9]{4}$")
    filing_year: int = Field(ge=2000, le=2100)
    relative_path: str = Field(pattern=r"^irs_form_990_index_[0-9]{4}\.csv$")
    source_url: OfficialIrsIndexUrl
    final_url: OfficialIrsIndexUrl
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

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        expected_url = annual_index_url(self.filing_year)
        if self.source_url != expected_url or self.final_url != expected_url:
            raise ValueError("source_url and final_url must equal the official annual index URL")
        if self.relative_path != annual_index_relative_path(self.filing_year):
            raise ValueError("relative_path does not match filing_year")
        if self.artifact_id != annual_index_artifact_id(self.filing_year):
            raise ValueError("artifact_id does not match filing_year")
        if self.retrieved_at.tzinfo is None or self.retrieved_at.utcoffset() is None:
            raise ValueError("artifact retrieved_at must be timezone-aware")
        if "csv" not in self.content_type.casefold():
            raise ValueError("annual index content type must identify CSV")
        return self


class IrsForm990IndexedFiling(BaseModel):
    """One source-native IRS index row selected by reviewed legal-filer queries."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    artifact_id: str = Field(pattern=r"^irs-form-990-index-[0-9]{4}$")
    filing_year: int = Field(ge=2000, le=2100)
    return_id: str | None = None
    filing_type: Literal["EFILE"]
    ein: str = Field(pattern=r"^[0-9]{9}$")
    tax_period: str = Field(pattern=r"^[0-9]{6}$")
    submission_date_raw: str | None = None
    taxpayer_name: str = Field(min_length=1, max_length=500)
    return_type: Literal["990", "990A"]
    amendment_status: Literal["original", "amended"]
    dln: str = Field(pattern=r"^[0-9]{14}$")
    object_id: ObjectId
    xml_batch_id: str = Field(pattern=r"^[0-9]{4}_TEOS_XML_[0-9]{2}[A-Za-z]$")
    canonical_row_sha256: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    query_ids: tuple[str, ...] = Field(min_length=1)

    @field_validator("return_id")
    @classmethod
    def validate_return_id(cls, value: str | None) -> str | None:
        if value is not None and (not value.isdigit() or len(value) > 32):
            raise ValueError("return_id must be digits when present")
        return value

    @field_validator("submission_date_raw")
    @classmethod
    def validate_submission_date_raw(cls, value: str | None) -> str | None:
        if value is not None and (not value.isdigit() or len(value) not in {4, 8}):
            raise ValueError("submission_date_raw must preserve a four- or eight-digit IRS value")
        return value

    @model_validator(mode="after")
    def validate_consistency(self) -> Self:
        if self.artifact_id != annual_index_artifact_id(self.filing_year):
            raise ValueError("filing artifact_id does not match filing_year")
        expected_status = "amended" if self.return_type == "990A" else "original"
        if self.amendment_status != expected_status:
            raise ValueError("amendment_status does not match return_type")
        if not self.object_id.startswith(str(self.filing_year)):
            raise ValueError("object_id must begin with filing_year")
        if len(self.query_ids) != len(set(self.query_ids)):
            raise ValueError("filing query_ids must be unique")
        return self


class IrsForm990QueryResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    query_id: str
    status: Literal["matched", "not_found_in_selected_annual_indexes"]
    expected_object_id: ObjectId | None = None
    matched_object_ids: tuple[ObjectId, ...]
    expected_object_id_present: bool

    @model_validator(mode="after")
    def validate_result(self) -> Self:
        if len(self.matched_object_ids) != len(set(self.matched_object_ids)):
            raise ValueError("matched_object_ids must be unique")
        if list(self.matched_object_ids) != sorted(self.matched_object_ids):
            raise ValueError("matched_object_ids must be sorted")
        if self.status == "matched" and not self.matched_object_ids:
            raise ValueError("matched query result must contain an Object ID")
        if self.status == "not_found_in_selected_annual_indexes" and self.matched_object_ids:
            raise ValueError("not-found query result cannot contain an Object ID")
        if self.expected_object_id is None and self.expected_object_id_present:
            raise ValueError("unanchored query cannot report an expected Object ID present")
        if self.expected_object_id is not None:
            observed = self.expected_object_id in self.matched_object_ids
            if observed != self.expected_object_id_present:
                raise ValueError("expected_object_id_present conflicts with matched Object IDs")
        return self


class IrsForm990IndexAssertions(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    artifact_count: int = Field(gt=0)
    query_count: int = Field(gt=0)
    matched_query_count: int = Field(ge=0)
    unmatched_query_count: int = Field(ge=0)
    selected_filing_count: int = Field(ge=0)
    unique_ein_count: int = Field(ge=0)
    original_return_count: int = Field(ge=0)
    amended_return_count: int = Field(ge=0)
    multi_filing_query_count: int = Field(ge=0)


class IrsForm990FilingIndexReceipt(BaseModel):
    """Public-safe receipt for bounded legal-filer index opportunities."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["ushso.irs-form-990-filing-index-receipt.v1"]
    dataset_id: Literal["irs_form_990_e_file_index"]
    release_id: str = Field(pattern=r"^irs-form-990-pilot-index-[0-9]{8}$")
    source_period: str = Field(pattern=r"^annual-index-years:[0-9]{4}(,[0-9]{4})*$")
    parser_version: Literal["irs-form-990-filing-index-receipt-v1"]
    retrieved_at: datetime
    receipt_status: Literal["admitted"]
    rights_classification: Literal["public_free"]
    landing_page: OfficialIrsPageUrl
    rights_basis_url: OfficialIrsPageUrl
    technical_documentation_urls: tuple[OfficialIrsPageUrl, OfficialIrsPageUrl]
    scope_id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{7,127}$")
    scope_checksum_sha256: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    release_checksum_sha256: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    artifacts: tuple[IrsForm990IndexArtifactReceipt, ...] = Field(min_length=1)
    filings: tuple[IrsForm990IndexedFiling, ...]
    query_results: tuple[IrsForm990QueryResult, ...] = Field(min_length=1)
    assertions: IrsForm990IndexAssertions
    caveats: tuple[str, ...] = Field(min_length=5, max_length=5)

    @model_validator(mode="after")
    def validate_graph(self) -> Self:
        if self.retrieved_at.tzinfo is None or self.retrieved_at.utcoffset() is None:
            raise ValueError("receipt retrieved_at must be timezone-aware")
        years = [artifact.filing_year for artifact in self.artifacts]
        if years != sorted(set(years)):
            raise ValueError("artifacts must be unique and sorted by filing_year")
        expected_retrieved_at = max(artifact.retrieved_at for artifact in self.artifacts)
        if self.retrieved_at != expected_retrieved_at:
            raise ValueError("receipt retrieved_at must equal the latest artifact retrieval time")
        if self.landing_page != IRS_FORM_990_LANDING_PAGE:
            raise ValueError("landing_page does not match the fixed IRS authority")
        if self.rights_basis_url != IRS_FORM_990_RIGHTS_BASIS_URL:
            raise ValueError("rights_basis_url does not match the fixed IRS authority")
        if self.technical_documentation_urls != (
            IRS_FORM_990_TEOS_FAQ_URL,
            IRS_FORM_990_DATA_DICTIONARY_URL,
        ):
            raise ValueError("technical documentation URLs do not match the fixed IRS authority")
        if self.caveats != IRS_FORM_990_RECEIPT_CAVEATS:
            raise ValueError("caveats do not match the fixed interpretation boundary")
        artifact_ids = {artifact.artifact_id for artifact in self.artifacts}
        if any(filing.artifact_id not in artifact_ids for filing in self.filings):
            raise ValueError("filing references an unknown artifact")
        object_ids = [filing.object_id for filing in self.filings]
        if len(object_ids) != len(set(object_ids)):
            raise ValueError("filing Object IDs must be unique")
        if object_ids != sorted(object_ids):
            raise ValueError("filings must be sorted by Object ID")
        filings_by_object = {filing.object_id: filing for filing in self.filings}
        query_ids = [result.query_id for result in self.query_results]
        if len(query_ids) != len(set(query_ids)):
            raise ValueError("query results must have unique query IDs")
        if query_ids != sorted(query_ids):
            raise ValueError("query results must be sorted by query ID")
        results_by_query = {result.query_id: result for result in self.query_results}
        object_id_set = set(object_ids)
        for result in self.query_results:
            if any(object_id not in object_id_set for object_id in result.matched_object_ids):
                raise ValueError("query result references an unknown filing Object ID")
        for filing in self.filings:
            if list(filing.query_ids) != sorted(filing.query_ids):
                raise ValueError("filing query_ids must be sorted")
            for query_id in filing.query_ids:
                result = results_by_query.get(query_id)
                if result is None:
                    raise ValueError("filing references an unknown query")
                if filing.object_id not in result.matched_object_ids:
                    raise ValueError("filing/query-result edges do not reconcile")
            if filing.canonical_row_sha256 != _canonical_row_hash(filing):
                raise ValueError("filing canonical row hash does not reconcile")
        for result in self.query_results:
            for object_id in result.matched_object_ids:
                filing = filings_by_object[object_id]
                if result.query_id not in filing.query_ids:
                    raise ValueError("query-result/filing edges do not reconcile")
        expected_assertions = _assertions(self.artifacts, self.filings, self.query_results)
        if self.assertions != expected_assertions:
            raise ValueError("receipt assertions do not reconcile")
        expected_source_period = "annual-index-years:" + ",".join(str(year) for year in years)
        if self.source_period != expected_source_period:
            raise ValueError("source_period does not match artifact years")
        expected_release_id = f"irs-form-990-pilot-index-{self.retrieved_at:%Y%m%d}"
        if self.release_id != expected_release_id:
            raise ValueError("release_id does not match receipt retrieval date")
        expected_release_checksum = _release_checksum(
            self.scope_id,
            self.scope_checksum_sha256.removeprefix("sha256:"),
            self.artifacts,
            self.filings,
            self.query_results,
        )
        if self.release_checksum_sha256 != f"sha256:{expected_release_checksum}":
            raise ValueError("release checksum does not reconcile")
        return self


def load_irs_form_990_query_scope(path: Path) -> IrsForm990FilerQueryScope:
    return IrsForm990FilerQueryScope.model_validate_json(path.read_text(encoding="utf-8"))


def build_irs_form_990_filing_index_receipt(
    artifact_paths: Mapping[int, Path],
    *,
    responses: Mapping[int, IrsForm990ResponseMetadata],
    scope: IrsForm990FilerQueryScope,
) -> IrsForm990FilingIndexReceipt:
    """Validate exact annual index bytes and build one deterministic receipt."""

    years = sorted({query.filing_year for query in scope.queries})
    if sorted(artifact_paths) != years or sorted(responses) != years:
        raise ValueError("artifact paths and response metadata must exactly cover scope filing years")
    rows_by_year: dict[int, list[dict[str, str]]] = {}
    artifacts: list[IrsForm990IndexArtifactReceipt] = []
    for year in years:
        rows, columns = _read_index(artifact_paths[year])
        _require_columns(columns)
        rows_by_year[year] = rows
        artifacts.append(_artifact_receipt(year, artifact_paths[year], columns, rows, responses[year]))

    filings_by_object: dict[str, IrsForm990IndexedFiling] = {}
    query_results: list[IrsForm990QueryResult] = []
    for query in sorted(scope.queries, key=lambda item: item.query_id):
        matching_rows = [
            row
            for row in rows_by_year[query.filing_year]
            if row["EIN"] == query.ein
            and row["TAX_PERIOD"].startswith(str(query.tax_period_year))
            and row["RETURN_TYPE"] in SUPPORTED_RETURN_TYPES
        ]
        matched_object_ids: list[str] = []
        for row in matching_rows:
            filing = _filing_from_row(row, query)
            existing = filings_by_object.get(filing.object_id)
            if existing is not None:
                if existing.model_dump(exclude={"query_ids"}) != filing.model_dump(exclude={"query_ids"}):
                    raise ValueError(f"conflicting canonical rows for Object ID {filing.object_id}")
                filing = existing.model_copy(update={"query_ids": tuple(sorted({*existing.query_ids, query.query_id}))})
            filings_by_object[filing.object_id] = filing
            matched_object_ids.append(filing.object_id)
        unique_ids = tuple(sorted(set(matched_object_ids)))
        expected_present = query.expected_object_id in unique_ids if query.expected_object_id else False
        if query.expected_object_id and not expected_present:
            raise ValueError(f"expected Object ID {query.expected_object_id} is missing for query {query.query_id}")
        query_results.append(
            IrsForm990QueryResult(
                query_id=query.query_id,
                status="matched" if unique_ids else "not_found_in_selected_annual_indexes",
                expected_object_id=query.expected_object_id,
                matched_object_ids=unique_ids,
                expected_object_id_present=expected_present,
            )
        )

    artifact_tuple = tuple(artifacts)
    filing_tuple = tuple(sorted(filings_by_object.values(), key=lambda item: item.object_id))
    result_tuple = tuple(query_results)
    scope_checksum = _sha256_json(scope.model_dump(mode="json"))
    retrieved_at = max(artifact.retrieved_at for artifact in artifact_tuple)
    release_checksum = _release_checksum(
        scope.scope_id,
        scope_checksum,
        artifact_tuple,
        filing_tuple,
        result_tuple,
    )
    return IrsForm990FilingIndexReceipt(
        schema_version=IRS_FORM_990_INDEX_SCHEMA_VERSION,
        dataset_id=IRS_FORM_990_INDEX_DATASET_ID,
        release_id=f"irs-form-990-pilot-index-{retrieved_at:%Y%m%d}",
        source_period="annual-index-years:" + ",".join(str(year) for year in years),
        parser_version="irs-form-990-filing-index-receipt-v1",
        retrieved_at=retrieved_at,
        receipt_status="admitted",
        rights_classification="public_free",
        landing_page=IRS_FORM_990_LANDING_PAGE,
        rights_basis_url=IRS_FORM_990_RIGHTS_BASIS_URL,
        technical_documentation_urls=(IRS_FORM_990_TEOS_FAQ_URL, IRS_FORM_990_DATA_DICTIONARY_URL),
        scope_id=scope.scope_id,
        scope_checksum_sha256=f"sha256:{scope_checksum}",
        release_checksum_sha256=f"sha256:{release_checksum}",
        artifacts=artifact_tuple,
        filings=filing_tuple,
        query_results=result_tuple,
        assertions=_assertions(artifact_tuple, filing_tuple, result_tuple),
        caveats=IRS_FORM_990_RECEIPT_CAVEATS,
    )


def validate_irs_form_990_filing_index_receipt(
    receipt: IrsForm990FilingIndexReceipt,
    artifact_paths: Mapping[int, Path],
    *,
    scope: IrsForm990FilerQueryScope,
) -> None:
    """Rebuild from frozen bytes and require exact logical equality."""

    responses = {
        artifact.filing_year: IrsForm990ResponseMetadata(
            final_url=artifact.final_url,
            status_code=artifact.response_status,
            content_type=artifact.content_type,
            retrieved_at=artifact.retrieved_at,
            etag=artifact.etag or "",
            last_modified=artifact.last_modified or "",
        )
        for artifact in receipt.artifacts
    }
    rebuilt = build_irs_form_990_filing_index_receipt(
        artifact_paths,
        responses=responses,
        scope=scope,
    )
    if rebuilt != receipt:
        raise ValueError("IRS Form 990 filing-index receipt drifted from frozen source bytes")


def write_irs_form_990_filing_index_receipt(path: Path, receipt: IrsForm990FilingIndexReceipt) -> None:
    write_atomic_json(path, receipt.model_dump(mode="json"))


def _read_index(path: Path) -> tuple[list[dict[str, str]], tuple[str, ...]]:
    if not path.is_file():
        raise ValueError(f"IRS annual index is missing: {path.name}")
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        columns = tuple(str(column).strip() for column in (reader.fieldnames or ()))
        rows: list[dict[str, str]] = []
        for row_number, row in enumerate(reader, start=2):
            if None in row or any(value is None for value in row.values()):
                raise ValueError(f"IRS annual index row {row_number} does not match the header width")
            rows.append({str(key).strip(): str(value).strip() for key, value in row.items()})
    if not rows:
        raise ValueError("IRS annual index is empty")
    return rows, columns


def _require_columns(columns: tuple[str, ...]) -> None:
    if columns != INDEX_COLUMNS:
        raise ValueError(f"IRS annual index schema drift: expected {INDEX_COLUMNS!r}, observed {columns!r}")


def _artifact_receipt(
    filing_year: int,
    path: Path,
    columns: tuple[str, ...],
    rows: list[dict[str, str]],
    response: IrsForm990ResponseMetadata,
) -> IrsForm990IndexArtifactReceipt:
    if response.status_code != 200:
        raise ValueError(f"IRS annual index response status was {response.status_code}")
    expected_url = annual_index_url(filing_year)
    if response.final_url != expected_url:
        raise ValueError("IRS annual index final URL drifted from the exact allowlist")
    content = path.read_bytes()
    return IrsForm990IndexArtifactReceipt(
        artifact_id=annual_index_artifact_id(filing_year),
        filing_year=filing_year,
        relative_path=annual_index_relative_path(filing_year),
        source_url=expected_url,
        final_url=response.final_url,
        response_status=200,
        content_type=response.content_type,
        etag=response.etag or None,
        last_modified=response.last_modified or None,
        retrieved_at=response.retrieved_at,
        checksum_sha256=f"sha256:{hashlib.sha256(content).hexdigest()}",
        content_length=len(content),
        row_count=len(rows),
        schema_fingerprint=f"sha256:{hashlib.sha256(','.join(columns).encode()).hexdigest()}",
    )


def _filing_from_row(row: Mapping[str, str], query: IrsForm990FilerQuery) -> IrsForm990IndexedFiling:
    raw = {column: str(row.get(column, "")).strip() for column in INDEX_COLUMNS}
    row_hash = _sha256_json(raw)
    return_type = raw["RETURN_TYPE"]
    return IrsForm990IndexedFiling(
        artifact_id=annual_index_artifact_id(query.filing_year),
        filing_year=query.filing_year,
        return_id=raw["RETURN_ID"] or None,
        filing_type=raw["FILING_TYPE"],
        ein=raw["EIN"],
        tax_period=raw["TAX_PERIOD"],
        submission_date_raw=raw["SUB_DATE"] or None,
        taxpayer_name=raw["TAXPAYER_NAME"],
        return_type=return_type,
        amendment_status="amended" if return_type == "990A" else "original",
        dln=raw["DLN"],
        object_id=raw["OBJECT_ID"],
        xml_batch_id=raw["XML_BATCH_ID"],
        canonical_row_sha256=f"sha256:{row_hash}",
        query_ids=(query.query_id,),
    )


def _canonical_row_hash(filing: IrsForm990IndexedFiling) -> str:
    raw = {
        "RETURN_ID": filing.return_id or "",
        "FILING_TYPE": filing.filing_type,
        "EIN": filing.ein,
        "TAX_PERIOD": filing.tax_period,
        "SUB_DATE": filing.submission_date_raw or "",
        "TAXPAYER_NAME": filing.taxpayer_name,
        "RETURN_TYPE": filing.return_type,
        "DLN": filing.dln,
        "OBJECT_ID": filing.object_id,
        "XML_BATCH_ID": filing.xml_batch_id,
    }
    return f"sha256:{_sha256_json(raw)}"


def _assertions(
    artifacts: tuple[IrsForm990IndexArtifactReceipt, ...],
    filings: tuple[IrsForm990IndexedFiling, ...],
    results: tuple[IrsForm990QueryResult, ...],
) -> IrsForm990IndexAssertions:
    return IrsForm990IndexAssertions(
        artifact_count=len(artifacts),
        query_count=len(results),
        matched_query_count=sum(result.status == "matched" for result in results),
        unmatched_query_count=sum(result.status != "matched" for result in results),
        selected_filing_count=len(filings),
        unique_ein_count=len({filing.ein for filing in filings}),
        original_return_count=sum(filing.return_type == "990" for filing in filings),
        amended_return_count=sum(filing.return_type == "990A" for filing in filings),
        multi_filing_query_count=sum(len(result.matched_object_ids) > 1 for result in results),
    )


def _release_checksum(
    scope_id: str,
    scope_checksum: str,
    artifacts: tuple[IrsForm990IndexArtifactReceipt, ...],
    filings: tuple[IrsForm990IndexedFiling, ...],
    results: tuple[IrsForm990QueryResult, ...],
) -> str:
    identity = {
        "schema_version": IRS_FORM_990_INDEX_SCHEMA_VERSION,
        "dataset_id": IRS_FORM_990_INDEX_DATASET_ID,
        "parser_version": "irs-form-990-filing-index-receipt-v1",
        "scope_id": scope_id,
        "scope_checksum_sha256": f"sha256:{scope_checksum}",
        "caveats": list(IRS_FORM_990_RECEIPT_CAVEATS),
        "artifacts": [artifact.model_dump(mode="json") for artifact in artifacts],
        "filings": [filing.model_dump(mode="json") for filing in filings],
        "query_results": [result.model_dump(mode="json") for result in results],
    }
    return _sha256_json(identity)


def _sha256_json(value: object) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


__all__ = [
    "INDEX_COLUMNS",
    "IRS_FORM_990_INDEX_SCHEMA_VERSION",
    "IRS_FORM_990_RECEIPT_CAVEATS",
    "IRS_FORM_990_SCOPE_SCHEMA_VERSION",
    "IrsForm990FilerQuery",
    "IrsForm990FilerQueryScope",
    "IrsForm990FilingIndexReceipt",
    "IrsForm990IndexArtifactReceipt",
    "IrsForm990IndexedFiling",
    "IrsForm990QueryResult",
    "IrsForm990ResponseMetadata",
    "annual_index_artifact_id",
    "annual_index_relative_path",
    "annual_index_url",
    "build_irs_form_990_filing_index_receipt",
    "load_irs_form_990_query_scope",
    "validate_irs_form_990_filing_index_receipt",
    "write_irs_form_990_filing_index_receipt",
]
