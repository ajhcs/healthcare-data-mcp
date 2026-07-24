"""Operator-side retrieval of official IRS e-file indexes and batch XML archives."""

from __future__ import annotations

from dataclasses import dataclass
import csv
from io import BytesIO, TextIOWrapper
from pathlib import Path
import subprocess
from zipfile import BadZipFile, ZipFile

from .fact_store import Form990FactStore, MAX_XML_BYTES, normalize_ein


IRS_EFILE_XML_BASE = "https://apps.irs.gov/pub/epostcard/990/xml"


@dataclass(frozen=True, slots=True)
class IrsEfileIndexEntry:
    return_id: str
    filing_year: int
    ein: str
    tax_period: str
    legal_filer: str
    form_type: str
    dln: str
    object_id: str
    xml_batch_id: str

    @property
    def index_url(self) -> str:
        return f"{IRS_EFILE_XML_BASE}/{self.filing_year}/index_{self.filing_year}.csv"

    @property
    def batch_url(self) -> str:
        return f"{IRS_EFILE_XML_BASE}/{self.filing_year}/{self.xml_batch_id.upper()}.zip"

    @property
    def candidate_xml_batch_ids(self) -> tuple[str, ...]:
        """Return bounded official archive parts for indexes that collapse split batches."""

        primary = self.xml_batch_id.upper()
        if primary.endswith("A"):
            return (primary, f"{primary[:-1]}B")
        return (primary,)

    @property
    def source_member(self) -> str:
        return f"{self.object_id}_public.xml"


def select_index_entry(
    index_csv: bytes,
    *,
    ein: str,
    tax_year: int,
    filing_year: int,
    object_id: str | None = None,
) -> IrsEfileIndexEntry:
    """Select one exact Form 990 row from an official annual IRS index."""

    normalized_ein = normalize_ein(ein)
    reader = csv.DictReader(TextIOWrapper(BytesIO(index_csv), encoding="utf-8-sig", newline=""))
    required_columns = {
        "RETURN_ID",
        "EIN",
        "TAX_PERIOD",
        "TAXPAYER_NAME",
        "RETURN_TYPE",
        "DLN",
        "OBJECT_ID",
        "XML_BATCH_ID",
    }
    if not reader.fieldnames or not required_columns.issubset(set(reader.fieldnames)):
        missing = sorted(required_columns - set(reader.fieldnames or ()))
        raise ValueError(f"IRS index is missing required columns: {', '.join(missing)}")

    matches: list[IrsEfileIndexEntry] = []
    for row in reader:
        row_ein = str(row.get("EIN", "")).strip().zfill(9)
        row_period = str(row.get("TAX_PERIOD", "")).strip()
        row_object_id = str(row.get("OBJECT_ID", "")).strip()
        if row_ein != normalized_ein:
            continue
        if not row_period.startswith(str(int(tax_year))):
            continue
        if str(row.get("RETURN_TYPE", "")).strip() != "990":
            continue
        if object_id is not None and row_object_id != object_id:
            continue
        matches.append(
            IrsEfileIndexEntry(
                return_id=str(row.get("RETURN_ID", "")).strip(),
                filing_year=int(filing_year),
                ein=row_ein,
                tax_period=row_period,
                legal_filer=str(row.get("TAXPAYER_NAME", "")).strip(),
                form_type="990",
                dln=str(row.get("DLN", "")).strip(),
                object_id=row_object_id,
                xml_batch_id=str(row.get("XML_BATCH_ID", "")).strip(),
            )
        )

    if not matches:
        scope = f"EIN {normalized_ein}, tax year {int(tax_year)}, filing year {int(filing_year)}"
        if object_id:
            scope += f", Object ID {object_id}"
        raise ValueError(f"No exact Form 990 row found for {scope}")
    if len(matches) > 1:
        ids = ", ".join(sorted(match.object_id for match in matches))
        raise ValueError(
            "The official index contains multiple Form 990 filings for this exact EIN/year; "
            f"pass an object_id explicitly. Candidates: {ids}"
        )
    entry = matches[0]
    if not entry.object_id.isdigit() or len(entry.object_id) != 18:
        raise ValueError("IRS index OBJECT_ID is not an 18-digit object identifier")
    if not entry.xml_batch_id:
        raise ValueError("IRS index XML_BATCH_ID is empty")
    return entry


def _read_unsupported_zip_member(
    archive_path: Path,
    member: str,
    expected_size: int,
) -> bytes:
    """Decode an official Deflate64 member using the host unzip utility."""

    try:
        completed = subprocess.run(
            ["unzip", "-p", str(archive_path), member],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=120,
        )
    except FileNotFoundError as exc:
        raise ValueError("Deflate64 IRS archives require the host unzip utility") from exc
    except subprocess.TimeoutExpired as exc:
        raise ValueError("Timed out decoding an IRS XML archive member") from exc
    if completed.returncode != 0:
        error = completed.stderr.decode("utf-8", errors="replace").strip()
        raise ValueError(f"Unable to decode IRS XML archive member: {error}")
    if len(completed.stdout) != expected_size:
        raise ValueError("Decoded IRS XML member size does not match the ZIP directory")
    return completed.stdout


def ingest_batch_member(
    store: Form990FactStore,
    *,
    entry: IrsEfileIndexEntry,
    archive_path: str | Path,
) -> dict:
    """Extract and ingest the selected XML member from one official IRS batch archive."""

    path = Path(archive_path)
    try:
        with ZipFile(path) as archive:
            member = next(
                (name for name in archive.namelist() if Path(name).name == entry.source_member),
                None,
            )
            if member is None:
                raise ValueError(f"IRS batch does not contain {entry.source_member}")
            member_info = archive.getinfo(member)
            if member_info.file_size > MAX_XML_BYTES:
                raise ValueError(f"IRS XML member exceeds {MAX_XML_BYTES} bytes")
            try:
                xml_bytes = archive.read(member)
            except NotImplementedError:
                xml_bytes = _read_unsupported_zip_member(path, member, member_info.file_size)
    except BadZipFile as exc:
        raise ValueError("IRS batch archive is not a valid ZIP file") from exc

    return store.ingest_xml(
        xml_bytes,
        source_url=entry.batch_url,
        source_member=entry.source_member,
        object_id=entry.object_id,
        filing_year=entry.filing_year,
        return_id=entry.return_id,
        dln=entry.dln,
        xml_batch_id=entry.xml_batch_id,
        expected_ein=entry.ein,
        expected_tax_year=int(entry.tax_period[:4]),
    )
