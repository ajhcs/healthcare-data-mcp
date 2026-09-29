"""Exact, source-backed Form 990 facts stored in a small local SQLite database."""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
import ipaddress
import json
from pathlib import Path
import re
import sqlite3
from typing import Any, Iterable
from urllib.parse import urlparse

from lxml import etree


MAX_XML_BYTES = 25 * 1024 * 1024
ALLOWED_IRS_SOURCE_HOSTS = frozenset({"apps.irs.gov", "www.irs.gov", "irs.gov"})
COMPENSATION_DEFINITION = "Form 990 Part VII columns D + E + F"
AGGREGATE_PERSON_NAME = re.compile(
    r"\b(?:EMPLOYEES|OFFICERS|DIRECTORS|TRUSTEES)\b|\bSEE\s+(?:SCHEDULE|SCH)\b"
    r"|^(?:\d+|ONE|TWO|THREE|FOUR|FIVE|SIX|SEVEN|EIGHT|NINE|TEN)\s+"
    r".*(?:EMPLOYEE|OFFICER|DIRECTOR|TRUSTEE)",
    re.IGNORECASE,
)

FIELD_MAPPINGS: dict[str, tuple[str, tuple[str, ...], str]] = {
    "total_revenue": (
        "Total revenue",
        ("CYTotalRevenueAmt", "TotalRevenueCurrentYear", "TotalRevenueAmt"),
        "USD",
    ),
    "total_expenses": (
        "Total expenses",
        ("CYTotalExpensesAmt", "TotalFunctionalExpensesAmt", "TotalExpensesCurrentYear"),
        "USD",
    ),
    "net_assets": (
        "Net assets or fund balances, end of year",
        ("NetAssetsOrFundBalancesEOYAmt", "NetAssetsOrFundBalancesEndOfYear"),
        "USD",
    ),
}


@dataclass(frozen=True, slots=True)
class ExtractedFact:
    metric_key: str
    reported_label: str
    value: int | str | None
    units: str
    status: str
    xml_field_path: str
    xml_field_paths: tuple[str, ...] = ()
    details: dict[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class ExtractedFiling:
    legal_filer: str
    ein: str
    tax_period_end: str
    tax_year: int
    form_type: str
    xml_sha256: str
    facts: tuple[ExtractedFact, ...]


def normalize_ein(ein: str) -> str:
    normalized = re.sub(r"\D", "", str(ein))
    if len(normalized) != 9:
        raise ValueError("EIN must contain exactly 9 digits")
    return normalized


def validate_lan_bind_host(host: str) -> str:
    """Return a normalized bind host, rejecting public and wildcard addresses."""

    candidate = host.strip()
    if candidate.lower() == "localhost":
        return "127.0.0.1"
    try:
        address = ipaddress.ip_address(candidate)
    except ValueError as exc:
        raise ValueError("MCP_HOST must be a literal loopback or private LAN IP address") from exc
    if address.is_unspecified or address.is_multicast or not (address.is_loopback or address.is_private):
        raise ValueError("Form 990 facts HTTP transport is LAN-only; use a loopback or private LAN IP address")
    return str(address)


def _local_name(element: etree._Element) -> str:
    return etree.QName(element).localname


def _elements_named(root: etree._Element, names: Iterable[str]) -> Iterable[etree._Element]:
    wanted = set(names)
    for element in root.iter():
        if not isinstance(element.tag, str):
            continue
        qname = etree.QName(element)
        if qname.namespace == "http://www.irs.gov/efile" and qname.localname in wanted:
            yield element


def _first_element(root: etree._Element, *names: str) -> etree._Element | None:
    for name in names:
        element = next(iter(_elements_named(root, (name,))), None)
        if element is not None:
            return element
    return None


def _text(element: etree._Element | None) -> str:
    return (element.text or "").strip() if element is not None else ""


def _local_path(element: etree._Element) -> str:
    names: list[str] = []
    current: etree._Element | None = element
    while current is not None:
        if isinstance(current.tag, str):
            name = _local_name(current)
            parent = current.getparent()
            if parent is not None:
                peers = [child for child in parent if child.tag == current.tag]
                if len(peers) > 1:
                    name = f"{name}[{peers.index(current) + 1}]"
            names.append(name)
        current = current.getparent()
    return "/" + "/".join(reversed(names))


def _money_value(element: etree._Element | None) -> int | None:
    value = _text(element).replace(",", "")
    if not value:
        return None
    if not re.fullmatch(r"-?\d+", value):
        raise ValueError(f"Expected an integer monetary amount at {_local_path(element)}")
    return int(value)


def _official_source_url(source_url: str) -> str:
    parsed = urlparse(source_url)
    if parsed.scheme != "https" or (parsed.hostname or "").lower() not in ALLOWED_IRS_SOURCE_HOSTS:
        raise ValueError("source_url must be an official HTTPS IRS URL")
    return source_url


def extract_filing(xml_bytes: bytes) -> ExtractedFiling:
    """Extract the exact initial fact set from one official IRS e-file XML return."""

    if not xml_bytes:
        raise ValueError("XML filing is empty")
    if len(xml_bytes) > MAX_XML_BYTES:
        raise ValueError(f"XML filing exceeds {MAX_XML_BYTES} bytes")
    if b"<!DOCTYPE" in xml_bytes.upper():
        raise ValueError("XML document types are not accepted")

    parser = etree.XMLParser(resolve_entities=False, no_network=True, recover=False, huge_tree=False)
    root = etree.fromstring(xml_bytes, parser=parser)
    root_name = etree.QName(root)
    if root_name.localname != "Return" or root_name.namespace != "http://www.irs.gov/efile":
        raise ValueError("XML must use the official IRS e-file Return namespace")
    return_header = _first_element(root, "ReturnHeader")
    return_data = _first_element(root, "ReturnData")
    form = _first_element(return_data if return_data is not None else root, "IRS990")
    if return_header is None or return_data is None or form is None:
        raise ValueError("XML must contain ReturnHeader, ReturnData, and IRS990")

    filer = _first_element(return_header, "Filer")
    filer_root = filer if filer is not None else return_header
    ein = normalize_ein(_text(_first_element(filer_root, "EIN")))
    legal_filer = _text(
        _first_element(
            filer_root,
            "BusinessNameLine1Txt",
            "BusinessNameLine1",
            "BusinessName",
        )
    )
    tax_period_end = _text(_first_element(return_header, "TaxPeriodEndDt", "TaxPeriodEndDate"))
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", tax_period_end):
        raise ValueError("ReturnHeader must contain an ISO TaxPeriodEnd date")
    form_type = _text(_first_element(return_header, "ReturnTypeCd", "ReturnType"))
    if form_type != "990":
        raise ValueError(f"Expected Form 990 XML, received {form_type or 'unknown form type'}")
    if not legal_filer:
        raise ValueError("ReturnHeader filer legal name is missing")

    facts: list[ExtractedFact] = []
    for metric_key, (label, candidates, units) in FIELD_MAPPINGS.items():
        element = _first_element(form, *candidates)
        value = _money_value(element)
        facts.append(
            ExtractedFact(
                metric_key=metric_key,
                reported_label=label,
                value=value,
                units=units,
                status="reported" if value is not None else "not_reported",
                xml_field_path=_local_path(element) if element is not None else "",
                xml_field_paths=(_local_path(element),) if element is not None else (),
            )
        )

    executive = _extract_top_reported_executive(form)
    facts.append(executive)
    return ExtractedFiling(
        legal_filer=legal_filer,
        ein=ein,
        tax_period_end=tax_period_end,
        tax_year=int(tax_period_end[:4]),
        form_type=form_type,
        xml_sha256=sha256(xml_bytes).hexdigest(),
        facts=tuple(facts),
    )


def _extract_top_reported_executive(form: etree._Element) -> ExtractedFact:
    candidates: list[tuple[int, str, str, etree._Element, tuple[str, ...]]] = []
    group_names = ("Form990PartVIISectionAGrp", "Form990PartVIISectionAListGrp")
    for group in _elements_named(form, group_names):
        name = _text(_first_element(group, "PersonNm", "NamePerson"))
        if not name:
            continue
        if AGGREGATE_PERSON_NAME.search(name):
            continue
        is_executive = any(_text(_first_element(group, role_name)) for role_name in ("OfficerInd", "KeyEmployeeInd"))
        if not is_executive:
            continue

        comp_elements = tuple(
            element
            for field_name in (
                "ReportableCompFromOrgAmt",
                "ReportableCompFromRltdOrgAmt",
                "OtherCompensationAmt",
            )
            if (element := _first_element(group, field_name)) is not None
        )
        comp_values = tuple(_money_value(element) for element in comp_elements)
        if not comp_values or all(value is None for value in comp_values):
            continue
        total = sum(value or 0 for value in comp_values)
        title = _text(_first_element(group, "TitleTxt", "Title"))
        candidates.append((total, name, title, group, tuple(_local_path(element) for element in comp_elements)))

    if not candidates:
        return ExtractedFact(
            metric_key="top_reported_executive",
            reported_label="Total compensation (Part VII columns D + E + F)",
            value=None,
            units="USD",
            status="not_reported",
            xml_field_path="",
            details={
                "name": None,
                "title": None,
                "total_reported_compensation": None,
                "definition": COMPENSATION_DEFINITION,
            },
        )

    total, name, title, group, source_paths = max(candidates, key=lambda row: row[0])
    organization_compensation = _money_value(_first_element(group, "ReportableCompFromOrgAmt"))
    related_organization_compensation = _money_value(_first_element(group, "ReportableCompFromRltdOrgAmt"))
    other_compensation = _money_value(_first_element(group, "OtherCompensationAmt"))
    reportable_compensation = (
        (organization_compensation or 0) + (related_organization_compensation or 0)
        if organization_compensation is not None or related_organization_compensation is not None
        else None
    )
    return ExtractedFact(
        metric_key="top_reported_executive",
        reported_label="Total compensation (Part VII columns D + E + F)",
        value=total,
        units="USD",
        status="reported",
        xml_field_path=_local_path(group),
        xml_field_paths=source_paths,
        details={
            "name": name,
            "title": title,
            "total_reported_compensation": total,
            "organization_reportable_compensation": organization_compensation,
            "related_organization_reportable_compensation": related_organization_compensation,
            "other_compensation": other_compensation,
            "reportable_compensation": reportable_compensation,
            "officer": bool(_text(_first_element(group, "OfficerInd"))),
            "key_employee": bool(_text(_first_element(group, "KeyEmployeeInd"))),
            "definition": COMPENSATION_DEFINITION,
        },
    )


class Form990FactStore:
    """Operator-write, query-read store for exact Form 990 facts."""

    def __init__(self, database_path: str | Path):
        self.database_path = Path(database_path).expanduser()

    def _connect(self) -> sqlite3.Connection:
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.database_path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA journal_mode = WAL")
        self._create_schema(connection)
        return connection

    @staticmethod
    def _create_schema(connection: sqlite3.Connection) -> None:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS filings (
                id INTEGER PRIMARY KEY,
                legal_filer TEXT NOT NULL,
                ein TEXT NOT NULL,
                tax_period_end TEXT NOT NULL,
                tax_year INTEGER NOT NULL,
                form_type TEXT NOT NULL,
                source_url TEXT NOT NULL,
                source_member TEXT NOT NULL,
                xml_sha256 TEXT NOT NULL,
                object_id TEXT NOT NULL UNIQUE,
                filing_year INTEGER NOT NULL,
                return_id TEXT NOT NULL DEFAULT '',
                dln TEXT NOT NULL DEFAULT '',
                xml_batch_id TEXT NOT NULL DEFAULT '',
                ingested_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS filings_ein_year_idx ON filings(ein, tax_year);
            CREATE TABLE IF NOT EXISTS facts (
                filing_id INTEGER NOT NULL REFERENCES filings(id) ON DELETE CASCADE,
                metric_key TEXT NOT NULL,
                reported_label TEXT NOT NULL,
                value_text TEXT,
                units TEXT NOT NULL,
                status TEXT NOT NULL,
                xml_field_path TEXT NOT NULL,
                xml_field_paths_json TEXT NOT NULL,
                details_json TEXT NOT NULL,
                PRIMARY KEY (filing_id, metric_key)
            );
            """
        )

    def ingest_xml(
        self,
        xml_bytes: bytes,
        *,
        source_url: str,
        source_member: str,
        object_id: str,
        filing_year: int,
        return_id: str = "",
        dln: str = "",
        xml_batch_id: str = "",
        expected_ein: str | None = None,
        expected_tax_year: int | None = None,
    ) -> dict[str, Any]:
        source_url = _official_source_url(source_url)
        if not re.fullmatch(r"\d{18}", object_id):
            raise ValueError("object_id must be the IRS 18-digit OBJECT_ID")
        extracted = extract_filing(xml_bytes)
        if expected_ein is not None and extracted.ein != normalize_ein(expected_ein):
            raise ValueError(f"XML EIN {extracted.ein} does not match requested EIN")
        if expected_tax_year is not None and extracted.tax_year != int(expected_tax_year):
            raise ValueError(f"XML tax year {extracted.tax_year} does not match requested tax year")

        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO filings (
                    legal_filer, ein, tax_period_end, tax_year, form_type,
                    source_url, source_member, xml_sha256, object_id,
                    filing_year, return_id, dln, xml_batch_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(object_id) DO UPDATE SET
                    legal_filer = excluded.legal_filer,
                    ein = excluded.ein,
                    tax_period_end = excluded.tax_period_end,
                    tax_year = excluded.tax_year,
                    form_type = excluded.form_type,
                    source_url = excluded.source_url,
                    source_member = excluded.source_member,
                    xml_sha256 = excluded.xml_sha256,
                    filing_year = excluded.filing_year,
                    return_id = excluded.return_id,
                    dln = excluded.dln,
                    xml_batch_id = excluded.xml_batch_id
                """,
                (
                    extracted.legal_filer,
                    extracted.ein,
                    extracted.tax_period_end,
                    extracted.tax_year,
                    extracted.form_type,
                    source_url,
                    source_member,
                    extracted.xml_sha256,
                    object_id,
                    int(filing_year),
                    return_id,
                    dln,
                    xml_batch_id,
                ),
            )
            filing_id = int(
                connection.execute(
                    "SELECT id FROM filings WHERE object_id = ?",
                    (object_id,),
                ).fetchone()["id"]
            )
            connection.execute("DELETE FROM facts WHERE filing_id = ?", (filing_id,))
            connection.executemany(
                """
                INSERT INTO facts (
                    filing_id, metric_key, reported_label, value_text, units,
                    status, xml_field_path, xml_field_paths_json, details_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    (
                        filing_id,
                        fact.metric_key,
                        fact.reported_label,
                        None if fact.value is None else str(fact.value),
                        fact.units,
                        fact.status,
                        fact.xml_field_path,
                        json.dumps(fact.xml_field_paths),
                        json.dumps(fact.details or {}, sort_keys=True),
                    )
                    for fact in extracted.facts
                ),
            )

        return {
            "status": "ingested",
            "legal_filer": extracted.legal_filer,
            "ein": extracted.ein,
            "tax_year": extracted.tax_year,
            "object_id": object_id,
            "xml_sha256": extracted.xml_sha256,
            "facts_stored": len(extracted.facts),
        }

    def get_facts(self, *, ein: str, tax_year: int) -> dict[str, Any]:
        normalized_ein = normalize_ein(ein)
        with self._connect() as connection:
            filing = connection.execute(
                """
                SELECT * FROM filings
                WHERE ein = ? AND tax_year = ? AND form_type = '990'
                ORDER BY filing_year DESC, id DESC
                LIMIT 1
                """,
                (normalized_ein, int(tax_year)),
            ).fetchone()
            if filing is None:
                return {
                    "status": "not_found",
                    "query": {"ein": normalized_ein, "tax_year": int(tax_year)},
                    "facts": {},
                    "caveat": "No ingested official Form 990 filing matches this exact EIN and tax year.",
                }
            fact_rows = connection.execute(
                "SELECT * FROM facts WHERE filing_id = ? ORDER BY metric_key",
                (filing["id"],),
            ).fetchall()

        common_provenance = {
            "legal_filer": filing["legal_filer"],
            "ein": filing["ein"],
            "tax_period_end": filing["tax_period_end"],
            "form_type": filing["form_type"],
            "source_url": filing["source_url"],
            "source_member": filing["source_member"],
            "xml_sha256": filing["xml_sha256"],
            "object_id": filing["object_id"],
            "return_id": filing["return_id"],
            "dln": filing["dln"],
            "xml_batch_id": filing["xml_batch_id"],
        }
        facts: dict[str, Any] = {}
        fact_provenance: dict[str, Any] = {}
        fact_status: dict[str, str] = {}
        for row in fact_rows:
            provenance = {
                **common_provenance,
                "reported_label": row["reported_label"],
                "metric_key": row["metric_key"],
                "units": row["units"],
                "xml_field_path": row["xml_field_path"],
                "xml_field_paths": json.loads(row["xml_field_paths_json"]),
            }
            fact_provenance[row["metric_key"]] = provenance
            fact_status[row["metric_key"]] = row["status"]
            if row["metric_key"] == "top_reported_executive":
                details = json.loads(row["details_json"])
                facts[row["metric_key"]] = {
                    **details,
                    "units": row["units"],
                }
            else:
                facts[row["metric_key"]] = {
                    "value": int(row["value_text"]) if row["value_text"] is not None else None,
                    "units": row["units"],
                    "status": row["status"],
                    "provenance": provenance,
                }

        return {
            "status": "ready",
            "filing": {
                "legal_filer": filing["legal_filer"],
                "ein": filing["ein"],
                "tax_period_end": filing["tax_period_end"],
                "tax_year": filing["tax_year"],
                "form_type": filing["form_type"],
            },
            "facts": facts,
            "fact_provenance": fact_provenance,
            "fact_status": fact_status,
            "caveat": (
                "Facts are reported by this exact legal filer. No natural-brand resolution, "
                "affiliate roll-up, or cross-system comparison was performed."
            ),
        }
