"""Shared public state-health data acquisition and query helpers.

This module is intentionally source/acquisition oriented. It does not model
market share; it fetches public artifacts, records provenance, and exposes
small query helpers that MCP servers can use to self-warm caches.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import re
from pathlib import Path
from typing import Any
from urllib.parse import urljoin
import zipfile

from bs4 import BeautifulSoup
import pandas as pd
from pypdf import PdfReader

from shared.utils.cache import write_atomic_bytes, write_atomic_dataframe_csv, write_atomic_json, write_atomic_parquet
from shared.utils.http_client import resilient_request


DEFAULT_CACHE_ROOT = Path.home() / ".healthcare-data-mcp" / "cache"
STATE_HEALTH_CACHE = DEFAULT_CACHE_ROOT / "state-health-data"
PHC4_CACHE = STATE_HEALTH_CACHE / "phc4"
PA_DOH_EXTRACT_CACHE = STATE_HEALTH_CACHE / "pa-doh-hospital-extract"
PUBLIC_RECORDS_CACHE = DEFAULT_CACHE_ROOT / "public-records"

PA_HOSPITAL_REPORTS_URL = "https://www.pa.gov/agencies/health/health-statistics/health-facilities/hospital-reports.html"
NJ_HOSPITAL_FINANCIAL_URL = "https://www.nj.gov/health/hcf/financial-reports/"
NJ_CHARITY_CARE_URL = "https://www.nj.gov/health/charitycare/subsidy-reports/"
DE_HOSPITAL_DISCHARGE_URL = "https://dhss.delaware.gov/dph/hp/hosp_dis_data/"
PHC4_REPORT_LIBRARY_URL = "https://www.phc4.org/reports-library/"
AHRQ_HFMD_URL = "https://www.ahrq.gov/data/innovations/hfmd.html"
AHRQ_HFMD_CSV_ZIP_URL = "https://www.ahrq.gov/sites/default/files/wysiwyg/data/hfmd_2016_2019_puf_csv.zip"

_PUBLIC_WEB_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

US_STATE_ABBREVIATIONS: tuple[str, ...] = (
    "AL",
    "AK",
    "AZ",
    "AR",
    "CA",
    "CO",
    "CT",
    "DE",
    "FL",
    "GA",
    "HI",
    "ID",
    "IL",
    "IN",
    "IA",
    "KS",
    "KY",
    "LA",
    "ME",
    "MD",
    "MA",
    "MI",
    "MN",
    "MS",
    "MO",
    "MT",
    "NE",
    "NV",
    "NH",
    "NJ",
    "NM",
    "NY",
    "NC",
    "ND",
    "OH",
    "OK",
    "OR",
    "PA",
    "RI",
    "SC",
    "SD",
    "TN",
    "TX",
    "UT",
    "VT",
    "VA",
    "WA",
    "WV",
    "WI",
    "WY",
)
STATE_SPECIFIC_PUBLIC_HOSPITAL_STATES: tuple[str, ...] = ("DE", "NJ", "PA")
NATIONAL_PUBLIC_BACKBONE_SOURCES: tuple[str, ...] = (
    "cms_hospital_general_info",
    "cms_hospital_quality",
    "cms_hsaf",
    "cms_geographic_variation",
    "cms_provider_enrollment",
    "cdc_places",
    "census_acs",
)


@dataclass(frozen=True)
class SourceStatus:
    source_id: str
    source_name: str
    source_url: str
    status: str
    cache_path: str = ""
    record_count: int = 0
    artifact_count: int = 0
    acquired_at: str = ""
    reason: str = ""
    next_step: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _cache_root(cache_root: Path | None = None) -> Path:
    return (cache_root or DEFAULT_CACHE_ROOT).expanduser()


def _slug(value: str, fallback: str = "artifact") -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug[:120] or fallback


def _text(value: object) -> str:
    return "" if value is None else str(value).strip()


def _infer_publication_year(text: str) -> int | None:
    match = re.search(r"\b(20\d{2}|19\d{2})\b", text)
    return int(match.group(1)) if match else None


def _infer_publication_date(text: str) -> str:
    year = _infer_publication_year(text)
    return str(year) if year else ""


def _artifact_type(url: str, title: str = "") -> str:
    suffix = Path(url.split("?", 1)[0]).suffix.lower().lstrip(".")
    if suffix:
        return suffix
    lower = f"{title} {url}".lower()
    for token in ("pdf", "xlsx", "xls", "csv", "zip", "html"):
        if token in lower:
            return token
    return "html"


def _state_for_source(source_id: str) -> str:
    if source_id.startswith("pa_") or source_id == "phc4_public_reports":
        return "PA"
    if source_id.startswith("nj_"):
        return "NJ"
    if source_id.startswith("de_"):
        return "DE"
    return ""


def source_coverage_summary() -> dict[str, Any]:
    """Describe national versus state-specific acquisition coverage."""

    state_specific = set(STATE_SPECIFIC_PUBLIC_HOSPITAL_STATES)
    return {
        "national_state_count": len(US_STATE_ABBREVIATIONS),
        "national_states": list(US_STATE_ABBREVIATIONS),
        "national_backbone_sources": list(NATIONAL_PUBLIC_BACKBONE_SOURCES),
        "state_specific_public_hospital_states": list(STATE_SPECIFIC_PUBLIC_HOSPITAL_STATES),
        "state_specific_public_hospital_state_count": len(STATE_SPECIFIC_PUBLIC_HOSPITAL_STATES),
        "state_specific_public_hospital_missing_states": [
            state for state in US_STATE_ABBREVIATIONS if state not in state_specific
        ],
        "coverage_note": (
            "National CMS, CDC PLACES, Census, HSAF, and provider-enrollment sources cover all 50 states when acquired or queried. "
            "PA/NJ/DE indexes are state-specific public hospital-report enhancements, not the national hospital/county coverage boundary."
        ),
    }


def _normalized_artifact_record(
    *,
    source_id: str,
    source_name: str,
    source_url: str,
    title: str,
    artifact_url: str,
    landing_page_url: str = "",
    state: str = "",
    publication_date: str = "",
    publication_year: int | None = None,
    artifact_type: str = "",
) -> dict[str, Any]:
    text = f"{title} {artifact_url}"
    year = publication_year if publication_year is not None else _infer_publication_year(text)
    pub_date = publication_date or _infer_publication_date(text)
    kind = artifact_type or _artifact_type(artifact_url, title)
    return {
        "state": state or _state_for_source(source_id),
        "source": source_name,
        "source_id": source_id,
        "source_name": source_name,
        "source_url": source_url,
        "title": title or Path(artifact_url.split("?", 1)[0]).name,
        "artifact_url": artifact_url,
        "url": artifact_url,
        "landing_page_url": landing_page_url or source_url,
        "publication_date": pub_date,
        "publication_year": year,
        "year": year,
        "artifact_type": kind,
        "type": kind,
        "cached_path": "",
    }


def _write_artifact_indexes(
    cache_dir: Path,
    *,
    source_id: str,
    source_name: str,
    source_url: str,
    artifacts: list[dict[str, Any]],
) -> Path:
    index_path = cache_dir / "artifact_index.json"
    metadata_path = cache_dir / "artifact_metadata.csv"
    payload = {
        "source_id": source_id,
        "source_name": source_name,
        "source_url": source_url,
        "landing_page_url": source_url,
        "acquired_at": _now(),
        "artifact_count": len(artifacts),
        "artifacts": artifacts,
    }
    write_atomic_json(index_path, payload)
    metadata_columns = [
        "state",
        "source",
        "source_id",
        "source_url",
        "title",
        "artifact_url",
        "landing_page_url",
        "publication_date",
        "publication_year",
        "year",
        "artifact_type",
        "type",
        "cached_path",
        "download_error",
    ]
    write_atomic_dataframe_csv(metadata_path, pd.DataFrame(artifacts).reindex(columns=metadata_columns), index=False)
    return index_path


async def _download(url: str, target: Path, *, force: bool = False) -> bool:
    if target.exists() and not force:
        return False
    target.parent.mkdir(parents=True, exist_ok=True)
    resp = await resilient_request(
        "GET",
        url,
        timeout=300.0,
        follow_redirects=True,
        headers=_PUBLIC_WEB_HEADERS,
    )
    write_atomic_bytes(target, resp.content)
    return True


async def _scrape_artifact_links(
    source_id: str,
    source_name: str,
    source_url: str,
    cache_dir: Path,
    *,
    force: bool = False,
    cache_artifacts: bool = True,
) -> SourceStatus:
    cache_dir.mkdir(parents=True, exist_ok=True)
    html_path = cache_dir / "source.html"

    try:
        resp = await resilient_request(
            "GET",
            source_url,
            timeout=60.0,
            follow_redirects=True,
            headers=_PUBLIC_WEB_HEADERS,
        )
    except Exception as exc:
        index_path = _write_artifact_indexes(
            cache_dir,
            source_id=source_id,
            source_name=source_name,
            source_url=source_url,
            artifacts=[],
        )
        return SourceStatus(
            source_id=source_id,
            source_name=source_name,
            source_url=source_url,
            status="not_automatable",
            cache_path=str(index_path),
            acquired_at=_now(),
            reason=f"source_request_failed: {exc}",
            next_step="Use the source website manually or provide a stable public artifact URL if one becomes available.",
        )
    write_atomic_bytes(html_path, resp.content)
    soup = BeautifulSoup(resp.text, "html.parser")

    artifacts: list[dict[str, Any]] = []
    for link in soup.find_all("a"):
        href = _text(link.get("href"))
        label = " ".join(link.get_text(" ", strip=True).split())
        if not href:
            continue
        absolute = urljoin(source_url, href)
        lower_url = absolute.lower()
        lower_label = label.lower()
        if not any(ext in lower_url for ext in (".pdf", ".xls", ".xlsx", ".csv", ".zip", ".html", ".htm")):
            continue
        if source_id.startswith("pa_") and "hospital" not in lower_url + lower_label:
            continue
        if source_id == "de_hospital_discharge":
            discharge_tokens = (
                "porigin",
                "change",
                "mshare",
                "distrn",
                "utilization",
                "hospital",
                "discharge",
            )
            if not any(token in lower_url + lower_label for token in discharge_tokens):
                continue
            if absolute.rstrip("/") == source_url.rstrip("/"):
                continue
        artifacts.append(
            _normalized_artifact_record(
                source_id=source_id,
                source_name=source_name,
                source_url=source_url,
                title=label or Path(absolute).name,
                artifact_url=absolute,
                landing_page_url=source_url,
            )
        )

    # De-duplicate while preserving order.
    seen: set[str] = set()
    unique: list[dict[str, Any]] = []
    for artifact in artifacts:
        if artifact["artifact_url"] in seen:
            continue
        seen.add(artifact["artifact_url"])
        if cache_artifacts:
            suffix = Path(artifact["artifact_url"].split("?", 1)[0]).suffix or ".html"
            url_stem = Path(artifact["artifact_url"].split("?", 1)[0]).stem
            target = cache_dir / "artifacts" / f"{_slug(artifact['title'])}-{_slug(url_stem)}{suffix}"
            try:
                await _download(artifact["artifact_url"], target, force=force)
                artifact["cached_path"] = str(target)
            except Exception as exc:
                artifact["download_error"] = str(exc)
        unique.append(artifact)

    index_path = _write_artifact_indexes(
        cache_dir,
        source_id=source_id,
        source_name=source_name,
        source_url=source_url,
        artifacts=unique,
    )
    return SourceStatus(
        source_id=source_id,
        source_name=source_name,
        source_url=source_url,
        status="ready" if unique else "empty",
        cache_path=str(index_path),
        artifact_count=len(unique),
        acquired_at=_now(),
        reason="" if unique else "no_download_links_discovered",
    )


async def acquire_pa_hospital_reports(cache_root: Path | None = None, *, force: bool = False) -> SourceStatus:  # noqa: ARG001
    status = await _scrape_artifact_links(
        "pa_hospital_reports",
        "Pennsylvania DOH Hospital Reports",
        PA_HOSPITAL_REPORTS_URL,
        _cache_root(cache_root) / "state-health-data" / "pa-hospital-reports",
        force=force,
    )
    normalized_count = normalize_pa_doh_hospital_extract(cache_root)
    status_dict = status.to_dict()
    status_dict["record_count"] = normalized_count
    return SourceStatus(**{key: status_dict[key] for key in SourceStatus.__dataclass_fields__ if key in status_dict})


def _pa_doh_extract_cache(cache_root: Path | None = None) -> Path:
    return _cache_root(cache_root) / "state-health-data" / "pa-doh-hospital-extract"


def normalize_pa_doh_hospital_extract(cache_root: Path | None = None) -> int:
    """Normalize PA DOH Hospital Reports record-level public extracts when cached."""

    root = _cache_root(cache_root)
    reports_cache = root / "state-health-data" / "pa-hospital-reports"
    index_path = reports_cache / "artifact_index.json"
    if not index_path.exists():
        return 0
    try:
        payload = json.loads(index_path.read_text(encoding="utf-8"))
    except Exception:
        return 0
    artifacts = payload.get("artifacts") if isinstance(payload, dict) else []
    if not isinstance(artifacts, list):
        return 0

    normalized_rows: list[dict[str, Any]] = []
    for artifact in artifacts:
        if not isinstance(artifact, dict):
            continue
        path = Path(_text(artifact.get("cached_path")))
        if not path.exists():
            continue
        title_url = f"{artifact.get('title', '')} {artifact.get('artifact_url', '')}".lower()
        suffix = path.suffix.lower()
        if suffix == ".csv" and _is_pa_record_level_extract(title_url):
            try:
                df = pd.read_csv(path, dtype=str, keep_default_na=False, low_memory=False)
            except Exception:
                continue
            normalized_rows.extend(_normalize_pa_doh_extract_dataframe(df, artifact))
        elif suffix in {".xlsx", ".xls"} and _is_pa_excel_report_fallback(title_url):
            try:
                sheets = pd.read_excel(path, sheet_name=None, dtype=str)
            except Exception:
                continue
            for sheet_name, df in sheets.items():
                sheet_artifact = {**artifact, "sheet_name": str(sheet_name)}
                normalized_rows.extend(_normalize_pa_doh_extract_dataframe(df.fillna(""), sheet_artifact))

    output_cache = _pa_doh_extract_cache(cache_root)
    output_cache.mkdir(parents=True, exist_ok=True)
    output_path = output_cache / "normalized.parquet"
    csv_path = output_cache / "normalized.csv"
    metadata_path = output_cache / "normalized.meta.json"
    normalized = pd.DataFrame(normalized_rows)
    if normalized.empty:
        write_atomic_json(metadata_path, {"record_count": 0, "source_url": PA_HOSPITAL_REPORTS_URL})
        return 0
    try:
        write_atomic_parquet(output_path, normalized, compression="zstd", index=False)
    except Exception:
        write_atomic_dataframe_csv(csv_path, normalized, index=False)
    write_atomic_json(
        metadata_path,
        {
            "source_id": "pa_doh_hospital_extract",
            "source_name": "Pennsylvania DOH Hospital Reports record-level extract",
            "source_url": PA_HOSPITAL_REPORTS_URL,
            "record_count": len(normalized),
            "generated_at": _now(),
        },
    )
    return int(len(normalized))


def _normalize_pa_doh_extract_dataframe(df: pd.DataFrame, artifact: dict[str, Any]) -> list[dict[str, Any]]:
    columns = [str(col) for col in df.columns]
    facility_col = find_matching_columns(columns, ["facility_name", "hospital_name", "facility", "hospital"])
    ccn_col = find_matching_columns(columns, ["ccn", "provider_number", "cms_certification_number", "medicare_provider_number"])
    state_id_col = find_matching_columns(columns, ["state_facility_id", "facility_id", "license_id", "licensure_id", "pa_id"])
    year_col = find_matching_columns(columns, ["report_year", "year", "calendar_year", "fiscal_year"])
    campus_col = find_matching_columns(columns, ["campus", "campus_name", "location"])
    report_year = _infer_publication_year(f"{artifact.get('title', '')} {artifact.get('artifact_url', '')}") or artifact.get("year")
    rows: list[dict[str, Any]] = []
    for _, raw in df.iterrows():
        facility_name = _text(raw.get(facility_col)) if facility_col else ""
        row_ccn = _text(raw.get(ccn_col)) if ccn_col else ""
        state_facility_id = _text(raw.get(state_id_col)) if state_id_col else ""
        row_year = _text(raw.get(year_col)) if year_col else str(report_year or "")
        row_scope = "ccn" if row_ccn else ("campus" if campus_col and _text(raw.get(campus_col)) else "license")
        for column in columns:
            metric_name = _pa_doh_bed_metric_name(column)
            if not metric_name:
                continue
            metric_value = _text(raw.get(column))
            if not metric_value:
                continue
            rows.append(
                {
                    "state": "PA",
                    "source": "Pennsylvania Department of Health Hospital Reports",
                    "source_url": PA_HOSPITAL_REPORTS_URL,
                    "source_artifact": _text(artifact.get("artifact_url") or artifact.get("url")),
                    "source_artifact_path": _text(artifact.get("cached_path")),
                    "source_sheet": _text(artifact.get("sheet_name")),
                    "report_year": row_year,
                    "facility_name": facility_name,
                    "state_facility_id": state_facility_id,
                    "ccn": row_ccn.zfill(6) if row_ccn else "",
                    "row_scope": row_scope,
                    "metric_name": metric_name,
                    "metric_value": metric_value,
                    "raw_column": column,
                    "confidence": "high_structured_state_extract" if row_ccn else "medium_structured_state_extract",
                }
            )
    return rows


def _is_pa_record_level_extract(title_url: str) -> bool:
    return "hospital extract" in title_url or "record level data" in title_url


def _is_pa_excel_report_fallback(title_url: str) -> bool:
    if "record format" in title_url or "data dictionary" in title_url:
        return False
    if "hospital" not in title_url:
        return False
    return any(token in title_url for token in ("report", "annual", "financial", "statistical", "survey"))


def _pa_doh_bed_metric_name(column: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "_", column.lower()).strip("_")
    if "bed_day" in normalized or ("beds" in normalized and "available" in normalized and "day" in normalized):
        return "bed_days_available"
    bed_tokens = ("licensed_beds", "approved_beds", "beds_set_up", "set_up_and_staffed", "staffed_beds")
    if any(token in normalized for token in bed_tokens):
        return "beds"
    if normalized in {"beds", "bed_count", "total_beds"}:
        return "beds"
    return ""


def load_pa_doh_bed_candidates(
    *,
    ccn: str = "",
    state_facility_id: str = "",
    facility_name: str = "",
    year: int = 0,
    cache_root: Path | None = None,
) -> list[dict[str, Any]]:
    """Load normalized PA DOH bed metric rows from cache."""

    cache = _pa_doh_extract_cache(cache_root)
    parquet = cache / "normalized.parquet"
    csv_path = cache / "normalized.csv"
    if parquet.exists():
        try:
            df = pd.read_parquet(parquet)
        except Exception:
            df = pd.DataFrame()
    elif csv_path.exists():
        try:
            df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
        except Exception:
            df = pd.DataFrame()
    else:
        return []
    if df.empty:
        return []
    mask = df.index >= 0
    if ccn and "ccn" in df.columns:
        mask = mask & (df["ccn"].astype(str).str.zfill(6) == str(ccn).zfill(6))
    if state_facility_id and "state_facility_id" in df.columns:
        mask = mask & (df["state_facility_id"].astype(str) == str(state_facility_id))
    if facility_name and "facility_name" in df.columns:
        mask = mask & df["facility_name"].astype(str).str.contains(facility_name, case=False, na=False, regex=False)
    if year and "report_year" in df.columns:
        mask = mask & df["report_year"].astype(str).str.contains(str(int(year)), regex=False, na=False)
    if "metric_name" in df.columns:
        mask = mask & df["metric_name"].astype(str).isin(["beds", "bed_days_available"])
    return df[mask].head(200).to_dict(orient="records")


async def acquire_nj_hospital_public_data(cache_root: Path | None = None, *, force: bool = False) -> SourceStatus:  # noqa: ARG001
    root = _cache_root(cache_root) / "state-health-data" / "nj-hospital-public-data"
    financial = await _scrape_artifact_links(
        "nj_hospital_financial",
        "New Jersey Hospital Financial Reports",
        NJ_HOSPITAL_FINANCIAL_URL,
        root,
        force=force,
    )
    charity = await _scrape_artifact_links(
        "nj_charity_care",
        "New Jersey Charity Care Reports",
        NJ_CHARITY_CARE_URL,
        root / "charity-care",
        force=force,
    )
    financial_artifacts = json.loads(Path(financial.cache_path).read_text(encoding="utf-8")).get("artifacts", [])
    charity_artifacts = json.loads(Path(charity.cache_path).read_text(encoding="utf-8")).get("artifacts", [])
    combined = financial_artifacts + charity_artifacts
    index_path = _write_artifact_indexes(
        root,
        source_id="nj_hospital_public_data",
        source_name="New Jersey Hospital Public Data",
        source_url=NJ_HOSPITAL_FINANCIAL_URL,
        artifacts=combined,
    )
    return SourceStatus(
        source_id="nj_hospital_public_data",
        source_name="New Jersey Hospital Public Data",
        source_url=NJ_HOSPITAL_FINANCIAL_URL,
        status="ready" if combined else "empty",
        cache_path=str(index_path),
        artifact_count=len(combined),
        acquired_at=_now(),
    )


async def acquire_de_hospital_discharge(cache_root: Path | None = None, *, force: bool = False) -> SourceStatus:  # noqa: ARG001
    return await _scrape_artifact_links(
        "de_hospital_discharge",
        "Delaware Hospital Discharge Public Data",
        DE_HOSPITAL_DISCHARGE_URL,
        _cache_root(cache_root) / "state-health-data" / "de-hospital-discharge",
        force=force,
    )


async def acquire_ahrq_hfmd(cache_root: Path | None = None, *, force: bool = False) -> SourceStatus:
    root = _cache_root(cache_root) / "state-health-data" / "ahrq-hfmd"
    root.mkdir(parents=True, exist_ok=True)
    page = await resilient_request(
        "GET",
        AHRQ_HFMD_URL,
        timeout=60.0,
        follow_redirects=True,
        headers=_PUBLIC_WEB_HEADERS,
    )
    soup = BeautifulSoup(page.text, "html.parser")
    csv_url = ""
    for link in soup.find_all("a"):
        href = _text(link.get("href"))
        label = link.get_text(" ", strip=True).lower()
        lower_href = href.lower()
        if "hfmd" in f"{label} {lower_href}" and "csv" in f"{label} {lower_href}" and lower_href.endswith(".zip"):
            csv_url = urljoin(AHRQ_HFMD_URL, href)
            break
    if not csv_url:
        csv_url = AHRQ_HFMD_CSV_ZIP_URL
    target = root / "hfmd.zip"
    await _download(csv_url, target, force=force)
    if not zipfile.is_zipfile(target):
        target.unlink(missing_ok=True)
        return SourceStatus(
            "ahrq_hfmd",
            "AHRQ Hospital Financial Measures Database",
            csv_url,
            "not_automatable",
            reason="ahrq_cloudfront_waf_challenge_for_direct_download",
            next_step="Download HFMD_2016_2019csv.zip from the AHRQ page in a browser, then place it at the cache path or add a non-WAF mirror.",
            acquired_at=_now(),
        )
    record_count = 0
    if target.suffix.lower() == ".zip":
        extracted_dir = root / "csv"
        extracted_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(target) as zf:
            for member in zf.namelist():
                if not member.lower().endswith(".csv"):
                    continue
                extracted = extracted_dir / Path(member).name
                if force or not extracted.exists():
                    write_atomic_bytes(extracted, zf.read(member))
                try:
                    record_count += len(pd.read_csv(extracted, dtype=str, keep_default_na=False))
                except Exception:
                    pass
    return SourceStatus(
        "ahrq_hfmd",
        "AHRQ Hospital Financial Measures Database",
        csv_url,
        "ready",
        str(target),
        record_count=record_count,
        acquired_at=_now(),
    )


async def acquire_phc4_public_reports(cache_root: Path | None = None, *, force: bool = False) -> SourceStatus:
    cache = _cache_root(cache_root) / "state-health-data" / "phc4"
    cache.mkdir(parents=True, exist_ok=True)
    resp = await resilient_request("GET", PHC4_REPORT_LIBRARY_URL, timeout=60.0, follow_redirects=True)
    soup = BeautifulSoup(resp.text, "html.parser")
    records: list[dict[str, Any]] = []
    for link in soup.find_all("a"):
        href = _text(link.get("href"))
        title = " ".join(link.get_text(" ", strip=True).split())
        if not href or not title:
            continue
        url = urljoin(PHC4_REPORT_LIBRARY_URL, href)
        text = f"{title} {url}"
        if not any(token in text.lower() for token in ("report", "hospital performance", "financial analysis", "common procedures")):
            continue
        year_match = re.search(r"(20\d{2}|19\d{2})", text)
        report_type = "public_report"
        lower = text.lower()
        if "financial" in lower:
            report_type = "financial_analysis"
        elif "hospital performance" in lower or "hpr" in lower:
            report_type = "hospital_performance"
        elif "common procedure" in lower:
            report_type = "common_procedure"
        record = _normalized_artifact_record(
            source_id="phc4_public_reports",
            source_name="PHC4 Public Reports Library",
            source_url=PHC4_REPORT_LIBRARY_URL,
            title=title,
            artifact_url=url,
            landing_page_url=PHC4_REPORT_LIBRARY_URL,
            publication_year=int(year_match.group(1)) if year_match else None,
        )
        record["report_type"] = report_type
        record["table_references"] = []
        records.append(record)

    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for record in records:
        if record["artifact_url"] in seen:
            continue
        seen.add(record["artifact_url"])
        if record["artifact_url"].lower().endswith((".pdf", ".xlsx", ".xls", ".csv", ".html", ".htm")):
            suffix = Path(record["artifact_url"].split("?", 1)[0]).suffix or ".html"
            target = cache / "artifacts" / f"{record.get('year') or 'unknown'}-{_slug(record['title'])}{suffix}"
            try:
                await _download(record["artifact_url"], target, force=force)
                record["cached_path"] = str(target)
                record["table_references"] = _extract_structured_tables(target, cache)
            except Exception as exc:
                record["download_error"] = str(exc)
        deduped.append(record)

    index = cache / "report_index.parquet"
    json_index = cache / "report_index.json"
    df = pd.DataFrame(deduped)
    if not df.empty:
        try:
            write_atomic_parquet(index, df, compression="zstd", index=False)
        except ImportError:
            index = json_index
    write_atomic_json(json_index, deduped)
    _write_artifact_indexes(
        cache,
        source_id="phc4_public_reports",
        source_name="PHC4 Public Reports Library",
        source_url=PHC4_REPORT_LIBRARY_URL,
        artifacts=deduped,
    )
    return SourceStatus(
        "phc4_public_reports",
        "PHC4 Public Reports Library",
        PHC4_REPORT_LIBRARY_URL,
        "ready" if deduped else "empty",
        str(index if index.exists() else json_index),
        record_count=len(deduped),
        artifact_count=sum(1 for item in deduped if item.get("cached_path")),
        acquired_at=_now(),
    )


def _extract_structured_tables(path: Path, cache_dir: Path) -> list[dict[str, Any]]:
    """Extract simple table references from public PHC4 artifacts when format permits."""
    suffix = path.suffix.lower()
    table_dir = cache_dir / "tables"
    table_dir.mkdir(parents=True, exist_ok=True)
    references: list[dict[str, Any]] = []
    try:
        if suffix == ".csv":
            tables = [pd.read_csv(path, dtype=str, keep_default_na=False)]
            names = ["csv"]
        elif suffix in {".xlsx", ".xls"}:
            sheets = pd.read_excel(path, sheet_name=None, dtype=str)
            tables = [df.fillna("") for df in sheets.values()]
            names = list(sheets.keys())
        elif suffix in {".html", ".htm"}:
            tables = [df.fillna("") for df in pd.read_html(path)]
            names = [f"html_table_{idx + 1}" for idx in range(len(tables))]
        elif suffix == ".pdf":
            return _extract_pdf_table_like_pages(path, table_dir)
        else:
            return []
    except Exception:
        return []

    for idx, df in enumerate(tables, start=1):
        if df.empty or len(df.columns) < 2:
            continue
        df = df.astype(str).head(5000)
        table_path = table_dir / f"{path.stem}-table-{idx}.json"
        write_atomic_json(
            table_path,
            {
                "source_artifact": str(path),
                "table_name": names[idx - 1] if idx - 1 < len(names) else f"table_{idx}",
                "columns": [str(col) for col in df.columns],
                "rows": df.to_dict(orient="records"),
            },
        )
        references.append(
            {
                "artifact_path": str(path),
                "table_index": idx,
                "table_name": names[idx - 1] if idx - 1 < len(names) else f"table_{idx}",
                "row_count": int(len(df)),
                "column_count": int(len(df.columns)),
                "extracted_path": str(table_path),
                "provenance": {"source_artifact": str(path), "page": None, "table": idx},
                "extraction_status": "structured_table_extracted",
            }
        )
    return references


def _extract_pdf_table_like_pages(path: Path, table_dir: Path) -> list[dict[str, Any]]:
    """Extract page-level table-like text from PDFs without claiming perfect table parsing."""
    references: list[dict[str, Any]] = []
    try:
        reader = PdfReader(str(path))
    except Exception:
        return references

    for page_idx, page in enumerate(reader.pages, start=1):
        try:
            text = page.extract_text() or ""
        except Exception:
            continue
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        table_like = [
            line
            for line in lines
            if re.search(r"\S+\s{2,}\S+", line) or len(re.findall(r"\$?[\d,]+(?:\.\d+)?%?", line)) >= 3
        ]
        if len(table_like) < 3:
            continue
        extracted_path = table_dir / f"{path.stem}-page-{page_idx}-table-text.json"
        parsed_rows = _parse_table_like_lines(table_like[:200])
        write_atomic_json(
            extracted_path,
            {
                "source_artifact": str(path),
                "page": page_idx,
                "extraction_method": "pdf_text_table_like_lines",
                "lines": table_like[:200],
                "parsed_rows": parsed_rows,
            },
        )
        parsed_columns = list((parsed_rows[:1] or [{}])[0].keys())
        has_facility = bool(find_matching_columns(parsed_columns, ["hospital", "facility", "provider", "name"]))
        has_year = bool(find_matching_columns(parsed_columns, ["year", "fiscal_year", "fy"]))
        references.append(
            {
                "artifact_path": str(path),
                "page": page_idx,
                "table_index": len(references) + 1,
                "table_name": f"pdf_page_{page_idx}_table_like_text",
                "row_count": len(table_like),
                "column_count": None,
                "extracted_path": str(extracted_path),
                "provenance": {"source_artifact": str(path), "page": page_idx, "table": len(references) + 1},
                "confidence": "medium_text_extraction",
                "extraction_status": "text_table_extracted" if has_facility and has_year else "not_structured_enough",
            }
        )
    return references


def _parse_table_like_lines(lines: list[str]) -> list[dict[str, str]]:
    if not lines:
        return []
    split_rows = [[cell.strip() for cell in re.split(r"\s{2,}", _text(line)) if cell.strip()] for line in lines]
    split_rows = [row for row in split_rows if len(row) >= 2]
    if len(split_rows) < 2:
        return []
    header = split_rows[0]
    if len(set(header)) != len(header):
        return []
    rows: list[dict[str, str]] = []
    for row in split_rows[1:]:
        if len(row) != len(header):
            continue
        rows.append(dict(zip(header, row, strict=True)))
    return rows


async def acquire_source(source_id: str, cache_root: Path | None = None, *, force: bool = False) -> SourceStatus:
    dispatch = {
        "pa_hospital_reports": acquire_pa_hospital_reports,
        "nj_hospital_public_data": acquire_nj_hospital_public_data,
        "de_hospital_discharge": acquire_de_hospital_discharge,
        "phc4_public_reports": acquire_phc4_public_reports,
        "ahrq_hfmd": acquire_ahrq_hfmd,
    }
    if source_id not in dispatch:
        raise ValueError(f"Unknown state health data source: {source_id}")
    return await dispatch[source_id](cache_root, force=force)


async def acquire_sources(source_ids: list[str], cache_root: Path | None = None, *, force: bool = False) -> list[dict[str, Any]]:
    statuses: list[dict[str, Any]] = []
    for source_id in source_ids:
        try:
            statuses.append((await acquire_source(source_id, cache_root, force=force)).to_dict())
        except Exception as exc:
            statuses.append(
                SourceStatus(source_id, source_id, "", "failed", reason=str(exc), acquired_at=_now()).to_dict()
            )
    return statuses


async def ensure_phc4_index(cache_root: Path | None = None) -> Path:
    cache = _cache_root(cache_root) / "state-health-data" / "phc4"
    index = cache / "report_index.json"
    if not index.exists():
        await acquire_phc4_public_reports(cache_root)
    return index


async def search_phc4_reports(query: str, year: str = "", report_type: str = "", cache_root: Path | None = None) -> dict[str, Any]:
    index = await ensure_phc4_index(cache_root)
    records = json.loads(index.read_text(encoding="utf-8")) if index.exists() else []
    q = query.strip().lower()
    filtered: list[dict[str, Any]] = []
    for record in records:
        haystack = json.dumps(record, default=str).lower()
        if q and q not in haystack:
            continue
        if year and str(record.get("year") or "") != str(year):
            continue
        if report_type and report_type.lower() not in _text(record.get("report_type")).lower():
            continue
        filtered.append(record)
    return {"query": query, "year": year, "report_type": report_type, "total_results": len(filtered), "reports": filtered[:50]}


async def phc4_report_profile(
    *,
    hospital_name: str = "",
    year: int = 0,
    report_type: str = "",
    procedure: str = "",
    fiscal_year: int = 0,
    cache_root: Path | None = None,
) -> dict[str, Any]:
    selected_year = str(year or fiscal_year or "")
    query = ""
    results = await search_phc4_reports(query=query, year=selected_year, report_type=report_type, cache_root=cache_root)
    table_rows = _matching_phc4_table_rows(
        results["reports"],
        hospital_name=hospital_name,
        procedure=procedure,
    )
    confidence = "high_extracted_table_row" if table_rows else ("medium_report_match" if results["reports"] else "no_public_report_match")
    if not table_rows and _phc4_reports_have_unstructured_tables(results["reports"]):
        confidence = "not_structured_enough"
    return {
        "hospital_name": hospital_name,
        "procedure": procedure,
        "year": year or fiscal_year or 0,
        "report_type": report_type,
        "source_status": "public_report_index",
        "confidence": confidence,
        "note": "Public PHC4 reports are indexed and cached; paid PHC4 discharge files are not used.",
        "table_rows": table_rows[:100],
        "reports": results["reports"],
    }


def _matching_phc4_table_rows(
    reports: list[dict[str, Any]],
    *,
    hospital_name: str = "",
    procedure: str = "",
) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    hospital_q = hospital_name.strip().lower()
    procedure_q = procedure.strip().lower()
    for report in reports:
        rows = _normalized_phc4_rows_for_report(report)
        for row in rows:
            haystack = json.dumps(row, default=str).lower()
            if hospital_q and hospital_q not in haystack:
                continue
            if procedure_q and procedure_q not in haystack:
                continue
            normalized.append(row)
    return normalized


def _phc4_reports_have_unstructured_tables(reports: list[dict[str, Any]]) -> bool:
    saw_table_ref = False
    for report in reports:
        if _normalized_phc4_rows_for_report(report):
            return False
        for table_ref in report.get("table_references") or []:
            saw_table_ref = True
            if table_ref.get("extraction_status") == "not_structured_enough":
                return True
    return saw_table_ref


def _normalized_phc4_rows_for_report(report: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for table_ref in report.get("table_references") or []:
        extracted_path = Path(_text(table_ref.get("extracted_path")))
        if not extracted_path.exists():
            continue
        try:
            payload = json.loads(extracted_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if isinstance(payload.get("rows"), list):
            rows.extend(_normalize_phc4_tabular_rows(report, table_ref, payload))
        elif isinstance(payload.get("lines"), list):
            rows.extend(_normalize_phc4_pdf_lines(report, table_ref, payload))
    return rows


def _normalize_phc4_tabular_rows(
    report: dict[str, Any],
    table_ref: dict[str, Any],
    payload: dict[str, Any],
) -> list[dict[str, Any]]:
    raw_rows = payload.get("rows") or []
    if not isinstance(raw_rows, list):
        return []
    columns = [str(col) for col in payload.get("columns") or []]
    hospital_col = find_matching_columns(columns, ["hospital", "facility", "provider", "hospital_name", "facility_name", "provider_name"])
    year_col = find_matching_columns(columns, ["fiscal_year", "fy", "year", "report_year"])
    procedure_col = find_matching_columns(columns, ["procedure", "service", "condition"])
    measure_col = find_matching_columns(columns, ["measure", "metric", "indicator"])
    value_col = find_matching_columns(columns, ["value", "rate", "score", "volume", "count", "amount"])
    if not hospital_col:
        return []
    normalized: list[dict[str, Any]] = []
    for raw in raw_rows:
        if not isinstance(raw, dict):
            continue
        base = _phc4_row_base(report, table_ref)
        base["hospital_name"] = _text(raw.get(hospital_col)) if hospital_col else ""
        base["facility_name"] = base["hospital_name"]
        base["fiscal_year"] = _text(raw.get(year_col)) if year_col else _text(report.get("year"))
        base["procedure"] = _text(raw.get(procedure_col)) if procedure_col else ""
        if not base["hospital_name"] or not base["fiscal_year"]:
            continue
        if measure_col or value_col:
            normalized.append(
                {
                    **base,
                    "measure_name": _text(raw.get(measure_col)) if measure_col else (value_col or ""),
                    "measure_value": _text(raw.get(value_col)) if value_col else "",
                    "raw_row": raw,
                    "confidence": "high_structured_table",
                }
            )
            continue
        for col, value in raw.items():
            if col in {hospital_col, year_col, procedure_col}:
                continue
            text_value = _text(value)
            if not text_value:
                continue
            normalized.append(
                {
                    **base,
                    "measure_name": str(col),
                    "measure_value": text_value,
                    "raw_row": raw,
                    "confidence": "medium_inferred_table_measure",
                }
            )
    return normalized


def _normalize_phc4_pdf_lines(
    report: dict[str, Any],
    table_ref: dict[str, Any],
    payload: dict[str, Any],
) -> list[dict[str, Any]]:
    parsed_rows = payload.get("parsed_rows")
    if isinstance(parsed_rows, list) and parsed_rows:
        parsed_payload = {"columns": list(parsed_rows[0].keys()), "rows": parsed_rows}
        return _normalize_phc4_tabular_rows(report, table_ref, parsed_payload)

    lines = payload.get("lines") or []
    if not isinstance(lines, list):
        return []
    parsed = _parse_table_like_lines([_text(line) for line in lines[:200]])
    if not parsed:
        return []
    parsed_payload = {"columns": list(parsed[0].keys()), "rows": parsed}
    return _normalize_phc4_tabular_rows(report, table_ref, parsed_payload)


def _phc4_row_base(report: dict[str, Any], table_ref: dict[str, Any]) -> dict[str, Any]:
    return {
        "report_title": _text(report.get("title")),
        "report_type": _text(report.get("report_type")),
        "report_year": report.get("year"),
        "page": table_ref.get("page"),
        "table_index": table_ref.get("table_index"),
        "source_artifact": _text(report.get("artifact_url") or report.get("url") or table_ref.get("artifact_path")),
        "source_artifact_path": _text(table_ref.get("artifact_path")),
        "landing_page_url": _text(report.get("landing_page_url")),
        "publication_date": _text(report.get("publication_date")),
        "publication_year": report.get("publication_year") or report.get("year"),
        "state": _text(report.get("state")),
        "source": _text(report.get("source") or report.get("source_name")),
    }


def find_matching_columns(columns: list[str], candidates: list[str]) -> str:
    normalized = {re.sub(r"[^a-z0-9]+", "_", col.lower()).strip("_"): col for col in columns}
    for candidate in candidates:
        key = re.sub(r"[^a-z0-9]+", "_", candidate.lower()).strip("_")
        if key in normalized:
            return normalized[key]
    for col in columns:
        col_key = re.sub(r"[^a-z0-9]+", "_", col.lower()).strip("_")
        if any(token in col_key for token in candidates):
            return col
    return ""
