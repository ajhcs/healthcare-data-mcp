"""Operator-only cold ingestion for official IRS Form 990 XML batches."""

from __future__ import annotations

from dataclasses import dataclass, replace
import os
from pathlib import Path
from typing import Iterable
from urllib.parse import urlparse

import httpx

from .fact_store import ALLOWED_IRS_SOURCE_HOSTS, Form990FactStore
from .irs_source import IrsEfileIndexEntry, ingest_batch_member, select_index_entry


MAX_INDEX_BYTES = 100 * 1024 * 1024
MAX_BATCH_BYTES = 1_500 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class IngestRequest:
    ein: str
    tax_year: int
    filing_year: int
    object_id: str | None = None


def default_database_path() -> Path:
    return Path(os.environ.get("HC_990_FACTS_DB", ".local/irs990-facts.sqlite3")).expanduser()


def default_cache_dir() -> Path:
    return Path(os.environ.get("HC_990_INGEST_CACHE", ".local/irs990-cache")).expanduser()


def download_official_file(
    url: str,
    destination: str | Path,
    *,
    max_bytes: int,
    client: httpx.Client | None = None,
) -> Path:
    """Stream one official IRS file to an atomic local cache entry."""

    parsed = urlparse(url)
    if parsed.scheme != "https" or (parsed.hostname or "").lower() not in ALLOWED_IRS_SOURCE_HOSTS:
        raise ValueError("download URL must be an official HTTPS IRS URL")
    destination_path = Path(destination)
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    if destination_path.exists():
        if destination_path.stat().st_size <= max_bytes:
            return destination_path
        raise ValueError(f"Cached file exceeds configured maximum: {destination_path}")

    temporary_path = destination_path.with_suffix(destination_path.suffix + ".part")
    owns_client = client is None
    http_client = client or httpx.Client(
        follow_redirects=True,
        timeout=httpx.Timeout(300.0, connect=30.0),
        headers={"User-Agent": "healthcare-data-mcp/irs990-prototype (official IRS public data ingestion)"},
    )
    try:
        with http_client.stream("GET", url) as response:
            response.raise_for_status()
            declared_length = int(response.headers.get("content-length", "0") or 0)
            if declared_length > max_bytes:
                raise ValueError(f"Official IRS download exceeds configured maximum of {max_bytes} bytes")
            written = 0
            with temporary_path.open("wb") as output:
                for chunk in response.iter_bytes():
                    written += len(chunk)
                    if written > max_bytes:
                        raise ValueError(f"Official IRS download exceeds configured maximum of {max_bytes} bytes")
                    output.write(chunk)
            if written == 0:
                raise ValueError("Official IRS download was empty")
        temporary_path.replace(destination_path)
        return destination_path
    except Exception:
        temporary_path.unlink(missing_ok=True)
        raise
    finally:
        if owns_client:
            http_client.close()


def resolve_official_entry(
    request: IngestRequest,
    *,
    cache_dir: str | Path,
    client: httpx.Client | None = None,
) -> IrsEfileIndexEntry:
    cache = Path(cache_dir)
    index_url = f"https://apps.irs.gov/pub/epostcard/990/xml/{request.filing_year}/index_{request.filing_year}.csv"
    index_path = download_official_file(
        index_url,
        cache / f"index_{request.filing_year}.csv",
        max_bytes=MAX_INDEX_BYTES,
        client=client,
    )
    return select_index_entry(
        index_path.read_bytes(),
        ein=request.ein,
        tax_year=request.tax_year,
        filing_year=request.filing_year,
        object_id=request.object_id,
    )


def ingest_official_requests(
    store: Form990FactStore,
    requests: Iterable[IngestRequest],
    *,
    cache_dir: str | Path,
    keep_archives: bool = False,
) -> list[dict]:
    """Ingest exact official filings, reusing an IRS batch across requested filers."""

    request_list = list(requests)
    if not request_list:
        raise ValueError("At least one exact EIN/year ingestion request is required")
    cache = Path(cache_dir)
    receipts: list[dict] = []
    downloaded_archives: set[Path] = set()
    with httpx.Client(
        follow_redirects=True,
        timeout=httpx.Timeout(300.0, connect=30.0),
        headers={"User-Agent": "healthcare-data-mcp/irs990-prototype (official IRS public data ingestion)"},
    ) as client:
        entries = [resolve_official_entry(request, cache_dir=cache, client=client) for request in request_list]
        try:
            for entry in entries:
                missing_member_error: ValueError | None = None
                for batch_id in entry.candidate_xml_batch_ids:
                    archive_entry = replace(entry, xml_batch_id=batch_id)
                    archive_path = download_official_file(
                        archive_entry.batch_url,
                        cache / f"{batch_id}.zip",
                        max_bytes=MAX_BATCH_BYTES,
                        client=client,
                    )
                    downloaded_archives.add(archive_path)
                    try:
                        receipts.append(ingest_batch_member(store, entry=archive_entry, archive_path=archive_path))
                    except ValueError as exc:
                        if str(exc) != f"IRS batch does not contain {entry.source_member}":
                            raise
                        missing_member_error = exc
                        continue
                    break
                else:
                    raise ValueError(
                        f"IRS archive parts do not contain indexed member {entry.source_member}"
                    ) from missing_member_error
        finally:
            if not keep_archives:
                for archive_path in downloaded_archives:
                    archive_path.unlink(missing_ok=True)
    return receipts
