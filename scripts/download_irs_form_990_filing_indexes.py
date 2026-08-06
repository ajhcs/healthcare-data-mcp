"""Acquire, double-verify, and receipt bounded official IRS Form 990 indexes."""

from __future__ import annotations

import argparse
from collections.abc import Iterator
from contextlib import contextmanager
import filecmp
import hashlib
import ipaddress
import os
import re
import shutil
import socket
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import BinaryIO, Mapping
from urllib.parse import urlsplit
from uuid import uuid4

import httpx

from shared.acquisition.irs_form_990_filing_index_receipt import (
    IrsForm990FilerQueryScope,
    IrsForm990FilingIndexReceipt,
    IrsForm990ResponseMetadata,
    annual_index_relative_path,
    annual_index_url,
    build_irs_form_990_filing_index_receipt,
    load_irs_form_990_query_scope,
    validate_irs_form_990_filing_index_receipt,
    write_irs_form_990_filing_index_receipt,
)

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SCOPE_PATH = ROOT / "contracts" / "source-receipts" / "irs-form-990-pilot-scope-v1.json"
DEFAULT_CACHE_DIR = Path.home() / ".healthcare-data-mcp" / "cache" / "irs-form-990-index"
DEFAULT_RECEIPT_NAME = "irs-form-990-pilot-receipt.json"
MAX_INDEX_BYTES = 250_000_000
ALLOWED_IRS_HOST = "apps.irs.gov"
REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})
OFFICIAL_INDEX_PATH = re.compile(
    r"^/pub/epostcard/990/xml/(?P<directory_year>[0-9]{4})/"
    r"index_(?P<file_year>[0-9]{4})\.csv$"
)


def download_official_annual_index(
    filing_year: int,
    destination: Path,
    *,
    max_bytes: int = MAX_INDEX_BYTES,
    client: httpx.Client | None = None,
    retrieved_at: datetime | None = None,
) -> IrsForm990ResponseMetadata:
    """Stream one exact allowlisted IRS annual index to a new local file."""

    if max_bytes <= 0 or max_bytes > MAX_INDEX_BYTES:
        raise ValueError(f"max_bytes must be between 1 and {MAX_INDEX_BYTES}")
    url = annual_index_url(filing_year)
    allowed_peer_addresses = _validate_official_public_url(url)
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        raise ValueError(f"download destination already exists: {destination.name}")

    owns_client = client is None
    http_client = client or httpx.Client(
        timeout=httpx.Timeout(300.0, connect=30.0),
        headers={"User-Agent": "healthcare-data-mcp/ushso-form-990-source-receipt"},
        trust_env=False,
    )
    temporary_path = destination.with_name(f".{destination.name}.{uuid4().hex}.part")
    try:
        with http_client.stream(
            "GET",
            url,
            follow_redirects=False,
            headers={"Accept": "text/csv,application/csv;q=0.9"},
        ) as response:
            if response.status_code in REDIRECT_STATUSES:
                raise ValueError("IRS annual index redirects are not admitted")
            if response.status_code != 200:
                raise ValueError("IRS annual index must return exact HTTP 200")
            if str(response.url) != url:
                raise ValueError("IRS annual index final URL drifted from the exact allowlist")
            _validate_connected_peer(
                response,
                allowed_peer_addresses,
                required=owns_client,
            )
            content_type = response.headers.get("content-type", "")
            if "csv" not in content_type.casefold():
                raise ValueError(f"IRS annual index content type is not CSV: {content_type or 'missing'}")
            declared_length = int(response.headers.get("content-length", "0") or 0)
            if declared_length > max_bytes:
                raise ValueError("IRS annual index exceeds the configured byte limit")
            written = 0
            with temporary_path.open("wb") as output:
                for chunk in response.iter_bytes():
                    written += len(chunk)
                    if written > max_bytes:
                        raise ValueError("IRS annual index exceeds the configured byte limit")
                    output.write(chunk)
            if written == 0:
                raise ValueError("IRS annual index download was empty")
            timestamp = retrieved_at or datetime.now(timezone.utc)
            metadata = IrsForm990ResponseMetadata(
                final_url=str(response.url),
                status_code=response.status_code,
                content_type=content_type,
                retrieved_at=timestamp,
                etag=response.headers.get("etag", ""),
                last_modified=response.headers.get("last-modified", ""),
            )
            _install_new_file(temporary_path, destination)
            return metadata
    except Exception:
        temporary_path.unlink(missing_ok=True)
        raise
    finally:
        if owns_client:
            http_client.close()


def acquire(
    output_dir: Path,
    *,
    scope_path: Path,
    receipt_path: Path,
    bootstrap: bool,
    expected_receipt_path: Path | None,
    force: bool,
) -> Path:
    """Acquire every scope year twice, validate, and atomically promote."""

    if bootstrap == (expected_receipt_path is not None):
        raise ValueError("choose exactly one of bootstrap or expected_receipt_path")
    scope = load_irs_form_990_query_scope(scope_path)
    years = sorted({query.filing_year for query in scope.queries})
    destinations = {year: output_dir / annual_index_relative_path(year) for year in years}
    _validate_destination_layout(output_dir, destinations, receipt_path)
    lock_targets = (*destinations.values(), receipt_path)
    with _exclusive_destination_locks(lock_targets):
        return _acquire_locked(
            destinations,
            years=years,
            scope=scope,
            receipt_path=receipt_path,
            bootstrap=bootstrap,
            expected_receipt_path=expected_receipt_path,
            force=force,
        )


def _acquire_locked(
    destinations: Mapping[int, Path],
    *,
    years: list[int],
    scope: IrsForm990FilerQueryScope,
    receipt_path: Path,
    bootstrap: bool,
    expected_receipt_path: Path | None,
    force: bool,
) -> Path:
    existing = [path for path in (*destinations.values(), receipt_path) if path.exists()]
    if existing and not force:
        raise RuntimeError("IRS index cache or receipt exists; pass --force after review")

    with tempfile.TemporaryDirectory(prefix="irs-form-990-index-receipt-") as temp_dir:
        temporary_root = Path(temp_dir)
        first_paths: dict[int, Path] = {}
        responses: dict[int, IrsForm990ResponseMetadata] = {}
        for retrieval_number in (1, 2):
            for year in years:
                path = temporary_root / f"pass-{retrieval_number}" / annual_index_relative_path(year)
                response = download_official_annual_index(year, path)
                if retrieval_number == 1:
                    first_paths[year] = path
                    responses[year] = response
                else:
                    _require_byte_identity(year, first_paths[year], path)

        temporary_receipt = temporary_root / DEFAULT_RECEIPT_NAME
        if bootstrap:
            receipt = build_irs_form_990_filing_index_receipt(
                first_paths,
                responses=responses,
                scope=scope,
            )
            write_irs_form_990_filing_index_receipt(temporary_receipt, receipt)
        else:
            if expected_receipt_path is None:
                raise AssertionError("expected receipt path is required outside bootstrap")
            expected_receipt_bytes = expected_receipt_path.read_bytes()
            receipt = IrsForm990FilingIndexReceipt.model_validate_json(expected_receipt_bytes)
            validate_irs_form_990_filing_index_receipt(receipt, first_paths, scope=scope)
            temporary_receipt.write_bytes(expected_receipt_bytes)

        _replace_validated_files(
            {
                **{destinations[year]: first_paths[year] for year in years},
                receipt_path: temporary_receipt,
            },
            commit_marker=receipt_path,
        )
    return receipt_path


def _validate_destination_layout(
    output_dir: Path,
    destinations: Mapping[int, Path],
    receipt_path: Path,
) -> None:
    repository_root = ROOT.resolve()
    raw_root = output_dir.expanduser().resolve(strict=False)
    if raw_root == repository_root or repository_root in raw_root.parents:
        raise ValueError("raw IRS annual indexes must remain outside the Git repository")
    resolved_destinations = {path.expanduser().resolve(strict=False) for path in destinations.values()}
    if len(resolved_destinations) != len(destinations):
        raise ValueError("IRS annual index destinations must be unique")
    if receipt_path.expanduser().resolve(strict=False) in resolved_destinations:
        raise ValueError("receipt_path cannot collide with an annual index destination")


@contextmanager
def _exclusive_destination_locks(destinations: tuple[Path, ...]) -> Iterator[None]:
    """Fail closed when another process is writing any destination."""

    lock_root = Path(tempfile.gettempdir()) / "healthcare-data-mcp-acquisition-locks"
    lock_root.mkdir(parents=True, exist_ok=True)
    handles: list[BinaryIO] = []
    try:
        normalized = sorted({str(path.expanduser().resolve(strict=False)) for path in destinations})
        for destination in normalized:
            lock_name = hashlib.sha256(destination.encode("utf-8")).hexdigest() + ".lock"
            handle = (lock_root / lock_name).open("a+b")
            try:
                _lock_file_nonblocking(handle)
            except OSError as exc:
                handle.close()
                raise RuntimeError("another IRS filing-index acquisition is using a destination") from exc
            handles.append(handle)
        yield
    finally:
        for handle in reversed(handles):
            _unlock_file(handle)
            handle.close()


def _lock_file_nonblocking(handle: BinaryIO) -> None:
    if os.name == "nt":
        import msvcrt

        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(b"\0")
            handle.flush()
        handle.seek(0)
        msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        return
    import fcntl

    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)


def _unlock_file(handle: BinaryIO) -> None:
    if os.name == "nt":
        import msvcrt

        handle.seek(0)
        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        return
    import fcntl

    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _install_new_file(temporary_path: Path, destination: Path) -> None:
    """Publish without overwriting a destination created by a concurrent caller."""

    try:
        os.link(temporary_path, destination)
    except FileExistsError as exc:
        raise ValueError(f"download destination already exists: {destination.name}") from exc
    temporary_path.unlink()


def _validate_official_public_url(url: str) -> frozenset[str]:
    parsed = urlsplit(url)
    path_match = OFFICIAL_INDEX_PATH.fullmatch(parsed.path)
    if (
        parsed.scheme != "https"
        or parsed.hostname != ALLOWED_IRS_HOST
        or parsed.port not in {None, 443}
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or path_match is None
        or path_match.group("directory_year") != path_match.group("file_year")
    ):
        raise ValueError("URL must be an exact public HTTPS IRS annual index URL")
    addresses = socket.getaddrinfo(parsed.hostname, 443, type=socket.SOCK_STREAM)
    if not addresses:
        raise ValueError("IRS annual index host did not resolve")
    validated: set[str] = set()
    for info in addresses:
        address = info[4][0]
        ip = ipaddress.ip_address(address)
        if not ip.is_global:
            raise ValueError("IRS annual index host resolved to a non-public address")
        validated.add(ip.compressed)
    return frozenset(validated)


def _validate_connected_peer(
    response: httpx.Response,
    allowed_addresses: frozenset[str],
    *,
    required: bool,
) -> None:
    network_stream = response.extensions.get("network_stream")
    if network_stream is None:
        if required:
            raise ValueError("IRS annual index connection did not expose its peer address")
        return
    server_address = network_stream.get_extra_info("server_addr")
    if not isinstance(server_address, tuple) or not server_address:
        raise ValueError("IRS annual index connection peer address was unavailable")
    peer = ipaddress.ip_address(str(server_address[0]))
    if not peer.is_global or peer.compressed not in allowed_addresses:
        raise ValueError("IRS annual index connected peer did not match validated public DNS")


def _require_byte_identity(filing_year: int, first: Path, second: Path) -> None:
    first_hash = _sha256(first)
    second_hash = _sha256(second)
    if first.stat().st_size != second.stat().st_size or first_hash != second_hash:
        raise RuntimeError(f"IRS annual index repeated retrieval byte drift for {filing_year}")
    if not filecmp.cmp(first, second, shallow=False):
        raise RuntimeError(f"IRS annual index repeated retrieval content drift for {filing_year}")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _replace_validated_files(
    files: Mapping[Path, Path],
    *,
    commit_marker: Path,
) -> None:
    """Promote data first and the receipt commit marker last, with rollback."""

    if not files:
        raise ValueError("At least one validated file is required")
    if commit_marker not in files:
        raise ValueError("commit_marker must be one of the validated destinations")
    destinations = tuple(path for path in files if path != commit_marker) + (commit_marker,)
    transaction_id = uuid4().hex
    staged: dict[Path, Path] = {}
    backups: dict[Path, Path] = {}
    promoted: set[Path] = set()
    try:
        for destination in destinations:
            source = files[destination]
            destination.parent.mkdir(parents=True, exist_ok=True)
            candidate = destination.with_name(f".{destination.name}.{transaction_id}.candidate")
            shutil.copyfile(source, candidate)
            staged[destination] = candidate
        for destination in destinations:
            if destination.exists():
                backup = destination.with_name(f".{destination.name}.{transaction_id}.backup")
                shutil.copyfile(destination, backup)
                backups[destination] = backup
            staged[destination].replace(destination)
            promoted.add(destination)
    except BaseException as exc:
        rollback_errors: list[str] = []
        for destination in reversed(destinations):
            try:
                if destination not in promoted:
                    continue
                backup = backups.get(destination)
                if backup is None:
                    destination.unlink(missing_ok=True)
                elif backup.exists():
                    backup.replace(destination)
            except OSError as rollback_exc:
                rollback_errors.append(f"{destination.name}: {rollback_exc}")
        if rollback_errors:
            raise RuntimeError(
                "IRS filing-index promotion failed and rollback was incomplete: " + "; ".join(rollback_errors)
            ) from exc
        raise
    finally:
        for temporary_path in (*staged.values(), *backups.values()):
            temporary_path.unlink(missing_ok=True)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_CACHE_DIR)
    parser.add_argument("--scope", type=Path, default=DEFAULT_SCOPE_PATH)
    parser.add_argument("--receipt-path", type=Path)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--bootstrap", action="store_true")
    mode.add_argument("--expected-receipt", type=Path)
    parser.add_argument("--force", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    receipt_path = args.receipt_path or args.output_dir / "manifests" / DEFAULT_RECEIPT_NAME
    try:
        admitted = acquire(
            args.output_dir,
            scope_path=args.scope,
            receipt_path=receipt_path,
            bootstrap=args.bootstrap,
            expected_receipt_path=args.expected_receipt,
            force=args.force,
        )
    except Exception as exc:
        print(f"IRS Form 990 filing-index acquisition failed closed: {exc}", file=sys.stderr)
        return 1
    print(f"IRS Form 990 filing-index receipt admitted: {admitted}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
