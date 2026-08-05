"""Acquire and admit the revised 2023 AHRQ Compendium with Playwright.

AHRQ protects direct CSV requests with a WAF. This connector uses a browser-
compatible request path, validates both files as one release, and only replaces
cache files after every cross-artifact assertion passes.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping
from uuid import uuid4

from shared.acquisition.ahrq_compendium_historical import (
    DEFAULT_AHRQ_RELEASE_KEY,
    SUPPORTED_AHRQ_RELEASE_KEYS,
    build_ahrq_historical_receipt,
    get_historical_ahrq_release,
    write_ahrq_historical_receipt,
)
from shared.acquisition.ahrq_compendium_receipt import (
    AHRQ_ARTIFACT_SPECS,
    AHRQ_LANDING_PAGE,
    AhrqResponseMetadata,
    build_ahrq_compendium_receipt,
    write_ahrq_compendium_receipt,
)
from shared.utils.cache import write_atomic_bytes

DEFAULT_CACHE_DIR = Path.home() / ".healthcare-data-mcp" / "cache"
DEFAULT_RECEIPT_NAME = "ahrq_compendium_2023_revised_source_receipt.json"


async def acquire(
    output_dir: Path,
    *,
    force: bool,
    receipt_path: Path | None = None,
    release: str = DEFAULT_AHRQ_RELEASE_KEY,
) -> Path:
    """Acquire one reviewed AHRQ release; revised 2023 remains the default."""

    if release == DEFAULT_AHRQ_RELEASE_KEY:
        return await _acquire_revised_2023(
            output_dir,
            force=force,
            receipt_path=receipt_path,
        )
    return await _acquire_historical(
        output_dir,
        release=release,
        force=force,
        receipt_path=receipt_path,
    )


async def _acquire_revised_2023(
    output_dir: Path,
    *,
    force: bool,
    receipt_path: Path | None,
) -> Path:
    try:
        from playwright.async_api import async_playwright
    except ImportError as exc:
        raise RuntimeError(
            "Install Playwright and Chromium before acquiring AHRQ files: "
            "pip install playwright && playwright install chromium"
        ) from exc

    destinations = {spec.role: output_dir / spec.relative_path for spec in AHRQ_ARTIFACT_SPECS}
    destination = receipt_path or output_dir / "manifests" / "source-receipts" / DEFAULT_RECEIPT_NAME
    if destination in destinations.values():
        raise ValueError("The AHRQ receipt path must be distinct from both source artifact paths")
    existing = [path for path in (*destinations.values(), destination) if path.exists()]
    if existing and not force:
        raise RuntimeError("AHRQ cache files already exist; pass --force to acquire and validate a new receipt")

    retrieved_at = datetime.now(timezone.utc)
    responses: dict[str, AhrqResponseMetadata] = {}
    downloaded: dict[str, bytes] = {}
    with tempfile.TemporaryDirectory(prefix="ahrq-compendium-receipt-") as temp_dir:
        temporary_paths: dict[str, Path] = {}
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=True)
            try:
                context = await browser.new_context(
                    user_agent=(
                        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
                    ),
                    extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
                )
                page = await context.new_page()
                landing_response = await page.goto(
                    AHRQ_LANDING_PAGE,
                    wait_until="networkidle",
                    timeout=60_000,
                )
                if landing_response is None or not landing_response.ok:
                    status = landing_response.status if landing_response is not None else "no_response"
                    raise RuntimeError(f"AHRQ landing-page acquisition failed: {status}")
                for spec in AHRQ_ARTIFACT_SPECS:
                    response = await context.request.get(
                        spec.source_url,
                        headers={
                            "Accept": "text/csv,application/csv;q=0.9,*/*;q=0.1",
                            "Referer": AHRQ_LANDING_PAGE,
                        },
                        timeout=60_000,
                    )
                    if not response.ok:
                        status = response.status
                        raise RuntimeError(f"AHRQ acquisition failed for {spec.role}: {status}")
                    headers = response.headers
                    body = await response.body()
                    content_type = headers.get("content-type", "")
                    if not body or "csv" not in content_type.casefold():
                        raise RuntimeError(
                            f"AHRQ acquisition did not return CSV bytes for {spec.role}: {content_type or 'missing'}"
                        )
                    temporary_path = Path(temp_dir) / spec.relative_path
                    temporary_path.write_bytes(body)
                    temporary_paths[spec.role] = temporary_path
                    downloaded[spec.role] = body
                    responses[spec.role] = AhrqResponseMetadata(
                        final_url=response.url,
                        status_code=response.status,
                        content_type=content_type,
                        etag=headers.get("etag", ""),
                        last_modified=headers.get("last-modified", ""),
                    )
            finally:
                await browser.close()

        receipt = build_ahrq_compendium_receipt(
            temporary_paths["system_universe"],
            temporary_paths["hospital_linkage"],
            retrieved_at=retrieved_at,
            responses=responses,
        )
        temporary_receipt = Path(temp_dir) / DEFAULT_RECEIPT_NAME
        write_ahrq_compendium_receipt(temporary_receipt, receipt)
        receipt_bytes = temporary_receipt.read_bytes()

    _replace_validated_files(
        {
            destinations["system_universe"]: downloaded["system_universe"],
            destinations["hospital_linkage"]: downloaded["hospital_linkage"],
            destination: receipt_bytes,
        }
    )
    return destination


async def _acquire_historical(
    output_dir: Path,
    *,
    release: str,
    force: bool,
    receipt_path: Path | None,
) -> Path:
    try:
        from playwright.async_api import async_playwright
    except ImportError as exc:
        raise RuntimeError(
            "Install Playwright and Chromium before acquiring AHRQ files: "
            "pip install playwright && playwright install chromium"
        ) from exc

    release_spec = get_historical_ahrq_release(release)
    destinations = {
        spec.role: output_dir / spec.relative_path for spec in release_spec.artifacts
    }
    default_receipt_name = f"ahrq-compendium-{release_spec.source_period}-source-receipt.json"
    destination = (
        receipt_path
        or output_dir / "manifests" / "source-receipts" / default_receipt_name
    )
    if destination in destinations.values():
        raise ValueError("The AHRQ receipt path must be distinct from both source artifact paths")
    existing = [path for path in (*destinations.values(), destination) if path.exists()]
    if existing and not force:
        raise RuntimeError(
            "AHRQ cache files already exist; pass --force to acquire and validate a new receipt"
        )

    retrieved_at = datetime.now(timezone.utc)
    responses: dict[str, AhrqResponseMetadata] = {}
    retrievals: dict[str, list[bytes]] = {
        spec.role: [] for spec in release_spec.artifacts
    }
    with tempfile.TemporaryDirectory(prefix="ahrq-compendium-historical-receipt-") as temp_dir:
        temporary_paths: dict[str, Path] = {}
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=True)
            try:
                context = await browser.new_context(
                    user_agent=(
                        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
                    ),
                    extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
                )
                page = await context.new_page()
                landing_response = await page.goto(
                    release_spec.landing_page,
                    wait_until="networkidle",
                    timeout=60_000,
                )
                if landing_response is None or not landing_response.ok:
                    status = (
                        landing_response.status
                        if landing_response is not None
                        else "no_response"
                    )
                    raise RuntimeError(f"AHRQ landing-page acquisition failed: {status}")
                for retrieval_index in range(2):
                    for artifact_spec in release_spec.artifacts:
                        response = await context.request.get(
                            artifact_spec.source_url,
                            headers={
                                "Accept": "text/csv,application/csv;q=0.9,*/*;q=0.1",
                                "Referer": release_spec.landing_page,
                            },
                            timeout=60_000,
                        )
                        if not response.ok:
                            raise RuntimeError(
                                f"AHRQ acquisition failed for {artifact_spec.role}: {response.status}"
                            )
                        headers = response.headers
                        body = await response.body()
                        content_type = headers.get("content-type", "")
                        if not body or "csv" not in content_type.casefold():
                            raise RuntimeError(
                                "AHRQ acquisition did not return CSV bytes for "
                                f"{artifact_spec.role}: {content_type or 'missing'}"
                            )
                        if response.url != artifact_spec.source_url:
                            raise RuntimeError(
                                f"AHRQ final URL drift for {artifact_spec.role}: {response.url}"
                            )
                        retrievals[artifact_spec.role].append(body)
                        if retrieval_index == 0:
                            responses[artifact_spec.role] = AhrqResponseMetadata(
                                final_url=response.url,
                                status_code=response.status,
                                content_type=content_type,
                                etag=headers.get("etag", ""),
                                last_modified=headers.get("last-modified", ""),
                            )
            finally:
                await browser.close()

        downloaded = _verify_matching_retrievals(retrievals)
        for artifact_spec in release_spec.artifacts:
            temporary_path = Path(temp_dir) / artifact_spec.relative_path
            temporary_path.write_bytes(downloaded[artifact_spec.role])
            temporary_paths[artifact_spec.role] = temporary_path

        receipt = build_ahrq_historical_receipt(
            release,
            temporary_paths["system_universe"],
            temporary_paths["hospital_linkage"],
            retrieved_at=retrieved_at,
            responses=responses,
        )
        temporary_receipt = Path(temp_dir) / default_receipt_name
        write_ahrq_historical_receipt(temporary_receipt, receipt)
        receipt_bytes = temporary_receipt.read_bytes()

    _replace_validated_files(
        {
            destinations["system_universe"]: downloaded["system_universe"],
            destinations["hospital_linkage"]: downloaded["hospital_linkage"],
            destination: receipt_bytes,
        }
    )
    return destination


def _verify_matching_retrievals(
    retrievals: Mapping[str, list[bytes]],
) -> dict[str, bytes]:
    """Require exactly two byte-identical retrievals for every artifact role."""

    verified: dict[str, bytes] = {}
    for role, versions in retrievals.items():
        if len(versions) != 2:
            raise RuntimeError(f"AHRQ {role} requires exactly two retrievals")
        if versions[0] != versions[1]:
            raise RuntimeError(f"AHRQ repeated retrieval byte drift for {role}")
        verified[role] = versions[0]
    if set(verified) != {"system_universe", "hospital_linkage"}:
        raise RuntimeError("AHRQ repeated retrieval verification requires both artifact roles")
    return verified


def _replace_validated_files(files: Mapping[Path, bytes]) -> None:
    """Promote a validated release together, restoring prior files on failure."""

    if not files:
        raise ValueError("At least one validated file is required")
    transaction_id = uuid4().hex
    staged: dict[Path, Path] = {}
    backups: dict[Path, Path] = {}
    promoted: set[Path] = set()
    try:
        for destination, content in files.items():
            destination.parent.mkdir(parents=True, exist_ok=True)
            candidate = destination.with_name(
                f".{destination.name}.{transaction_id}.candidate"
            )
            write_atomic_bytes(candidate, content)
            staged[destination] = candidate

        for destination in files:
            if destination.exists():
                backup = destination.with_name(
                    f".{destination.name}.{transaction_id}.backup"
                )
                destination.replace(backup)
                backups[destination] = backup
            staged[destination].replace(destination)
            promoted.add(destination)
    except Exception as exc:
        rollback_errors: list[str] = []
        for destination in reversed(tuple(files)):
            try:
                if destination in promoted:
                    destination.unlink(missing_ok=True)
                backup = backups.get(destination)
                if backup is not None and backup.exists():
                    backup.replace(destination)
            except OSError as rollback_exc:
                rollback_errors.append(f"{destination.name}: {rollback_exc}")
        if rollback_errors:
            detail = "; ".join(rollback_errors)
            raise RuntimeError(
                f"AHRQ release promotion failed and rollback was incomplete: {detail}"
            ) from exc
        raise
    finally:
        for temporary_path in (*staged.values(), *backups.values()):
            temporary_path.unlink(missing_ok=True)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_CACHE_DIR)
    parser.add_argument("--receipt-path", type=Path)
    parser.add_argument(
        "--release",
        choices=SUPPORTED_AHRQ_RELEASE_KEYS,
        default=DEFAULT_AHRQ_RELEASE_KEY,
        help="Reviewed AHRQ source release (default: revised 2023)",
    )
    parser.add_argument("--force", action="store_true", help="Replace existing cache files only after validation passes")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    try:
        receipt_path = asyncio.run(
            acquire(
                args.output_dir,
                force=args.force,
                receipt_path=args.receipt_path,
                release=args.release,
            )
        )
    except Exception as exc:
        print(f"AHRQ acquisition failed closed: {exc}", file=sys.stderr)
        return 1
    print(f"AHRQ source receipt admitted: {receipt_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
