from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import socket

import httpx
import pytest

from scripts import download_irs_form_990_filing_indexes as downloader
from shared.acquisition.irs_form_990_filing_index_receipt import (
    INDEX_COLUMNS,
    IrsForm990FilingIndexReceipt,
    annual_index_url,
)

RETRIEVED_AT = datetime(2026, 8, 6, 12, 30, tzinfo=timezone.utc)
OBJECT_ID = "202523169349306307"


def _index_bytes(*, taxpayer_name: str = "PROVIDENCE ST JOSEPH HEALTH") -> bytes:
    row = (
        "",
        "EFILE",
        "811244422",
        "202412",
        "2025",
        taxpayer_name,
        "990",
        "93493316063075",
        OBJECT_ID,
        "2025_TEOS_XML_11C",
    )
    return (",".join(INDEX_COLUMNS) + "\n" + ",".join(row) + "\n").encode()


def _scope(path: Path) -> Path:
    path.write_text(
        json.dumps(
            {
                "authority": "source_custody_only_not_system_identity_or_public_release",
                "interpretation_limits": [
                    "Legal filer only, not a health system.",
                    "No ownership or affiliation inference.",
                    "No public or financial authority.",
                ],
                "queries": [
                    {
                        "ein": "811244422",
                        "expected_object_id": OBJECT_ID,
                        "filing_year": 2025,
                        "query_id": "ein-811244422-2024-2025",
                        "tax_period_year": 2024,
                    }
                ],
                "reviewed_at": "2026-08-06T12:28:36Z",
                "schema_version": "ushso.irs-form-990-filer-query-scope.v1",
                "scope_id": "test-exact-legal-filer-scope",
                "scope_status": "reviewed_source_query_scope",
                "source_basis": "Test-only exact legal-filer source query with no system relationship authority.",
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return path


def test_public_url_policy_rejects_non_irs_and_private_resolution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(ValueError, match="exact public HTTPS IRS"):
        downloader._validate_official_public_url("https://example.com/index_2025.csv")

    monkeypatch.setattr(
        socket,
        "getaddrinfo",
        lambda *_args, **_kwargs: [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", 443))],
    )
    with pytest.raises(ValueError, match="non-public address"):
        downloader._validate_official_public_url(annual_index_url(2025))


def test_download_rejects_redirects_without_following(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(downloader, "_validate_official_public_url", lambda _url: None)

    def redirect(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(302, headers={"location": "https://example.com/escape.csv"})

    with httpx.Client(transport=httpx.MockTransport(redirect)) as client:
        with pytest.raises(ValueError, match="redirects are not admitted"):
            downloader.download_official_annual_index(2025, tmp_path / "index.csv", client=client)
    assert not (tmp_path / "index.csv").exists()


def test_download_streams_csv_and_preserves_response_metadata(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(downloader, "_validate_official_public_url", lambda _url: None)
    body = _index_bytes()

    def response(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            request=request,
            headers={
                "content-type": "text/csv; charset=utf-8",
                "content-length": str(len(body)),
                "etag": "example-etag",
            },
            content=body,
        )

    destination = tmp_path / "index.csv"
    with httpx.Client(transport=httpx.MockTransport(response)) as client:
        metadata = downloader.download_official_annual_index(
            2025,
            destination,
            client=client,
            retrieved_at=RETRIEVED_AT,
        )

    assert destination.read_bytes() == body
    assert metadata.final_url == annual_index_url(2025)
    assert metadata.status_code == 200
    assert metadata.retrieved_at == RETRIEVED_AT
    assert metadata.etag == "example-etag"


def test_download_rejects_non_csv_and_size_overflow(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(downloader, "_validate_official_public_url", lambda _url: None)

    def non_csv(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            request=request,
            headers={"content-type": "text/html"},
            content=b"not csv",
        )

    with httpx.Client(transport=httpx.MockTransport(non_csv)) as client:
        with pytest.raises(ValueError, match="content type is not CSV"):
            downloader.download_official_annual_index(2025, tmp_path / "bad.csv", client=client)

    body = b"12345"

    def oversized(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            request=request,
            headers={"content-type": "text/csv"},
            content=body,
        )

    with httpx.Client(transport=httpx.MockTransport(oversized)) as client:
        with pytest.raises(ValueError, match="byte limit"):
            downloader.download_official_annual_index(2025, tmp_path / "large.csv", max_bytes=4, client=client)


def test_download_requires_exact_http_200(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        downloader,
        "_validate_official_public_url",
        lambda _url: frozenset({"93.184.216.34"}),
    )

    def partial(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            206,
            request=request,
            headers={"content-type": "text/csv"},
            content=_index_bytes(),
        )

    destination = tmp_path / "partial.csv"
    with httpx.Client(transport=httpx.MockTransport(partial)) as client:
        with pytest.raises(ValueError, match="exact HTTP 200"):
            downloader.download_official_annual_index(2025, destination, client=client)
    assert not destination.exists()


def test_connected_peer_must_match_validated_public_dns() -> None:
    class NetworkStream:
        def get_extra_info(self, _name: str) -> tuple[str, int]:
            return ("93.184.216.35", 443)

    response = httpx.Response(
        200,
        extensions={"network_stream": NetworkStream()},
    )

    with pytest.raises(ValueError, match="did not match validated public DNS"):
        downloader._validate_connected_peer(
            response,
            frozenset({"93.184.216.34"}),
            required=True,
        )


def test_repeated_retrieval_requires_exact_bytes(tmp_path: Path) -> None:
    first = tmp_path / "first.csv"
    second = tmp_path / "second.csv"
    first.write_bytes(_index_bytes())
    second.write_bytes(_index_bytes())

    downloader._require_byte_identity(2025, first, second)

    second.write_bytes(_index_bytes(taxpayer_name="DRIFTED NAME"))
    with pytest.raises(RuntimeError, match="byte drift"):
        downloader._require_byte_identity(2025, first, second)


def test_atomic_promotion_restores_previous_files_after_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_one = tmp_path / "source-one"
    source_two = tmp_path / "source-two"
    destination_one = tmp_path / "destination-one"
    destination_two = tmp_path / "destination-two"
    source_one.write_bytes(b"new-one")
    source_two.write_bytes(b"new-two")
    destination_one.write_bytes(b"old-one")
    destination_two.write_bytes(b"old-two")
    original_replace = Path.replace

    def fail_second_candidate(source: Path, target: Path) -> Path:
        if source.name.endswith(".candidate") and target == destination_two:
            raise OSError("injected promotion failure")
        return original_replace(source, target)

    monkeypatch.setattr(Path, "replace", fail_second_candidate)

    with pytest.raises(OSError, match="injected promotion failure"):
        downloader._replace_validated_files(
            {destination_one: source_one, destination_two: source_two},
            commit_marker=destination_two,
        )

    assert destination_one.read_bytes() == b"old-one"
    assert destination_two.read_bytes() == b"old-two"
    assert not tuple(tmp_path.glob(".*.candidate"))
    assert not tuple(tmp_path.glob(".*.backup"))


def test_acquire_bootstrap_and_expected_receipt_replay_are_idempotent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    scope_path = _scope(tmp_path / "scope.json")
    output_dir = tmp_path / "cache"
    receipt_path = tmp_path / "receipt.json"
    calls: list[int] = []

    def fake_download(
        filing_year: int,
        destination: Path,
        **_kwargs: object,
    ) -> downloader.IrsForm990ResponseMetadata:
        calls.append(filing_year)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(_index_bytes())
        return downloader.IrsForm990ResponseMetadata(
            final_url=annual_index_url(filing_year),
            status_code=200,
            content_type="text/csv",
            retrieved_at=RETRIEVED_AT,
        )

    monkeypatch.setattr(downloader, "download_official_annual_index", fake_download)

    admitted = downloader.acquire(
        output_dir,
        scope_path=scope_path,
        receipt_path=receipt_path,
        bootstrap=True,
        expected_receipt_path=None,
        force=False,
    )
    receipt_bytes = admitted.read_bytes()
    receipt = IrsForm990FilingIndexReceipt.model_validate_json(receipt_bytes)

    assert calls == [2025, 2025]
    assert receipt.assertions.selected_filing_count == 1
    assert (output_dir / "irs_form_990_index_2025.csv").read_bytes() == _index_bytes()

    calls.clear()
    downloader.acquire(
        output_dir,
        scope_path=scope_path,
        receipt_path=receipt_path,
        bootstrap=False,
        expected_receipt_path=receipt_path,
        force=True,
    )
    assert calls == [2025, 2025]
    assert receipt_path.read_bytes() == receipt_bytes


def test_acquire_requires_one_explicit_admission_mode(tmp_path: Path) -> None:
    scope_path = _scope(tmp_path / "scope.json")

    with pytest.raises(ValueError, match="choose exactly one"):
        downloader.acquire(
            tmp_path / "cache",
            scope_path=scope_path,
            receipt_path=tmp_path / "receipt.json",
            bootstrap=False,
            expected_receipt_path=None,
            force=False,
        )


def test_acquire_rejects_receipt_collision_and_in_repository_raw_custody(
    tmp_path: Path,
) -> None:
    scope_path = _scope(tmp_path / "scope.json")
    output_dir = tmp_path / "cache"
    collision = output_dir / "irs_form_990_index_2025.csv"

    with pytest.raises(ValueError, match="cannot collide"):
        downloader.acquire(
            output_dir,
            scope_path=scope_path,
            receipt_path=collision,
            bootstrap=True,
            expected_receipt_path=None,
            force=False,
        )

    with pytest.raises(ValueError, match="outside the Git repository"):
        downloader.acquire(
            downloader.ROOT / "contracts" / "unsafe-raw-custody",
            scope_path=scope_path,
            receipt_path=tmp_path / "receipt.json",
            bootstrap=True,
            expected_receipt_path=None,
            force=False,
        )


def test_destination_lock_rejects_a_concurrent_writer(tmp_path: Path) -> None:
    destination = tmp_path / "receipt.json"

    with downloader._exclusive_destination_locks((destination,)):
        with pytest.raises(RuntimeError, match="another IRS filing-index acquisition"):
            with downloader._exclusive_destination_locks((destination,)):
                raise AssertionError("nested acquisition must not obtain the lock")
