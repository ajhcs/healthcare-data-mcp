"""Acquisition, parser, and adversarial tests for the Scale roster/bed slice."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import sys
from types import SimpleNamespace
from zipfile import ZIP_DEFLATED, ZipFile

import httpx
import pytest
from pydantic import ValidationError

from scripts.acquire_scale_roster_beds import main as acquisition_main

from shared.acquisition.scale_roster_bed_models import (
    AcquisitionSpec,
    EntitySpec,
    FactSpec,
    FrozenAcquisition,
    FrozenArtifact,
    SourceSpec,
)
from shared.acquisition.scale_roster_beds import (
    _extract_fact,
    acquire,
    build_bundle_input,
    verify_frozen_bytes,
)
from shared.contracts.public_evidence import build_public_evidence_bundle, canonical_sha256

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "scale_roster_beds"
SYSTEMS = [f"data-mcp:system:test-{index}" for index in range(6)]


def _source(*, parser_kind: str = "html", media: str = "text/html") -> SourceSpec:
    return SourceSpec(
        source_id="official",
        source_name="Official fixture",
        dataset_id="official-fixture",
        registry_id="fixture:official",
        registry_version="v1",
        url="https://www.pa.gov/example",
        landing_page="https://www.pa.gov/example",
        source_period="fixture",
        expected_media_type=media,
        rights_classification="public_domain",
        parser_kind=parser_kind,
    )


def _spec(*, source: SourceSpec | None = None, fact: FactSpec | None = None) -> AcquisitionSpec:
    chosen_source = source or _source()
    chosen_fact = fact or FactSpec(
        fact_id="identity:test-0",
        entity_id=SYSTEMS[0],
        measure_id="system_identity",
        value_type="string",
        unit="name",
        period_label="fixture",
        denominator_scope="public identity",
        source_id=chosen_source.source_id,
        row_locator="h1",
        match_basis="exact name",
        confidence="high",
        extraction_pattern="Example Health",
        literal_value="Example Health",
    )
    return AcquisitionSpec(
        bundle_id="fixture:scale-roster-beds",
        producer_version="0.4.0",
        systems=SYSTEMS,
        market={"scope": "test"},
        periods=["fixture"],
        sources=[chosen_source],
        entities=[
            EntitySpec(
                entity_id=entity_id,
                canonical_name=f"Test System {index}",
                entity_type="health_system",
                system_slug=f"test-{index}",
            )
            for index, entity_id in enumerate(SYSTEMS)
        ],
        facts=[chosen_fact],
    )


@pytest.mark.asyncio
async def test_acquire_freezes_then_reparses_bytes(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    body = (FIXTURES / "official.html").read_bytes()

    async def fake_request(*args: object, **kwargs: object) -> httpx.Response:
        request = httpx.Request("GET", "https://www.pa.gov/example")
        return httpx.Response(
            200,
            content=body,
            headers={"content-type": "text/html", "last-modified": "Wed, 16 Jul 2025 12:00:00 GMT"},
            request=request,
        )

    monkeypatch.setattr("shared.acquisition.scale_roster_beds.resilient_request", fake_request)
    spec = _spec()
    frozen = await acquire(spec, cache_root=tmp_path, cache_run_id="run-1")
    assert frozen.artifacts[0].portable_uri == "hc-cache://scale-roster-bed-basis.v1/run-1/official.html"
    assert frozen.artifacts[0].source_modified == datetime(2025, 7, 16, 12, tzinfo=timezone.utc)
    assert "/" not in frozen.model_dump(mode="json")["artifacts"][0].get("local_path", "")
    assert verify_frozen_bytes(spec, frozen, cache_root=tmp_path) == frozen

    bundle = build_public_evidence_bundle(build_bundle_input(spec, frozen))
    assert bundle.observations[0].value == "Example Health"
    assert bundle.sources[0].receipt.artifact == bundle.input_artifacts[0]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("final_url", "media_type", "message"),
    [
        ("https://evil.example/redirect", "text/html", "redirect escaped"),
        ("https://www.pa.gov/example", "application/pdf", "unexpected media type"),
    ],
)
async def test_acquire_rejects_redirect_escape_or_media_drift(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    final_url: str,
    media_type: str,
    message: str,
) -> None:
    async def fake_request(*args: object, **kwargs: object) -> httpx.Response:
        return httpx.Response(
            200,
            content=b"<h1>Example Health</h1>",
            headers={"content-type": media_type},
            request=httpx.Request("GET", final_url),
        )

    monkeypatch.setattr("shared.acquisition.scale_roster_beds.resilient_request", fake_request)
    with pytest.raises(ValueError, match=message):
        await acquire(_spec(), cache_root=tmp_path, cache_run_id="run-drift")
    workflow_root = tmp_path / "scale-roster-bed-basis.v1"
    assert not (workflow_root / "run-drift").exists()
    assert not (workflow_root / ".run-drift.partial").exists()


@pytest.mark.asyncio
async def test_acquire_propagates_inaccessible_source(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    async def fake_request(*args: object, **kwargs: object) -> httpx.Response:
        raise httpx.ConnectError("fixture source unavailable")

    monkeypatch.setattr("shared.acquisition.scale_roster_beds.resilient_request", fake_request)
    with pytest.raises(httpx.ConnectError, match="unavailable"):
        await acquire(_spec(), cache_root=tmp_path, cache_run_id="run-inaccessible")


@pytest.mark.asyncio
async def test_acquire_rejects_cache_run_path_traversal(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="portable path segment"):
        await acquire(_spec(), cache_root=tmp_path, cache_run_id="../escape")


def test_csv_parser_matches_exact_identifier_and_preserves_field() -> None:
    fact = FactSpec(
        fact_id="pos-bed",
        entity_id=SYSTEMS[0],
        measure_id="bed_count.cms_pos",
        value_type="integer",
        unit="beds",
        period_label="Q4 2025",
        denominator_scope="facility-level; basis=CMS POS BED_CNT",
        source_id="official",
        row_locator="PRVDR_NUM=390001; field=BED_CNT",
        match_basis="exact CCN",
        confidence="high",
        table_match={"PRVDR_NUM": "390001"},
        table_value_field="BED_CNT",
    )
    assert _extract_fact(fact, FIXTURES / "cms-pos.csv", "csv") == 125


def test_state_table_parser_matches_exact_license_id() -> None:
    fact = FactSpec(
        fact_id="state-licensed-bed",
        entity_id=SYSTEMS[0],
        measure_id="bed_count.licensed",
        value_type="integer",
        unit="beds",
        period_label="fixture",
        denominator_scope="facility-level; basis=state licensed_beds",
        source_id="official",
        row_locator="license_id=PA-001; field=licensed_beds",
        match_basis="exact state license ID",
        confidence="high",
        table_match={"license_id": "PA-001"},
        table_value_field="licensed_beds",
    )
    assert _extract_fact(fact, FIXTURES / "state-hospitals.csv", "csv") == 125


def test_xlsx_parser_uses_configured_header_and_exact_state_row(tmp_path: Path) -> None:
    path = tmp_path / "state.xlsx"
    shared_strings = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" count="6" uniqueCount="6">'
        "<si><t>State report title</t></si><si><t>license_id</t></si><si><t>facility_name</t></si>"
        "<si><t>licensed_beds</t></si><si><t>PA-001</t></si><si><t>Example Hospital</t></si></sst>"
    )
    worksheet = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>'
        '<row r="1"><c r="A1" t="s"><v>0</v></c></row>'
        '<row r="2"><c r="A2" t="s"><v>1</v></c><c r="B2" t="s"><v>2</v></c>'
        '<c r="C2" t="s"><v>3</v></c></row>'
        '<row r="3"><c r="A3" t="s"><v>4</v></c><c r="B3" t="s"><v>5</v></c>'
        '<c r="C3"><v>125</v></c></row></sheetData></worksheet>'
    )
    with ZipFile(path, "w", ZIP_DEFLATED) as workbook:
        workbook.writestr("xl/sharedStrings.xml", shared_strings)
        workbook.writestr("xl/worksheets/sheet1.xml", worksheet)
    fact = FactSpec(
        fact_id="state-xlsx-bed",
        entity_id=SYSTEMS[0],
        measure_id="bed_count.licensed",
        value_type="integer",
        unit="beds",
        period_label="fixture",
        denominator_scope="facility-level; basis=state licensed_beds",
        source_id="official",
        row_locator="license_id=PA-001; field=licensed_beds",
        match_basis="exact state license ID",
        confidence="high",
        table_match={"license_id": "PA-001"},
        table_value_field="licensed_beds",
    )
    assert _extract_fact(fact, path, "xlsx", header_row=2) == 125


def test_unavailable_public_requires_mechanical_absence_check() -> None:
    with pytest.raises(ValidationError, match="mechanically verified absence"):
        FactSpec(
            fact_id="missing-without-search",
            entity_id=SYSTEMS[0],
            measure_id="bed_count.declared",
            value_type="integer",
            unit="beds",
            period_label="fixture",
            denominator_scope="facility-level; basis=not established",
            missingness="unavailable_public",
            missingness_reason="No row was found.",
        )


def test_governed_offline_cli_requires_frozen_cache_bytes(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "acquire_scale_roster_beds.py",
            "--offline",
            "--frozen",
            str(tmp_path / "frozen.json"),
            "--output",
            str(tmp_path / "input.json"),
        ],
    )
    with pytest.raises(SystemExit, match="2"):
        acquisition_main()


@pytest.mark.parametrize(
    ("match", "field", "message"),
    [
        ({"PRVDR_NUM": "missing"}, "BED_CNT", "matched 0 rows"),
        ({"PRVDR_NUM": "390001"}, "MISSING_FIELD", "missing field"),
        ({}, "BED_CNT", "exactly one complete extractor"),
    ],
)
def test_csv_parser_rejects_missing_identifiers_or_fields(match: dict[str, str], field: str, message: str) -> None:
    payload = {
        "fact_id": "bad-pos-row",
        "entity_id": SYSTEMS[0],
        "measure_id": "bed_count.cms_pos",
        "value_type": "integer",
        "unit": "beds",
        "period_label": "Q4 2025",
        "denominator_scope": "facility-level; basis=CMS POS BED_CNT",
        "source_id": "official",
        "row_locator": "fixture",
        "match_basis": "exact CCN",
        "confidence": "high",
        "table_match": match,
        "table_value_field": field,
    }
    if not match:
        with pytest.raises(ValidationError, match=message):
            FactSpec.model_validate(payload)
        return
    fact = FactSpec.model_validate(payload)
    with pytest.raises(ValueError, match=message):
        _extract_fact(fact, FIXTURES / "cms-pos.csv", "csv")


def test_csv_parser_rejects_duplicate_shared_identity(tmp_path: Path) -> None:
    path = tmp_path / "duplicate.csv"
    path.write_text("PRVDR_NUM,BED_CNT\n390001,100\n390001,120\n", encoding="utf-8")
    fact = FactSpec(
        fact_id="duplicate-pos",
        entity_id=SYSTEMS[0],
        measure_id="bed_count.cms_pos",
        value_type="integer",
        unit="beds",
        period_label="Q4 2025",
        denominator_scope="facility-level; basis=CMS POS BED_CNT",
        source_id="official",
        row_locator="PRVDR_NUM=390001",
        match_basis="exact CCN",
        confidence="high",
        table_match={"PRVDR_NUM": "390001"},
        table_value_field="BED_CNT",
    )
    with pytest.raises(ValueError, match="matched 2 rows"):
        _extract_fact(fact, path, "csv")


def test_html_and_pdf_extractors_cover_page_text(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    html_fact = _spec().facts[0]
    assert _extract_fact(html_fact, FIXTURES / "official.html", "html") == "Example Health"

    pdf_path = tmp_path / "fixture.pdf"
    pdf_path.write_bytes(b"synthetic PDF fixture; reader is isolated below")
    page_text = (FIXTURES / "official-pdf-extract.txt").read_text(encoding="utf-8")
    monkeypatch.setattr(
        "shared.acquisition.scale_roster_beds.PdfReader",
        lambda path: SimpleNamespace(pages=[SimpleNamespace(extract_text=lambda: page_text)]),
    )
    bed_fact = FactSpec(
        fact_id="pdf-bed",
        entity_id=SYSTEMS[0],
        measure_id="bed_count.licensed",
        value_type="integer",
        unit="beds",
        period_label="fixture",
        denominator_scope="facility-level; basis=Licensed Beds",
        source_id="official",
        row_locator="page 1 table; field=Licensed Beds",
        match_basis="exact facility",
        confidence="high",
        extraction_pattern=r"Example Hospital\W+Licensed Beds\W+(?P<value>125)",
    )
    assert _extract_fact(bed_fact, pdf_path, "pdf") == 125
    drifted = bed_fact.model_copy(update={"extraction_pattern": r"Approved Beds\W+(?P<value>125)"})
    with pytest.raises(ValueError, match="pattern did not match"):
        _extract_fact(drifted, pdf_path, "pdf")


@pytest.mark.parametrize("raw", ["-1", "1.5", "NaN", "Infinity"])
def test_bed_parser_rejects_negative_fractional_or_nonfinite_value(tmp_path: Path, raw: str) -> None:
    path = tmp_path / "bad.csv"
    path.write_text(f"license_id,beds\nPA-001,{raw}\n", encoding="utf-8")
    fact = FactSpec(
        fact_id="bad-bed",
        entity_id=SYSTEMS[0],
        measure_id="bed_count.licensed",
        value_type="integer",
        unit="beds",
        period_label="fixture",
        denominator_scope="facility-level; basis=licensed",
        source_id="official",
        row_locator="license_id=PA-001; field=beds",
        match_basis="exact license",
        confidence="high",
        table_match={"license_id": "PA-001"},
        table_value_field="beds",
    )
    with pytest.raises((ValueError, OverflowError)):
        _extract_fact(fact, path, "csv")


def test_bed_fact_rejects_basis_loss() -> None:
    with pytest.raises(ValidationError, match="explicit basis"):
        FactSpec(
            fact_id="basis-loss",
            entity_id=SYSTEMS[0],
            measure_id="bed_count.licensed",
            value_type="integer",
            unit="beds",
            period_label="fixture",
            denominator_scope="facility-level",
            source_id="official",
            row_locator="fixture",
            match_basis="exact",
            confidence="high",
            extraction_pattern=r"(?P<value>125)",
        )


def test_spec_rejects_identity_collisions_and_unknown_edges() -> None:
    spec = _spec()
    with pytest.raises(ValidationError, match="duplicate entity_id"):
        AcquisitionSpec.model_validate({**spec.model_dump(mode="json"), "entities": [*spec.entities, spec.entities[0]]})
    bad_fact = spec.facts[0].model_copy(update={"entity_id": "missing"})
    with pytest.raises(ValidationError, match="unknown fact entity"):
        AcquisitionSpec.model_validate({**spec.model_dump(mode="json"), "facts": [bad_fact]})


def test_frozen_manifest_rejects_nonportable_locator_and_duplicate_artifacts() -> None:
    now = datetime.now(timezone.utc)
    artifact = FrozenArtifact(
        source_id="official",
        artifact_id="artifact:official",
        source_url="https://www.pa.gov/example",
        final_url="https://www.pa.gov/example",
        retrieved_at=now,
        media_type="text/html",
        checksum_sha256="sha256:" + "a" * 64,
        content_length=1,
        cache_run_id="run",
        portable_uri="hc-cache://scale-roster-bed-basis.v1/run/official.html",
        schema_fingerprint="sha256:" + "b" * 64,
    )
    with pytest.raises(ValidationError, match="portable_uri"):
        FrozenArtifact.model_validate({**artifact.model_dump(mode="json"), "portable_uri": "/tmp/raw.html"})
    with pytest.raises(ValidationError, match="duplicate frozen source_id"):
        FrozenAcquisition(
            acquired_at=now,
            cache_run_id="run",
            artifacts=[artifact, artifact.model_copy(update={"artifact_id": "artifact:other"})],
            extracted_facts=[],
        )
    second = artifact.model_copy(
        update={
            "source_id": "other",
            "artifact_id": "artifact:other",
            "portable_uri": "hc-cache://scale-roster-bed-basis.v1/run/other.html",
        }
    )
    with pytest.raises(ValidationError, match="duplicate artifact checksum"):
        FrozenAcquisition(
            acquired_at=now,
            cache_run_id="run",
            artifacts=[artifact, second],
            extracted_facts=[],
        )


@pytest.mark.asyncio
async def test_frozen_verification_rejects_content_and_receipt_drift(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    body = (FIXTURES / "official.html").read_bytes()

    async def fake_request(*args: object, **kwargs: object) -> httpx.Response:
        return httpx.Response(
            200,
            content=body,
            headers={"content-type": "text/html"},
            request=httpx.Request("GET", "https://www.pa.gov/example"),
        )

    monkeypatch.setattr("shared.acquisition.scale_roster_beds.resilient_request", fake_request)
    spec = _spec()
    frozen = await acquire(spec, cache_root=tmp_path, cache_run_id="run-tamper")
    raw = tmp_path / "scale-roster-bed-basis.v1" / "run-tamper" / "official.html"
    raw.write_bytes(body + b"tampered")
    with pytest.raises(ValueError, match="artifact content drift"):
        verify_frozen_bytes(spec, frozen, cache_root=tmp_path)

    raw.write_bytes(body)
    altered = frozen.model_copy(
        update={
            "extracted_facts": [
                frozen.extracted_facts[0].model_copy(
                    update={"normalized_content_checksum": canonical_sha256({"tampered": True})}
                )
            ]
        }
    )
    with pytest.raises(ValueError, match="parser output drifted"):
        verify_frozen_bytes(spec, altered, cache_root=tmp_path)
