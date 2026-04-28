"""Audited financial statement PDF parsing helpers."""

from __future__ import annotations

from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any
import re

import httpx


_METRIC_LABELS: dict[str, tuple[str, ...]] = {
    "cash_and_cash_equivalents": ("Cash and cash equivalents",),
    "total_current_assets": ("Total current assets",),
    "total_assets": ("Total assets",),
    "total_current_liabilities": ("Total current liabilities",),
    "long_term_obligations": ("Long-term obligations",),
    "total_liabilities": ("Total liabilities",),
    "total_net_assets_without_donor_restriction": ("Total net assets without donor restriction",),
    "total_net_assets": ("Total net assets",),
    "net_patient_service_revenue": ("Net patient service revenue",),
    "insurance_premium_revenue": ("Insurance premium revenue",),
    "total_operating_revenues": ("Total operating revenues, gains and other support",),
    "total_operating_expenses": ("Total operating expenses",),
    "operating_income_loss": ("(Loss) Income from operations", "Income from operations", "Loss from operations"),
    "increase_in_net_assets": ("Increase in net assets",),
    "net_cash_used_in_operating_activities": (
        "Net cash used in operating activities",
        "Net cash provided by operating activities",
    ),
}


_STATEMENT_ANCHORS: dict[str, tuple[str, ...]] = {
    "balance_sheet": ("Consolidated Balance Sheets", "Consolidated Statements of Financial Position"),
    "operations": (
        "Consolidated Statements of Operations",
        "Consolidated Statements of Activities",
        "Consolidated Statements of Operations and Changes in Net Assets",
    ),
    "cash_flows": ("Consolidated Statements of Cash Flows",),
    "notes": ("Notes to Consolidated Financial Statements",),
}


def parse_audited_financial_pdf(url_or_path: str, entity_name: str, fiscal_year: int | str) -> dict[str, Any]:
    """Parse high-level audited financial statement metrics from a PDF URL or local path."""
    pages, source_url = _extract_pdf_pages(url_or_path)
    fiscal_year_text = str(fiscal_year)
    scale = _detect_scale(pages)
    metrics: dict[str, Any] = {}
    citations: dict[str, dict[str, Any]] = {}

    for metric, labels in _METRIC_LABELS.items():
        found = _find_metric(pages, labels)
        if not found:
            continue
        raw_value, page_number, label, line = found
        value = raw_value * scale
        metrics[metric] = value
        citations[metric] = {
            "page": page_number,
            "label": label,
            "snippet": line,
            "source_url": source_url,
            "fiscal_year": fiscal_year_text,
        }

    return {
        "entity_name": entity_name,
        "fiscal_year": fiscal_year_text,
        "source_url": source_url,
        "scale": "thousands" if scale == 1000 else "units",
        "metrics": metrics,
        "citations": citations,
        "page_anchors": _find_statement_anchors(pages),
        "pages_parsed": len(pages),
        "parser": "audited_financial_pdf.v1",
    }


def _extract_pdf_pages(url_or_path: str) -> tuple[list[tuple[int, str]], str]:
    source = str(url_or_path).strip()
    if not source:
        raise ValueError("url_or_path is required")

    if source.startswith(("http://", "https://")):
        with httpx.Client(follow_redirects=True, timeout=60.0) as client:
            response = client.get(source)
            response.raise_for_status()
        with NamedTemporaryFile(suffix=".pdf") as tmp:
            tmp.write(response.content)
            tmp.flush()
            return _read_pdf_pages(Path(tmp.name)), source

    path = Path(source).expanduser()
    if not path.exists() or not path.is_file():
        raise FileNotFoundError(f"PDF not found: {source}")
    return _read_pdf_pages(path), str(path)


def _read_pdf_pages(path: Path) -> list[tuple[int, str]]:
    try:
        from pypdf import PdfReader
    except ModuleNotFoundError as exc:
        if exc.name != "pypdf":
            raise
        raise RuntimeError(
            "pypdf is required to parse audited financial PDFs. "
            "Install the optional PDF parsing dependency with `pip install pypdf`."
        ) from exc

    reader = PdfReader(str(path))
    pages: list[tuple[int, str]] = []
    for index, page in enumerate(reader.pages, start=1):
        pages.append((index, page.extract_text() or ""))
    return pages


def _detect_scale(pages: list[tuple[int, str]]) -> int:
    joined = "\n".join(text for _, text in pages[:15])
    if re.search(r"\(\s*in\s+thousands\s*\)", joined, flags=re.IGNORECASE):
        return 1000
    return 1


def _find_metric(pages: list[tuple[int, str]], labels: tuple[str, ...]) -> tuple[int, int, str, str] | None:
    for page_number, text in pages:
        for raw_line in text.splitlines():
            line = _normalize_space(raw_line)
            for label in labels:
                if label.lower() not in line.lower():
                    continue
                if label.lower() == "total net assets" and "without donor" in line.lower():
                    continue
                values = _numbers_after_label(line, label)
                if values:
                    return values[0], page_number, label, line
    return None


def _numbers_after_label(line: str, label: str) -> list[int]:
    start = line.lower().find(label.lower())
    tail = line[start + len(label):] if start >= 0 else line
    values: list[int] = []
    for match in re.finditer(r"\(?\$?\s*-?\d[\d,]*\)?", tail):
        raw = match.group(0).replace("$", "").replace(",", "").replace(" ", "")
        negative = raw.startswith("(") and raw.endswith(")")
        raw = raw.strip("()")
        try:
            value = int(raw)
        except ValueError:
            continue
        values.append(-value if negative else value)
    return values


def _find_statement_anchors(pages: list[tuple[int, str]]) -> dict[str, dict[str, Any]]:
    anchors: dict[str, dict[str, Any]] = {}
    for anchor_name, labels in _STATEMENT_ANCHORS.items():
        for page_number, text in pages:
            normalized = _normalize_space(text)
            matched = next((label for label in labels if label.lower() in normalized.lower()), "")
            if matched:
                anchors[anchor_name] = {"page": page_number, "label": matched}
                break
    return anchors


def _normalize_space(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()
