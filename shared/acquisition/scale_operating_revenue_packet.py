"""Reviewed all-six operating-revenue acquisition declaration."""

from __future__ import annotations

from shared.acquisition.scale_input_family import (
    SYSTEM_SLUGS,
    ScaleInputFamilyAcquisition,
    build_acquisition,
)

ACQUIRED_AT = "2026-07-17T22:10:00Z"


def acquisition() -> ScaleInputFamilyAcquisition:
    """Return the immutable first-cycle revenue acquisition."""

    artifacts = [
        _artifact(
            "christianacare",
            "Christiana Care Health System and Affiliates FY2024 audited financial statements",
            "https://hscrc.maryland.gov/Documents/Strong%20als%20Folder/Audited%20Financials%20-%20ar-rev/FY%202024/ChristianaCare%20Union%20of%20Cecil%20%20FY2024%20AFS.pdf",
            "https://hscrc.maryland.gov/Pages/hospitals.aspx",
            "FY2024",
            "Christiana Care Health System and Affiliates; exact relationship to the frozen product roster requires review.",
            "PDF page 6, Total operating revenues and other support, 2024 column, thousands",
            "5ab7710494b4eb5f9a7cbba4d593d084c66f9a83013050f1aeda24d525725cd2",
            638476,
        ),
        _artifact(
            "jefferson-health",
            "Thomas Jefferson University FY2022 consolidated audited financial statements",
            "https://www.jefferson.edu/content/dam/academic/finance/documents/TJU%20FY2022%20Audited%20Financial%20Statements-10182022.pdf",
            "https://www.jefferson.edu/finance/finance-for-staff/university-tax-audit-compliance.html",
            "FY2022",
            "Thomas Jefferson University consolidated boundary is broader than the frozen Jefferson Health delivery-system roster.",
            "PDF page 6, Total operating revenues, gains and other support, 2022 column, thousands",
            "7e6702695d42e6235f347382a2af4c8b1509e211282e62b30005a72c00e92c70",
            683530,
        ),
        _artifact(
            "temple-health",
            "Temple University Health System June 30, 2025 Year-End Report (UNAUDITED)",
            "https://tuhsfinance.templehealth.org/CURRENT%20STATEMENTS/TUHS%20Investor%20Quarterly%20Report%20-%20June%2030%2C%202025.pdf",
            "https://tuhsfinance.templehealth.org/audited.html",
            "FY2025",
            "TUHS consolidated reporting boundary is unresolved against the frozen Temple Health product roster; the indexed audit URL was unavailable.",
            "PDF page 8, Revenues and other support without donor restrictions, 2025 column, thousands",
            "c512dac3a6da51cd0fdd9b23c6bbc85efe580f1dab1a65183506ee521ea34711",
            753181,
            audit_status="unaudited",
        ),
        _artifact(
            "penn-medicine",
            "University of Pennsylvania Health System FY2025 combined audited financial statements",
            "https://tupa-q-001.sitecorecontenthub.cloud/api/public/content/PHS-Audited-Financial-Statement.pdf",
            "https://www.pennmedicine.org/locations/entity/princeton-health/financial-reporting",
            "FY2025",
            "UPHS combined boundary includes Doylestown Health from April 1, 2025 and is not period-aligned with every other system.",
            "physical PDF page 6 (statement page 4), Total revenues and other support, 2025 column, thousands",
            "e83eab7626abc8f7e012297a70367f510a2ddb3548435fe16eed505a1ef00b55",
            2041738,
        ),
        {
            "artifact_id": "artifact:cooper-university-health-care:operating-revenue:fy2025",
            "system_slug": "cooper-university-health-care",
            "source_name": "The Cooper Health System FY2025 consolidated audited financial statements",
            "document_title": "The Cooper Health System Financial Statements 2025 and 2024",
            "audit_status": "unavailable",
            "source_url": "https://www.cooperhealth.org/sites/default/files/2026-06/Cooper%20Health%20System%20Financial%20Statements%2025%2024.pdf",
            "landing_page": "https://www.cooperhealth.org/cooper-university-health-care-financial-reporting",
            "source_period": "FY2025",
            "entity_boundary": "The public landing page lists the audit, but the governed connector received HTTP 403 and could not freeze the audited payload bytes.",
            "statement_locator": "HTTP 403 response; no source row admitted",
            "retrieved_at": ACQUIRED_AT,
            "http_status": 403,
            "media_type": "text/html",
            "payload_sha256": "sha256:33acb41a6489d077bb258d1b49dd1546a5e057263a2c470d0002a7c9102eff69",
            "content_length": 511,
            "rights_classification": "unknown_review_required",
            "custody_state": "blocked_http_response",
        },
        _artifact(
            "main-line-health",
            "Main Line Health System and Affiliates FY2022 audited financial statements",
            "https://emma.msrb.org/P21619447-P21247636-P21672202.pdf",
            "https://emma.msrb.org/",
            "FY2022",
            "Main Line Health System and Affiliates consolidated boundary is stale relative to FY2024/FY2025 candidates and requires roster reconciliation.",
            "PDF page 6, Total revenues, gains and other support, 2022 column, thousands",
            "f63496c6eddb610952d6b245cd07b3e33927a8542fd76ed0fe9001e14cdf20a0",
            610290,
            rights="public_domain",
        ),
    ]
    rows = [
        _candidate("christianacare", 3_080_783_000, "FY2024", "Total operating revenues and other support", "Christiana Care Health System and Affiliates", "period_alignment", "FY2024 candidate is not yet comparable to every system and its product-roster boundary remains unapproved.", page=6, period_marker="Years Ended June 30, 2024 and 2023", units_marker="(Dollars in thousands)", row_pattern=r"Total operating revenues and other support\s+(?P<value>3,080,783)\s+2,872,845", raw_value=3_080_783),
        _candidate("jefferson-health", 7_914_485_000, "FY2022", "Total operating revenues, gains and other support", "Thomas Jefferson University", "entity_boundary", "FY2022 university-consolidated candidate is stale and broader than the frozen Jefferson Health delivery roster.", page=6, period_marker="2022 2021", units_marker="(In Thousands)", row_pattern=r"Total operating revenues, gains and other support\s+(?P<value>7,914,485)\s+5,662,873", raw_value=7_914_485),
        _candidate("temple-health", None, "FY2025", "unaudited year-end revenues and other support context only", "Temple University Health System consolidated", "unaudited_source", "The Year-End Report is explicitly unaudited and cannot substitute for the unavailable indexed audit; no candidate value is admitted."),
        _candidate("penn-medicine", 11_995_614_000, "FY2025", "Total revenues and other support", "University of Pennsylvania Health System", "period_and_membership_boundary", "FY2025 includes a partial-year Doylestown membership substitution and lacks a common all-six period.", page=6, period_marker="Years Ended June 30, 2025 and 2024", units_marker="(thousands of dollars)", row_pattern=r"Total revenues and other support\s+(?P<value>11,995,614)\s+10,899,080", raw_value=11_995_614),
        _candidate("cooper-university-health-care", None, "FY2025", "reported consolidated operating revenue sought", "The Cooper Health System consolidated", "raw_byte_custody", "The official landing page identifies a current audit, but governed retrieval returned HTTP 403; no value is admitted."),
        _candidate("main-line-health", 2_107_785_000, "FY2022", "Total revenues, gains and other support", "Main Line Health System and Affiliates", "period_alignment", "FY2022 is stale relative to current candidates and its product-roster boundary remains unapproved.", page=6, period_marker="For the years ended June 30, 2022 and 2021", units_marker="(in thousands)", row_pattern=r"Total revenues, gains and other support\s+(?P<value>2,107,785)\s+1,984,631", raw_value=2_107_785),
    ]
    return build_acquisition(
        {
            "schema_version": "ushso.scale-input-family-acquisition.v1",
            "acquisition_id": "scale-operating-revenue-all-six-2026-07-17",
            "workflow_id": "scale-input-family-acquisition.v1",
            "input_family": "operating_revenue_usd",
            "systems": list(SYSTEM_SLUGS),
            "acquired_at": ACQUIRED_AT,
            "producer_version": "HDM-pzz",
            "source_artifacts": artifacts,
            "candidates": rows,
            "prohibited_outputs": [
                "scale_score",
                "component_score",
                "sensitivity_result",
                "projection",
                "adjudication",
                "recommendation",
                "promotion",
            ],
        }
    )


def _artifact(
    slug: str,
    name: str,
    url: str,
    landing_page: str,
    period: str,
    boundary: str,
    locator: str,
    sha256: str,
    content_length: int,
    *,
    rights: str = "unknown_review_required",
    audit_status: str = "audited",
) -> dict[str, object]:
    return {
        "artifact_id": f"artifact:{slug}:operating-revenue:{period.casefold()}",
        "system_slug": slug,
        "source_name": name,
        "document_title": name,
        "audit_status": audit_status,
        "source_url": url,
        "landing_page": landing_page,
        "source_period": period,
        "entity_boundary": boundary,
        "statement_locator": locator,
        "retrieved_at": ACQUIRED_AT,
        "http_status": 200,
        "media_type": "application/pdf",
        "payload_sha256": f"sha256:{sha256}",
        "content_length": content_length,
        "rights_classification": rights,
        "custody_state": "frozen_verified",
    }


def _candidate(
    slug: str,
    value: int | None,
    period: str,
    definition: str,
    basis: str,
    blocker: str,
    finding: str,
    *,
    page: int | None = None,
    period_marker: str = "",
    units_marker: str = "",
    row_pattern: str = "",
    raw_value: int | None = None,
) -> dict[str, object]:
    artifact_ref = f"artifact:{slug}:operating-revenue:{period.casefold()}"
    extraction = None
    if value is not None:
        extraction = {
            "artifact_ref": artifact_ref,
            "page_number": page,
            "period_marker": period_marker,
            "units_marker": units_marker,
            "definition_marker": definition,
            "basis_marker": basis,
            "row_pattern": row_pattern,
            "raw_value": raw_value,
            "scale_multiplier": 1000,
        }
    return {
        "system_slug": slug,
        "input_family": "operating_revenue_usd",
        "candidate_value": value,
        "unit": "USD",
        "source_period": period,
        "definition": definition,
        "basis": basis,
        "source_artifact_refs": [artifact_ref],
        "extraction": extraction,
        "missingness": "blocked_source_conflict",
        "blocker_codes": [blocker, "no_common_audited_period"],
        "finding": finding,
        "imputed": False,
        "approved_for_scale": False,
    }


__all__ = ["acquisition"]
