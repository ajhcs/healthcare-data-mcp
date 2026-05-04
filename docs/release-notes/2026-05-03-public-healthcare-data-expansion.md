# Public Healthcare Data Expansion

Released: 2026-05-03

This release expands public-source healthcare intelligence across PHC4,
PA/NJ/DE state hospital artifacts, public financial health, staffing,
throughput, and cyber incident enrichment. It keeps the original boundary:
public data only, no paid PHC4 discharge files, no internal imports, and no
unsupported MAP revenue-cycle KPI proxies.

## Public Cache Acquisition

- Added setup and discovery coverage for `state_health_data`,
  `phc4_public_reports`, `ahrq_hfmd`, `pa_hospital_reports`,
  `nj_hospital_public_data`, and `de_hospital_discharge`.
- Added acquisition flags for PHC4 public reports, AHRQ HFMD, PA hospital
  reports, NJ hospital public data, and DE hospital discharge artifacts.

## PHC4 and State Public Data

- Added PHC4 public report indexing with report title, type, year, artifact URL,
  landing page URL, publication date, and extracted table provenance where
  available.
- Added normalized PA/NJ/DE public state-health artifact metadata and query
  helpers with state, source, facility, metric, page/table, confidence, and
  source URL fields.
- Added structured operations helpers so throughput metrics can prefer state
  public fields, then public cost-report fields, then public summary/linkage
  sources.

## Finance, Staffing, and Throughput

- Added public financial health/community-benefit profiles using high-confidence
  public fields from HCRIS, IRS 990 Schedule H, and AHRQ HFMD caches.
- Added explicit confidence values for public financial metrics and regression
  tests preventing HFMA MAP KPI calculations or HCRIS days-in-A/R proxy output.
- Added staffing and productivity outputs for FTE, adjusted patient day, SNF
  nursing hours, resident FTE, staff-to-bed, staff-to-discharge, peer groups,
  provider-year selection, and case-mix-adjusted discharges per FTE only when
  required public fields are present.
- Added public throughput, ED volume, OR/procedure volume, and modality volume
  profiles with source ranking and provenance.

## Cyber Enrichment

- Added OCR enforcement action indexing, SEC cyber disclosure lookup support,
  PA/NJ/DE state breach notice source status, and CISA KEV vulnerability
  context.
- Cyber outputs now distinguish entity-match confidence, incident-type
  confidence, disclosed timelines, source type, and vulnerability context from
  actual incident evidence.
- Added `get_cyber_attestation_source_status` so broad CMS cybersecurity
  attestation requests return `not_publicly_available` unless a reviewed public
  attestation source is configured.

## Exact Source Boundaries

- Tightened `hospital_quality.get_quality_measure_rows` so AMI mortality,
  hospital-wide readmission, and CLABSI SIR require exact CMS measure rows and
  never substitute PHC4 mortality, HRRP condition readmissions, or HAC totals.
- Added ACGME source-status and exact Program Code lookup tools backed by
  imported public Program Search exports and adjacent metadata JSON.
- Added ClinicalTrials.gov sponsor and site inventory tools with explicit
  role/geography dedupe rules, unresolved location counts, truncation metadata,
  and ambiguity warnings.
- Added `docs/SOURCE_CAPABILITY_LEDGER.md` to document exact sources, caveats,
  and unsupported substitutions.

## Verification

- `.venv/bin/pytest -q`
- `.venv/bin/ruff check <touched files>`
- `git diff --check`
