# IRS Form 990 e-file XML research

Research checked 2026-07-24. This note supports a small post-ingestion lookup
service for a known EIN and tax period. It does not address brand resolution,
national bulk coverage, or cross-filer comparison.

## Recommended official source

Use the IRS Tax Exempt Organization Search (TEOS) Form 990-series XML
downloads as the source of truth. The IRS describes the feed as its most
recent Form 990-series filings on record, published as annual indices and
month/part XML ZIPs. The broader bulk-data page says TEOS datasets are updated
monthly.

- [IRS Form 990 series downloads](https://www.irs.gov/charities-non-profits/form-990-series-downloads)
- [IRS TEOS bulk data downloads](https://www.irs.gov/charities-non-profits/tax-exempt-organization-search-bulk-data-downloads)

For submission/download year `Y`, current official URLs follow this contract:

```text
Index:
https://apps.irs.gov/pub/epostcard/990/xml/{Y}/index_{Y}.csv

Batch:
https://apps.irs.gov/pub/epostcard/990/xml/{Y}/{XML_BATCH_ID}.zip

Member within the batch:
{OBJECT_ID}_public.xml
```

The annual CSV currently has:

```text
RETURN_ID,FILING_TYPE,EIN,TAX_PERIOD,SUB_DATE,TAXPAYER_NAME,
RETURN_TYPE,DLN,OBJECT_ID,XML_BATCH_ID
```

The [official TEOS data dictionary](https://www.irs.gov/pub/irs-tege/teos-data-dictionary-v2.csv)
defines:

- `TAX_PERIOD` as `YYYYMM`.
- XML `SUB_DATE` as only the year submitted to the IRS, not a full date.
- `DLN` as the IRS Document Locator Number.
- `OBJECT_ID` as another IRS system identifier.
- `XML_BATCH_ID` as the ZIP containing the return.
- `RETURN_ID` as an IRS system return identifier, while also warning that it
  is blank in the XML index. Current rows can be populated or blank, so it
  must remain nullable.

The IRS FAQ confirms that Object ID is the key used to find a return in the
download and lists Return ID, filing type, EIN, tax period, submission date,
taxpayer name, DLN, and Object ID as index fields.
([IRS TEOS bulk-data FAQ](https://www.irs.gov/charities-non-profits/tax-exempt-organization-search-teos-faqs))

Do not equate download year with tax year. Query identity should be the legal
filer EIN plus `TaxPeriodEndDt` (or index `TAX_PERIOD` during discovery);
submission/download year is separate provenance. A late-filed or amended
return can appear in a later annual feed.

The index distinguishes `990`, `990EZ`, `990PF`, `990T`, and amended variants
such as `990A`. The first extractor should require Form 990 and must not apply
Form 990 mappings to another form family. Preserve every same-EIN,
same-period match. For the small verified corpus, pin the intended return by
Object ID instead of inferring a "latest" filing from filename order; IRS
materials do not document a complete ordering rule for multiple same-period
XML submissions.

## Current schemas and mapping resources

The IRS Modernized e-File (MeF) system uses XML schemas plus separate
business rules. Multiple schema versions can be valid, and the IRS publishes
valid exempt-organization versions by tax year. Store the root
`Return/@returnVersion`; do not choose a mapping from the download year.

- [IRS MeF overview](https://www.irs.gov/e-file-providers/modernized-e-file-overview)
- [Current valid EO MeF schemas and business rules](https://www.irs.gov/e-file-providers/current-valid-xml-schemas-and-business-rules-for-exempt-organizations-and-other-tax-exempt-entities-modernized-e-file)

The currently documented public TEOS redacted package for Form 990 is the
2024 Form 990X package. It contains `ReturnHeader990x.xsd`, `IRS990.xsd`,
return-data schemas, and schedule schemas, with types, cardinality, labels,
and form line numbers.

- [IRS TEOS redacted schemas](https://www.irs.gov/charities-non-profits/tax-exempt-organization-search-teos-schemas)
- [2024 Form 990X redacted schema package](https://www.irs.gov/pub/irs-tege/990x-schema-2024v5.0.zip)

Two additional official mappings are useful for validation:

- [IRS TEOS annotated forms](https://www.irs.gov/charities-non-profits/tax-exempt-organization-search-teos-annotated-forms),
  including the [2024 annotated Form 990](https://www.irs.gov/pub/irs-tege/2024form990withfieldnames.pdf).
- [IRS TEOS data dictionaries and indices](https://www.irs.gov/charities-non-profits/tax-exempt-organization-search-teos-data-dictionaries-indices),
  including the [Form 990 2021-2024 field index](https://www.irs.gov/pub/irs-tege/form990index-2021-2024.csv).

The official [MeF stylesheets](https://www.irs.gov/e-file-providers/modernized-e-file-mef-stylesheets)
are useful for human verification but are unnecessary in the extraction
path.

The community-maintained
[IRS e-file Master Concordance File](https://github.com/Nonprofit-Open-Data-Collective/irs-efile-master-concordance-file)
is a compatible cross-version mapping aid when older years are added. It is
not an IRS artifact and must never override the XML, applicable XSD, or IRS
annotated form.

## Compact exact field map

Public instances use the `http://www.irs.gov/efile` namespace. Paths below
omit a prefix for readability, but the parser should match that namespace and
the complete anchored path. Local-name-only searches are unsafe because
`EIN`, `BusinessName`, and `PersonNm` recur in schedules and preparer data.

| Metric | Reported label / location | XML path |
|---|---|---|
| Legal filer | Filer business name | `/Return/ReturnHeader/Filer/BusinessName/BusinessNameLine1Txt` plus optional `BusinessNameLine2Txt` |
| EIN | Employer Identification Number | `/Return/ReturnHeader/Filer/EIN` |
| Period end | Tax period end date | `/Return/ReturnHeader/TaxPeriodEndDt` |
| Period begin | Tax period begin date | `/Return/ReturnHeader/TaxPeriodBeginDt` |
| Form type | Return type | `/Return/ReturnHeader/ReturnTypeCd` |
| Total revenue | Total revenue - current year; Part I line 12 | `/Return/ReturnData/IRS990/CYTotalRevenueAmt` |
| Total expenses | Total expenses - current year; Part I line 18 | `/Return/ReturnData/IRS990/CYTotalExpensesAmt` |
| Net assets | Net assets or fund balances, EOY; Part I line 22 | `/Return/ReturnData/IRS990/NetAssetsOrFundBalancesEOYAmt` |
| Compensation row | Part VII Section A | `/Return/ReturnData/IRS990/Form990PartVIISectionAGrp[n]` |
| Person and title | Part VII line 1a(A) | `.../PersonNm`, `.../TitleTxt` |
| Executive flags | Part VII line 1a(C) | `.../OfficerInd`, `.../KeyEmployeeInd` |
| Organization reportable compensation | Part VII line 1a(D) | `.../ReportableCompFromOrgAmt` |
| Related-organization reportable compensation | Part VII line 1a(E) | `.../ReportableCompFromRltdOrgAmt` |
| Other compensation | Part VII line 1a(F) | `.../OtherCompensationAmt` |

These names and form-line mappings are defined by the
[2024 redacted XSD](https://www.irs.gov/pub/irs-tege/990x-schema-2024v5.0.zip)
and repeated in the
[2021-2024 Form 990 field index](https://www.irs.gov/pub/irs-tege/form990index-2021-2024.csv)
and [annotated form](https://www.irs.gov/pub/irs-tege/2024form990withfieldnames.pdf).
The three financial fields are required in the 2024 Form 990 XSD; the
individual compensation elements are optional. Amounts use the schema's
`USAmountType`; normalize them as whole U.S. dollars while retaining the
original XML text.

## Deterministic top reported executive

IRS terminology distinguishes "reportable compensation" in columns (D) and
(E) from the estimated "other compensation" in column (F). The Form 990
instructions require Part VII people to be listed highest-to-lowest by the sum
of columns (D), (E), and (F). Expose both components and explicit derived
values:

```text
reportable_compensation_usd =
    ReportableCompFromOrgAmt + ReportableCompFromRltdOrgAmt

total_reported_compensation_usd =
    ReportableCompFromOrgAmt
  + ReportableCompFromRltdOrgAmt
  + OtherCompensationAmt
```

`total_reported_compensation_usd` is an application-defined composite that
matches the IRS ordering basis. Do not label the D+E+F result simply "IRS
reportable compensation."
([2025 Instructions for Form 990, Part VII](https://www.irs.gov/instructions/i990))

For the initial "executive" fact:

1. Consider Part VII rows with a `PersonNm` and either `OfficerInd` or
   `KeyEmployeeInd`.
2. Rank by the D+E+F total, computed rather than trusted from document order.
3. Break exact ties by original row order and preserve the row ordinal in the
   XML path.
4. Return name, title, role flags, all three reported components, D+E
   reportable compensation, and D+E+F total compensation.

This intentionally excludes a person marked only as a director/trustee or
only as `HighestCompensatedEmployeeInd`; the IRS treats officers, key
employees, and the five other highest-compensated employees as separate
categories.
([IRS Part VII people included](https://www.irs.gov/charities-non-profits/form-990-part-vii-and-schedule-j-reporting-executive-compensation-individuals-included))

Persist an omitted optional amount as `null`. A derived total can treat an
absent component as zero only if at least one of D/E/F exists, while retaining
the null raw value. If all three are absent, total compensation is missing,
not zero. This preserves the difference between an explicit XML zero and an
omitted optional field.

## Provenance and integrity

Store at least these filing-level fields:

```text
source_kind = "irs_teos_efile_xml"
index_url
index_download_year
index_fetched_at
filing_type
return_id                 # nullable
dln
object_id
xml_batch_id
source_url                # official ZIP URL
source_member             # {OBJECT_ID}_public.xml
xml_sha256                 # exact raw member bytes
xml_namespace
xml_return_version         # /Return/@returnVersion
xml_return_timestamp       # /Return/ReturnHeader/ReturnTs, nullable
```

Store these fact-level fields:

```text
legal_filer_name_line_1
legal_filer_name_line_2    # nullable; join only for display
ein
tax_period_begin
tax_period_end
form_type
metric_key
reported_label
value
units
xml_path
source_member
xml_sha256
```

For a derived executive total, provenance is the repeated-row path plus the
three component paths and values, not a fictitious single XML field.

TEOS does not publish a checksum in its index. It also does not expose the MeF
`SubmissionId` in the public index or public Form 990 return header. The
official dictionary instead documents Return ID, DLN, and Object ID. Compute
SHA-256 over the unmodified extracted XML member and store each IRS identifier
under its real name; do not relabel Object ID or DLN as a submission ID.
([IRS TEOS data dictionary](https://www.irs.gov/pub/irs-tege/teos-data-dictionary-v2.csv);
[2024 redacted XSD](https://www.irs.gov/pub/irs-tege/990x-schema-2024v5.0.zip))

Because TEOS is updated, record fetch time and hash both the annual index
snapshot and every extracted XML member. That distinguishes a newly published
return from a changed source artifact.

## Deterministic validation cases

- Normalize input EIN to exactly nine digits, then verify index EIN,
  `TAX_PERIOD`, and `RETURN_TYPE` against the XML header.
- Require `/Return/ReturnData/IRS990`; reject an out-of-scope form family.
- Preserve multi-line legal filer names; do not replace them with the index's
  potentially truncated `TAXPAYER_NAME`.
- Test negative financial amounts.
- Test explicit zero separately from an omitted compensation element.
- Test several Part VII rows, executive role filtering, related-organization
  compensation, other compensation, and tie-breaking.
- Test no qualifying compensated officer/key employee as sourced missing data,
  not zero and not the best-paid director.
- Test same EIN/tax period with an amended return and preserve both source
  records until a verified Object ID is selected.
- Benchmark warm `(EIN, tax-period end)` retrieval only. Index discovery,
  network download, decompression, hashing, and parsing are cold ingestion.

## Empirical check

On 2026-07-24, the official
[2026 index](https://apps.irs.gov/pub/epostcard/990/xml/2026/index_2026.csv)
and [January batch](https://apps.irs.gov/pub/epostcard/990/xml/2026/2026_TEOS_XML_01A.zip)
were inspected. Members followed `{OBJECT_ID}_public.xml`. Form 990 member
`202630139349300908_public.xml` contained the namespace, versioned root,
header paths, financial fields, and repeated Part VII compensation rows
documented above. Stable tests should use small source-attributed fixtures,
not the live ZIP.
