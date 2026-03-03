# IRS 990 E-File Index Integration — Design Document

**Date:** 2026-03-03
**Scope:** Fix `get_form990_details` to use IRS e-file index for XML URLs when ProPublica doesn't provide them

## Problem

ProPublica Nonprofit Explorer API v2 does not return `xml_url` in filing responses. The `get_form990_details` tool currently falls back to summary data only, missing Schedule H, officer compensation, and program descriptions.

## Solution

Use the IRS TEOS (Tax Exempt Organization Search) annual index CSV files to look up the OBJECT_ID for a given EIN and tax period, then construct the XML download URL.

## Data Source

- **Index URL:** `https://apps.irs.gov/pub/epostcard/990/xml/{YEAR}/index_{YEAR}.csv`
- **Index columns:** RETURN_ID, FILING_TYPE, EIN, TAX_PERIOD, SUB_DATE, TAXPAYER_NAME, RETURN_TYPE, DLN, OBJECT_ID
- **XML URL pattern:** `https://apps.irs.gov/pub/epostcard/990/xml/{YEAR}/{OBJECT_ID}_public.xml`
- **Auth:** None required
- **Availability:** 2019-present (IRS migrated from AWS S3 to apps.irs.gov)

## Changes

### `irs990_parser.py` — add two functions

```python
async def load_efile_index(year: str) -> pd.DataFrame
    # Download index CSV for given year, cache permanently
    # Cache key: "irs990_index_{year}", suffix ".csv"
    # Read with dtype=str, keep_default_na=False
    # Return DataFrame with normalized column names

async def lookup_xml_url(ein: str, tax_period: str) -> str | None
    # Extract year from tax_period (YYYYMM → YYYY, or YYYY directly)
    # Load index for that year
    # Filter by EIN column == ein
    # If multiple matches, prefer latest TAX_PERIOD
    # Construct URL from OBJECT_ID
    # Return URL or None
```

### `server.py` — modify `get_form990_details`

Insert between the ProPublica xml_url check and the fallback:

```python
# If no xml_url from ProPublica, try IRS e-file index
if not xml_url:
    xml_url = await lookup_xml_url(ein, tax_period) or ""

if xml_url:
    # existing XML download + parse path
    ...
```

## Caching

| Data | Cache | TTL |
|------|-------|-----|
| Index CSV (~10-20MB/year) | Filesystem | Permanent (immutable) |
| Individual 990 XMLs | Filesystem | Permanent (already cached) |
