# April 2026 Provider, Community, Research, and Compliance Expansion

Released: 2026-04-23

## Summary

This release expands `healthcare-data-mcp` with new public-data capabilities for provider enrollment and ownership, community health, research activity, and federal exclusion screening. It adds three MCP servers, extends `public-records`, and updates discovery, Docker, local MCP client config, docs, and smoke-test hooks.

## New Servers

- `provider-enrollment` on port `8017`: CMS PECOS-derived provider enrollment, hospital/SNF ownership, CHOW history, ownership graph tracing, and provider-control profiles.
- `community-health` on port `8018`: CDC PLACES measure search, geography profiles, comparisons, and market community profiles for counties, places, tracts, and ZCTAs.
- `research-trials` on port `8019`: NIH RePORTER project search/detail/funding profiles and ClinicalTrials.gov v2 study search/detail/research-activity profiles.

## Compliance Screening

`public-records` now includes:

- HHS OIG LEIE cache/download tooling for `UPDATED.csv`, normalized Parquet cache, metadata, NPI search, individual search, entity search, batch screening, and source metadata.
- SAM.gov Exclusions API v4 tooling for identifier/name search, batch screening, source metadata, missing-key handling, and API-key redaction in error metadata.

Exclusion results are screening support, not final identity verification. Responses preserve source metadata, match basis, match score where applicable, and caveat language. Batch tools reject SSN/EIN/TIN-style fields.

## Shared Foundation

- Added shared source catalog helpers for CMS `data.json` and Socrata catalog resolution with fixture-friendly manifests.
- Added shared identity normalization for NPI, CCN, UEI, PAC/enrollment IDs, names, state, ZIP, address, and conservative fuzzy matching.
- New dataset loaders use source discovery and manifests rather than introducing more dated CMS URL assumptions.

## Discovery, Docs, and Runtime

- Discovery metadata now includes PECOS enrollment/ownership/CHOW, CDC PLACES, NIH RePORTER, ClinicalTrials.gov, HHS OIG LEIE, and SAM.gov Exclusions datasets.
- Docker Compose, `.mcp.json`, `hc-mcp --list`, README, MCP client docs, discovery docs, and gateway docs now reflect the new servers and compliance tools.
- The gateway remains metadata-only for these datasets; it does not proxy full live screening/query workflows.
- `smoke_test.py` includes optional live checks gated by `HC_MCP_LIVE_EXPANSION`, `HC_MCP_LIVE_LEIE`, and `SAM_GOV_API_KEY`.

## Verification

Verified locally after implementation and review fixes:

```bash
.venv/bin/python -m pytest -q
# 176 passed

.venv/bin/python -m ruff check .
.venv/bin/python -m compileall -q servers shared tests smoke_test.py
docker compose config --quiet
python3 -m json.tool .mcp.json
.venv/bin/python -m servers._launcher --list
```

## Known Limits

- Live external source smoke tests were not run by default because the new live checks are intentionally gated and `SAM_GOV_API_KEY` may not be present.
- SAM `match_score` is a conservative local confidence score, not an upstream SAM.gov confidence score.
- Provider-enrollment first-release coverage focuses on Medicare FFS public provider enrollment plus hospital/SNF ownership and CHOW workflows.
- CDC PLACES market profiles start with county/ZCTA-style community estimates and do not represent patient-level facts.
- State Medicaid exclusion lists and LEIE online SSN/EIN verification automation remain out of scope.
