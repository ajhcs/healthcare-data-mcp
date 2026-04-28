# General System Reconciliation

Status: Unreleased

## Summary

This update extends `health-system-profiler` facility reconciliation beyond the
curated Jefferson Health/LVHN merger ledger. Non-Jefferson systems now receive a
generic AHRQ/CMS facility ledger with stable report-facing fields.

## Highlights

- Added generic AHRQ Compendium plus CMS Provider of Services reconciliation for
  non-Jefferson systems.
- Attached `facility_reconciliation` to generic `get_system_profile` responses.
- Preserved the Jefferson/LVHN curated merger resolver as the special-case path.
- Standardized generic facility rows with `npi`, `subsystem`, `legacy_system`,
  `source_refs`, `confidence`, `active_status`, and `no_ccn_reason` where needed.
- Avoided loading the national provider-enrollment cache during generic profile
  generation; provider-enrollment cross-references remain limited to the explicit
  reconciliation tool path.

## Verification

- Local health-system-profiler tests passed.
- Full local test suite passed.
- PR CI passed for lint, typecheck, tests, and Docker.
