# Health System Metrics QA Summary

Status: PASS WITH WARNINGS

## Source Files Tested

- /home/plumbob/.healthcare-data-mcp/cache/ahrq_system_2023.csv
- /home/plumbob/.healthcare-data-mcp/cache/ahrq_hospital_linkage_2023.csv

## Golden Counts

| Check | Count |
|---|---:|
| ahrq_system_rows | 639 |
| duplicate_health_sys_ids | 0 |
| ahrq_hospital_linkage_rows | 6800 |
| linked_hospitals | 4193 |
| linked_acute_hospitals | 3602 |
| linked_hospitals_with_ccn | 4184 |
| linked_hospitals_missing_ccn | 9 |
| linked_hospitals_with_hos_beds | 3719 |
| linked_hospitals_missing_hos_beds | 474 |
| linked_zips_with_leading_zero | 253 |
| linked_ccns_with_leading_zero | 645 |

## MCP Reconciliation

- Returned systems: 639
- Duplicate returned system IDs: 0
- Missing system IDs: 0
- Extra system IDs: 0
- Metric mismatch count: 0
- Page count: 7
- Stable snapshot IDs: True

## API / Tool Coverage

- all_639_systems_returned_once: PASS
- snapshot_id_stable: PASS
- all_system_metric_rows_reconcile: PASS
- exact_id_lookup_works: PASS
- fuzzy_exact_name_lookup_works: PASS
- include_facilities_true_returns_rows: PASS
- include_facilities_false_omits_payload: PASS
- ccn_and_zip_are_strings: PASS
- source_metadata_present: PASS
- snapshot_values_primary: PASS
- overlay_values_labeled: PASS
- invalid_argument_error: PASS
- state_filter_returns_systems: PASS
- legacy_get_system_profile_works: PASS
- legacy_build_profile_evidence_pack_works: PASS

## Warnings

- HCRIS bed-gap coverage is explicitly unavailable for this cache state.
- Profile evidence pack reported optional unavailable public findings.


## Performance Notes

- Full acceptance script elapsed seconds: 93.453

## Security / Read-Only Notes

- Acceptance calls were read-only MCP stdio calls.
- Raw AHRQ files were read independently; no production parser was imported by the reconciliation script.
- No PHI was used or generated.

## Required Fixes Before Release

- None blocking from this acceptance run.
