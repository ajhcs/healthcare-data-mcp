# Payer source boundaries

Payer TOC candidates are enrollment observations, not population estimates.
The producer accepts these official CMS source families:

| Type of coverage | Source family | Denominator basis |
| --- | --- | --- |
| Medicare Advantage | `cms_ma_state_county_enrollment` | CMS enrollment by state/county/plan |
| Medicare Part D | `cms_part_d_state_county_enrollment` | CMS enrollment by state/county/plan |
| Marketplace | `cms_marketplace_effectuated_enrollment` | CMS effectuated enrollment by plan/geography |

Each candidate must retain the source period, geography, plan or contract
identifier, numerator when present, denominator value, and denominator scope.
Missing or unresolved plan, county, period, or boundary identity remains
structured in `unresolved_identifiers` or an explicit missingness state.

## Census boundary

Census and ACS population, insurance, and modeled-population rows may describe
the surrounding geography, but they do not identify payer enrollment and must
never supply a payer denominator. Such rows are marked for review with
`census_not_payer_denominator` and block the pack with
`blocked_source_conflict`; they are not silently converted into payer shares.

The payer value schema is producer-private because the generic bundle is frozen:
`contracts/healthcare-data-platform/payer/v1/payer-value.schema.json` (version
1.0.0, SHA-256
`83ed34cbcbe2a67f5339623fa157f0982b50e47ea6feca0aa208eea6a66a0900`).
Blocked-conflict references are caller-attested. The authority map must name
the exact observation ID, mark `authority_state=source_scoped`, bind source and
release, and include artifact and receipt references.

## Coverage boundary

Coverage is complete only when every requested TOC has a supported official
candidate. An empty source pass is `not_yet_researched`; missing requested
types after a non-empty pass are `unavailable_public`/unresolved. The evidence
pack is read-only and does not calculate payer mix or write profile values.
