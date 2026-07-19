# Scale essential-service designation count acquisition

## Mission and disposition

`HDM-tmj` is the seventh and final initial Data MCP acquisition slice in the
Scale packet sequence. It freezes source custody and the exact six-system
identity graph for `essential_service_designation_count`. It does not calculate
or propose a count. The result is six `unavailable_public` cells and six open
taxonomy/period/boundary conflicts.

The packet remains `blocked` with `human_review_required`. Scale scores,
sensitivities, projections, adjudications, recommendations, and promotions are
prohibited.

## Frozen lineage

The direct predecessor is the emergency-department cycle: Data feature
`95e7f51dfe9ec8c3f7b49e5145685fdc54df049c`, Data merge
`ec350c6a0b4ed62aefc9c6e5e1be0a0c0e6b5f62`, Toolkit binding merge
`1154c2bfc85f193b0bfc18773e12aa21ab4d2fba`, Agents review merge
`335f3f44c65554a6a0be67507db85a67784e4be5`, and Toolkit admission merge
`c4adbb0444ffac141247a170dd03a538a80855d3`. The v7 contract freezes the
remaining tracker pins and every predecessor packet, review, assurance, and
manifest root.

## Source custody and rights

Raw bytes remain outside Git. The contract records exact locators, byte sizes,
hashes, retrieval context, and rights:

- AHRQ Compendium 2023 system and hospital-linkage CSVs provide frozen identity
  and historical CCN membership only.
- CMS April 2026 PSF ZIP is frozen as
  `sha256:979aa5997d0e7cf309d2ce19b52aa500e62fd0df8df19f0d530ff3fa3924a3ba`.
  Its LRO Parquet member is separately frozen as
  `sha256:d35f2489bdd61279a3817a93282d72c1a014f301ac434f83b860415f5df68925`
  with its provider-record schema.
- CMS Claims Processing Manual Chapter 3, revision 13757, is frozen as
  `sha256:b02bd622dd8494a5120c2409d3d4cd48512df998d5a4b9efe354c6cc851a714c`.
- The CMS PSF release page is frozen as
  `sha256:fd7edb400dd79908c99d1f07ad943e3a97f0f0b08a7017b4dccd7f2663ceea76`.

The three CMS sources and the AHRQ hospital linkage are public-domain inputs.
The inherited AHRQ system artifact retains its existing
`unknown_review_required` rights classification. Anti-bot eCFR response bodies
were rejected and are not admitted.

## Non-countability decision

CMS `providerType` values and their effective/termination dates are facility
payment and handling context. They cannot become a product-system
essential-service designation count until all of the following are approved and
frozen: eligible codes, issuer reconciliation, effective/expiry treatment,
eligible facility classes, combination-code expansion and deduplication, a
current facility-to-product-system crosswalk, and a common period.

The implementation therefore prohibits provider-type aggregation,
combination-code expansion, stale AHRQ rollup, inclusion of expired or
terminated records, state/federal taxonomy mixing, narrative/service/safety-net
substitution, missing-as-zero, imputation, and fabricated zeroes.

## Handoff and verification

The public evidence bundle contains exactly ten ordered receipts: six AHRQ
identity receipts followed by AHRQ hospital linkage, CMS PSF, CMS manual, and
CMS release-page receipts. Every conflict references all ten. There are no
observations.

The additive v7 runtime contract, JSON Schema, acquisition fixture, and evidence
fixture are regenerated deterministically. Source-byte verification checks the
outer ZIP, the inner Parquet byte hash and required schema columns, manual-page
markers, release-page marker, and inherited AHRQ custody without committing raw
data. v1-v6 contract bytes remain unchanged.

After merge, the next serial owner may claim only the Toolkit binding bead once
this exact producer merge and artifact hashes have been verified.
