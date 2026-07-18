# Public transport contracts

Healthcare Data MCP owns acquisition, source normalization, receipts, cache
artifact lineage, and portable evidence bundles. It does not own Toolkit metric
keys, Scale formulas, strategic posture conclusions, product approval, or
database writes.

`v1/public-evidence-bundle.schema.json` is the released machine contract for
`ushso.public-evidence-bundle.v1`. The canonical producer implementation is
`shared.contracts.public_evidence`. Regenerate the schema with:

```bash
.venv/bin/python scripts/export_contract_schemas.py
```

Build a bundle from an acquired/normalized input packet without hand-editing an
intermediate artifact:

```bash
hc-mcp-contract build-public-evidence \
  --input contracts/v1/fixtures/public-evidence-input.json \
  --producer-commit "$(git rev-parse HEAD)" \
  --output /tmp/public-evidence-bundle.json
```

The fixture is synthetic contract evidence and must never be promoted as a
public health-system fact. Production callers must replace it with connector
outputs and real cache-artifact checksums while preserving the same schema.

The governed all-six roster and bed-basis slice uses three generated files:

- `v1/fixtures/scale-roster-bed-basis-acquisition.json` is the reviewed source,
  entity, extraction, missingness, and conflict specification.
- `v1/fixtures/scale-roster-bed-basis-frozen.json` is the portable manifest of
  the immutable cache run and mechanically extracted rows. It intentionally
  contains no local filesystem paths or official-site bytes.
- `v1/fixtures/scale-roster-bed-basis-input.json` is the mechanically generated
  `PublicEvidenceBundleInput` for a pinned producer checkout.

Acquire a new reviewed run or rebuild the checked-in input from frozen bytes:

```bash
python -m scripts.acquire_scale_roster_beds \
  --write-spec contracts/v1/fixtures/scale-roster-bed-basis-acquisition.json \
  --frozen contracts/v1/fixtures/scale-roster-bed-basis-frozen.json \
  --output contracts/v1/fixtures/scale-roster-bed-basis-input.json \
  --cache-root ~/.healthcare-data-mcp/cache \
  --cache-run-id <unique-run-id>

python -m scripts.acquire_scale_roster_beds \
  --spec contracts/v1/fixtures/scale-roster-bed-basis-acquisition.json \
  --frozen contracts/v1/fixtures/scale-roster-bed-basis-frozen.json \
  --output /tmp/scale-roster-bed-basis-input.json \
  --cache-root ~/.healthcare-data-mcp/cache \
  --offline
```

The slice contains source-local roster dispositions and basis-specific facility
bed observations only. It is not authority to count hospitals, roll up beds,
calculate Scale, or promote a result.

The remaining Scale input families use the additive field-neutral acquisition
workflow. The first cycle has two generated files:

- `v1/fixtures/scale-operating-revenue-acquisition.json` records the ordered
  all-six candidate/missingness matrix, raw-payload hashes, periods,
  definitions, boundaries, and open blockers.
- `v1/fixtures/scale-operating-revenue-input.json` is its deterministic Public
  Evidence Bundle input. Source-local candidate totals use a distinct measure
  ID; the actual `operating_revenue_usd` coverage remains
  `blocked_source_conflict` for every system.

The checked-in input fixture deliberately uses the forty-zero pre-merge
producer placeholder. A downstream handoff is valid only when the clean rebuild
CLI replaces it with the exact validated source commit.

Rebuild only with the frozen disposable cache present:

```bash
python -m scripts.acquire_scale_input_family \
  --family operating_revenue_usd \
  --source-commit <full-clean-checkout-sha> \
  --cache-root /tmp/scale-input-family-cache \
  --acquisition-output /tmp/scale-operating-revenue-acquisition.json \
  --evidence-output /tmp/scale-operating-revenue-input.json
```

HTTP-error custody evidence is recorded as blocked and is never treated as
source content. Every successfully retrieved payload must pass exact byte
length and SHA-256 verification before either generated file is emitted. Every
reported numeric candidate is also re-extracted from the frozen audited PDF at
its exact page, row label, period, units, boundary, column, and scale. The CLI
rejects dirty or commit-drifted source trees and writes only outside that tree.

Contract v1 is immutable. Add a new version and compatibility adapter for any
breaking change; do not silently change the meaning of existing fields.

The annual-discharges cycle uses the additive tabular acquisition contract
`v2/scale-tabular-input-family-acquisition.schema.json`. It preserves exact
AHRQ Compendium 2023 system/linkage rows and validated-cache hashes without
labeling the source audited or changing Public Evidence Bundle v1. The raw
CSVs remain outside Git; the generated acquisition is the portable hash-bound
extract.

Rebuild from a clean committed tree and the validated disposable cache:

```bash
python -m scripts.acquire_scale_input_family \
  --family annual_discharges \
  --source-commit <full-clean-checkout-sha> \
  --cache-root ~/.healthcare-data-mcp/cache \
  --acquisition-output /tmp/scale-annual-discharges-acquisition.json \
  --evidence-output /tmp/scale-annual-discharges-input.json
```

All six numeric values are source-local candidates only. The official
technical definition for `sys_dsch` was not present in the frozen local
custody, so all six coverage rows remain `blocked_source_conflict` and no
facility aggregation or Scale execution is permitted.

The physician-count cycle uses additive contract v3 at
`v3/scale-physician-count-acquisition.schema.json`. It preserves the exact
`total_mds` cells from the same validated AHRQ system CSV without broadening
the annual-specific v2 contract or treating an AHRQ count as a verified active
physician roster. The v3 fixture contains only the system CSV artifact and six
exact system rows; hospital linkage rows are not fabricated as
physician-count evidence.

```bash
python -m scripts.acquire_scale_input_family \
  --family physician_count \
  --source-commit <full-clean-checkout-sha> \
  --cache-root ~/.healthcare-data-mcp/cache \
  --acquisition-output /tmp/scale-physician-count-acquisition.json \
  --evidence-output /tmp/scale-physician-count-input.json
```

The six values remain source-local, nonapproved candidates. AHRQ's technical
definition, employed/affiliated/total basis, active-status rules,
deduplication, current organizational boundary, and post-vintage membership
are unresolved. All six coverage rows therefore remain
`blocked_source_conflict`; no physician aggregation, Scale execution, or
downstream authority is produced.
