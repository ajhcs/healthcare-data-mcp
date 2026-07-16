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

Contract v1 is immutable. Add a new version and compatibility adapter for any
breaking change; do not silently change the meaning of existing fields.
