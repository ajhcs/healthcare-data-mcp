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

Contract v1 is immutable. Add a new version and compatibility adapter for any
breaking change; do not silently change the meaning of existing fields.
