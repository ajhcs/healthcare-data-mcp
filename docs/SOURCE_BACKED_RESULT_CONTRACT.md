# Source-Backed Result Contract

Healthcare Data MCP results that cross the live gateway provenance boundary must be traceable from report-ready fact to public source evidence.

## Canonical Terms

- **Source-Backed Result Contract**: the shared result shape that keeps evidence, source metadata, identity maps, row receipts, caveats, confidence, and next steps attached to healthcare facts.
- **Evidence Receipt**: the public-source receipt for a fact.
- **Identity Map**: the source-scoped map of exact identifiers, candidate aliases, join keys, conflicts, and source-claim paths.
- **Source Claim Path**: the traceability link from a result field or row to the evidence receipt and identity map that support the claim.
- **Report-Ready Fact**: a healthcare fact intended to be cited, routed through the live gateway, or copied into downstream reporting.
- **Live Gateway Provenance Boundary**: the point where report-ready facts must have full traceability before leaving local tool execution.

## Rule

Exploratory tool output may remain compatible with existing result shapes while it is being migrated.

Report-ready facts crossing the live gateway provenance boundary require:

- `identity_map`
- `identity_map.source_claims[]`
- `source_claims[].evidence_path`
- `source_claims[].source_metadata_path`
- `source_claims[].row_evidence_paths[]` when a cited value comes from a row collection
- contentful evidence receipts at every referenced evidence path

Use `shared.utils.source_backed_result.validate_source_claim_paths(..., require_boundary_traceability=True)` before a result crosses the boundary.

## Top-Level Fact

```python
from shared.utils.source_backed_result import source_claim

payload["identity_map"] = {
    "source_claims": [
        source_claim(
            collection="cms_provider_of_services",
            evidence_path="evidence",
            source_metadata_path="source_metadata",
        )
    ]
}
```

## Row-Level Fact

```python
payload["identity_map"] = {
    "source_claims": [
        source_claim(
            collection="cms_provider_of_services",
            evidence_path="evidence",
            source_metadata_path="source_metadata",
            row_evidence_paths=("results[].evidence",),
            identity_paths=("results[].identity",),
        )
    ]
}
```

## Migration Guidance

Migrate one tool or server at a time.

1. Add `source_metadata_path` to existing `identity_map.source_claims[]`.
2. Replace singular `row_evidence_path` with plural `row_evidence_paths` when touching a tool, but keep compatibility where needed.
3. Ensure row collections have row-level `evidence` before treating their values as report-ready facts.
4. Add `validate_source_claim_paths(..., require_boundary_traceability=True)` tests for migrated report-ready or live-gateway-bound outputs.
5. Only then move implementation code behind deeper modules such as public source catalog, health system profiling, public records, or live gateway policy.

Do not start by moving large server files. Add traceability through the shared module first, then refactor behind that seam.

See [Source-Backed Result Migration Plan](SOURCE_BACKED_RESULT_MIGRATION_PLAN.md) for the parallel migration waves and test gates.
