# Healthcare Data Platform v1 adoption

Status: bounded, test-only consumer adoption. This change does not acquire
source data, schedule work, add credentials, deploy a service, or activate a
production/runtime path.

## Exact pin

The consumer pin was admitted from the exact Data MCP dispatch base and the
reviewed Toolkit W6 candidate:

- Data MCP dispatch base: `50348e13c90c56a5b5726cbddf9155c0cc923e14`
- Toolkit W6 candidate head: `f5e60ae9eb33d117a621d0d1fdda5205c8e93690`
- Toolkit contract bundle: `bundle:healthcare-data-platform:v1`
- Bundle SemVer: `1.0.0` (compatibility floor `1.0`)
- Canonical bundle file: `contracts/healthcare-data-platform/bundle/v1/contract-bundle.json`
- Canonical bundle SHA-256: `sha256:dda28dc777b264892e9f0817bbf40fb73f12ff34d35115d4d3d4a146e73b371c`

The copied bundle artifacts are hash-verified against the Toolkit manifest:

| Artifact | Relative path | SHA-256 |
| --- | --- | --- |
| ontology source schema | `contracts/healthcare-data-platform/ontology/v1/ontology-delta.schema.json` | `sha256:5d4c9187421b232afad9fad31240396f1fcadf6acbcd30bfa5ae5572a3ec5eb5` |
| observation source schema | `contracts/healthcare-data-platform/observation/v1/observation-envelope.schema.json` | `sha256:75f3f28f6682cb1ab665aff7a036722ce0bd3cb4ed083c03101bffd53b951d6c` |
| fact/coverage source schema | `contracts/healthcare-data-platform/fact/v1/fact-coverage-projection-delta.schema.json` | `sha256:6ec20e6694b2b58dbf0cacdde086a540b9496c49adb8e722f5968461231a7250` |
| storage workload source schema | `contracts/healthcare-data-platform/storage/v1/storage-workload.schema.json` | `sha256:e5ace5ba95186712ff43ad33f3571098982c863cd4a4931726daa6b650dc98b4` |
| storage benchmark source schema | `contracts/healthcare-data-platform/storage/v1/storage-benchmark.schema.json` | `sha256:b647136b040f56f8cb98b410358b78b83e931477c8930bfb6cc9a6cf43606143` |
| bundle schema | `contracts/healthcare-data-platform/bundle/v1/contract-bundle.schema.json` | `sha256:38deffb645e7f1ebd054277e6d249df30b7125c0b11364f90c0303c7b23d2136` |
| generated OpenAPI view | `contracts/healthcare-data-platform/bundle/v1/openapi.yaml` | `sha256:48c738a9868861674620cea25409f1c1955d31b32913eb5390d7e7b6acb9fbd0` |
| generated Arrow logical view | `contracts/healthcare-data-platform/bundle/v1/arrow-schema.json` | `sha256:51c3907dc616668ae7bd6a07bd551d62df12e74a1f0de71979db6ccb37357d44` |

The bundle manifest remains the source of truth for artifact identity. The
consumer verifies its bytes, every listed artifact path and hash, the five
source-contract bindings, and the generated-view source hashes before using a
contract.

## Repository path mapping

The mission packet names Toolkit paths under
`services/healthcare-data-mcp/src/...` and
`services/healthcare-data-mcp/tests/...`. Those paths do not exist in this
Data MCP checkout. The bounded mapping is:

| Mission-packet path | Data MCP path used here |
| --- | --- |
| `services/healthcare-data-mcp/src/healthcare_data_mcp/contracts/**` | `shared/contracts/**` |
| Toolkit contract artifacts | `contracts/healthcare-data-platform/**` |
| `services/healthcare-data-mcp/tests/test_healthcare_data_platform_contract_adoption.py` | `tests/test_healthcare_data_platform_contract_adoption.py` |

Only the required v1 bundle/source schemas and the conformance fixtures are
copied. No source acquisition or generated-code rewrite is introduced.

## Consumer binding and compatibility policy

`shared.contracts.healthcare_data_platform` exposes the hash-pinned consumer
binding and fail-closed validators. It validates source-scoped observation
envelopes against the manifest's observation schema and preserves the
receipt/artifact/activity/source-scope lineage. The validator also requires
explicit missingness, deterministic replay lineage, immutable custody, and
disabled authority limits.

The v1 consumer accepts only bundle SemVer `1.0.x` at the pinned compatibility
floor. Unknown fields, unknown contract families, stale artifact hashes,
required-field removal, source-scope narrowing, temporal/unit semantic drift,
and unsupported breaking-change categories are rejected. Minor releases and
new artifact families require explicit consumer opt-in; no silent fallback or
implicit unit conversion is allowed.

`release.runtime_adoption` remains `not_adopted`; all authority limits remain
false. A future update must copy a reviewed bundle, update the pin and hashes
in one focused change, and pass the same schema, compatibility, and fixture
gate before human merge approval.

## Rollback

Revert the three focused commits in reverse order:

1. `test(contracts): run shared conformance fixtures`
2. `feat(contracts): add producer bindings and validation`
3. `chore(contracts): pin healthcare data contract v1`

This removes only the consumer pin, bindings, and tests; it does not delete
prior Data MCP contracts or alter runtime behavior.
