# Healthcare Data MCP

Healthcare Data MCP turns public healthcare data into source-disciplined MCP tools for agents. The language below names the domain concepts that should stay consistent across servers, workflows, caches, gateways, and reports.

## Language

**Public Source**:
A publicly available healthcare, government, or public-record data source used as evidence for agent-facing results.
_Avoid_: data feed, upstream, source system

**Source-Backed Result Contract**:
The shared result shape that keeps public source evidence, source metadata, identity maps, row receipts, caveats, confidence, and next steps attached to healthcare facts.
_Avoid_: response format, output schema, payload wrapper

**Evidence Receipt**:
A structured receipt that records the public source, source period, retrieval/cache context, match basis, confidence, caveat, and next step for a healthcare fact.
_Avoid_: provenance blob, citation metadata, source note

**Identity Map**:
A source-scoped map of exact identifiers, candidate aliases, join keys, conflicts, and source-claim paths used to prevent unsafe merges across healthcare sources.
_Avoid_: entity map, ID object, matching metadata

**Source Claim Path**:
A traceability link from a result field or row to the evidence receipt and identity map that support that claim.
_Avoid_: pointer, reference path, provenance path

**Report-Ready Fact**:
A healthcare fact that is intended to be cited, routed through the live gateway, or copied into downstream reporting.
_Avoid_: final value, cited output, report row

**Live Gateway Provenance Boundary**:
The point where a source-backed result is allowed to leave local tool execution through the live gateway; report-ready facts crossing this boundary require full traceability through source claim paths.
_Avoid_: gateway validation, live API edge, provenance check

**No-Data Finding**:
A source-scoped result showing that a searched public source did not return usable records for the requested scope.
_Avoid_: negative result, absence proof, no match
