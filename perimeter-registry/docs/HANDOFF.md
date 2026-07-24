# Perimeter Registry handoff

## Boundary

This workspace owns a read-only, deterministic registry: system concepts, legal/reporting entities and identifiers, typed effective-dated relationships, evidence observations, document-specific reporting perimeters, representative facilities, explicit gaps, and request resolution. It contains the existing Jefferson fixture plus deliberately minimal Penn Medicine/UPHS and UPMC benchmark fixtures. Brand concepts remain distinct from legal filers; no fixture is an exhaustive entity census.

The workspace does **not** own Form 990 extraction, source crawling, an MCP/server/UI, national coverage, or production configuration. It does not read, import, move, deploy, or alter the existing 990 Evidence implementation or LAN service. No relationship is weakened to `affiliate`; unknown dates and missing inventories remain explicit.

## Future 990 Evidence contract

[`adapter.py`](../src/perimeter_registry/adapter.py) defines the entire future boundary:

- input: one exact `EIN` plus one `tax_period_year`;
- output: official-XML facts with filing URL and fact locator;
- invariant: the adapter never chooses a filer, reporting perimeter, or additive total;
- current state: protocol and validation only—no implementation, network call, service discovery, or deployment.

The Perimeter Registry must select and label the legal filer first. Separate filer results are non-additive unless a document-specific perimeter independently proves consolidation.

## Automation later

After a separate approval, automate only this evidence pipeline: ingest authoritative candidate documents; normalize identifiers and source periods; propose typed, effective-dated edges; run deterministic referential/schema checks; route unresolved or conflicting proposals to a small exception review; persist accepted observations; rerun fixture regressions. Normal lookup remains model-free. AI may assist the exception queue but cannot silently create an edge.

## Deferred

Deferred work is the actual 990 Evidence adapter, systematic Schedule R/H ingestion, complete post-merger legal-entity and obligated-group inventories, insurer statutory perimeter inventory, CMS layered-owner reconciliation, plan-alias history, any nationwide expansion, crawling, UI/API/MCP serving, and every production or global configuration change. The fixture intentionally leaves the delivery-only audited revenue gap unresolved.

## Next smallest expansion and gate

The next slice is one offline, injected adapter implementation exercised only against saved 990 Evidence responses for TJUH EIN `23-2829095` and one tax year. It should prove exact-EIN/year provenance and the non-additive rule without changing any perimeter edge. Calling the running LAN service, importing its code, expanding beyond the bounded fixtures, or deploying anything requires a new explicit approval.
