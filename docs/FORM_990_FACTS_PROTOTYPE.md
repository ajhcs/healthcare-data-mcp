# LAN-only Form 990 exact-facts prototype

This prototype answers one deliberately narrow question: for an exact
legal-filer EIN and tax-period end year that an operator already ingested, what
did that Form 990 report? It does not resolve brands, roll up affiliates,
compare systems, offer user-side ingestion, or expose a customer UI.

## Source and fact contract

The source of truth is official IRS Tax Exempt Organization Search e-file XML:

- [IRS Form 990 series downloads](https://www.irs.gov/charities-non-profits/form-990-series-downloads)
- [IRS TEOS schemas](https://www.irs.gov/charities-non-profits/tax-exempt-organization-search-teos-schemas)
- [IRS TEOS data dictionaries and indices](https://www.irs.gov/charities-non-profits/tax-exempt-organization-search-teos-data-dictionaries-indices)

Cold ingestion reads the annual `index_<year>.csv`, selects an exact EIN, tax
period, return type `990`, and pinned `OBJECT_ID`, then downloads the official
archive containing that indexed XML member. The large archives are deleted
after ingestion unless an operator explicitly keeps them.

SQLite retains:

- legal filer, EIN, tax-period end, form type, and filing/index year;
- actual official source ZIP URL and exact XML member;
- IRS `OBJECT_ID`, `RETURN_ID`, DLN, and actual XML archive part;
- SHA-256 of the exact XML bytes;
- reported label, compact metric key, exact value and units;
- namespace-independent XML source claim path, including all three compensation
  component paths for the calculated Part VII total.

The initial fact set is legal filer/reporting period, total revenue, total
expenses, end-of-year net assets or fund balances, and the highest-compensated
Part VII officer or key employee. Total reported compensation is defined as
Part VII columns D + E + F. Trustee/director-only and highest-compensated-
employee-only rows are not treated as executives. Explicit aggregate Part VII placeholders such as count-plus-role group rows are not treated as people; when the exact filer reports no individual officer or key employee, the executive fact is `not_reported` even if the filing points to a different-EIN group return. Missing elements are never converted to zero.

## Curated corpus

`configs/irs990-prototype-filers.csv` pins 36 official returns for 16 exact
legal filers. Every filer has its two most recent available electronic Form 990
periods; four compatible fiscal-year filers include a third period. Official
tax-period-2025 returns are present only for Thomas Jefferson University
Hospitals, Rush University Medical Center, CommonSpirit Health, CHRISTUS Health
Plan, and Luminis Health in the 2026 index snapshot checked on 2026-07-24.

The researched identities, diversity evidence, exact Object IDs, merger and
public-system limits, and current filing availability are in
`docs/research/form-990-expanded-corpus.md`.

The 2025 and 2026 IRS May downloads are split into `05A` and `05B` even though
their indexes collapse rows under `05A`; ingestion checks the bounded official
part B only when the indexed part A lacks the exact member, and stores the
actual source URL. Some part-B members use Deflate64; the operator path uses the
host `unzip` utility only when Python's ZIP reader reports that unsupported
method, then applies the same size, XML, EIN, period, form, path, and hash
validation.

## Operator-only ingestion

```bash
python3 scripts/ingest_irs_990_facts.py \
  --manifest configs/irs990-prototype-filers.csv \
  --database .local/irs990-facts.sqlite3
```

Use an explicit Object ID whenever the official index contains amendments or
multiple exact Form 990 rows for the same EIN/year. The command rejects
mismatches between the requested EIN/year and the XML header. Users never run
this command and the MCP server has no ingestion tool.

## LAN query interface

The MCP server exposes only `get_form_990_facts(ein, tax_year)`. It is annotated
read-only, non-destructive, idempotent, and closed-world because warm queries
touch only the pre-ingested local store.

Before binding, inspect listeners with `ss -tlnp`. Then use Plumbob's literal
private address:

```bash
HC_990_FACTS_DB=.local/irs990-facts.sqlite3 \
MCP_TRANSPORT=streamable-http \
MCP_HOST=192.168.1.60 \
MCP_PORT=8022 \
python3 -m servers.form_990_facts.server
```

The server rejects wildcard, public, and hostname bind values, including
`0.0.0.0` and `::`. It is not included in gateway exposure metadata. Do not add
public DNS, tunnels, external firewall rules, Caddy, or Cloudflare routes.

The private Codex plugin and persistent user-service runbook are documented in
`docs/FORM_990_FACTS_PLUGIN.md`.

## Validation and benchmarking

Run deterministic tests:

```bash
pytest -q tests/servers/form_990_facts tests/plugins
```

Run the warm local query benchmark after ingestion:

```bash
python3 scripts/benchmark_irs_990_facts.py \
  --database .local/irs990-facts.sqlite3 \
  --manifest configs/irs990-prototype-filers.csv
```

Cold official archive retrieval is reported separately and is not part of
user-facing query latency. The sealed model benchmark keeps gold answers
outside both conditions and compares matched GPT-5.6 plugin-enabled versus
plugin-disabled/native research runs for fact accuracy, provenance, coverage,
scope honesty, latency, calls, observable bytes, and observable tokens.
