# MR-Explore Extraction

Source: `/mnt/d/Coding Projects/MR-Explore`

This extraction keeps the reusable MCP and local data-platform pieces while
excluding the large raw hospital standard-charge CSV cache.

## Contents

- `docs/mcp-contract-v1.md`: proposed tool contract for AI-safe, paginated rate
  data queries.
- `docs/ai-mcp-pivot-plan.md`: staged plan for a local MCP-first healthcare
  data platform.
- `data-layer/`: importer, normalizer, dataset registry, DuckDB/parquet store,
  pack validator, and comparison prototypes.
- `query-core/`: guarded query service and limit policy prototypes.
- `health-intel/`: healthcare intelligence collector/report generator prototype.
- `templates/` and `data-templates/`: MRF and vendor template concepts.
- `examples/`: selected data-pack build and validation scripts.

## Likely Uses

- Convert the MCP contract into concrete Healthcare Data MCP server tools.
- Reuse query limit and response budgeting patterns for all AI-facing endpoints.
- Treat `data-layer/` as a reference implementation, not a drop-in module.
