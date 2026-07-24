# Health-System Perimeter Registry

This isolated Python workspace is the first deterministic slice of the **Health-System Perimeter Registry**. It resolves the Jefferson brand to documented legal entities and document-specific reporting perimeters. It does not crawl, call an LLM, run a service, or integrate with **IRS 990 Evidence Service** (“990 Evidence”).

The inspectable source of truth is [`src/perimeter_registry/data/jefferson.json`](src/perimeter_registry/data/jefferson.json), governed by [`schema/registry.schema.json`](schema/registry.schema.json). Source URL, locator, source period, confidence, effective dates, and unresolved status travel with the records.

```python
from perimeter_registry import Registry

registry = Registry.jefferson()

menu = registry.resolve("Jefferson revenue")
enterprise = registry.resolve("Jefferson FY25 enterprise revenue")
historical_lvhn = registry.lookup_entity("LVHN", as_of="2025-07-01")
```

Every resolver result is a labeled `Resolution`; revenue requests never return a scalar by itself. Unknown intent produces one short clarification question.

## Local validation

```bash
python3 -m pytest tests -q
ruff check .
ruff format --check .
```

The package uses only the Python standard library at runtime. See [`docs/HANDOFF.md`](docs/HANDOFF.md) for boundaries, the future adapter contract, deferred work, and the next approval gate.
