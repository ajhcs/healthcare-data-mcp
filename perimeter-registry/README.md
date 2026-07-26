# Health-System Perimeter Registry

This isolated Python workspace is the first deterministic slice of the **Health-System Perimeter Registry**. It resolves documented brands, legal entities, facilities, and reporting perimeters for three bounded fixtures: Jefferson, Penn Medicine/UPHS, and UPMC. It does not crawl, call an LLM, run a service, or integrate with **IRS 990 Evidence Service** (“990 Evidence”).

The inspectable sources of truth are the JSON records in [`src/perimeter_registry/data`](src/perimeter_registry/data), governed by [`schema/registry.schema.json`](schema/registry.schema.json). Source URL, locator, source period, confidence, effective dates, and unresolved status travel with the records. The Penn and UPMC fixtures are deliberately small benchmark fixtures, not exhaustive entity censuses.

```python
from perimeter_registry import Registry

jefferson = Registry.jefferson()
penn = Registry.penn()
upmc = Registry.upmc()

enterprise = jefferson.resolve("Jefferson FY25 enterprise revenue")
penn_scope = penn.resolve("Penn Medicine or the whole University?")
plan_ein = upmc.resolve("What EIN should I use for UPMC Health Plan?")
```

Every resolver result is a labeled `Resolution`; revenue requests never return a scalar by itself. Unknown intent produces one short clarification question.

## Local validation

```bash
python3 -m pytest tests -q
ruff check .
ruff format --check .
```

The package uses only the Python standard library at runtime. Completed July
2026 evaluation artifacts are quarantined under [`benchmarks`](benchmarks) as
historical studies of **pre-resolved scope assistance**, not fair end-to-end
financial retrieval comparisons. There is no active agent-input packet. See
[`benchmarks/BOUNDARY.md`](benchmarks/BOUNDARY.md) for the data boundary and
[`docs/HANDOFF.md`](docs/HANDOFF.md) for the future adapter contract.
