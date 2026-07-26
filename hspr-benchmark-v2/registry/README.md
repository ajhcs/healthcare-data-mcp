# Identity packet boundary

Packets use the narrow allowlist enforced by `hspr_benchmark.leakage`. They may
contain aliases, legal entities, EINs/CCNs, relationships, time bounds,
ambiguity warnings, and provenance for identity claims. They may not contain
financial values/metrics, audit status, report URLs, page/line locators,
answer-ready excerpts, query hints, resolver answer menus, or gold/scoring data.

The product registry's historical `Measurement`, `ScopeOption`, trend, and
financial-evidence routes are not valid v2 packets. Passing the example is not
sufficient: every real packet must also pass normalized comparison against the
sealed adjudicated key immediately before execution.

