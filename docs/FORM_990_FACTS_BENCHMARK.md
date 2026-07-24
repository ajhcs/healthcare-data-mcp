# Form 990 plugin benchmark

Benchmark date: 2026-07-24. Primary artifact:
`qa/reports/form_990_plugin_benchmark.json`.

## Method

Four exact legal-filer questions were frozen before collection: The Methodist
Hospital 2024, Thomas Jefferson University Hospitals 2025, MaineHealth 2024,
and Rush University Medical Center 2025. Gold facts came from the already
ingested official XML store and were never placed in either prompt or run
directory.

Each condition used a fresh ephemeral Codex session, GPT-5.6 Sol with xhigh
reasoning, priority tier, live web availability, the same prompt, the same JSON
schema, a read-only sandbox, and an empty temporary working directory. The
plugin-enabled and plugin-disabled conditions alternated AB/BA. Before every
run, `codex plugin list` and `codex debug prompt-input` verified both persisted
enablement and skill visibility. The controller restored the original Codex
configuration byte-for-byte by SHA-256 and finished with the plugin enabled.

## Primary result

| Measure | Plugin enabled | Plugin disabled/native |
|---|---:|---:|
| Valid runs | 4/4 | 2/4 |
| Median end-to-end latency | 23.055 s | 290.548 s |
| Exact fact checks, scheduled | 40/40 (100%) | 20/40 (50%) |
| Exact provenance checks, scheduled | 112/112 (100%) | 40/112 (35.7%) |
| Coverage checks, scheduled | 16/16 | 8/16 |
| Scope-honest answers | 4/4 | 2/4 |
| Tool calls | 4 MCP | 62 MCP-backed research + 37 web |
| Input tokens | 266,452 | 2,750,654 |
| Cached input tokens | 162,816 | 2,547,968 |
| Output tokens | 4,508 | 18,148 |
| Reasoning output tokens | 1,212 | 8,276 |
| Observable MCP result bytes | 56,048 | 269,878 |
| Native web response bytes | not applicable | not observable |

The plugin condition was about 12.6 times faster by median full-session latency,
used about one tenth the input tokens, and made one direct fact call per case.
The two native runs that completed had all 20 fact checks correct but only
40/56 provenance checks; the Jefferson and Rush native runs reached the shared
300-second timeout and count as zero in scheduled accuracy. No wrong answer was
retried.

Native research used MCP-backed tools other than this plugin. Those calls are
valid native research activity: the Form 990 plugin was disabled and its skill
was absent before each native run. The retained JSONL summary did not preserve
per-server names for those calls, so the report does not attribute them more
specifically.

## Retrieval and cold validation

Across all 36 loaded returns, 2,000 warm local lookups measured 0.263 ms median
and 0.388 ms p95. That storage-only measure is roughly 1.1 million times faster
than the observed 290.548-second native model-session median, but it excludes
the model and MCP session overhead and should not be confused with the 23.055-
second plugin end-to-end result.

The final full cold ingestion of 36 official returns took 205.4 seconds,
validated exact member/header/hash provenance, and deleted all large archives.
Cold ingestion is operator-only and excluded from user-facing latency. See
`qa/reports/form_990_cold_ingestion_validation.json`.

## Failures and limits

An initial controller preflight used an obsolete Codex `-a` shorthand. All
eight processes exited before session startup, were excluded from the primary
benchmark, and remain recorded in
`qa/reports/form_990_plugin_benchmark_preflight_failed.json`.

The benchmark is small: four exact filers, one repetition, and one model/
reasoning configuration. Native web response byte counts are not exposed by
Codex JSONL, so they remain `null`; MCP result bytes and total JSONL/final
response bytes are the only observable byte measures. Results do not establish
brand resolution, affiliate rollups, national discovery, cross-system
comparison, or non-990 coverage.
