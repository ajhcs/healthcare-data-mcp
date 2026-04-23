# Discovery Server

The discovery server exposes metadata resources and workflow prompts for the
healthcare-data-mcp server collection. It does not download data, import the
dataset loaders, or require API keys at import time.

## Module

```bash
hc-mcp discovery
```

Default transport is `stdio`. For HTTP transports, set the same environment
variables used by the other servers:

```bash
MCP_TRANSPORT=streamable-http MCP_HOST=127.0.0.1 MCP_PORT=8015 \
  hc-mcp discovery
```

The discovery server is registered in the `hc-mcp` launcher, Docker Compose,
and the checked-in local `.mcp.json` on port 8015.

## Resources

| URI | Description |
| --- | --- |
| `healthcare-data://datasets/catalog` | Catalog summary with dataset ids, owning servers, categories, grain, source systems, and common workflows. |
| `healthcare-data://datasets/{dataset_id}` | Full metadata for one dataset. |
| `healthcare-data://datasets/{dataset_id}/schema` | Dataset grain, identity fields, common fields, and join keys. |
| `healthcare-data://datasets/{dataset_id}/source` | Source system, source URLs, owning servers, and expected cache files. |
| `healthcare-data://cache/status` | Filesystem-only status for expected cache files under `~/.healthcare-data-mcp/cache`. |
| `healthcare-data://runbooks/cache` | List of available cache and source-audit runbooks. |
| `healthcare-data://runbooks/{runbook_id}` | One runbook by id. |

Useful dataset ids include:

```text
ahrq_health_system_compendium
cms_hospital_general_info
cms_provider_of_services
cms_hospital_quality
cms_hsaf
cms_medicare_claims_pufs
cms_price_transparency_mrf
docgraph_referrals
public_records
web_intelligence
workforce_labor
```

## Prompts

| Prompt | Purpose |
| --- | --- |
| `healthcare_market_scan` | Plan a market scan across system, facility, quality, claims, service-area, and demographics data. |
| `hospital_competitive_profile` | Build a competitive profile for a hospital CCN. |
| `service_line_opportunity` | Analyze market opportunity for a clinical service line. |
| `referral_leakage_review` | Plan physician referral leakage and network analysis. |
| `public_records_due_diligence` | Plan public-records and compliance due diligence. |

## Cache Status

`healthcare-data://cache/status` checks file existence, size, modified time, and
age for known cache paths. It does not create directories or validate file
contents. Paths with globs or templates, such as MRF directories and API
response cache files, are reported as `pattern` so clients know to inspect the
owning cache directory.

Status values:

| Status | Meaning |
| --- | --- |
| `ready` | File exists and is inside the configured TTL. |
| `stale` | File exists but is older than the configured TTL. |
| `missing` | File does not exist at the expected path. |
| `pattern` | Entry is a glob/template rather than one concrete file. |

## Client Flow

1. Read `healthcare-data://datasets/catalog` to choose datasets for the task.
2. Read `/schema` resources for join keys and grain before combining outputs.
3. Read `/source` resources for source URLs and cache file expectations.
4. Read `healthcare-data://cache/status` before long workflows to identify
   missing or stale local data.
5. Use the workflow prompts to generate a client-specific execution plan.
