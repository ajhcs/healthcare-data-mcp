# MCP Client and Packaging Notes

## Current Position

This repo supports two practical modes:

- Local stdio: `hc-mcp <server-name>` for Claude Desktop, Claude Code, Codex, and other local MCP clients.
- Local Streamable HTTP: `docker compose up --build`, then use `configs/http-clients.json` with the localhost ports.

OpenAI API and ChatGPT connectors require remote MCP servers reachable over HTTP/SSE or Streamable HTTP. This repo includes a local-safe metadata gateway with bearer-token hooks, Host/Origin validation, and `search`/`fetch` tools, but production deployment still needs HTTPS termination and edge identity policy.

## Best-Practice Checklist

- Keep server names short and descriptive so agents choose the right server.
- Prefer stdio for local filesystem/development use and Streamable HTTP for shared or remote clients.
- Use `.mcp.json` for project-scoped Claude Code configuration when the same servers should be shared by the team.
- Use Codex's own MCP config for Codex CLI/App; `.mcp.json` is not its primary config.
- Keep secrets out of shared config; use environment variables and `.env.example`.
- Return bounded tool outputs. Large MCP outputs can overwhelm the model context and trigger client warnings.
- For OpenAI remote MCP/deep research style integrations, expose `search` and `fetch` when the server acts as a retrievable knowledge source.
- Require approval for sensitive actions and narrow `allowed_tools` when connecting through OpenAI Responses API.
- Treat prompt injection as a first-class risk when tools fetch web pages or external documents.
- Use structured schemas, specific tool descriptions, and failure messages with actionable recovery steps.
- Validate with MCP Inspector plus the target clients, not only unit tests.
- Bind local HTTP servers to `127.0.0.1` by default. Use `MCP_HOST=0.0.0.0` only for containers or trusted reverse-proxy deployments.

## One-Time Setup

Install and create `.env`:

```bash
python3 -m pip install -e ".[dev]"
hc-mcp-setup --interactive
hc-mcp --list
```

The setup wizard and universal installer prompt from the canonical registry's
environment-key metadata, preserve existing values, avoid echoing secret inputs,
validate important settings, and can print client snippets:

```bash
hc-mcp-setup --validate-only
hc-mcp-setup --print-client-snippets
hc-mcp-setup --set SAM_GOV_API_KEY=... --set 'SEC_USER_AGENT=HealthcareData contact@example.org'
```

`hc-mcp` automatically loads `.env` from the current working directory. For GUI clients that launch from a different directory, set `HC_MCP_ENV_FILE=/absolute/path/to/.env` in the client configuration or pass `--env-file /absolute/path/to/.env`.

## Client Examples

Claude Code project config can use this repo's registry-rendered `.mcp.json`
for local stdio. It does not require Docker or open HTTP ports.

Local HTTP ports are rendered into `configs/http-clients.json`:

| Server | Local HTTP URL | Gateway exposure |
| --- | --- | --- |
| `service-area` | `http://localhost:8002/mcp` | `metadata` |
| `geo-demographics` | `http://localhost:8003/mcp` | `metadata` |
| `drive-time` | `http://localhost:8004/mcp` | `metadata` |
| `hospital-quality` | `http://localhost:8005/mcp` | `metadata`, `live` |
| `cms-facility` | `http://localhost:8006/mcp` | `metadata` |
| `health-system-profiler` | `http://localhost:8007/mcp` | `metadata` |
| `financial-intelligence` | `http://localhost:8008/mcp` | `metadata`, `live` |
| `price-transparency` | `http://localhost:8009/mcp` | `metadata` |
| `physician-referral-network` | `http://localhost:8010/mcp` | `metadata` |
| `workforce-analytics` | `http://localhost:8011/mcp` | `metadata`, `live` |
| `claims-analytics` | `http://localhost:8012/mcp` | `metadata`, `live` |
| `public-records` | `http://localhost:8013/mcp` | `metadata`, `live` |
| `web-intelligence` | `http://localhost:8014/mcp` | `metadata` |
| `discovery` | `http://localhost:8015/mcp` | `metadata` |
| `gateway` | `http://localhost:8016/mcp` | `metadata` |
| `provider-enrollment` | `http://localhost:8017/mcp` | `metadata`, `live` |
| `community-health` | `http://localhost:8018/mcp` | `metadata`, `live` |
| `research-trials` | `http://localhost:8019/mcp` | `metadata`, `live` |
| `live-gateway` | `http://localhost:8020/mcp` | `live` |
| `cache-manager` | `http://localhost:8021/mcp` | none |

Regenerate checked-in client configs after registry changes:

```bash
python scripts/render_client_configs.py project-mcp > .mcp.json
python scripts/render_client_configs.py claude-desktop-stdio > examples/claude-desktop-stdio.json
python scripts/render_client_configs.py codex > examples/codex-config.toml
python scripts/render_client_configs.py http-clients > configs/http-clients.json
python scripts/render_client_configs.py claude-desktop > configs/claude-desktop.json
```

Validate the checked-in examples without writing files:

```bash
python scripts/render_client_configs.py project-mcp --check
python scripts/render_client_configs.py claude-desktop-stdio --check
python scripts/render_client_configs.py codex --check
python scripts/render_client_configs.py http-clients --check
python scripts/render_client_configs.py claude-desktop --check
```

Claude Desktop stdio entry:

```json
{
  "mcpServers": {
    "cms-facility": {
      "command": "hc-mcp",
      "args": ["cms-facility"]
    }
  }
}
```

For servers that need keys, add the env-file pointer:

```json
{
  "mcpServers": {
    "public-records": {
      "command": "hc-mcp",
      "args": ["public-records"],
      "env": {
        "HC_MCP_ENV_FILE": "/absolute/path/to/healthcare-data-mcp/.env"
      }
    }
  }
}
```

Codex local stdio:

```bash
scripts/register-codex.sh --dry-run
codex mcp add cms-facility -- hc-mcp cms-facility
codex mcp add publicRecords --env HC_MCP_ENV_FILE=/absolute/path/to/.env -- hc-mcp public-records
```

`scripts/register-codex.sh --dry-run --http` previews the registry-backed localhost HTTP registrations for Docker Compose without mutating Codex config.

Codex/OpenAI remote HTTP config should point at the deployed HTTPS gateway endpoint, not localhost:

```toml
[mcp_servers.healthcareData]
url = "https://your-domain.example/mcp"
```

The remote `gateway` is metadata-only. It advertises dataset availability and source metadata. Use the separate `live-gateway` for one-endpoint live provider-enrollment, LEIE, SAM exclusion, PLACES, RePORTER, or ClinicalTrials.gov calls, and only behind HTTPS/auth.

Codex local stdio config in `~/.codex/config.toml`:

```toml
[mcp_servers.cmsFacility]
command = "hc-mcp"
args = ["cms-facility"]

[mcp_servers.publicRecords]
command = "hc-mcp"
args = ["public-records"]
env = { HC_MCP_ENV_FILE = "/absolute/path/to/healthcare-data-mcp/.env" }
```

Claude Code can import Claude Desktop server config and also supports local/project/user MCP scopes. For team use, keep the checked-in `.mcp.json` stdio-only and put secrets in `.env` or in each user's local MCP config.

## Compatibility Matrix

| Client/system | Recommended mode | Config source |
| --- | --- | --- |
| Codex CLI / Codex IDE/App | Stdio for local work; Streamable HTTP when Docker is already running | `~/.codex/config.toml`, `codex mcp add`, or `examples/codex-config.toml` |
| Claude Code | Project HTTP via `.mcp.json` or stdio via `claude mcp add` | `.mcp.json`, local/user/project MCP scope |
| Claude Desktop | Stdio JSON or MCPB Desktop Extension | `examples/claude-desktop-stdio.json` or generated `.mcpb` |
| Claude Desktop cowork/shared machine | Docker Compose HTTP on localhost plus per-user client config | `.mcp.json` plus `.env` on the host |
| Generic MCP clients | Stdio command or Streamable HTTP URL | `hc-mcp <server>` or `http://localhost:<port>/mcp` |
| OpenAI API / ChatGPT remote MCP metadata integrations | HTTPS remote gateway | `hc-mcp gateway --transport streamable-http` behind HTTPS/auth |
| OpenAI API / ChatGPT remote MCP live integrations | HTTPS live gateway with auth | `hc-mcp live-gateway --transport streamable-http` behind HTTPS/auth |

## Gaps To Close Before Public Remote Use

- Deploy `hc-mcp gateway` behind HTTPS with OAuth/OIDC or a trusted identity-aware proxy.
- Configure `hc-mcp live-gateway` with `MCP_LIVE_GATEWAY_*` bearer auth or equivalent edge identity before exposing live tools.
- Validate stdio and Streamable HTTP with MCP Inspector and the CI-friendly protocol smoke runner:

```bash
scripts/mcp_inspector_smoke.sh
python scripts/mcp_smoke.py --server discovery --expect-tool list_workflows --expect-resource healthcare-data://workflows/catalog --call-tool list_workflows --expect-structured-path-all workflows[].identity_join_keys --expect-structured-path-all workflows[].source_resolution
python scripts/mcp_smoke.py --server discovery --expect-tool get_workflow_plan --call-tool get_workflow_plan --tool-args '{"workflow_id":"quality_measure_lookup","inputs":{"ccn":"390223","measure":"clabsi_sir"}}' --expect-structured-key workflow_id --expect-structured-key steps --expect-structured-key report_ingest_contract --expect-structured-path identity_map.join_keys --expect-structured-path-all steps[].identity_contract --expect-structured-path-all report_ingest_contract.fact_rows[].evidence_path --expect-structured-path-all report_ingest_contract.fact_rows[].source_metadata_path --expect-structured-path-all report_ingest_contract.fact_rows[].identity_path --expect-structured-path-all report_ingest_contract.fact_rows[].identity_map_path
python scripts/mcp_smoke.py --server discovery --expect-tool get_workflow_plan --call-tool get_workflow_plan --tool-args '{"workflow_id":"system_reconciliation","inputs":{"query":"Jefferson Health","system_slug":"jefferson-health"}}' --expect-structured-key workflow_id --expect-structured-key identity_map --expect-structured-key steps --expect-structured-key report_ingest_contract --expect-structured-path identity_map.join_keys --expect-structured-path-all identity_map.resolution_plan[].qualified_tool --expect-structured-path-all identity_map.resolution_plan[].merge_action --expect-structured-path-all steps[].identity_contract --expect-structured-path-all steps[].source_resolution --expect-structured-path-all report_ingest_contract.fact_rows[].evidence_path --expect-structured-path-all report_ingest_contract.fact_rows[].identity_map_path
python scripts/mcp_smoke.py --server discovery --expect-tool list_presets --expect-resource healthcare-data://presets/catalog --call-tool list_presets --expect-structured-key presets
python scripts/mcp_smoke.py --server discovery --expect-tool get_preset_plan --call-tool get_preset_plan --tool-args '{"preset_id":"market-strategy"}' --expect-structured-key preset_id --expect-structured-key workflow_summaries --expect-structured-path-all workflow_summaries[].identity_join_keys --expect-structured-path-all workflow_summaries[].source_resolution
python scripts/mcp_smoke.py --server gateway --expect-tool search --expect-tool fetch
python scripts/mcp_smoke.py --server live-gateway --expect-tool list_live_tools --call-tool list_live_tools --expect-structured-key tools --expect-structured-path-all tools[].allowed_scopes --expect-structured-path-all tools[].request_size_limit_bytes --expect-structured-path-all tools[].result_size_limit_bytes --expect-structured-path-all tools[].rate_limit_class --expect-structured-path-all tools[].source_caveat_class --expect-structured-path-all tools[].requires_provenance
```

- Keep Streamable HTTP auth integration tests in CI for `gateway` and `live-gateway` before exposing either endpoint remotely.
- Replace date-specific CMS download URLs with catalog discovery where possible.
- Validate the generated `.mcpb` artifact inside Claude Desktop before broad desktop distribution.

## Primary References

- Model Context Protocol server concepts: https://modelcontextprotocol.io/docs/learn/server-concepts
- MCP server build guidance: https://modelcontextprotocol.io/docs/develop/build-server
- MCP Inspector: https://modelcontextprotocol.io/docs/tools
- Claude Code MCP configuration: https://docs.anthropic.com/en/docs/claude-code/mcp
- Anthropic MCP connector: https://docs.anthropic.com/en/docs/agents-and-tools/mcp-connector
- OpenAI remote MCP guide: https://platform.openai.com/docs/guides/tools-remote-mcp
- OpenAI MCP server guide for ChatGPT/API integrations: https://platform.openai.com/docs/mcp/overview
