# MCP Client and Packaging Notes

## Current Position

This repo supports two practical modes:

- Local stdio: `hc-mcp <server-name>` for Claude Desktop, Claude Code, Codex, and other local MCP clients.
- Local Streamable HTTP: `docker compose up --build`, then use `.mcp.json` with the localhost ports.

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

## Client Examples

Claude Code project config can use this repo's `.mcp.json` after Docker Compose starts the HTTP servers.

April 2026 local HTTP additions in `.mcp.json`:

| Server | URL |
| --- | --- |
| `provider-enrollment` | `http://localhost:8017/mcp` |
| `community-health` | `http://localhost:8018/mcp` |
| `research-trials` | `http://localhost:8019/mcp` |

`public-records` remains on `http://localhost:8013/mcp` for HHS OIG LEIE and SAM.gov Exclusions. Set `SAM_GOV_API_KEY` in the server environment for SAM.gov API-backed exclusion checks.

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

Codex local stdio:

```bash
codex mcp add cms-facility -- hc-mcp cms-facility
```

Codex/OpenAI remote HTTP config should point at the deployed HTTPS gateway endpoint, not localhost:

```toml
[mcp_servers.healthcareData]
url = "https://your-domain.example/mcp"
```

The remote gateway is metadata-only. It should advertise dataset availability and source metadata, not proxy live provider-enrollment, LEIE, SAM exclusion, PLACES, RePORTER, or ClinicalTrials.gov queries without a separate authenticated gateway design.

Codex local stdio config in `~/.codex/config.toml`:

```toml
[mcp_servers.cmsFacility]
command = "hc-mcp"
args = ["cms-facility"]
```

## Gaps To Close Before Public Remote Use

- Deploy `hc-mcp gateway` behind HTTPS with OAuth/OIDC or a trusted identity-aware proxy.
- Add integration tests with MCP Inspector against stdio and Streamable HTTP.
- Replace date-specific CMS download URLs with catalog discovery where possible.
- Expand unit tests beyond `health-system-profiler`.
- Package Claude Desktop distribution as a Desktop Extension (`.mcpb`) if the target is one-click desktop install.

## Primary References

- Model Context Protocol server concepts: https://modelcontextprotocol.io/docs/learn/server-concepts
- MCP server build guidance: https://modelcontextprotocol.io/docs/develop/build-server
- MCP Inspector: https://modelcontextprotocol.io/docs/tools
- Claude Code MCP configuration: https://docs.anthropic.com/en/docs/claude-code/mcp
- Anthropic MCP connector: https://docs.anthropic.com/en/docs/agents-and-tools/mcp-connector
- OpenAI remote MCP guide: https://platform.openai.com/docs/guides/tools-remote-mcp
- OpenAI MCP server guide for ChatGPT/API integrations: https://platform.openai.com/docs/mcp/overview
