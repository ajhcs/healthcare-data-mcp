# Remote MCP Gateway

`servers.gateway.server` is a production-shaped, local-safe gateway for remote MCP clients such as OpenAI and Claude connectors. It exposes only static metadata about the repository's public healthcare datasets through OpenAI-compatible `search` and `fetch` tools.

The gateway is intentionally not a deployment manifest. It does not require TLS certificates, OAuth credentials, or cloud secrets to run locally.

## Local Run

```bash
python3 -m pip install -e ".[dev]"
hc-mcp gateway --transport streamable-http --port 8016
```

Default local behavior:

- Binds to `127.0.0.1`.
- Enables FastMCP Host and Origin validation for localhost.
- Does not require bearer auth unless a token or token hash is configured.
- Returns dataset metadata only; it does not proxy live server calls or expose PHI.

## Tools

The gateway presents two retrievable-knowledge tools:

- `search(query, max_results=10)` returns dataset IDs, titles, descriptions, tags, and stable `healthcare-data-mcp://datasets/<id>` URLs.
- `fetch(id)` returns the full static metadata document for an ID returned by `search`.

These names and response fields are shaped for OpenAI remote MCP search/fetch expectations while staying useful for Claude and other MCP clients.

April 2026 metadata can include provider enrollment/ownership, CDC PLACES, NIH RePORTER, ClinicalTrials.gov, HHS OIG LEIE, and SAM.gov Exclusions dataset descriptions. The gateway must remain metadata-only for these domains: do not expose live exclusion screening, full provider-enrollment queries, or other source API proxies through the remote gateway unless a separate authenticated design is approved.

For live calls through one MCP endpoint, use the separate `live-gateway` server. It is an allowlisted router over existing provider-enrollment, hospital-quality, claims-analytics, public-records exclusion, community-health, and research-trials tools.

## Security Configuration

Use environment variables to move from local development to a locked-down remote service:

| Variable | Purpose |
|---|---|
| `MCP_GATEWAY_AUTH_REQUIRED=true` | Require bearer auth. Fails startup unless a token or token hash is also configured. |
| `MCP_GATEWAY_BEARER_TOKEN` / `MCP_GATEWAY_BEARER_TOKENS` | Static local/edge bearer token(s). Tokens must be non-placeholder values at least 16 characters long. |
| `MCP_GATEWAY_BEARER_TOKEN_SHA256` / `MCP_GATEWAY_BEARER_TOKEN_SHA256_LIST` | SHA-256 hex digests of allowed tokens, preferred over storing raw shared secrets. |
| `MCP_GATEWAY_REQUIRED_SCOPES` | Comma-separated scopes returned by the token verifier. Defaults to `mcp:read`. |
| `MCP_GATEWAY_ALLOWED_ORIGINS` | Comma-separated allowed browser origins, for example `https://chatgpt.com,https://claude.ai`. |
| `MCP_GATEWAY_ALLOWED_HOSTS` | Comma-separated allowed Host headers, for example `gateway.example.com`. |
| `MCP_GATEWAY_PUBLIC_URL` | Public MCP resource URL, normally `https://gateway.example.com/mcp`. |
| `MCP_GATEWAY_ISSUER_URL` | OAuth issuer URL advertised in MCP auth metadata. |

To avoid storing the raw token in process config, generate a hash:

```bash
python3 - <<'PY'
import hashlib
token = "replace-with-a-long-random-token"
print(hashlib.sha256(token.encode()).hexdigest())
PY
```

## HTTPS and Reverse Proxy

Expose the gateway only behind HTTPS. A reverse proxy should terminate TLS, enforce request size limits, set conservative timeouts, and forward only to the local FastMCP process.

Example Caddy shape:

```caddyfile
gateway.example.com {
  encode zstd gzip
  request_body {
    max_size 1MB
  }
  reverse_proxy 127.0.0.1:8016
}
```

Run the gateway behind that proxy with locked Host/Origin settings:

```bash
MCP_TRANSPORT=streamable-http \
MCP_HOST=127.0.0.1 \
MCP_PORT=8016 \
MCP_GATEWAY_AUTH_REQUIRED=true \
MCP_GATEWAY_BEARER_TOKEN_SHA256=<sha256-token-hash> \
MCP_GATEWAY_ALLOWED_HOSTS=gateway.example.com \
MCP_GATEWAY_ALLOWED_ORIGINS=https://chatgpt.com,https://claude.ai \
MCP_GATEWAY_PUBLIC_URL=https://gateway.example.com/mcp \
MCP_GATEWAY_ISSUER_URL=https://auth.example.com \
hc-mcp gateway
```

## OAuth Position

For production connector deployments, prefer OAuth or an identity-aware proxy at the edge. This skeleton provides FastMCP bearer-token verification and advertises MCP auth metadata, but it does not mint OAuth tokens or manage client registration.

Recommended production pattern:

- Terminate HTTPS at Caddy, nginx, Cloudflare, or a managed load balancer.
- Put OAuth/OIDC enforcement in the proxy or an authorization gateway.
- Forward only validated requests to `127.0.0.1:8016`.
- Set `MCP_GATEWAY_ALLOWED_HOSTS`, `MCP_GATEWAY_ALLOWED_ORIGINS`, `MCP_GATEWAY_PUBLIC_URL`, and `MCP_GATEWAY_ISSUER_URL` explicitly.
- Keep raw tokens out of shared config; use SHA-256 token hashes for static-token fallback.

## Client Notes

OpenAI/Codex remote MCP config should point at the HTTPS endpoint:

```toml
[mcp_servers.healthcareData]
url = "https://gateway.example.com/mcp"
```

Claude remote MCP connectors should use the same HTTPS MCP URL. Local Claude/Codex development can continue to use the existing stdio servers for live data workflows.

## Live Gateway

`hc-mcp live-gateway` is separate from the metadata gateway:

- Port: `8020`.
- Stdio: allowed for local desktop/CLI use without bearer auth.
- HTTP/SSE: requires bearer-token or SHA-256 token-hash configuration by default.
- Auth variables use the `MCP_LIVE_GATEWAY_*` prefix, for example `MCP_LIVE_GATEWAY_BEARER_TOKEN_SHA256`, `MCP_LIVE_GATEWAY_ALLOWED_HOSTS`, and `MCP_LIVE_GATEWAY_PUBLIC_URL`.
- The public tool `list_live_tools()` returns the approved router surface.

Example local HTTP run with a token hash:

```bash
MCP_TRANSPORT=streamable-http \
MCP_HOST=127.0.0.1 \
MCP_PORT=8020 \
MCP_LIVE_GATEWAY_BEARER_TOKEN_SHA256=<sha256-token-hash> \
MCP_LIVE_GATEWAY_ALLOWED_HOSTS=localhost:8020,127.0.0.1:8020 \
hc-mcp live-gateway
```
