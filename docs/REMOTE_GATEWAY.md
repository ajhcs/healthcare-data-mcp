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

- `search(query, max_results=10)` returns dataset IDs, titles, descriptions, tags, stable `healthcare-data-mcp://datasets/<id>` URLs, and registry-backed server metadata including port, profiles, workflow roles, dataset IDs, cache needs, and safety notes.
- `fetch(id)` returns the full static metadata document for an ID returned by `search`, including the canonical server capability record: module, port, required/optional environment keys, cache needs, canonical dataset IDs, zero-config eligibility, gateway exposure, profile membership, workflow roles, and safety notes.

These names and response fields are shaped for OpenAI remote MCP search/fetch expectations while staying useful for Claude and other MCP clients.
CI validates the gateway dataset documents against the canonical registry and
registered server source using an AST-based drift check, so metadata-exposed
servers, owning modules, and advertised tool names stay aligned without
importing API-dependent server modules.

April 2026 metadata can include provider enrollment/ownership, CDC PLACES, NIH RePORTER, ClinicalTrials.gov, HHS OIG LEIE, and SAM.gov Exclusions dataset descriptions. The gateway must remain metadata-only for these domains: do not expose live exclusion screening, full provider-enrollment queries, or other source API proxies through the remote gateway unless a separate authenticated design is approved.

For live calls through one MCP endpoint, use the separate `live-gateway` server. It is an allowlisted router over existing registry-approved live tools. Each allowlisted server must also be marked `gateway_exposure=("live", ...)` in the canonical registry, and the gateway validates that registry linkage at startup.

Registry-backed live-gateway server catalog:

| Live-routed server | Domain | Required/optional env keys |
| --- | --- | --- |
| `hospital-quality` | CMS quality, readmission, and safety data | none |
| `financial-intelligence` | IRS 990, SEC EDGAR, and nonprofit finance intelligence | `SEC_USER_AGENT` |
| `workforce-analytics` | BLS and ACGME workforce analytics | `BLS_API_KEY`, `ACGME_PROGRAMS_CSV` |
| `claims-analytics` | DRG, service-line, and claims analytics | none |
| `public-records` | SAM.gov, USAspending, CHPL, accreditation, and exclusion screening | `SAM_GOV_API_KEY`, `CHPL_API_KEY` |
| `provider-enrollment` | CMS PECOS-derived provider enrollment, ownership, and CHOW | none |
| `community-health` | CDC PLACES community-health estimates for counties, places, tracts, and ZCTAs | `PLACES_CACHE_DIR` |
| `research-trials` | NIH RePORTER funding and ClinicalTrials.gov study activity | `CLINICAL_TRIALS_INVENTORY_HARD_MAX` |

## Security Configuration

Use environment variables to move from local development to a locked-down remote service:

| Variable | Purpose |
|---|---|
| `MCP_GATEWAY_AUTH_REQUIRED=true` | Require bearer auth. Fails startup unless a token or token hash is also configured. |
| `MCP_GATEWAY_BEARER_TOKEN` / `MCP_GATEWAY_BEARER_TOKENS` | Static local/edge bearer token(s). Tokens must be non-placeholder values at least 16 characters long. |
| `MCP_GATEWAY_BEARER_TOKEN_SHA256` / `MCP_GATEWAY_BEARER_TOKEN_SHA256_LIST` | SHA-256 hex digests of allowed tokens, preferred over storing raw shared secrets. |
| `MCP_GATEWAY_REQUIRED_SCOPES` | Comma-separated scopes returned by the token verifier. Defaults to `mcp:read`. |
| `MCP_GATEWAY_TOKEN_SCOPES` | Optional semicolon-separated `<sha256>=scope+scope` overrides for selected static-token principals. |
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
- Docker Compose live-gateway env comes from the canonical registry for every server exposed with `gateway_exposure="live"`, so routed tools receive their own required/optional keys such as `SEC_USER_AGENT`, `SAM_GOV_API_KEY`, BLS/CHPL keys, PLACES cache overrides, and ClinicalTrials.gov inventory limits.
- HTTP/SSE live-gateway refuses wildcard network binds such as `MCP_HOST=0.0.0.0` unless `MCP_LIVE_GATEWAY_ALLOW_NETWORK_BIND=true` is set with an HTTPS `MCP_LIVE_GATEWAY_PUBLIC_URL` and explicit Host/Origin allow-lists. The generated Docker Compose file uses `MCP_LIVE_GATEWAY_CONTAINER_LOCAL_BIND=true` only for the container-internal wildcard bind that is host-published to `127.0.0.1`. The normal remote deployment shape is still `MCP_HOST=127.0.0.1` behind a trusted HTTPS reverse proxy.
- Every routed tool call passes through a policy wrapper that preserves the owning tool's structured result, including `evidence`, `source_metadata`, and identity fields, while adding `live_gateway_policy` with the enforced scopes, request/result byte limits, result count limit, rate-limit class, source caveat class, registry dataset IDs, registry cache needs, registry safety notes, and provenance status.
- The gateway policy enforces request byte limits, recursive request array bounds, result item/byte limits, positive numeric result-limit argument bounds even when clients send values as strings, per-tool scopes, per-tool rate-limit classes, source caveat classes, provenance-status checks, missing/malformed evidence-receipt blocking for both top-level and nested row receipts, non-empty source/caveat content in routed receipts, sensitive SSN/EIN/TIN-style argument-key rejection, and non-secret audit events.
- Rate-limit windows are scoped by rate-limit class, tool name, and caller subject so one authenticated principal does not consume another principal's live-tool window. For authenticated HTTP/SSE calls, the subject is derived from the verified access token with a non-secret token fingerprint; client-supplied tool arguments cannot grant scopes or override the audit subject.
- The public tool `list_live_tools()` returns the approved router surface plus policy metadata: allowed scopes, auth posture, request/result limits, rate-limit class, audit event shape, source caveat class, registry dataset IDs/cache needs/safety notes, provenance requirement/status, and safe defaults.
- The public tool `get_live_gateway_audit_events()` returns recent non-secret audit events for local operations review. Set `MCP_LIVE_GATEWAY_AUDIT_LOG_PATH` to append the same non-secret events to a JSONL file for operational retention. Audit events intentionally omit request payload values, source metadata payloads, evidence payloads, and secrets. When provenance validation blocks a response, the audit event may include `invalid_evidence_paths` with receipt paths and validation errors.
- CI runs Streamable HTTP integration coverage for both blocked and successful live-gateway paths: unauthenticated and wrong-token clients are rejected, bulk tools are denied without `mcp:bulk`, sensitive identifier arguments are blocked before routing, and an authenticated routed metadata tool must return upstream `evidence`, `source_metadata`, `live_gateway_policy`, and a non-secret allowed audit event.
- Batch exclusion screening tools require the additional `mcp:bulk` scope. Prefer token-specific scope overrides, for example `MCP_LIVE_GATEWAY_TOKEN_SCOPES=<sha256>=mcp:read+mcp:bulk`, so only selected static-token principals can run bulk screening tools. HTTP/SSE startup still rejects global `MCP_LIVE_GATEWAY_REQUIRED_SCOPES=mcp:read,mcp:bulk` unless `MCP_LIVE_GATEWAY_ALLOW_GLOBAL_BULK_SCOPE=true` is also set. Use that global opt-in only where every live-gateway principal may run bulk screening.
- There is no wildcard proxy mode. Additions must be made in the live gateway allowlist, backed by `gateway_exposure="live"` registry metadata, and reviewed with source caveats.

Example local HTTP run with a token hash:

```bash
MCP_TRANSPORT=streamable-http \
MCP_HOST=127.0.0.1 \
MCP_PORT=8020 \
MCP_LIVE_GATEWAY_BEARER_TOKEN_SHA256=<sha256-token-hash> \
MCP_LIVE_GATEWAY_ALLOWED_HOSTS=localhost:8020,127.0.0.1:8020 \
hc-mcp live-gateway
```

Docker Compose publishes gateway and live-gateway ports on `127.0.0.1` by default. For remote access, keep the process bound locally and place only the intended gateway behind a trusted HTTPS reverse proxy with the auth variables above.
