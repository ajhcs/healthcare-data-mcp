# Subscription-native credential boundary

The benchmark uses ChatGPT-plan Codex authentication and categorically rejects
OpenAI API keys. It does not ask for, create, or fall back to an API key.

The trusted host controller reads the local Codex auth document without
following a symlink, requires `auth_mode=chatgpt`, rejects any nonempty
`OPENAI_API_KEY`, and requires the access-token expiry claim to outlive the full
bounded trial window plus a safety margin. A managed-auth ID token must be
present, but its expiry is not a runtime authorization boundary because it is
not forwarded or consumed by external auth. The controller constructs a new
document containing only the access token and account ID. The
ID token, refresh token, API key, and auth metadata never enter the answer
container. Source and minimized mutable
buffers are zeroed independently, including the supervisor's original stdin
chunks. Transient immutable Python byte copies can remain in the trusted host
controller until garbage collection, so this is best-effort process hygiene,
not a complete memory-erasure claim.

Inside the isolated container, the supervisor passes the minimized fields to
Codex app-server's experimental external ChatGPT-auth protocol in memory. No
`auth.json` is created. Only after that request succeeds may the supervisor
start a thread. The answer context has no repository, sealed-key, historical,
Docker-socket, host-root, or host-output mount. Its shell has no credential path
or credential environment variables and cannot read any other process memory or
reopen supervisor/app-server pipe or socket descriptors under the tested
container/sandbox policy. Native web remains a trusted Codex
runtime service and native events are timestamped by the host controller. The
current local/app native-web interface does not expose an enforceable per-domain
deny policy. Answer prompts explicitly prohibit seeking benchmark, repository,
history, or scoring material, and observable tool/web activity is retained for
review. These are operational mitigations rather than an isolation boundary;
the protocol does not claim that public repository material is unreachable.

The remaining limitation is explicit: the trusted Codex runtime necessarily
holds a short-lived access token in process memory while it contacts the
subscription service. This is practical OS/process isolation, not cryptographic
separation from the runtime vendor or host administrator. Fresh subscription
tokens must already exist at trial construction. Either stale short-lived token
blocks execution. The trusted host may use Codex's normal managed-auth refresh
flow before constructing a new minimized document, but the refresh token never
crosses into the container.

The earlier 17 completed pilot trials used the prior full-auth startup protocol
and are superseded for the subscription-native confirmatory design. They remain
preserved as historical engineering evidence and must not be mixed with new
results. No new official arm may run until the exact active cohort has a ready
sealed key, the registry/key leakage audit passes, and the fileless-auth proof,
hostile mount probe, tests, review, and non-official live boundary smoke pass on
the locked revision. Public-history, GitHub-metadata, fork, and opaque-artifact
audits are optional diagnostic evidence and do not gate execution.
