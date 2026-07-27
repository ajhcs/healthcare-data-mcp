# HSPR financial retrieval benchmark v2

Install the repository with `python -m pip install -e ".[benchmark,dev]"`
before running the benchmark harness. The benchmark extra is deliberately
separate from production runtime dependencies.

This is the leakage-resistant successor to the quarantined July 2026 studies.
Those studies tested curated, answer-bearing identity/financial evidence against
fresh identity research; they were not valid end-to-end financial retrieval
comparisons.

The v2 benchmark compares four matched Luna arms. Every arm must retrieve the
financial result from a live authoritative source; only access to an
identity/perimeter-only HSPR packet and approved reasoning level differ. Gold
is stored outside every repository under the protected Plumbob scorer root
declared by `config/sealed-manifest.json`. The legacy ignored worktree copy was
relocated intact on 2026-07-27 and its adjudicated-key hash was unchanged. Only
the host leakage auditor and scorer may read that root; it is never copied or
mounted into an answer context. `.benchmark-sealed/` remains ignored to prevent
future accidental staging but is no longer the source of truth.

Cleanup commit `6cbfc2e` is merged into the agreed base and its CI passed. Every
active batch must separately pass sealed-gold readiness and the
registry-versus-gold leakage audit before the run gate can open.

The answer launcher creates an allowlisted, non-root, read-only Docker context
with no repository, sealed-key, Docker-socket, host-root, or writable host-output
mount. Output is bounded tmpfs and crosses the boundary through a supervisor
event into a host-owned result directory. A hostile no-network mount probe and
a separate fake-credential regression proof pass. Authentication is never
mounted or written to a file. The supervisor accepts only a short-lived access
token and account ID on stdin, uses Codex app-server's fileless external
ChatGPT-auth mode, clears its mutable fields, and starts the turn only after
external auth succeeds. Native command, web-search, final-answer, usage, and
monotonic receipt events are captured.

This is practical process/filesystem separation, not cryptographic isolation.
Docker, the host kernel, Codex, and the controller remain trusted. The Codex
parent uses the Docker bridge for API/web tools, while model-generated shell
commands use Codex's legacy Landlock `read-only` sandbox because nested
bubblewrap namespaces are unavailable in this hardened container. The launcher
never uses bypass mode. A bounded, non-official live smoke validated fileless
external auth, denial of other-process memory and IPC descriptors,
shell-network denial, native web access, trace capture, and cleanup; see
`SMOKE_VALIDATION.md`. No scored pilot answer arm had run at that validation
point.

The stricter subscription-native restart is specified in
`SUBSCRIPTION_PROTOCOL.md`. It rejects API keys and prevents refresh credentials
from entering answer containers. Earlier pilot trials are preserved but
superseded. Local native web has no enforceable repository-domain exclusion, so
the defensible local claim is deliberately narrower: the container cannot mount
the repository, sealed key, historical material, host outputs, or reusable
credentials. The prompt prohibits seeking benchmark or repository material and
observable native tool/web events are retained, but those are operational
mitigations rather than access controls. Public-history and metadata audits are
preserved as optional diagnostics, not execution gates; see
`WEB_BOUNDARY_OPTIONS.md`.
