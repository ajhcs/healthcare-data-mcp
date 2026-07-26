# HSPR financial retrieval benchmark v2

This is the leakage-resistant successor to the quarantined July 2026 studies.
Those studies tested curated, answer-bearing identity/financial evidence against
fresh identity research; they were not valid end-to-end financial retrieval
comparisons.

The v2 benchmark compares four matched Luna arms. Every arm must retrieve the
financial result from a live authoritative source; only access to an
identity/perimeter-only HSPR packet and approved reasoning level differ. Gold
is stored locally under `.benchmark-sealed/`, ignored by Git, and must be copied
only into a scorer context that is unavailable to answer agents.

Cleanup commit `6cbfc2e` is merged into the agreed base and its CI passed. The
pilot questions and analysis are preregistered, and the identity packet passes
the automated leakage audit against the adjudicated sealed key.

The answer launcher creates an allowlisted, non-root, read-only Docker context
with no repository, sealed-key, Docker-socket, host-root, or writable host-output
mount. Output is bounded tmpfs and crosses the boundary through a supervisor
event into a host-owned result directory. A hostile no-network mount probe and
a separate fake-credential supervisor proof pass. Authentication is never
mounted: the supervisor accepts it on stdin, stores it briefly in container
tmpfs, and unlinks it before releasing `turn.started`. Native command,
web-search, final-answer, usage, and monotonic receipt events are captured.

This is practical process/filesystem separation, not cryptographic isolation.
Docker, the host kernel, Codex, and the controller remain trusted. The Codex
parent uses the Docker bridge for API/web tools, while model-generated shell
commands request Codex's `read-only` sandbox and the launcher never uses bypass
mode. Live enforcement of that nested sandbox is not yet credential-validated.
The official pilot remains fail-closed until reusable Codex authentication is
explicitly authorized for this networked context; no live answer arm has run.
