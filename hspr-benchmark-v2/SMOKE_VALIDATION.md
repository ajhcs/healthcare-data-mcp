# Non-official live launcher validation

Date: 2026-07-26 UTC.

This was a bounded launcher smoke against one harmless public U.S. Treasury
Fiscal Data question. It was not a pilot or confirmatory answer and is excluded
from all benchmark analyses. No HSPR packet or sealed-gold data was used.

The live run passed all predeclared launcher gates:

- the credential was supplied over stdin, unlinked from `/auth/auth.json` before
  `turn.started`, and observed absent by the answer context; the controller's
  exact-value redaction was also active before trace persistence;
- a model-generated `wget` attempt returned the expected blocked marker under
  Codex's legacy Landlock `read-only` sandbox, while native Codex web operations
  remained available;
- six native web events included an official `fiscaldata.treasury.gov` API
  source;
- the schema-constrained answer, final-answer timing, usage telemetry, run
  start/end, and native tool-event receipt times were captured;
- the process exited successfully and the uniquely named container was absent
  from the Docker daemon afterward.

The smoke used `@openai/codex` 0.145.0, `gpt-5.6-luna` at medium reasoning, the
digest-pinned Node image, and the same bounded container controls intended for
all four arms. Its wall duration was approximately 30 seconds. The raw trace was
temporary and intentionally not committed because it was generated using live
authentication.

This establishes practical enforcement for the tested path, not cryptographic
isolation. The host kernel, Docker daemon, Codex/backend, controller, and native
web implementation remain trusted. An uncatchable host/controller failure is
not covered by the normal cleanup guarantee. The single public `wget` failure is
not an exhaustive network-policy test and does not distinguish every possible
transport failure. Repository, sealed-root, Docker-socket, and host-root denial
are established by the separate hostile no-network isolation probe and mount
allowlist, not by this live prompt. Normal `--rm` cleanup was observed here;
timeout/abort cleanup is covered by the controller's repeated daemon-side
removal implementation. Because legacy Landlock is version-sensitive, any Codex
runtime upgrade requires a fresh boundary smoke.
