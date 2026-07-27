# Non-official live launcher validation

Superseding validation date: 2026-07-27 UTC.

This was a bounded launcher smoke against one harmless public OpenAI Codex fact.
It was not a financial, pilot, or confirmatory answer and is excluded from every
benchmark analysis. No HSPR packet or sealed-gold data was used.

The live run passed the current production-path gates:

- the host required fresh ChatGPT access and identity JWTs, but supplied only
  the short-lived access token and account ID over stdin;
- Codex app-server accepted those fields through fileless external auth; no ID
  token, refresh token, API key, auth file, argv value, environment value, bind
  mount, or trace field carried authentication;
- the real model-generated probe observed no auth path or credential environment
  and could not read any other process memory or reopen another process's pipe
  or socket descriptors;
- generated shell networking was denied while four native Codex web events were
  captured;
- the schema-constrained answer, supervisor sequence, monotonic run/tool/final
  receipt events, and available usage were preserved in a protected redacted
  trace; and
- the process exited successfully under the same unique-name and forced-cleanup
  controller used by trials.

The smoke used `@openai/codex` 0.145.0, `gpt-5.6-luna` at medium reasoning, the
digest-pinned Node image, and the bounded container controls intended for all
four arms. The successful final run took about 20 seconds. Failed precursor
smokes were preserved under protected non-official storage; they caught an
incorrect CLI option position, lazy auth-file loading, invalid structured-output
schema constants, and a missing absolute Node path. None reached a financial
question.

This establishes practical enforcement for the tested runtime, not
cryptographic isolation. The host kernel, Docker daemon, Codex/backend,
controller, and native web implementation remain trusted. Short-lived access
token strings can remain in immutable runtime objects until their processes
exit, although the hostile tool context could not read those processes. Native
web still lacks a per-domain denylist, so official validity additionally depends
on a fresh unpublished cohort and a zero-hit identity-aware public-history
audit. Any Codex/runtime/container-policy change requires a fresh smoke.

The earlier 2026-07-26 auth-file/unlink smoke is preserved as superseded
engineering evidence and is not the current production boundary.
