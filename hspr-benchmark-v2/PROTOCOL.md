# Protocol

## Design

- Cohort: 35 declared standard systems, 50 reproducibly sampled systems, and 15
  declared tricky systems. The 4-system pilot is disjoint from all 100.
- Questions: three per system—latest annual top-line revenue, operating result
  (or an explicit unavailable answer with closest precisely labeled subtotal),
  and fiscal-year-end total assets.
- Arms: `hspr-medium`, `hspr-xhigh`, `native-medium`, `native-xhigh`, all using
  Luna and identical live-research tools/runtime. HSPR access is the sole
  treatment difference; reasoning level is the other factor.
- Run order: paired and counterbalanced by system/question/repetition. Target
  three fresh repetitions per arm, subject to pilot variance and exact power.

## Isolation

Questions, registry packets, runtime configs/traces, and sealed gold occupy
separate roots. Each answer context is a non-root Docker container with a
read-only root filesystem, dropped capabilities, `no-new-privileges`, resource
limits, bounded tmpfs work/auth/output directories, read-only packet/runtime and
Codex-package mounts, and no writable host-output mount. The
repository, sealed root, Docker socket, and host root are absent. HSPR arms get
the audited identity packet; native arms do not. The scorer receives answers and
the Sol High lead-validated final key but no answer-agent context. Confirmatory
key builders never run or monitor answer arms.

Only subscription-backed ChatGPT authentication is accepted; API keys fail
closed. The host requires both access and identity JWT expiry claims to outlive
the bounded trial, but forwards only the short-lived access token and account
ID. The refresh token, ID token, API key, and auth metadata never enter the
container. The minimized document is supplied over stdin, never argv,
environment, trace, file, or bind mount. Codex app-server's external
ChatGPT-auth protocol accepts it in memory before the thread starts; the
controller aborts if `turn.started` arrives without that readiness event.

This is not cryptographic secrecy: the host kernel, Docker daemon,
Codex/backend, and controller are trusted, and the short-lived access token
exists in trusted supervisor/Codex process memory. The real hostile tool probe
shows that its answer context cannot access an auth path, credential environment
variables, any other process memory, or supervisor/app-server IPC descriptors
under the current container policy. Native in-process subagents are not valid answer
arms in this environment because they share the repository filesystem; prompts
and logging are not treated as access controls.

Local native web is outside the shell-network sandbox and has no enforceable
per-domain exclusion. The technical boundary is therefore deliberately narrow:
the answer container cannot mount the repository, sealed key, historical files,
reusable credentials, or host outputs. The prompt also directs the answer agent
not to seek benchmark or repository material, and the controller records all
observable native tool/web activity. That instruction and trace review are
operational mitigations, not access controls, and do not prove that public
repository material is unreachable.

Public-history, GitHub-metadata, fork, and opaque-artifact audits are preserved
as scorer-side diagnostic evidence. Their protected, digest-bound outputs may
be attached to a run manifest, including failed or incomplete captures, but are
not prerequisites for execution. There is currently no evidence that an answer
agent accessed those surfaces. This evidence-proportionate policy avoids an
absolute leakage-prevention claim: unobserved native-web behavior, public
caches, force-pushed history, external attachments, and repository metadata
remain residual limitations and must be reported with results.

Handled aborts issue repeated daemon-side `docker rm --force` calls using the
unique container name; an uncatchable host/controller crash remains outside
this guarantee. The Codex
parent retains bridge access for API and web-search operations, but generated
shell commands request Codex's `read-only` sandbox rather than bypass mode.

## Telemetry

Use monotonic timestamps for run start/end. HSPR lookup, local reads, web tool
calls, first/last authoritative financial evidence, and final answer use the
host monotonic timestamp taken as each native Codex JSONL event is received.
HSPR lookup is the native command start/end that reads `hspr-identity.json`;
packet-copy timing is separately labeled materialization. Evidence timing is
derived only by matching lead-validated authoritative URLs in native events.
Keyword matches do not establish source authority. Missing
event timestamps, token counts, and cost stay null. Never infer hidden timing.

## Outcomes and analysis

Score financial correctness, entity/perimeter correctness, provenance/locator
quality, caveat quality, false aggregation, tool calls, wall latency, observable
step timing, available token/cost telemetry, and HSPR construction/maintenance
burden. Primary comparisons are paired; uncertainty uses system-level clusters.
The AHRQ frame supports selection, not an unqualified national-prevalence claim.
