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
adjudicated gold but no answer-agent context.

Only subscription-backed ChatGPT authentication is accepted; API keys fail
closed. The host parses the access and identity JWT expiry claims from the
trusted local auth document and requires them to outlive the bounded trial,
then strips the long-lived refresh token (the installed Codex schema receives
an empty placeholder), and separately zeroes the source and minimized byte
arrays. The minimized document is supplied over stdin, never argv, environment,
trace, or a host bind mount. The supervisor unlinks the tmpfs credential and
scans container process descriptors before forwarding `thread.started`; the
controller aborts if `turn.started` arrives without the unlink event. It never
refreshes or modifies the user's subscription credentials.

This is not cryptographic secrecy: the host kernel, Docker daemon,
Codex/backend, and controller are trusted, and short-lived access/identity
tokens exist in trusted Codex process memory. The fake hostile tool probe shows
that its answer context cannot access the auth path, retained descriptors,
credential environment variables, or parent-process memory under the current
container policy. An equivalent real-Codex tool-context probe is still required
by the non-official live smoke. Native in-process subagents are not valid answer
arms in this environment because they share the repository filesystem; prompts
and logging are not treated as access controls.

Local native web is also outside the shell-network sandbox and does not expose
an enforceable per-domain exclusion. Because the benchmark repository is
public, the current launcher cannot prevent an arm from retrieving published
repository material through that channel. Official web-enabled execution is
fail-closed in the runner until an external enforcement mechanism is proven;
fresh prompts, voluntary instructions, and post-hoc trace rejection do not
resolve this boundary.

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
derived only by matching adjudicated authoritative URLs in native events.
Keyword matches do not establish source authority. Missing
event timestamps, token counts, and cost stay null. Never infer hidden timing.

## Outcomes and analysis

Score financial correctness, entity/perimeter correctness, provenance/locator
quality, caveat quality, false aggregation, tool calls, wall latency, observable
step timing, available token/cost telemetry, and HSPR construction/maintenance
burden. Primary comparisons are paired; uncertainty uses system-level clusters.
The AHRQ frame supports selection, not an unqualified national-prevalence claim.
