# Public-repository web boundary

## Decision

The local subscription-native launcher is not eligible for official answering
runs while native web is enabled. Native web is a model-side service rather
than model-generated shell networking, and Codex CLI 0.145.0 exposes only an
on/off `--search` control for it. The local Codex App does not provide an
enforceable per-domain allowlist or denylist. Therefore Docker mount isolation
cannot prevent retrieval of this public repository through native web.

Disabling native web without supplying another live retrieval channel would
violate the benchmark design. Prompt instructions and rejecting observed
GitHub URLs after a run would detect some failures but would not prevent access,
so neither is an isolation boundary.

## Defensible candidate

Codex Cloud documents per-environment agent internet allowlists and HTTP-method
controls in its
[agent internet access documentation](https://learn.chatgpt.com/docs/cloud/internet-access).
A possible subscription-native design would use a purpose-built
answer environment whose agent phase:

1. contains only the current question, response schema, and (for treated arms)
   the audited identity packet;
2. has no checkout, Git metadata, cache, setup artifact, or secret referring to
   this repository or the sealed scorer;
3. excludes GitHub, GitHub content/CDN domains, repository mirrors, and archival
   caches from its enforced internet allowlist while permitting equal GET/HEAD
   research access for every arm; and
4. emits exportable native tool/work-log events sufficient for the preregistered
   timestamps and telemetry.

This is a candidate, not a validated protocol. Before adoption it must prove
that setup-time repository material is absent from the agent phase, redirects
and alternate GitHub hostnames fail closed, the requested Luna model and
reasoning levels are lockable, native traces are exportable with adequate
timing, and active packets can remain non-public. Creating or reconfiguring a
cloud environment or separate repository is external state and is not performed
by this preparatory change.

## Current gate

`hspr_benchmark.trial_executor.OFFICIAL_WEB_BOUNDARY_VALIDATED` remains `False`. This is
an operational interlock, not a substitute for the external access control.
The prior local launcher proofs remain useful evidence for credential and
filesystem isolation only.
