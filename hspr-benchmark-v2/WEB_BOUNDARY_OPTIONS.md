# Public-repository web boundary

## Decision

The local subscription-native launcher cannot make this public repository
unreachable while native web is enabled. Native web is a model-side service
rather than model-generated shell networking, and Codex 0.145.0 exposes only an
on/off search control. Docker mount isolation therefore cannot support a claim
of general repository-domain exclusion.

The narrow local protocol instead uses a fresh, protected, unpublished active
cohort and blocks unless every active ID, canonical name, alias, legal-entity
name, and identifier has zero answer-bearing hits across all fetched public Git
history. Old public artifacts can remain reachable, but they cannot answer the
active questions. Disabling native web would violate the benchmark design;
prompt instructions and post-hoc GitHub URL rejection are not boundaries.

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

`hspr_benchmark.trial_executor.OFFICIAL_WEB_BOUNDARY_VALIDATED` remains `False`
while the replacement pilot's sealed key and final identity-aware audit are
pending. This is an operational interlock, not a substitute for the protected
active manifest and exact public-ref attestations.

## Selected active-pilot remediation

Public history cannot be made blank without destructive history rewriting, and
no such rewrite or public deletion is authorized or planned. Instead, the next
pilot uses a fresh protected active packet outside every Git repository. Its
systems are disjoint from the published confirmatory cohort and every prior
answer-bearing benchmark system. Questions, HSPR identity claims, researcher
records, and adjudicated gold remain unpublished until all answer trials are
immutable.

`python -m hspr_benchmark.public_history_audit` scans every commit reachable
from the explicitly enumerated public branch tips, including removed blobs. It
fails if an active system name or ID occurs outside the declared national
sampling-frame derivative. The first disjoint packet (v3) was excluded before
adjudication because a key researcher improperly delegated one system to a
third helper. The next candidate (v4) was excluded before research because a
generic system name produced public-history collisions. Both attempts and their
evidence are preserved outside Git, and no answer arm ran. Pilot v5 passed the
name/ID preflight but failed sealed-gold readiness: only six of twelve records
were defensible, so it was preserved and excluded without any answer run.
Replacement pilot v6 is disjoint from the confirmatory 100 and all prior pilots
and passed the same initial six-branch name/ID audit with zero blocking hits.
Its audit must be rerun over every adjudicated alias, legal-entity name, and
identifier; only that identity-aware, versioned result may enter the active
manifest. The audit records exact public tip SHAs and must be rerun after a
remote-ref refresh immediately before every official batch.

This resolves active-answer leakage, not general access to the public project:
an answer agent could still retrieve old, irrelevant benchmark artifacts. The
validity claim is therefore limited to the fresh disjoint active questions. If
the active identities or questions are published, or a future public branch
adds a blocking hit, the run gate must fail closed.
