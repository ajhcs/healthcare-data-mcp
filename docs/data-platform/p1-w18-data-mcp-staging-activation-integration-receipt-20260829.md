# P1-W18 Data MCP staging activation receipt preparation

Status: locally merged as a pending-approval receipt contract. No staging
activation occurred. This document is preparation evidence for the later
operationally approved activation run; it is not a runtime success receipt.

## Immutable heads and review

| Item | SHA / value |
| --- | --- |
| P1-33 base / paired bundle | `ce43147c30793a4dbb35ed97649998a3a5426441` |
| Writer correction head | `244bada53ba1bdd7d17dc2eb1dfb669c4c6b57e0` |
| Canonical merge commit | `7b11b83268c72634dbfe36f4fe5f61b447975770` |
| Agent task | `agt_20260829T145210Z_399528c0066f` |
| Agentctl receipt digest | `22c84b3f69a87fa4c4284bf8ba148b4cd26fe9143c5ea0dfa0864ffcbff20dde` |
| Handoff | `handoff-f7bf07aa9d49` |
| Handoff SHA-256 | `04c69e36c0261b58d77e7b77bc1dd54b2f2aee25166c8e10c330daa266403c33` |
| Independent reviewer | Luna Max `p1_15_luna_writer` |
| Review result | APPROVE at exact range `ce43147c..244bada5` |

## Evidence boundary

The exact four changed paths are the staging activation receipt schema,
pending-approval fixture, focused tests, and the P1-33 activation-receipt
runbook. The P1-31 release runbook is unchanged. The Draft 2020-12 schema
requires the seven restart/no-op/change/custody/lifecycle/telemetry/rollback
outcomes, approval state, exact bundle commit, and non-mutating prohibitions.

Three focused tests passed, together with Ruff check/format, Pyright,
compilation, JSON Schema validation, negative schema probes, and
`git diff --check`. The fixture remains `state: pending_approval` with
`activation_performed: false`; every outcome remains pending and
`mutation_performed: false`.

## Operational gate and rollback

Human approval for `staging-activation` is still required before restart,
migration, credential resolution, source probes, or runtime-state writes. No
service was started, no port was bound, no migration was applied or downgraded,
and no source or production system was contacted. The pending receipt must be
retained and must not be rewritten as a success receipt. After approval, the
operator must capture a new immutable receipt for the exact outcomes and retain
this preparation receipt alongside it.

If an approved canary fails, pause scheduling, drain workers, snapshot the
control store, restore the prior verified snapshot, verify raw-custody
manifests and queue/receipt preservation, validate the exact bundle, and only
then resume. Rollback is non-destructive and does not delete evidence.
