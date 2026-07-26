# Running the frozen pilot

The official controller is `python -m hspr_benchmark.runner`. It accepts only
the frozen pilot questions, arms, registry, response schema, and runtime roots.
The sealed key is read by the host solely for the mandatory registry leakage
audit and is never copied or mounted into an answer context.

Use an ignored `.benchmark-runs/` host directory for traces and answers. Pass
the local Codex package and authentication files by their explicit absolute
paths. Execution requires both `--execute` and `--allow-live-credential`.
`--start` is the one-based frozen schedule position and `--trial-count` is the
number of trials in a deliberate batch. It does not limit web calls or prescribe
a research sequence inside a trial. `--attempt` defaults to 1 and is part of the
immutable trial identifier.

Each trial gets a new immutable directory containing the native trace, answer,
observable summary, and schedule metadata. A batch refuses to overwrite an
existing trial directory. Verified completed trials are skipped when resuming a
slice. A trial writes first to an `in-progress-*` directory and is atomically
renamed only after all required artifacts and metadata exist. After an
interrupted trial, inspect and preserve its partial trace, then rerun the same
schedule position with the next explicit `--attempt` value. Failed attempts
remain excluded from scoring and are never silently reused or overwritten.

The ignored run root contains immutable schedule and input manifests. Before
every slice, the controller verifies SHA-256 digests for questions, arms,
registry, response schema, runtime, sealed manifest/adjudicated key, and the
full Codex package tree, together with the clean Git revision and pinned image.
The adjudicated key must resolve to the exact location declared by the tracked
sealed manifest and match its digest prefix and record count. These controller
manifests are never mounted into answer contexts.

The credential is opened without following a final symlink, capped at 1 MiB,
copied into a fresh byte array for one trial, passed to the container supervisor
over stdin, and zeroed by the host controller after that trial. Credential paths
and values are not written to run metadata.
