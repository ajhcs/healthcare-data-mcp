# Running the frozen pilot

The official controller is `python -m hspr_benchmark.runner`. It accepts only
the frozen pilot questions, arms, registry, response schema, and runtime roots.
The sealed key is read from the protected, manifest-declared Plumbob scorer root
solely for the mandatory registry leakage audit and scoring. It is never copied
or mounted into an answer context. The old `.benchmark-sealed/` worktree path is
not a valid source of truth.

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
The protected active manifest also binds the frozen pilot preregistration; its
question count must match the protected question packet, its public-history
audit must be less than one hour old, and every fetched public ref must still
match the audit's exact SHA.
The adjudicated key must resolve to the exact location declared by the tracked
sealed manifest and match its digest prefix and record count. These controller
manifests are never mounted into answer contexts.

The host auth document is opened without following a final symlink and capped at
1 MiB. The controller validates fresh access and identity JWTs, constructs a new
document containing only the access token and account ID, passes that document
to the fileless app-server supervisor over stdin, and zeroes its mutable arrays
after the trial. The ID token, refresh token, API key, auth metadata, paths, and
values are not written to run metadata or forwarded to the answer context.
