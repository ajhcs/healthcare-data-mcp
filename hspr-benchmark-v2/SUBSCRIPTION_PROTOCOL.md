# Subscription-native credential boundary

The benchmark uses ChatGPT-plan Codex authentication and categorically rejects
OpenAI API keys. It does not ask for, create, or fall back to an API key.

The trusted host controller reads the local Codex auth document without
following a symlink, requires `auth_mode=chatgpt`, rejects any nonempty
`OPENAI_API_KEY`, and parses the access-token and ID-token expiry claims from
the trusted local auth document against the full bounded trial window plus a
safety margin. It constructs a new auth
document containing only the short-lived access token, ID token, account ID,
and an empty `refresh_token` placeholder required by Codex 0.145.0. The actual
refresh token never enters the answer container. Source and minimized mutable
buffers are zeroed independently, including the supervisor's original stdin
chunks. Transient immutable Python byte copies can remain in the trusted host
controller until garbage collection, so this is best-effort process hygiene,
not a complete memory-erasure claim.

Inside the isolated container, the supervisor writes the minimized document to
auth tmpfs only for Codex startup. Before it releases `thread.started`, it
unlinks the file and rejects the run if any container process retains a file
descriptor to it. The answer context has no repository, sealed-key, historical,
Docker-socket, host-root, or host-output mount. Its shell has no credential path
or credential environment variables and cannot read parent-process memory under
the enforced container/sandbox policy. Native web remains a trusted Codex
runtime service and native events are timestamped by the host controller. The
current local/app native-web interface does not expose an enforceable
per-domain deny policy. Because this repository is public, filesystem isolation
alone cannot prevent an answer arm from retrieving published HSPR or historical
material through native web. This is a blocking boundary for official
web-enabled arms, not something prompts or URL logging can cure.

The remaining limitation is explicit: the trusted Codex runtime necessarily
holds short-lived access/identity tokens in process memory while it contacts the
subscription service. This is practical OS/process isolation, not cryptographic
separation from the runtime vendor or host administrator. Fresh subscription
tokens must already exist; the benchmark does not refresh or mutate reusable
credentials. A stale token blocks execution.

The earlier 17 completed pilot trials used the prior full-auth startup protocol
and are superseded for the subscription-native confirmatory design. They remain
preserved as historical engineering evidence and must not be mixed with new
results. No new official arm may run until the public-repository web boundary is
solved outside the model and the minimized-auth proof, hostile mount probe,
fake-supervisor `/proc` probe, leakage audit, tests, independent review, and a
non-official live boundary smoke all pass on the merged revision.
