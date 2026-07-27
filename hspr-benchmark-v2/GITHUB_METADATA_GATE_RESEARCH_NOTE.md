# GitHub metadata leakage gate research note

> **Status: optional scorer-side diagnostic; not an execution gate.**
> `hspr_benchmark.github_metadata_collector` captures a protected two-pass
> snapshot and can revalidate it before and after a batch;
> `hspr_benchmark.github_metadata_audit` validates and scans that capture. Two
> protected live attempts are preserved as compatibility evidence: v1 failed
> safely with HTTP 415 at the Actions-log endpoint, and v2 passed that media-type
> boundary but stopped safely on a nonterminal Actions run. Neither is a passing
> attestation, and this note must not be cited as proof that GitHub metadata is
> leakage-free. Under the evidence-proportionate protocol, incomplete or failed
> captures are recorded but do not block answer execution.

## Observed public surface

Read-only GitHub queries on 2026-07-27 UTC identified repository numeric ID
`1206377365` as public. Issues and the wiki feature were enabled; discussions
were disabled. The wiki Git endpoint was uninitialized (`404`), and the Pages
API returned `404`. The current enumerated surface contained:

- 63 pull requests and 1 non-PR issue;
- 6 releases with 3 release assets;
- 177 GitHub Actions workflow runs and 0 current Actions artifacts;
- 2 forks;
- 0 discussions, milestones, and commit comments.

These counts are observations, not frozen facts. They must be re-enumerated by
an implemented gate.

## Proposed collection policy

Pin REST requests to `https://api.github.com`, repository numeric ID
`1206377365`, `Accept: application/vnd.github+json` (using the raw-body media
type where applicable), and `X-GitHub-Api-Version: 2026-03-10`. Fetch serially
with `per_page=100` and follow only GitHub's returned `Link: rel="next"` URLs.
Reject host or repository drift, pagination cycles, duplicate IDs, inconsistent
counts, truncation, malformed responses, unexpected redirects, and any
unresolved `4xx` or `5xx` response. GraphQL-only collections must use cursor
pagination, require unique node IDs, finish with `hasNextPage=false`, and match
`totalCount`.

The audit must cover these current-public collections:

1. Repository metadata and topics: description, homepage, topic names,
   visibility, feature flags, default branch, repository ID, and node ID.
2. All issues and pull requests, including titles, raw bodies, labels,
   milestones, and repository-wide issue comments. Pull requests are also
   issues, but their PR-specific representation must be collected separately.
3. All pull-request review bodies and repository-wide review comments. Reviews
   and review comments are distinct API resources.
4. Repository commit comments.
5. All releases and assets. Scan release names, bodies, tags, target references,
   asset names, and labels. Download each asset, verify its declared size and
   API SHA-256 digest when present, compute an independent SHA-256 digest, and
   inspect its bytes. GitHub-generated tag archives need not be duplicated
   because the Git-ref audit already covers their source objects.
6. Discussions, comments, replies, and category text via GraphQL. A disabled,
   empty collection is itself an attested state rather than an omitted surface.
7. Every public wiki Git ref using the existing Git-history audit. An absent
   wiki is acceptable only when both the public Git probe and feature/web probe
   establish that it is uninitialized or absent.
8. Actions and checks: unfiltered workflow runs; job and step names; check-run
   titles, summaries, text, and annotations; retained downloadable logs; and
   every unexpired artifact. An API result cap must block rather than truncate.
9. Deployments and deployment statuses, including descriptions, environment
   names, and URLs; repository labels and milestones; and public repository
   security advisories.
10. The complete current fork inventory. Each public fork whose parent or
    source is repository ID `1206377365` must have all public Git refs and the
    same repository-scoped metadata audited. A configured resource cap must
    block rather than silently skip forks.
11. GitHub-hosted uploads linked from collected Markdown, including current
    `user-attachments`, legacy `user-images`, and release-download URLs. Fetch
    and inspect each object. Record arbitrary external links; an external link
    adjacent to an active identity requires manual blocking review. If a Pages
    site is live, block official execution instead of treating a finite crawl
    as exhaustive.

Use exactly the active questions and registry to derive system IDs, names,
aliases, legal-entity names, relationship names, and identifiers. For GitHub
metadata, every such identity hit is blocking; do not require a nearby financial
value signal. Any unavailable, oversized, malformed, opaque, encrypted, or
incompletely inspectable GitHub-hosted payload is also blocking.

## Stabilization and immutable evidence

Write raw evidence outside Git in the protected scorer context with exclusive
creation and private permissions. Canonicalize leakage-relevant fields as
UTF-8, sorted-key compact JSON; sort records by `(surface, id)`. Record and hash
each canonical record, raw response page, and downloaded payload. The protected
manifest must include each request URL, status, `ETag`, `Last-Modified`, `Link`
chain, API version, retrieval time, object count, and digest, while excluding
authorization headers and viewer identity.

Use a two-pass stabilization check: enumerate and fetch every parent and child
collection, then repeat every collection index/page and require identical
canonical ID, update-time, and digest sets. Active or queued Actions runs block
stabilization. This is immutable-by-convention evidence bound by SHA-256, not a
cryptographic transparency log, and GitHub does not provide an atomic snapshot
across endpoints.

The audit result must bind:

- the audit schema, policy, implementation SHA-256, and endpoint-spec SHA-256;
- GitHub API version, API host, repository numeric ID/node ID, owner/name, and
  public visibility;
- exact question and registry SHA-256 digests;
- the exact `public_ref_shas` accepted by the Git-history audit;
- the canonical metadata snapshot root SHA-256;
- per-surface object counts and digests; and
- audit start and completion times in UTC.

An implementation should add `github_metadata_audit` to the protected active
manifest. The runner must verify its protected-file digest, implementation and
input bindings, exact public-ref equality, `passed=true`, and empty blocking,
uninspectable, and incomplete-surface lists.

## Freshness and rate limits

The full metadata audit should finish no more than 10 minutes before a batch.
Immediately before execution, the runner must traverse the saved collections
from page one using `If-None-Match`. Accept `304`, or `200` only when the newly
canonicalized digest and current page/ID/`Link` set are identical. A missing
`ETag` requires a complete canonical `GET`, never a skip. Revalidate asset
metadata and content digests, fork and wiki refs, Pages/discussions state, and
repository feature flags. Repeat the same validation after the batch; any drift
quarantines and invalidates that batch.

Handle `403`/`429` responses using `Retry-After` or `X-RateLimit-Reset`; if the
freshness window expires, regenerate the audit. Trusted-host authentication is
preferred and must never be recorded or forwarded to answer contexts. GitHub
documents 5,000 REST requests per hour for ordinary authenticated users and 60
per hour without authentication. Unauthenticated fallback is valid only when a
preflight proves that the remaining budget exceeds the complete worst-case
request count plus a 20% margin. It must never reduce coverage. For the observed
63 PRs, per-PR review enumeration alone makes the ordinary unauthenticated
budget insufficient.

## Residual limits

Even a passing implementation cannot prove the absence of all Internet copies.
GitHub provides no atomic cross-endpoint snapshot. Deleted or edited content
may remain in GitHub or search-engine caches, notifications, GH Archive, or web
archives. Deleted or unlisted forks, orphaned objects, expired Actions logs and
artifacts, and deleted or unlinked uploads cannot be exhaustively enumerated.
Organization/user gists, packages, projects, social-preview images, arbitrary
external links, dynamic/custom Pages sites, and GitHub search-index internals
also remain outside a repository-scoped proof. Private, draft, pending, or
moderated objects are not part of the public answer-agent surface; a privileged
audit may safely over-include them. The defensible claim is therefore limited
to a stable, enumerated current GitHub surface, not all historical public
material.

The collector now recursively applies all 27 declared surfaces to every public
fork enumerated in the canonical repository's network. Fork records and request
ledger entries are repository-ID namespaced and bound to verified parent/source
relationships; cycles, descendants outside the canonical network, incomplete
fork surfaces, and repository/request resource-cap exhaustion fail closed.
Initialized wikis and live Pages sites still require separate content
collectors. Expired Actions artifacts and `404` run logs are recorded as not
publicly retrievable at capture time, while unexpired-but-unretrievable
artifacts still block. GitHub delivery redirects are restricted to a documented
allowlist of GitHub object, Actions, S3 asset, and Azure results hosts, and all
capture/revalidation downloads share a bounded total-byte cap.

## Primary GitHub documentation

- [Using pagination in the REST API](https://docs.github.com/en/rest/using-the-rest-api/using-pagination-in-the-rest-api)
- [REST API versions](https://docs.github.com/en/rest/about-the-rest-api/api-versions)
- [Rate limits for the REST API](https://docs.github.com/en/enterprise-cloud@latest/rest/using-the-rest-api/rate-limits-for-the-rest-api)
- [REST API best practices and conditional requests](https://docs.github.com/en/rest/using-the-rest-api/best-practices-for-using-the-rest-api)
- [Issue comments](https://docs.github.com/en/rest/issues/comments)
- [Pull requests](https://docs.github.com/en/rest/pulls/pulls)
- [Pull-request review comments](https://docs.github.com/en/rest/pulls/comments)
- [Pull-request reviews](https://docs.github.com/en/rest/pulls/reviews)
- [Releases and release assets](https://docs.github.com/en/rest/releases/releases)
- [Workflow runs and logs](https://docs.github.com/en/rest/actions/workflow-runs)
