import hashlib
import json
import stat
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest

from hspr_benchmark import github_metadata_collector
from hspr_benchmark.github_metadata_audit import audit_github_metadata_capture
from hspr_benchmark.github_metadata_collector import (
    CollectionError,
    GitHubMetadataCollector,
    revalidate_capture,
)

NOW = datetime(2026, 7, 27, 14, 0, tzinfo=UTC)
TOKEN = "trusted-scorer-token-value"  # pragma: allowlist secret
PUBLIC_REFS = {"refs/remotes/origin/main": "a" * 40}


def _private_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value), encoding="utf-8")
    path.chmod(0o600)


def _inputs(tmp_path: Path) -> tuple[Path, Path]:
    questions = tmp_path / "questions.json"
    identity = tmp_path / "identity.json"
    _private_json(
        questions,
        {
            "publication_status": "protected_unpublished_active_packet",
            "questions": [{"question_id": "q1", "system_id": "HSI1", "system": "Hidden Health"}],
        },
    )
    _private_json(
        identity,
        {
            "systems": [
                {
                    "system_id": "HSI1",
                    "canonical_name": "Hidden Health",
                    "aliases": [],
                    "legal_entities": [],
                    "identifiers": [],
                    "relationships": [],
                }
            ]
        },
    )
    return questions, identity


class FakeGitHub:
    def __init__(
        self,
        *,
        paginated_issues: bool = False,
        paginated_discussions: bool = False,
        release_asset: bool = False,
        public_fork: bool = False,
        fork_leak: bool = False,
        paginated_fork_issues: bool = False,
        incomplete_fork: bool = False,
        expired_artifact: bool = False,
        drift_issues: bool = False,
        echo_token: bool = False,
    ) -> None:
        self.paginated_issues = paginated_issues
        self.paginated_discussions = paginated_discussions
        self.release_asset = release_asset
        self.public_fork = public_fork
        self.fork_leak = fork_leak
        self.paginated_fork_issues = paginated_fork_issues
        self.incomplete_fork = incomplete_fork
        self.expired_artifact = expired_artifact
        self.drift_issues = drift_issues
        self.echo_token = echo_token
        self.issue_requests = 0
        self.fork_issue_requests = 0
        self.requests: list[httpx.Request] = []
        self.force_drift = False

    @staticmethod
    def _etag(content: bytes) -> str:
        return f'"{hashlib.sha256(content).hexdigest()}"'

    def _response(self, request: httpx.Request, status: int, value: object) -> httpx.Response:
        content = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
        etag = self._etag(content)
        if request.headers.get("if-none-match") == etag and not self.force_drift:
            return httpx.Response(304, headers={"etag": etag}, request=request)
        if self.force_drift:
            content += b" "
            etag = self._etag(content)
        return httpx.Response(status, content=content, headers={"etag": etag}, request=request)

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if request.url.host == "api.github.com":
            assert request.headers.get("authorization") == f"Bearer {TOKEN}"
            assert request.headers.get("x-github-api-version") == "2026-03-10"
        else:
            assert "authorization" not in request.headers
        path = request.url.path
        if path == "/graphql":
            request_document = json.loads(request.content)
            query = request_document["query"]
            variables = request_document["variables"]
            if "discussionCategories(first" in query:
                nodes = (
                    [{"id": "unused-category", "name": "Unused", "description": "No discussions yet"}]
                    if self.paginated_discussions
                    else []
                )
                return self._response(
                    request,
                    200,
                    {
                        "data": {
                            "repository": {
                                "discussionCategories": {
                                    "totalCount": len(nodes),
                                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                                    "nodes": nodes,
                                }
                            }
                        }
                    },
                )
            if self.paginated_discussions and "discussions(first" in query:
                cursor = variables.get("cursor")
                index = 1 if cursor is None else 2
                connection = {
                    "totalCount": 2,
                    "pageInfo": {"hasNextPage": index == 1, "endCursor": "discussion-cursor"},
                    "nodes": [
                        {
                            "id": f"discussion-{index}",
                            "title": f"ordinary topic {index}",
                            "body": "unrelated public discussion",
                            "updatedAt": NOW.isoformat(),
                            "category": {"name": "General", "description": "General discussion"},
                        }
                    ],
                }
                return self._response(request, 200, {"data": {"repository": {"discussions": connection}}})
            if self.paginated_discussions and "comments(first" in query:
                return self._response(
                    request,
                    200,
                    {
                        "data": {
                            "node": {
                                "comments": {
                                    "totalCount": 0,
                                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                                    "nodes": [],
                                }
                            }
                        }
                    },
                )
            return self._response(
                request,
                200,
                {
                    "data": {
                        "repository": {
                            "discussions": {
                                "totalCount": 0,
                                "pageInfo": {"hasNextPage": False, "endCursor": None},
                                "nodes": [],
                            }
                        }
                    }
                },
            )
        if request.url.host == "github.com" and path.endswith(".wiki.git/info/refs"):
            return self._response(request, 404, {"message": "Not Found"})
        if request.url.host == "objects.githubusercontent.com" and path == "/release-asset.bin":
            content = b"asset-bytes!"
            return httpx.Response(
                200,
                content=content,
                headers={"etag": self._etag(content), "content-length": str(len(content))},
                request=request,
            )
        if path.endswith("/pages"):
            return self._response(request, 404, {"message": "Not Found"})
        if path == "/repos/ajhcs/healthcare-data-mcp":
            body: dict[str, object] = {
                "id": 1206377365,
                "node_id": "repository-node-id",
                "name": "healthcare-data-mcp",
                "visibility": "public",
                "owner": {"login": "ajhcs"},
                "has_discussions": self.paginated_discussions,
                "updated_at": NOW.isoformat(),
                "description": "public healthcare integrations",
            }
            if self.echo_token:
                body["reflected"] = TOKEN
            return self._response(request, 200, body)
        if path in {
            "/repos/fork-owner-22/healthcare-data-mcp",
            "/repos/fork-owner-23/healthcare-data-mcp",
        }:
            fork_id = int(path.split("/")[2].removeprefix("fork-owner-"))
            return self._response(
                request,
                200,
                {
                    "id": fork_id,
                    "node_id": f"fork-node-{fork_id}",
                    "name": "healthcare-data-mcp",
                    "visibility": "public",
                    "owner": {"login": f"fork-owner-{fork_id}"},
                    "parent": {"id": 1206377365},
                    "source": {"id": 1206377365},
                    "has_discussions": False,
                    "updated_at": NOW.isoformat(),
                    "description": "Hidden Health" if self.fork_leak and fork_id == 22 else "ordinary fork",
                },
            )
        if self.incomplete_fork and path == "/repos/fork-owner-22/healthcare-data-mcp/security-advisories":
            return self._response(request, 403, {"message": "Forbidden"})
        if path.endswith("/actions/runs"):
            return self._response(request, 200, {"total_count": 0, "workflow_runs": []})
        if path.endswith("/actions/artifacts"):
            artifacts = (
                [
                    {
                        "id": 31,
                        "name": "expired-output",
                        "expired": True,
                        "archive_download_url": "https://api.github.com/repos/ajhcs/healthcare-data-mcp/actions/artifacts/31/zip",
                        "size_in_bytes": 10,
                        "updated_at": NOW.isoformat(),
                    }
                ]
                if self.expired_artifact
                else []
            )
            return self._response(request, 200, {"total_count": len(artifacts), "artifacts": artifacts})
        if path == "/repos/ajhcs/healthcare-data-mcp/forks":
            forks = (
                [
                    {
                        "id": fork_id,
                        "name": "healthcare-data-mcp",
                        "owner": {"login": f"fork-owner-{fork_id}"},
                    }
                    for fork_id in (22, 23)
                ]
                if self.public_fork
                else []
            )
            return self._response(request, 200, forks)
        if path.endswith("/forks"):
            return self._response(request, 200, [])
        if path.endswith("/releases"):
            releases = [{"id": 7, "name": "ordinary release", "body": "maintenance", "updated_at": NOW.isoformat()}]
            return self._response(request, 200, releases if self.release_asset else [])
        if path.endswith("/releases/7/assets"):
            content = b"asset-bytes!"
            return self._response(
                request,
                200,
                [
                    {
                        "id": 9,
                        "name": "evidence.txt",
                        "url": "https://api.github.com/repos/ajhcs/healthcare-data-mcp/releases/assets/9",
                        "size": len(content),
                        "digest": f"sha256:{hashlib.sha256(content).hexdigest()}",
                        "updated_at": NOW.isoformat(),
                    }
                ],
            )
        if path.endswith("/releases/assets/9"):
            return httpx.Response(
                302,
                headers={"location": "https://objects.githubusercontent.com/release-asset.bin"},
                request=request,
            )
        if path.endswith("/issues") and request.url.params.get("state") == "all":
            if path.startswith("/repos/fork-owner-"):
                self.fork_issue_requests += 1
                fork_id = int(path.split("/")[2].removeprefix("fork-owner-"))
                issue = {
                    "id": 1000 + fork_id,
                    "number": 1,
                    "title": "ordinary fork maintenance",
                    "body": "unrelated public fork issue",
                    "updated_at": NOW.isoformat(),
                }
                if self.paginated_fork_issues and request.url.params.get("page") is None:
                    response = self._response(request, 200, [issue])
                    response.headers["link"] = (
                        f'<https://api.github.com{path}?state=all&per_page=100&page=2>; rel="next"'
                    )
                    return response
                return (
                    self._response(request, 200, [])
                    if self.paginated_fork_issues
                    else self._response(request, 200, [issue])
                )
            self.issue_requests += 1
            issue = {
                "id": 11,
                "number": 1,
                "title": "ordinary maintenance",
                "body": "unrelated public issue",
                "updated_at": NOW.isoformat(),
            }
            if self.drift_issues and self.issue_requests >= 2:
                issue["body"] = "changed between passes"
            if self.paginated_issues and request.url.params.get("page") is None:
                content = [issue]
                response = self._response(request, 200, content)
                response.headers["link"] = (
                    "<https://api.github.com/repos/ajhcs/healthcare-data-mcp/issues?state=all&per_page=100&page=2>; "
                    'rel="next"'
                )
                return response
            if self.paginated_issues:
                return self._response(request, 200, [])
            return self._response(request, 200, [issue])
        return self._response(request, 200, [])


def _collector(tmp_path: Path, fake: FakeGitHub) -> tuple[GitHubMetadataCollector, httpx.Client, Path, Path]:
    questions, identity = _inputs(tmp_path)
    client = httpx.Client(transport=httpx.MockTransport(fake))
    collector = GitHubMetadataCollector(
        client=client,
        token=TOKEN,
        output_root=tmp_path / "capture",
        questions_path=questions,
        identity_path=identity,
        public_ref_shas=PUBLIC_REFS,
        now=lambda: NOW,
    )
    return collector, client, questions, identity


def test_collector_builds_private_stable_auditable_capture_without_persisting_auth(tmp_path: Path) -> None:
    fake = FakeGitHub(paginated_issues=True)
    collector, client, questions, identity = _collector(tmp_path, fake)
    try:
        manifest_path = collector.collect()
    finally:
        client.close()
    assert stat.S_IMODE(manifest_path.stat().st_mode) == 0o600
    evidence = list((manifest_path.parent / "evidence").iterdir())
    assert evidence and all(stat.S_IMODE(path.stat().st_mode) == 0o600 for path in evidence)
    assert TOKEN.encode() not in manifest_path.read_bytes()
    assert all(TOKEN.encode() not in path.read_bytes() for path in evidence)
    manifest = json.loads(manifest_path.read_text())
    assert manifest["surfaces"]["issues"]["expected_count"] == 1
    assert manifest["surfaces"]["issues"]["first_pass_sha256"] == manifest["surfaces"]["issues"]["second_pass_sha256"]
    assert {entry["surface"] for entry in manifest["revalidation"]} == set(manifest["surfaces"])
    issue_pages = [entry for entry in manifest["revalidation"] if entry["surface"] == "issues"]
    assert any(entry["next_url"] for entry in issue_pages)
    assert all(
        entry["next_url"] is None or any(candidate["url"] == entry["next_url"] for candidate in issue_pages)
        for entry in issue_pages
    )
    result = audit_github_metadata_capture(manifest_path, questions, identity, now=NOW)
    assert result["passed"]


def test_collector_fails_closed_on_two_pass_drift(tmp_path: Path) -> None:
    collector, client, _, _ = _collector(tmp_path, FakeGitHub(drift_issues=True))
    try:
        with pytest.raises(CollectionError, match="changed during capture: issues"):
            collector.collect()
    finally:
        client.close()
    assert not (tmp_path / "capture/manifest.json").exists()


def test_collector_downloads_redirected_release_asset_and_checks_digest(tmp_path: Path) -> None:
    collector, client, _, _ = _collector(tmp_path, FakeGitHub(release_asset=True))
    try:
        manifest = json.loads(collector.collect().read_text())
    finally:
        client.close()
    asset = manifest["surfaces"]["release_assets"]["records"][0]["content"]
    assert asset["captured_sha256"] == hashlib.sha256(b"asset-bytes!").hexdigest()


def test_collector_fails_closed_at_total_download_byte_cap(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(github_metadata_collector, "MAX_TOTAL_DOWNLOAD_BYTES", 1)
    collector, client, _, _ = _collector(tmp_path, FakeGitHub(release_asset=True))
    try:
        with pytest.raises(CollectionError, match="total capture byte cap"):
            collector.collect()
    finally:
        client.close()


def test_collector_fully_captures_two_public_forks(tmp_path: Path) -> None:
    collector, client, questions, identity = _collector(tmp_path, FakeGitHub(public_fork=True))
    try:
        manifest_path = collector.collect()
    finally:
        client.close()
    manifest = json.loads(manifest_path.read_text())
    assert manifest["surfaces"]["forks"]["expected_count"] == 2
    assert {record["id"] for record in manifest["surfaces"]["fork_metadata"]["records"]} == {
        "fork:22",
        "fork:23",
    }
    assert {entry["repository_id"] for entry in manifest["revalidation"]} == {22, 23, 1206377365}
    assert audit_github_metadata_capture(manifest_path, questions, identity, now=NOW)["passed"]


def test_collector_scans_fork_metadata_for_identity_leakage(tmp_path: Path) -> None:
    collector, client, questions, identity = _collector(tmp_path, FakeGitHub(public_fork=True, fork_leak=True))
    try:
        manifest_path = collector.collect()
    finally:
        client.close()
    result = audit_github_metadata_capture(manifest_path, questions, identity, now=NOW)
    assert not result["passed"]
    assert any(hit["surface"] == "fork_metadata" for hit in result["blocking_hits"])


def test_collector_exhausts_nested_fork_pagination(tmp_path: Path) -> None:
    fake = FakeGitHub(public_fork=True, paginated_fork_issues=True)
    collector, client, _, _ = _collector(tmp_path, fake)
    try:
        manifest = json.loads(collector.collect().read_text())
    finally:
        client.close()
    assert manifest["surfaces"]["issues"]["expected_count"] == 3
    assert any(
        entry["repository_id"] in {22, 23} and entry["surface"] == "issues" and entry["next_url"]
        for entry in manifest["revalidation"]
    )


def test_collector_blocks_when_any_fork_surface_is_incomplete(tmp_path: Path) -> None:
    collector, client, _, _ = _collector(tmp_path, FakeGitHub(public_fork=True, incomplete_fork=True))
    try:
        with pytest.raises(CollectionError, match="security_advisories returned HTTP 403"):
            collector.collect()
    finally:
        client.close()


def test_collector_blocks_when_fork_network_exceeds_resource_cap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(github_metadata_collector, "MAX_NETWORK_REPOSITORIES", 2)
    collector, client, _, _ = _collector(tmp_path, FakeGitHub(public_fork=True))
    try:
        with pytest.raises(CollectionError, match="repository cap"):
            collector.collect()
    finally:
        client.close()


def test_expired_actions_artifact_is_recorded_as_not_publicly_retrievable(tmp_path: Path) -> None:
    collector, client, _, _ = _collector(tmp_path, FakeGitHub(expired_artifact=True))
    try:
        manifest = json.loads(collector.collect().read_text())
    finally:
        client.close()
    artifact = manifest["surfaces"]["actions_artifacts"]["records"][0]["content"]
    assert artifact["availability"] == "expired_not_publicly_retrievable"


def test_collector_exhausts_graphql_cursor_pagination(tmp_path: Path) -> None:
    fake = FakeGitHub(paginated_discussions=True)
    collector, client, _, _ = _collector(tmp_path, fake)
    try:
        manifest = json.loads(collector.collect().read_text())
    finally:
        client.close()
    assert manifest["surfaces"]["discussions"]["expected_count"] == 3
    assert any(record["content"].get("name") == "Unused" for record in manifest["surfaces"]["discussions"]["records"])
    graphql_requests = [request for request in fake.requests if request.url.path == "/graphql"]
    assert any(
        json.loads(request.content)["variables"].get("cursor") == "discussion-cursor" for request in graphql_requests
    )


def test_collector_does_not_persist_reflected_scorer_credential(tmp_path: Path) -> None:
    collector, client, _, _ = _collector(tmp_path, FakeGitHub(echo_token=True))
    try:
        with pytest.raises(CollectionError, match="reflected"):
            collector.collect()
    finally:
        client.close()
    assert TOKEN.encode() not in b"".join(
        path.read_bytes() for path in (tmp_path / "capture").rglob("*") if path.is_file()
    )


def test_api_download_uses_github_media_type_while_release_assets_use_octet_stream(tmp_path: Path) -> None:
    collector, client, _, _ = _collector(tmp_path, FakeGitHub())
    try:
        api_url = "https://api.github.com/repos/ajhcs/healthcare-data-mcp/actions/runs/1/logs"
        assert collector._headers(api_url, download=True, api_download=True)["Accept"] == (
            "application/vnd.github+json"
        )
        assert collector._headers(api_url, download=True)["Accept"] == "application/octet-stream"
    finally:
        client.close()


def test_conditional_pre_and_post_revalidation_and_drift_detection(tmp_path: Path) -> None:
    fake = FakeGitHub()
    collector, client, _, _ = _collector(tmp_path, fake)
    manifest = collector.collect()
    pre = revalidate_capture(manifest, client=client, token=TOKEN, phase="pre_batch", now=NOW)
    assert pre["passed"] and pre["request_count"] > 0
    post_path = tmp_path / "post-revalidation.json"
    post = revalidate_capture(
        manifest,
        client=client,
        token=TOKEN,
        phase="post_batch",
        output_path=post_path,
        now=NOW + timedelta(hours=2),
    )
    assert post["passed"] and stat.S_IMODE(post_path.stat().st_mode) == 0o600
    fake.force_drift = True
    with pytest.raises(CollectionError, match="content drifted"):
        revalidate_capture(manifest, client=client, token=TOKEN, phase="post_batch", now=NOW)
    client.close()


def test_pre_revalidation_rejects_stale_capture(tmp_path: Path) -> None:
    fake = FakeGitHub()
    collector, client, _, _ = _collector(tmp_path, fake)
    manifest = collector.collect()
    with pytest.raises(CollectionError, match="stale"):
        revalidate_capture(
            manifest,
            client=client,
            token=TOKEN,
            phase="pre_batch",
            now=NOW + timedelta(minutes=11),
        )
    client.close()
