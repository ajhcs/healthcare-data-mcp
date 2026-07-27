"""Trusted scorer-side GitHub metadata capture and conditional revalidation."""

from __future__ import annotations

import json
import os
import re
import stat
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urljoin, urlsplit

import httpx

from .github_metadata_audit import (
    API_HOST,
    API_VERSION,
    CAPTURE_POLICY,
    CAPTURE_SCHEMA_VERSION,
    MAX_CAPTURE_AGE_SECONDS,
    PARENT_SURFACES,
    REPOSITORY,
    REQUIRED_SURFACES,
    _canonical_bytes,
    _protected_capture_file,
    _sha256,
    _snapshot_material,
    _strict_json,
    _validate_revalidation,
    allowed_github_download_host,
    collector_implementation_sha256,
    endpoint_spec_sha256,
)
from .public_history_audit import _searchable_blob

REST_PAGE_CAP = 1000
GRAPHQL_PAGE_CAP = 1000
REDIRECT_CAP = 5
MAX_RESPONSE_BYTES = 64 * 1024 * 1024
MAX_GITHUB_UPLOADS = 10_000
MAX_TOTAL_DOWNLOAD_BYTES = 512 * 1024 * 1024
MAX_NETWORK_REPOSITORIES = 100
MAX_LOGICAL_REQUESTS = 100_000
_LINK_NEXT = re.compile(r'<([^>]+)>;\s*rel="next"')
_UPLOAD_URL = re.compile(
    r"https://(?:github\.com/user-attachments/assets/|user-images\.githubusercontent\.com/|"
    r"github\.com/[^/]+/[^/]+/releases/download/)[^\s\]\[()<>'\"]+",
    re.IGNORECASE,
)
_VOLATILE_KEYS = {"download_count"}


class RequestClient(Protocol):
    def request(self, method: str, url: str, **kwargs: Any) -> httpx.Response: ...


class CollectionError(RuntimeError):
    """The public surface could not be captured completely and safely."""


def _write_private_new(path: Path, data: bytes) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(data)
    except BaseException:
        try:
            os.close(descriptor)
        except OSError:
            pass
        raise


def _clean_content(value: object) -> object:
    if isinstance(value, dict):
        return {key: _clean_content(item) for key, item in value.items() if key not in _VOLATILE_KEYS}
    if isinstance(value, list):
        return [_clean_content(item) for item in value]
    return value


def _record(value: dict[str, Any], *, prefix: str = "") -> dict[str, Any]:
    candidate = value.get("id") or value.get("node_id") or value.get("number") or value.get("ref") or value.get("url")
    if candidate is None:
        candidate = _sha256(_canonical_bytes(value))
    record_id = f"{prefix}{candidate}"
    updated_at = next(
        (
            value[key]
            for key in ("updated_at", "submitted_at", "published_at", "created_at")
            if isinstance(value.get(key), str)
        ),
        None,
    )
    return {"id": str(record_id), "updated_at": updated_at, "content": _clean_content(value)}


def _records_digest(records: list[dict[str, Any]]) -> str:
    ordered = sorted(records, key=lambda item: str(item["id"]))
    if len({str(item["id"]) for item in ordered}) != len(ordered):
        raise CollectionError("GitHub collection returned duplicate record IDs")
    return _sha256(_canonical_bytes(ordered))


def _next_link(header: str | None) -> str | None:
    if not header:
        return None
    match = _LINK_NEXT.search(header)
    return match.group(1) if match else None


class GitHubMetadataCollector:
    """Capture every declared GitHub surface without persisting scorer credentials."""

    def __init__(
        self,
        *,
        client: RequestClient,
        token: str,
        output_root: Path,
        questions_path: Path,
        identity_path: Path,
        public_ref_shas: dict[str, str],
        now: Callable[[], datetime] | None = None,
    ) -> None:
        if not token or any(character.isspace() for character in token):
            raise ValueError("a non-empty trusted scorer GitHub token is required")
        self.client = client
        self._token = token
        self._token_bytes = token.encode("utf-8")
        self.output_root = output_root
        self.questions_path = questions_path
        self.identity_path = identity_path
        self.public_ref_shas = public_ref_shas
        self.now = now or (lambda: datetime.now(UTC))
        self.evidence: dict[str, dict[str, Any]] = {}
        self._surface_evidence: dict[int, dict[str, list[str]]] = {}
        self._pass_revalidation: list[dict[str, Any]] = []
        self._request_counter = 0
        self._downloaded_bytes = 0

    @property
    def repo_api(self) -> str:
        return f"https://{API_HOST}/repos/{REPOSITORY['owner']}/{REPOSITORY['name']}"

    def _headers(self, url: str, *, etag: str | None = None, download: bool = False) -> dict[str, str]:
        parsed = urlsplit(url)
        headers = {
            "Accept": "application/octet-stream" if download else "application/vnd.github+json",
            "X-GitHub-Api-Version": API_VERSION,
            "User-Agent": "hspr-benchmark-metadata-audit",
        }
        if parsed.hostname == API_HOST:
            headers["Authorization"] = f"Bearer {self._token}"
        if etag:
            headers["If-None-Match"] = etag
        return headers

    def _validate_url(self, url: str, *, download: bool = False) -> None:
        parsed = urlsplit(url)
        allowed = (
            allowed_github_download_host(parsed.hostname) if download else parsed.hostname in {API_HOST, "github.com"}
        )
        if parsed.scheme != "https" or not allowed or parsed.username or parsed.password:
            raise CollectionError("GitHub request URL escaped the allowed hosts")

    def _raw_request(
        self,
        method: str,
        url: str,
        *,
        request_json: dict[str, Any] | None = None,
        download: bool = False,
        etag: str | None = None,
    ) -> httpx.Response:
        self._validate_url(url, download=download)
        response = self.client.request(
            method,
            url,
            headers=self._headers(url, etag=etag, download=download),
            json=request_json,
            follow_redirects=False,
        )
        if len(response.content) > MAX_RESPONSE_BYTES:
            raise CollectionError("GitHub response exceeded the capture size limit")
        if self._token_bytes in response.content:
            raise CollectionError("GitHub response reflected the scorer credential")
        return response

    def _logical_request(
        self,
        surface: str,
        pass_number: int,
        method: str,
        url: str,
        *,
        request_json: dict[str, Any] | None = None,
        kind: str = "json",
        accepted_statuses: set[int] | None = None,
        repository_id: int = int(REPOSITORY["id"]),
    ) -> httpx.Response:
        accepted = accepted_statuses or {200}
        original_url = url
        response = self._raw_request(method, url, request_json=request_json, download=kind == "download")
        redirects = 0
        while response.status_code in {301, 302, 303, 307, 308}:
            if kind != "download" or redirects >= REDIRECT_CAP:
                raise CollectionError("unexpected or excessive GitHub redirect")
            location = response.headers.get("location")
            if not location:
                raise CollectionError("GitHub download redirect omitted Location")
            url = urljoin(url, location)
            response = self._raw_request("GET", url, download=True)
            redirects += 1
        if response.status_code not in accepted:
            raise CollectionError(f"GitHub surface {surface} returned HTTP {response.status_code}")
        if kind == "download" and response.headers.get("content-encoding") is None:
            declared_length = response.headers.get("content-length")
            if declared_length is not None:
                try:
                    expected_length = int(declared_length)
                except ValueError as error:
                    raise CollectionError("GitHub download returned an invalid Content-Length") from error
                if expected_length != len(response.content):
                    raise CollectionError("GitHub download Content-Length did not match its bytes")
        if kind == "download" and response.status_code == 200:
            self._downloaded_bytes += len(response.content)
            if self._downloaded_bytes > MAX_TOTAL_DOWNLOAD_BYTES:
                raise CollectionError("GitHub downloads exceeded the total capture byte cap")
        self._request_counter += 1
        if self._request_counter > MAX_LOGICAL_REQUESTS:
            raise CollectionError("GitHub capture exceeded the logical request cap")
        request_id = f"p{pass_number:01d}-r{repository_id}-{self._request_counter:05d}-{surface}"
        suffix = ".json" if kind in {"json", "probe"} else ".bin"
        relative = Path("evidence") / f"{request_id}{suffix}"
        _write_private_new(self.output_root / relative, response.content)
        self.evidence[request_id] = {
            "path": relative.as_posix(),
            "sha256": _sha256(response.content),
            "size": len(response.content),
        }
        self._surface_evidence[pass_number][surface].append(request_id)
        if pass_number == 2:
            self._pass_revalidation.append(
                {
                    "id": request_id,
                    "repository_id": repository_id,
                    "surface": surface,
                    "method": method,
                    "url": original_url,
                    "request_json": request_json,
                    "kind": kind,
                    "status_code": response.status_code,
                    "etag": response.headers.get("etag"),
                    "last_modified": response.headers.get("last-modified"),
                    "link": response.headers.get("link"),
                    "next_url": _next_link(response.headers.get("link")),
                    "response_sha256": _sha256(response.content),
                    "response_size": len(response.content),
                }
            )
        return response

    def _json(self, response: httpx.Response, surface: str) -> object:
        try:
            return response.json()
        except (json.JSONDecodeError, UnicodeDecodeError) as error:
            raise CollectionError(f"GitHub surface {surface} returned invalid JSON") from error

    def _rest_list(
        self,
        surface: str,
        pass_number: int,
        url: str,
        *,
        item_key: str | None = None,
        repository_id: int = int(REPOSITORY["id"]),
    ) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        visited: set[str] = set()
        page_count = 0
        declared_total: int | None = None
        while url:
            if url in visited or page_count >= REST_PAGE_CAP:
                raise CollectionError(f"GitHub pagination did not terminate for {surface}")
            visited.add(url)
            page_count += 1
            response = self._logical_request(surface, pass_number, "GET", url, repository_id=repository_id)
            document = self._json(response, surface)
            if item_key is None:
                page_items = document
            else:
                if not isinstance(document, dict) or not isinstance(document.get(item_key), list):
                    raise CollectionError(f"GitHub surface {surface} omitted {item_key}")
                page_items = document[item_key]
                total = document.get("total_count")
                if type(total) is int:
                    declared_total = total
            if not isinstance(page_items, list) or not all(isinstance(item, dict) for item in page_items):
                raise CollectionError(f"GitHub surface {surface} returned a malformed list")
            items.extend(page_items)
            next_url = _next_link(response.headers.get("link"))
            if next_url:
                self._validate_url(next_url)
            url = next_url or ""
        if declared_total is not None and declared_total != len(items):
            raise CollectionError(f"GitHub surface {surface} was truncated")
        return items

    def _graphql_connection(
        self,
        surface: str,
        pass_number: int,
        *,
        query: str,
        variables: dict[str, Any],
        extract: Callable[[dict[str, Any]], dict[str, Any]],
        repository_id: int = int(REPOSITORY["id"]),
    ) -> list[dict[str, Any]]:
        nodes: list[dict[str, Any]] = []
        cursor: str | None = None
        seen_cursors: set[str] = set()
        for _ in range(GRAPHQL_PAGE_CAP):
            request_json = {"query": query, "variables": {**variables, "cursor": cursor}}
            response = self._logical_request(
                surface,
                pass_number,
                "POST",
                f"https://{API_HOST}/graphql",
                request_json=request_json,
                repository_id=repository_id,
            )
            document = self._json(response, surface)
            if not isinstance(document, dict) or document.get("errors"):
                raise CollectionError(f"GitHub GraphQL surface {surface} returned errors")
            connection = extract(document)
            page_nodes = connection.get("nodes")
            page_info = connection.get("pageInfo")
            total_count = connection.get("totalCount")
            if (
                not isinstance(page_nodes, list)
                or not all(isinstance(node, dict) for node in page_nodes)
                or not isinstance(page_info, dict)
            ):
                raise CollectionError(f"GitHub GraphQL surface {surface} is malformed")
            nodes.extend(page_nodes)
            if not page_info.get("hasNextPage"):
                if type(total_count) is int and total_count != len(nodes):
                    raise CollectionError(f"GitHub GraphQL surface {surface} was truncated")
                return nodes
            cursor = page_info.get("endCursor")
            if not isinstance(cursor, str) or not cursor or cursor in seen_cursors:
                raise CollectionError(f"GitHub GraphQL cursor did not advance for {surface}")
            seen_cursors.add(cursor)
        raise CollectionError(f"GitHub GraphQL pagination exceeded its cap for {surface}")

    def _download(
        self,
        surface: str,
        pass_number: int,
        url: str,
        *,
        allow_unavailable: bool = False,
        repository_id: int = int(REPOSITORY["id"]),
    ) -> tuple[str | None, int, bytes]:
        accepted = {200, 404} if allow_unavailable else {200}
        response = self._logical_request(
            surface,
            pass_number,
            "GET",
            url,
            kind="download",
            accepted_statuses=accepted,
            repository_id=repository_id,
        )
        if response.status_code == 404:
            return None, 0, response.content
        return _sha256(response.content), len(response.content), response.content

    def _collect_discussions(
        self,
        pass_number: int,
        *,
        owner: str | None = None,
        name: str | None = None,
        repository_id: int = int(REPOSITORY["id"]),
        record_prefix: str = "",
    ) -> list[dict[str, Any]]:
        owner = owner or str(REPOSITORY["owner"])
        name = name or str(REPOSITORY["name"])
        categories_query = """
        query($owner:String!,$name:String!,$cursor:String){repository(owner:$owner,name:$name){
          discussionCategories(first:100,after:$cursor){totalCount pageInfo{hasNextPage endCursor}
            nodes{id name description}}}}
        """
        categories = self._graphql_connection(
            "discussions",
            pass_number,
            query=categories_query,
            variables={"owner": owner, "name": name},
            extract=lambda doc: doc["data"]["repository"]["discussionCategories"],
            repository_id=repository_id,
        )
        records = [_record(item, prefix=f"{record_prefix}category:") for item in categories]
        discussions_query = """
        query($owner:String!,$name:String!,$cursor:String){repository(owner:$owner,name:$name){
          discussions(first:100,after:$cursor){totalCount pageInfo{hasNextPage endCursor}
            nodes{id title body updatedAt category{name description}}}}}
        """
        discussions = self._graphql_connection(
            "discussions",
            pass_number,
            query=discussions_query,
            variables={"owner": owner, "name": name},
            extract=lambda doc: doc["data"]["repository"]["discussions"],
            repository_id=repository_id,
        )
        records.extend(_record(item, prefix=f"{record_prefix}discussion:") for item in discussions)
        comments_query = """
        query($id:ID!,$cursor:String){node(id:$id){... on Discussion{
          comments(first:100,after:$cursor){totalCount pageInfo{hasNextPage endCursor} nodes{id body updatedAt}}}}}
        """
        replies_query = """
        query($id:ID!,$cursor:String){node(id:$id){... on DiscussionComment{
          replies(first:100,after:$cursor){totalCount pageInfo{hasNextPage endCursor} nodes{id body updatedAt}}}}}
        """
        for discussion in discussions:
            comments = self._graphql_connection(
                "discussions",
                pass_number,
                query=comments_query,
                variables={"id": discussion["id"]},
                extract=lambda doc: doc["data"]["node"]["comments"],
                repository_id=repository_id,
            )
            records.extend(_record(item, prefix=f"{record_prefix}comment:") for item in comments)
            for comment in comments:
                replies = self._graphql_connection(
                    "discussions",
                    pass_number,
                    query=replies_query,
                    variables={"id": comment["id"]},
                    extract=lambda doc: doc["data"]["node"]["replies"],
                    repository_id=repository_id,
                )
                records.extend(_record(item, prefix=f"{record_prefix}reply:") for item in replies)
        return records

    def _collect_fork_scope(
        self,
        pass_number: int,
        fork: dict[str, Any],
        *,
        canonical_fork_ids: set[int],
    ) -> tuple[dict[str, list[dict[str, Any]]], dict[str, str]]:
        """Collect every declared surface for one canonical-network public fork."""
        fork_id = fork.get("id")
        owner_value = fork.get("owner")
        owner = owner_value.get("login") if isinstance(owner_value, dict) else None
        name = fork.get("name")
        if type(fork_id) is not int or not isinstance(owner, str) or not owner or not isinstance(name, str) or not name:
            raise CollectionError("GitHub fork enumeration returned malformed identity metadata")
        prefix = f"fork:{fork_id}:"
        repo_api = f"https://{API_HOST}/repos/{owner}/{name}"
        records = {surface: [] for surface in REQUIRED_SURFACES}
        states = {surface: "complete" for surface in REQUIRED_SURFACES}
        evidence_before = {surface: len(self._surface_evidence[pass_number][surface]) for surface in REQUIRED_SURFACES}

        response = self._logical_request("fork_metadata", pass_number, "GET", repo_api, repository_id=fork_id)
        metadata = self._json(response, "fork_metadata")
        if not isinstance(metadata, dict):
            raise CollectionError("GitHub fork repository response is malformed")
        detailed_owner = metadata.get("owner")
        expected = {"id": fork_id, "name": name, "visibility": "public"}
        if (
            any(metadata.get(key) != value for key, value in expected.items())
            or not isinstance(detailed_owner, dict)
            or str(detailed_owner.get("login", "")).casefold() != owner.casefold()
            or not isinstance(metadata.get("node_id"), str)
            or not metadata["node_id"]
        ):
            raise CollectionError("GitHub fork identity or visibility changed")
        parent = metadata.get("parent")
        source = metadata.get("source")
        parent_id = parent.get("id") if isinstance(parent, dict) else None
        source_id = source.get("id") if isinstance(source, dict) else None
        if int(REPOSITORY["id"]) not in {parent_id, source_id}:
            raise CollectionError("GitHub fork does not resolve to the canonical repository network")
        records["fork_metadata"] = [_record(metadata, prefix="fork:")]

        for ref_kind in ("heads", "tags"):
            refs = self._rest_list(
                "fork_git_refs",
                pass_number,
                f"{repo_api}/git/matching-refs/{ref_kind}/?per_page=100",
                repository_id=fork_id,
            )
            records["fork_git_refs"].extend(_record(item, prefix=f"{prefix}{ref_kind}:") for item in refs)

        simple_lists = {
            "issues": (f"{repo_api}/issues?state=all&per_page=100", None),
            "issue_comments": (f"{repo_api}/issues/comments?per_page=100", None),
            "pull_requests": (f"{repo_api}/pulls?state=all&per_page=100", None),
            "pull_request_review_comments": (f"{repo_api}/pulls/comments?per_page=100", None),
            "commit_comments": (f"{repo_api}/comments?per_page=100", None),
            "releases": (f"{repo_api}/releases?per_page=100", None),
            "actions_runs": (f"{repo_api}/actions/runs?per_page=100", "workflow_runs"),
            "actions_artifacts": (f"{repo_api}/actions/artifacts?per_page=100", "artifacts"),
            "deployments": (f"{repo_api}/deployments?per_page=100", None),
            "labels": (f"{repo_api}/labels?per_page=100", None),
            "milestones": (f"{repo_api}/milestones?state=all&per_page=100", None),
            "security_advisories": (f"{repo_api}/security-advisories?per_page=100", None),
            "forks": (f"{repo_api}/forks?per_page=100", None),
        }
        raw: dict[str, list[dict[str, Any]]] = {}
        for surface, (url, key) in simple_lists.items():
            raw[surface] = self._rest_list(surface, pass_number, url, item_key=key, repository_id=fork_id)
            if surface != "forks":
                records[surface] = [_record(item, prefix=prefix) for item in raw[surface]]
        for child in raw["forks"]:
            child_id = child.get("id")
            if type(child_id) is not int or child_id == fork_id:
                raise CollectionError("GitHub fork recursion returned a malformed cycle")
            if child_id not in canonical_fork_ids:
                raise CollectionError("GitHub fork recursion discovered an unenumerated descendant")

        for pull in raw["pull_requests"]:
            number = pull.get("number")
            if type(number) is not int:
                raise CollectionError("GitHub fork pull request is missing its number")
            children = self._rest_list(
                "pull_request_reviews",
                pass_number,
                f"{repo_api}/pulls/{number}/reviews?per_page=100",
                repository_id=fork_id,
            )
            records["pull_request_reviews"].extend(_record(item, prefix=prefix) for item in children)

        for release in raw["releases"]:
            release_id = release.get("id")
            if type(release_id) is not int:
                raise CollectionError("GitHub fork release is missing its ID")
            assets = self._rest_list(
                "release_assets",
                pass_number,
                f"{repo_api}/releases/{release_id}/assets?per_page=100",
                repository_id=fork_id,
            )
            for asset in assets:
                url = asset.get("url")
                if not isinstance(url, str):
                    raise CollectionError("GitHub fork release asset is missing its API URL")
                digest, size, _ = self._download("release_assets", pass_number, url, repository_id=fork_id)
                if digest is None or asset.get("size") != size:
                    raise CollectionError("GitHub fork release asset was not captured completely")
                declared_digest = asset.get("digest")
                if (
                    isinstance(declared_digest, str)
                    and declared_digest.startswith("sha256:")
                    and declared_digest.removeprefix("sha256:") != digest
                ):
                    raise CollectionError("GitHub fork release asset digest did not match metadata")
                records["release_assets"].append(_record({**asset, "captured_sha256": digest}, prefix=prefix))

        for run in raw["actions_runs"]:
            run_id = run.get("id")
            suite_id = run.get("check_suite_id")
            if type(run_id) is not int or type(suite_id) is not int or run.get("status") != "completed":
                raise CollectionError("GitHub fork Actions run is missing terminal IDs")
            jobs = self._rest_list(
                "actions_jobs",
                pass_number,
                f"{repo_api}/actions/runs/{run_id}/jobs?per_page=100",
                item_key="jobs",
                repository_id=fork_id,
            )
            records["actions_jobs"].extend(_record(item, prefix=prefix) for item in jobs)
            digest, size, _ = self._download(
                "actions_logs",
                pass_number,
                f"{repo_api}/actions/runs/{run_id}/logs",
                allow_unavailable=True,
                repository_id=fork_id,
            )
            records["actions_logs"].append(
                _record(
                    {
                        "id": run_id,
                        "run_id": run_id,
                        "availability": "captured" if digest else "not_retained_or_not_publicly_retrievable",
                        "captured_sha256": digest,
                        "size": size,
                    },
                    prefix=prefix,
                )
            )
            checks = self._rest_list(
                "check_runs",
                pass_number,
                f"{repo_api}/check-suites/{suite_id}/check-runs?per_page=100",
                item_key="check_runs",
                repository_id=fork_id,
            )
            records["check_runs"].extend(_record(item, prefix=prefix) for item in checks)
            for check in checks:
                check_id = check.get("id")
                if type(check_id) is not int:
                    raise CollectionError("GitHub fork check run is missing its ID")
                annotations = self._rest_list(
                    "check_annotations",
                    pass_number,
                    f"{repo_api}/check-runs/{check_id}/annotations?per_page=100",
                    repository_id=fork_id,
                )
                records["check_annotations"].extend(
                    _record(item, prefix=f"{prefix}{check_id}:") for item in annotations
                )

        for artifact in raw["actions_artifacts"]:
            artifact_id = artifact.get("id")
            if artifact.get("expired") is True:
                records["actions_artifacts"] = [
                    record for record in records["actions_artifacts"] if record["id"] != f"{prefix}{artifact_id}"
                ]
                records["actions_artifacts"].append(
                    _record({**artifact, "availability": "expired_not_publicly_retrievable"}, prefix=prefix)
                )
                continue
            url = artifact.get("archive_download_url")
            if not isinstance(url, str):
                raise CollectionError("GitHub fork Actions artifact is missing its download URL")
            digest, size, _ = self._download("actions_artifacts", pass_number, url, repository_id=fork_id)
            if digest is None or artifact.get("size_in_bytes") != size:
                raise CollectionError("GitHub fork Actions artifact was not captured completely")
            records["actions_artifacts"] = [
                record for record in records["actions_artifacts"] if record["id"] != f"{prefix}{artifact_id}"
            ]
            records["actions_artifacts"].append(
                _record({**artifact, "captured_sha256": digest, "captured_size": size}, prefix=prefix)
            )

        for deployment in raw["deployments"]:
            deployment_id = deployment.get("id")
            if type(deployment_id) is not int:
                raise CollectionError("GitHub fork deployment is missing its ID")
            statuses = self._rest_list(
                "deployment_statuses",
                pass_number,
                f"{repo_api}/deployments/{deployment_id}/statuses?per_page=100",
                repository_id=fork_id,
            )
            records["deployment_statuses"].extend(_record(item, prefix=prefix) for item in statuses)

        records["discussions"] = self._collect_discussions(
            pass_number,
            owner=owner,
            name=name,
            repository_id=fork_id,
            record_prefix=prefix,
        )
        if metadata.get("has_discussions") is False:
            states["discussions"] = "complete" if records["discussions"] else "disabled"
        wiki = self._logical_request(
            "wiki",
            pass_number,
            "GET",
            f"https://github.com/{owner}/{name}.wiki.git/info/refs?service=git-upload-pack",
            kind="probe",
            accepted_statuses={200, 404},
            repository_id=fork_id,
        )
        if wiki.status_code == 404:
            states["wiki"] = "absent"
        else:
            raise CollectionError("initialized GitHub fork wiki requires Git-protocol history capture")
        pages = self._logical_request(
            "pages",
            pass_number,
            "GET",
            f"{repo_api}/pages",
            kind="probe",
            accepted_statuses={200, 404},
            repository_id=fork_id,
        )
        if pages.status_code == 404:
            states["pages"] = "absent"
        else:
            raise CollectionError("live GitHub fork Pages requires an external site-content audit")

        upload_urls: set[str] = set()
        for surface_records in records.values():
            for item in surface_records:
                upload_urls.update(_UPLOAD_URL.findall(_canonical_bytes(item["content"]).decode("utf-8")))
        fork_evidence_ids = {
            evidence_id
            for surface in REQUIRED_SURFACES
            for evidence_id in self._surface_evidence[pass_number][surface][evidence_before[surface] :]
        }
        for evidence_id in fork_evidence_ids:
            evidence_path = self.output_root / str(self.evidence[evidence_id]["path"])
            searchable, _ = _searchable_blob(evidence_path.read_bytes(), {evidence_path.name})
            upload_urls.update(_UPLOAD_URL.findall(searchable))
        captured_upload_urls: set[str] = set()
        while upload_urls - captured_upload_urls:
            if len(upload_urls) > MAX_GITHUB_UPLOADS:
                raise CollectionError("GitHub fork upload graph exceeded the capture cap")
            url = min(upload_urls - captured_upload_urls)
            digest, size, content = self._download("github_uploads", pass_number, url, repository_id=fork_id)
            if digest is None:
                raise CollectionError("GitHub fork upload was not retrievable")
            captured_upload_urls.add(url)
            records["github_uploads"].append(
                _record(
                    {"id": _sha256(url.encode()), "url": url, "captured_sha256": digest, "size": size},
                    prefix=prefix,
                )
            )
            searchable, _ = _searchable_blob(content, {urlsplit(url).path})
            upload_urls.update(_UPLOAD_URL.findall(searchable))

        for surface in REQUIRED_SURFACES:
            if len(self._surface_evidence[pass_number][surface]) == evidence_before[surface]:
                self._logical_request(
                    surface,
                    pass_number,
                    "GET",
                    repo_api,
                    kind="probe",
                    repository_id=fork_id,
                )
        return records, states

    def _collect_pass(self, pass_number: int) -> tuple[dict[str, list[dict[str, Any]]], dict[str, str]]:
        self._surface_evidence[pass_number] = {name: [] for name in REQUIRED_SURFACES}
        records: dict[str, list[dict[str, Any]]] = {name: [] for name in REQUIRED_SURFACES}
        states = {name: "complete" for name in REQUIRED_SURFACES}

        repo_response = self._logical_request("repository", pass_number, "GET", self.repo_api)
        repo = self._json(repo_response, "repository")
        if not isinstance(repo, dict):
            raise CollectionError("GitHub repository response is malformed")
        expected_repo = {"id": REPOSITORY["id"], "name": REPOSITORY["name"], "visibility": "public"}
        if any(repo.get(key) != value for key, value in expected_repo.items()):
            raise CollectionError("GitHub repository identity or visibility changed")
        owner = repo.get("owner")
        if not isinstance(owner, dict) or str(owner.get("login", "")).casefold() != str(REPOSITORY["owner"]).casefold():
            raise CollectionError("GitHub repository owner changed")
        if not isinstance(repo.get("node_id"), str) or not repo["node_id"]:
            raise CollectionError("GitHub repository node ID is missing")
        records["repository"] = [_record(repo)]

        simple_lists = {
            "issues": (f"{self.repo_api}/issues?state=all&per_page=100", None),
            "issue_comments": (f"{self.repo_api}/issues/comments?per_page=100", None),
            "pull_requests": (f"{self.repo_api}/pulls?state=all&per_page=100", None),
            "pull_request_review_comments": (f"{self.repo_api}/pulls/comments?per_page=100", None),
            "commit_comments": (f"{self.repo_api}/comments?per_page=100", None),
            "releases": (f"{self.repo_api}/releases?per_page=100", None),
            "actions_runs": (f"{self.repo_api}/actions/runs?per_page=100", "workflow_runs"),
            "actions_artifacts": (f"{self.repo_api}/actions/artifacts?per_page=100", "artifacts"),
            "deployments": (f"{self.repo_api}/deployments?per_page=100", None),
            "labels": (f"{self.repo_api}/labels?per_page=100", None),
            "milestones": (f"{self.repo_api}/milestones?state=all&per_page=100", None),
            "security_advisories": (f"{self.repo_api}/security-advisories?per_page=100", None),
            "forks": (f"{self.repo_api}/forks?per_page=100", None),
        }
        raw_items: dict[str, list[dict[str, Any]]] = {}
        for surface, (url, key) in simple_lists.items():
            raw_items[surface] = self._rest_list(surface, pass_number, url, item_key=key)
            record_prefix = "fork:" if surface == "forks" else ""
            records[surface] = [_record(item, prefix=record_prefix) for item in raw_items[surface]]

        for pull in raw_items["pull_requests"]:
            number = pull.get("number")
            if type(number) is not int:
                raise CollectionError("GitHub pull request is missing its number")
            reviews = self._rest_list(
                "pull_request_reviews", pass_number, f"{self.repo_api}/pulls/{number}/reviews?per_page=100"
            )
            records["pull_request_reviews"].extend(_record(item) for item in reviews)

        for release in raw_items["releases"]:
            release_id = release.get("id")
            if type(release_id) is not int:
                raise CollectionError("GitHub release is missing its ID")
            assets = self._rest_list(
                "release_assets", pass_number, f"{self.repo_api}/releases/{release_id}/assets?per_page=100"
            )
            for asset in assets:
                url = asset.get("url")
                if not isinstance(url, str):
                    raise CollectionError("GitHub release asset is missing its API URL")
                digest, size, _ = self._download("release_assets", pass_number, url)
                if digest is None:
                    raise CollectionError("GitHub release asset was not retrievable")
                declared_size = asset.get("size")
                declared_digest = asset.get("digest")
                if type(declared_size) is not int or declared_size != size:
                    raise CollectionError("GitHub release asset size did not match metadata")
                if (
                    isinstance(declared_digest, str)
                    and declared_digest.startswith("sha256:")
                    and declared_digest.removeprefix("sha256:") != digest
                ):
                    raise CollectionError("GitHub release asset digest did not match metadata")
                records["release_assets"].append(_record({**asset, "captured_sha256": digest}))

        for run in raw_items["actions_runs"]:
            run_id = run.get("id")
            suite_id = run.get("check_suite_id")
            if type(run_id) is not int or type(suite_id) is not int:
                raise CollectionError("GitHub Actions run is missing IDs")
            if run.get("status") != "completed":
                raise CollectionError("GitHub Actions run is not terminal")
            jobs = self._rest_list(
                "actions_jobs", pass_number, f"{self.repo_api}/actions/runs/{run_id}/jobs?per_page=100", item_key="jobs"
            )
            records["actions_jobs"].extend(_record(item) for item in jobs)
            log_digest, log_size, _ = self._download(
                "actions_logs",
                pass_number,
                f"{self.repo_api}/actions/runs/{run_id}/logs",
                allow_unavailable=True,
            )
            records["actions_logs"].append(
                _record(
                    {
                        "id": run_id,
                        "run_id": run_id,
                        "availability": "captured"
                        if log_digest is not None
                        else "not_retained_or_not_publicly_retrievable",
                        "captured_sha256": log_digest,
                        "size": log_size,
                    }
                )
            )
            checks = self._rest_list(
                "check_runs",
                pass_number,
                f"{self.repo_api}/check-suites/{suite_id}/check-runs?per_page=100",
                item_key="check_runs",
            )
            records["check_runs"].extend(_record(item) for item in checks)
            for check in checks:
                check_id = check.get("id")
                if type(check_id) is not int:
                    raise CollectionError("GitHub check run is missing its ID")
                annotations = self._rest_list(
                    "check_annotations",
                    pass_number,
                    f"{self.repo_api}/check-runs/{check_id}/annotations?per_page=100",
                )
                records["check_annotations"].extend(_record(item, prefix=f"{check_id}:") for item in annotations)

        for artifact in raw_items["actions_artifacts"]:
            if artifact.get("expired") is True:
                artifact_id = artifact.get("id")
                records["actions_artifacts"] = [
                    record for record in records["actions_artifacts"] if record["id"] != str(artifact_id)
                ]
                records["actions_artifacts"].append(
                    _record({**artifact, "availability": "expired_not_publicly_retrievable"})
                )
                continue
            url = artifact.get("archive_download_url")
            if not isinstance(url, str):
                raise CollectionError("GitHub Actions artifact is missing its download URL")
            digest, size, _ = self._download("actions_artifacts", pass_number, url)
            if digest is None:
                raise CollectionError("unexpired GitHub Actions artifact was not retrievable")
            declared_size = artifact.get("size_in_bytes")
            if type(declared_size) is not int or declared_size != size:
                raise CollectionError("GitHub Actions artifact size did not match metadata")
            artifact_id = artifact.get("id")
            records["actions_artifacts"] = [
                record for record in records["actions_artifacts"] if record["id"] != str(artifact_id)
            ]
            records["actions_artifacts"].append(_record({**artifact, "captured_sha256": digest, "captured_size": size}))

        for deployment in raw_items["deployments"]:
            deployment_id = deployment.get("id")
            if type(deployment_id) is not int:
                raise CollectionError("GitHub deployment is missing its ID")
            statuses = self._rest_list(
                "deployment_statuses",
                pass_number,
                f"{self.repo_api}/deployments/{deployment_id}/statuses?per_page=100",
            )
            records["deployment_statuses"].extend(_record(item) for item in statuses)

        if len(raw_items["forks"]) + 1 > MAX_NETWORK_REPOSITORIES:
            raise CollectionError("GitHub fork network exceeded the repository cap")
        canonical_fork_ids: set[int] = set()
        for fork in raw_items["forks"]:
            fork_id = fork.get("id")
            if type(fork_id) is not int or fork_id == int(REPOSITORY["id"]) or fork_id in canonical_fork_ids:
                raise CollectionError("GitHub fork enumeration contains an invalid ID or cycle")
            canonical_fork_ids.add(fork_id)
        fork_states: list[dict[str, str]] = []
        for fork in sorted(raw_items["forks"], key=lambda item: int(item["id"])):
            fork_records, scoped_states = self._collect_fork_scope(
                pass_number, fork, canonical_fork_ids=canonical_fork_ids
            )
            fork_states.append(scoped_states)
            for surface in REQUIRED_SURFACES:
                records[surface].extend(fork_records[surface])

        records["discussions"] = self._collect_discussions(pass_number)
        if repo.get("has_discussions") is False:
            states["discussions"] = "complete" if records["discussions"] else "disabled"

        wiki_url = (
            f"https://github.com/{REPOSITORY['owner']}/{REPOSITORY['name']}.wiki.git/info/refs?service=git-upload-pack"
        )
        wiki_response = self._logical_request(
            "wiki", pass_number, "GET", wiki_url, kind="probe", accepted_statuses={200, 404}
        )
        if wiki_response.status_code == 404:
            states["wiki"] = "absent"
        else:
            raise CollectionError("initialized GitHub wiki requires Git-protocol history capture")

        pages_response = self._logical_request(
            "pages", pass_number, "GET", f"{self.repo_api}/pages", kind="probe", accepted_statuses={200, 404}
        )
        if pages_response.status_code == 404:
            states["pages"] = "absent"
        else:
            raise CollectionError("live GitHub Pages requires an external site-content audit")

        for state_name in ("discussions", "wiki", "pages"):
            scoped = [states[state_name], *(item[state_name] for item in fork_states)]
            states[state_name] = "complete" if "complete" in scoped else scoped[0]

        upload_urls: set[str] = set()
        for surface_records in records.values():
            for item in surface_records:
                if str(item["id"]).startswith("fork:"):
                    continue
                upload_urls.update(_UPLOAD_URL.findall(_canonical_bytes(item["content"]).decode("utf-8")))
        canonical_marker = f"-r{int(REPOSITORY['id'])}-"
        for evidence_id, entry in self.evidence.items():
            if canonical_marker not in evidence_id:
                continue
            evidence_path = self.output_root / str(entry["path"])
            searchable, _ = _searchable_blob(evidence_path.read_bytes(), {evidence_path.name})
            upload_urls.update(_UPLOAD_URL.findall(searchable))
        captured_upload_urls: set[str] = set()
        while upload_urls - captured_upload_urls:
            if len(upload_urls) > MAX_GITHUB_UPLOADS:
                raise CollectionError("GitHub-hosted upload graph exceeded the capture cap")
            url = min(upload_urls - captured_upload_urls)
            digest, size, content = self._download("github_uploads", pass_number, url)
            if digest is None:
                raise CollectionError("GitHub-hosted upload was not retrievable")
            captured_upload_urls.add(url)
            records["github_uploads"].append(
                _record({"id": _sha256(url.encode()), "url": url, "captured_sha256": digest, "size": size})
            )
            searchable, _ = _searchable_blob(content, {urlsplit(url).path})
            upload_urls.update(_UPLOAD_URL.findall(searchable))

        canonical_marker = f"-r{int(REPOSITORY['id'])}-"
        for surface in REQUIRED_SURFACES:
            if not any(canonical_marker in evidence_id for evidence_id in self._surface_evidence[pass_number][surface]):
                self._logical_request(surface, pass_number, "GET", self.repo_api, kind="probe")
        return records, states

    def collect(self) -> Path:
        if self.output_root.exists():
            raise FileExistsError(f"GitHub capture root already exists: {self.output_root}")
        self.output_root.mkdir(mode=0o700, parents=False)
        if self.output_root.is_symlink() or self.output_root.absolute() != self.output_root.resolve(strict=True):
            raise ValueError("GitHub capture root may not traverse symlinks")
        self.output_root.chmod(0o700)
        started_at = self.now().astimezone(UTC)
        first_records, first_states = self._collect_pass(1)
        second_records, second_states = self._collect_pass(2)
        if first_states != second_states:
            raise CollectionError("GitHub feature states changed during capture")
        surfaces: dict[str, dict[str, Any]] = {}
        surface_digests: dict[str, str] = {}
        for surface in sorted(REQUIRED_SURFACES):
            first_digest = _records_digest(first_records[surface])
            second_digest = _records_digest(second_records[surface])
            if first_digest != second_digest:
                raise CollectionError(f"GitHub surface changed during capture: {surface}")
            surface_digests[surface] = second_digest
            parent = PARENT_SURFACES.get(surface)
            covered_parent_ids = (
                sorted(str(record["id"]) for record in second_records[parent]) if parent is not None else []
            )
            surfaces[surface] = {
                "state": second_states[surface],
                "expected_count": len(second_records[surface]),
                "records": sorted(second_records[surface], key=lambda item: str(item["id"])),
                "covered_parent_ids": covered_parent_ids,
                "evidence_ids": self._surface_evidence[1][surface] + self._surface_evidence[2][surface],
                "first_pass_sha256": first_digest,
                "second_pass_sha256": second_digest,
            }
        revalidation, revalidation_sha256 = _validate_revalidation(self._pass_revalidation)
        completed_at = self.now().astimezone(UTC)
        repo_content = second_records["repository"][0]["content"]
        if not isinstance(repo_content, dict):
            raise CollectionError("GitHub repository record is malformed")
        repository = {**REPOSITORY, "node_id": repo_content["node_id"]}
        snapshot_root = _sha256(
            _canonical_bytes(_snapshot_material(surfaces, surface_digests, self.evidence, revalidation_sha256))
        )
        manifest = {
            "schema_version": CAPTURE_SCHEMA_VERSION,
            "capture_policy": CAPTURE_POLICY,
            "collector_implementation_sha256": collector_implementation_sha256(),
            "endpoint_spec_sha256": endpoint_spec_sha256(),
            "api_version": API_VERSION,
            "api_host": API_HOST,
            "repository": repository,
            "questions_sha256": _sha256(self.questions_path.read_bytes()),
            "identity_sha256": _sha256(self.identity_path.read_bytes()),
            "public_ref_shas": self.public_ref_shas,
            "capture_started_at_utc": started_at.isoformat(),
            "capture_completed_at_utc": completed_at.isoformat(),
            "surfaces": surfaces,
            "evidence": self.evidence,
            "revalidation": revalidation,
            "revalidation_sha256": revalidation_sha256,
            "snapshot_root_sha256": snapshot_root,
        }
        manifest_path = self.output_root / "manifest.json"
        _write_private_new(
            manifest_path, json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2).encode() + b"\n"
        )
        return manifest_path


def _revalidation_request(
    client: RequestClient,
    token: str,
    entry: dict[str, Any],
) -> httpx.Response:
    helper = object.__new__(GitHubMetadataCollector)
    helper.client = client
    helper._token = token
    helper._token_bytes = token.encode()
    response = helper._raw_request(
        entry["method"],
        entry["url"],
        request_json=entry["request_json"],
        download=entry["kind"] == "download",
        etag=entry["etag"] if entry["method"] == "GET" else None,
    )
    redirects = 0
    url = entry["url"]
    while response.status_code in {301, 302, 303, 307, 308}:
        if entry["kind"] != "download" or redirects >= REDIRECT_CAP:
            raise CollectionError("unexpected redirect during GitHub revalidation")
        location = response.headers.get("location")
        if not location:
            raise CollectionError("GitHub revalidation redirect omitted Location")
        url = urljoin(url, location)
        response = helper._raw_request("GET", url, download=True)
        redirects += 1
    return response


def revalidate_capture(
    capture_manifest: Path,
    *,
    client: RequestClient,
    token: str,
    phase: str,
    output_path: Path | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Conditionally verify every second-pass request before or after a batch."""
    if phase not in {"pre_batch", "post_batch"}:
        raise ValueError("GitHub revalidation phase must be pre_batch or post_batch")
    if not token or any(character.isspace() for character in token):
        raise ValueError("a non-empty trusted scorer GitHub token is required")
    capture_root = capture_manifest.parent.resolve(strict=True)
    if stat.S_IMODE(capture_root.stat().st_mode) & 0o077:
        raise CollectionError("protected GitHub capture root permits group or other access")
    protected_manifest = _protected_capture_file(capture_manifest, capture_root)
    capture_bytes, manifest = _strict_json(protected_manifest)
    repository = manifest.get("repository")
    if (
        manifest.get("schema_version") != CAPTURE_SCHEMA_VERSION
        or manifest.get("capture_policy") != CAPTURE_POLICY
        or manifest.get("endpoint_spec_sha256") != endpoint_spec_sha256()
        or manifest.get("api_version") != API_VERSION
        or manifest.get("api_host") != API_HOST
        or not isinstance(repository, dict)
        or any(repository.get(key) != value for key, value in REPOSITORY.items())
        or not isinstance(repository.get("node_id"), str)
        or not repository["node_id"]
    ):
        raise CollectionError("GitHub capture binding is invalid during revalidation")
    revalidation, digest = _validate_revalidation(manifest.get("revalidation"))
    if manifest.get("revalidation_sha256") != digest:
        raise CollectionError("GitHub revalidation ledger digest mismatch")
    checked_at = (now or datetime.now(UTC)).astimezone(UTC)
    try:
        completed_at = datetime.fromisoformat(str(manifest["capture_completed_at_utc"])).astimezone(UTC)
    except (KeyError, TypeError, ValueError) as error:
        raise CollectionError("GitHub capture completion timestamp is invalid") from error
    if phase == "pre_batch" and (checked_at - completed_at).total_seconds() > MAX_CAPTURE_AGE_SECONDS:
        raise CollectionError("GitHub capture is stale before the batch")
    downloaded_bytes = 0
    for entry in revalidation:
        response = _revalidation_request(client, token, entry)
        if response.status_code == 304:
            if not entry["etag"]:
                raise CollectionError("GitHub returned 304 without a captured ETag")
            continue
        if response.status_code != entry["status_code"]:
            raise CollectionError(f"GitHub surface changed status during {phase}: {entry['surface']}")
        downloaded_bytes += len(response.content)
        if downloaded_bytes > MAX_TOTAL_DOWNLOAD_BYTES:
            raise CollectionError("GitHub revalidation exceeded the total response byte cap")
        if response.headers.get("link") != entry["link"]:
            raise CollectionError(f"GitHub pagination Link changed during {phase}: {entry['surface']}")
        if len(response.content) != entry["response_size"] or _sha256(response.content) != entry["response_sha256"]:
            raise CollectionError(f"GitHub surface content drifted during {phase}: {entry['surface']}")
    result = {
        "schema_version": 1,
        "phase": phase,
        "capture_manifest_sha256": _sha256(capture_bytes),
        "revalidation_sha256": digest,
        "checked_at_utc": checked_at.isoformat(),
        "request_count": len(revalidation),
        "passed": True,
    }
    if output_path is not None:
        _write_private_new(output_path, json.dumps(result, sort_keys=True, indent=2).encode() + b"\n")
    return result
