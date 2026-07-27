"""Docker-isolated answer packet launcher and native JSONL trace capture."""

from __future__ import annotations

import json
import os
import queue
import shutil
import stat
import subprocess
import tempfile
import threading
import time
import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

PINNED_NODE_IMAGE = "node@sha256:968df39aedcea65eeb078fb336ed7191baf48f972b4479711397108be0966920"
REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


def is_sealed_location(path: Path) -> bool:
    parts = path.absolute().parts
    return ".benchmark-sealed" in parts or any(
        parts[index : index + 2] == ("hspr-benchmark", "sealed") for index in range(len(parts) - 1)
    )


@dataclass(frozen=True)
class ContainerLimits:
    memory: str = "512m"
    cpus: str = "1.0"
    pids: int = 256


def _canonical_directory(path: Path) -> Path:
    absolute = path.absolute()
    resolved = path.resolve(strict=True)
    if absolute != resolved or path.is_symlink() or not resolved.is_dir() or resolved == Path("/"):
        raise ValueError(f"unsafe mount directory: {path}")
    return resolved


def _regular_file(path: Path, *, approved_root: Path | None = None) -> Path:
    absolute = path.absolute()
    if path.is_symlink():
        raise ValueError(f"symlink is forbidden: {path}")
    resolved = path.resolve(strict=True)
    if absolute != resolved or not resolved.is_file():
        raise ValueError(f"not a regular file: {path}")
    if is_sealed_location(resolved):
        raise ValueError(f"sealed files are forbidden in answer inputs: {path}")
    if approved_root is not None:
        root = approved_root.resolve(strict=True)
        if resolved != root and root not in resolved.parents:
            raise ValueError(f"file is outside the approved source root: {path}")
    return resolved


def read_untrusted_regular(path: Path, *, max_bytes: int = 2 * 1024 * 1024) -> bytes:
    """Read a container-produced file without following a final symlink."""
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > max_bytes:
            raise ValueError(f"unsafe container output: {path}")
        return os.read(descriptor, max_bytes + 1)
    finally:
        os.close(descriptor)


def write_new_text(path: Path, content: str) -> None:
    """Create a host result without following or replacing any existing path."""
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        os.write(descriptor, content.encode("utf-8"))
    finally:
        os.close(descriptor)


def _container_name(command: list[str]) -> str:
    try:
        return command[command.index("--name") + 1]
    except (ValueError, IndexError) as error:
        raise ValueError("Docker command is missing a container name") from error


def _kill_container(command: list[str], process: subprocess.Popen[bytes]) -> None:
    """Stop the daemon-owned container as well as its local Docker client."""
    name = _container_name(command)
    try:
        for _ in range(5):
            removed = subprocess.run(
                ["docker", "rm", "--force", name],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=15,
                check=False,
            )
            if removed.returncode == 0:
                break
            time.sleep(0.1)
    finally:
        if process.poll() is None:
            process.kill()
        process.wait(timeout=15)
        subprocess.run(
            ["docker", "rm", "--force", name],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=15,
            check=False,
        )


def _redact(value: Any, secrets: tuple[str, ...]) -> Any:
    if isinstance(value, str):
        result = value
        for secret in secrets:
            result = result.replace(secret, "[REDACTED]")
        return result
    if isinstance(value, list):
        return [_redact(item, secrets) for item in value]
    if isinstance(value, dict):
        return {key: _redact(item, secrets) for key, item in value.items()}
    return value


def _credential_secrets(credential_json: bytes) -> tuple[str, ...]:
    try:
        document = json.loads(credential_json)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return ()
    values: list[str] = []

    def collect(value: Any) -> None:
        if isinstance(value, str) and len(value) >= 12:
            values.append(value)
        elif isinstance(value, list):
            for item in value:
                collect(item)
        elif isinstance(value, dict):
            for item in value.values():
                collect(item)

    collect(document)
    return tuple(sorted(set(values), key=len, reverse=True))


def assemble_answer_packet(
    destination: Path,
    *,
    question: dict[str, Any],
    response_schema: Path,
    registry_packet: Path | None,
    registry_approved_root: Path | None = None,
) -> Path:
    """Create the exact allowlisted files visible to one answer context."""
    resolved = destination.resolve()
    if is_sealed_location(resolved):
        raise ValueError("answer packet cannot be created under the sealed root")
    resolved.mkdir(parents=True, exist_ok=False)
    (resolved / "question.json").write_text(json.dumps(question, indent=2), encoding="utf-8")
    locked_schema = REPOSITORY_ROOT / "hspr-benchmark-v2/config/response-schema.json"
    if response_schema.resolve(strict=True) != locked_schema.resolve(strict=True):
        raise ValueError("response schema is not the locked benchmark schema")
    shutil.copyfile(
        _regular_file(response_schema, approved_root=locked_schema.parent), resolved / "response-schema.json"
    )
    if registry_packet is not None:
        approved_root = (
            registry_approved_root
            if registry_approved_root is not None
            else REPOSITORY_ROOT / "hspr-benchmark-v2/registry"
        )
        started = time.monotonic_ns()
        shutil.copyfile(
            _regular_file(
                registry_packet,
                approved_root=approved_root,
            ),
            resolved / "hspr-identity.json",
        )
        ended = time.monotonic_ns()
        (resolved / "hspr-materialization-timing.json").write_text(
            json.dumps(
                {"event": "hspr_packet_materialization", "start_monotonic_ns": started, "end_monotonic_ns": ended}
            ),
            encoding="utf-8",
        )
    for path in resolved.iterdir():
        path.chmod(0o444)
    resolved.chmod(0o555)
    return resolved


def stage_runtime(source: Path, destination: Path, *, include_test_mock: bool = False) -> Path:
    """Copy only approved runtime assets outside the repository before mounting."""
    locked_runtime = (REPOSITORY_ROOT / "hspr-benchmark-v2/runtime").resolve(strict=True)
    if source.resolve(strict=True) != locked_runtime:
        raise ValueError("runtime source is not the locked benchmark runtime")
    destination.mkdir(parents=True, exist_ok=False)
    names = [
        "isolation-probe.mjs",
        "credential-supervisor.mjs",
        "app-server-supervisor.mjs",
        "live-boundary-probe.mjs",
    ]
    if include_test_mock:
        names.append("mock-codex.mjs")
    for name in names:
        shutil.copyfile(
            _regular_file(source / name, approved_root=locked_runtime),
            destination / name,
        )
        (destination / name).chmod(0o444)
    destination.chmod(0o555)
    return destination.resolve()


def docker_command(
    *,
    image: str,
    packet_dir: Path,
    output_dir: Path,
    runtime_dir: Path,
    runtime_command: Iterable[str],
    network: str,
    limits: ContainerLimits | None = None,
    environment: dict[str, str] | None = None,
    output_tmpfs: bool = False,
) -> list[str]:
    """Return a fail-closed Docker command with no host/repository mount."""
    if network not in {"none", "bridge"}:
        raise ValueError("network must be none or bridge")
    limits = limits or ContainerLimits()
    source_paths = (packet_dir, runtime_dir) if output_tmpfs else (packet_dir, runtime_dir, output_dir)
    mount_sources = tuple(_canonical_directory(path) for path in source_paths)
    if len(set(mount_sources)) != len(mount_sources):
        raise ValueError("mount sources must be distinct")
    for source in mount_sources:
        if "," in str(source):
            raise ValueError("Docker mount sources may not contain commas")
    for first in mount_sources:
        for second in mount_sources:
            if first != second and (first in second.parents or second in first.parents):
                raise ValueError("mount sources must be disjoint")
    packet_mount, runtime_mount = mount_sources[:2]
    command = [
        "docker",
        "run",
        "--rm",
        "--name",
        f"hspr-answer-{uuid.uuid4().hex[:12]}",
        "--user",
        "65534:65534",
        "--read-only",
        "--cap-drop",
        "ALL",
        "--security-opt",
        "no-new-privileges:true",
        "--pids-limit",
        str(limits.pids),
        "--memory",
        limits.memory,
        "--cpus",
        limits.cpus,
        "--network",
        network,
        "--shm-size",
        "128m",
        "--mount",
        f"type=bind,src={packet_mount},dst=/input,readonly",
        "--mount",
        f"type=bind,src={runtime_mount},dst=/runtime,readonly",
    ]
    if output_tmpfs:
        command.extend(["--tmpfs", "/output:rw,noexec,nosuid,nodev,size=4m,mode=0700,uid=65534,gid=65534"])
    else:
        output_mount = mount_sources[2]
        command.extend(["--mount", f"type=bind,src={output_mount},dst=/output"])
    command.extend(
        [
            "--tmpfs",
            "/tmp:rw,noexec,nosuid,nodev,size=128m",
            "--tmpfs",
            "/work:rw,nosuid,nodev,size=128m",
            "--workdir",
            "/work",
        ]
    )
    for key, value in sorted((environment or {}).items()):
        if any(token in key.upper() for token in ("KEY", "TOKEN", "SECRET", "PASSWORD")):
            raise ValueError(f"secret-like environment variable is forbidden: {key}")
        command.extend(["--env", f"{key}={value}"])
    return [*command, image, *runtime_command]


def codex_docker_command(
    *,
    packet_dir: Path,
    output_dir: Path,
    runtime_dir: Path,
    codex_package_dir: Path,
    model: str,
    reasoning: str,
    prompt: str,
) -> list[str]:
    allowed_model_reasoning = {
        ("gpt-5.6-luna", "medium"),
        ("gpt-5.6-luna", "xhigh"),
        ("gpt-5.6-sol", "medium"),
    }
    if (model, reasoning) not in allowed_model_reasoning:
        raise ValueError("benchmark runtime model/reasoning pair is not allowlisted")
    codex_mount = _canonical_directory(codex_package_dir)
    if codex_mount.name != "codex" or codex_mount.parent.name != "@openai":
        raise ValueError("Codex mount must be the @openai/codex package directory")
    _regular_file(codex_mount / "bin/codex.js")
    package = json.loads(_regular_file(codex_mount / "package.json").read_text(encoding="utf-8"))
    if package.get("name") != "@openai/codex" or package.get("version") != "0.145.0":
        raise ValueError("Codex package identity/version does not match the locked runtime")
    if "," in str(codex_mount):
        raise ValueError("Docker mount sources may not contain commas")
    for source in (_canonical_directory(packet_dir), _canonical_directory(runtime_dir)):
        if codex_mount == source or codex_mount in source.parents or source in codex_mount.parents:
            raise ValueError("Codex and answer-context mount roots must be disjoint")
    base = docker_command(
        image=PINNED_NODE_IMAGE,
        packet_dir=packet_dir,
        output_dir=output_dir,
        runtime_dir=runtime_dir,
        runtime_command=(),
        network="bridge",
        limits=ContainerLimits(memory="2g", cpus="2.0", pids=256),
        environment={"CODEX_HOME": "/auth"},
        output_tmpfs=True,
    )
    image = base.pop()
    base.insert(2, "--interactive")
    base.extend(["--tmpfs", "/auth:rw,noexec,nosuid,nodev,size=64m,mode=0700,uid=65534,gid=65534"])
    mounts = [
        "--mount",
        f"type=bind,src={codex_mount},dst=/opt/codex,readonly",
    ]
    command = ["node", "/runtime/app-server-supervisor.mjs", "--", model, reasoning, prompt]
    return [*base, *mounts, image, *command]


def run_codex_and_capture(
    command: list[str], trace_path: Path, credential_json: bytes, *, timeout_seconds: int = 600
) -> dict[str, Any]:
    """Supply auth over stdin and capture supervisor/Codex JSONL at receipt time."""
    if len(credential_json) > 1024 * 1024:
        raise ValueError("credential input exceeds 1 MiB")
    try:
        credential_document = json.loads(credential_json)
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise ValueError("credential input is not valid JSON") from error
    if not isinstance(credential_document, dict):
        raise TypeError("credential input must be a JSON object")
    started = time.monotonic_ns()
    deadline = time.monotonic() + timeout_seconds
    events: list[dict[str, Any]] = [{"event": "run_start", "monotonic_ns": started}]
    process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    lines: queue.Queue[tuple[int, bytes] | None] = queue.Queue()
    stderr_chunks: list[bytes] = []
    writer_errors: list[OSError] = []

    def write_stdin() -> None:
        assert process.stdin is not None
        try:
            process.stdin.write(credential_json)
        except OSError as error:
            writer_errors.append(error)
        finally:
            try:
                process.stdin.close()
            except OSError as error:
                writer_errors.append(error)

    def read_stdout() -> None:
        assert process.stdout is not None
        for line in process.stdout:
            lines.put((time.monotonic_ns(), line))
        lines.put(None)

    def read_stderr() -> None:
        assert process.stderr is not None
        captured = 0
        limit = 2 * 1024 * 1024
        while True:
            chunk = process.stderr.read(65536)
            if not chunk:
                break
            if captured < limit:
                retained = chunk[: limit - captured]
                stderr_chunks.append(retained)
                captured += len(retained)

    try:
        assert process.stdin is not None
        assert process.stdout is not None
        assert process.stderr is not None
        secrets = _credential_secrets(credential_json)
        stdin_thread = threading.Thread(target=write_stdin, daemon=True)
        stdout_thread = threading.Thread(target=read_stdout, daemon=True)
        stderr_thread = threading.Thread(target=read_stderr, daemon=True)
        stdin_thread.start()
        stdout_thread.start()
        stderr_thread.start()
    except BaseException:
        _kill_container(command, process)
        raise
    sequence: list[str] = []
    answer: dict[str, Any] | None = None
    trace_bytes = 0
    try:
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise subprocess.TimeoutExpired(command, timeout_seconds)
            try:
                item = lines.get(timeout=remaining)
            except queue.Empty as error:
                raise subprocess.TimeoutExpired(command, timeout_seconds) from error
            if item is None:
                break
            captured, raw_line = item
            trace_bytes += len(raw_line)
            if len(raw_line) > 1024 * 1024 or trace_bytes > 32 * 1024 * 1024:
                raise RuntimeError("native trace exceeded its size limit")
            line = raw_line.decode("utf-8", errors="replace")
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                payload = {"type": "unparsed_stdout", "text": line.rstrip("\n")}
            payload = _redact(payload, secrets)
            payload_type = str(payload.get("type", ""))
            if payload_type in {
                "supervisor.credential_unlinked",
                "supervisor.external_auth_ready",
                "thread.started",
                "turn.started",
                "turn.completed",
                "supervisor.answer",
            }:
                sequence.append(payload_type)
            if payload_type == "supervisor.answer":
                candidate = payload.get("answer")
                if not isinstance(candidate, dict):
                    raise RuntimeError("supervisor returned a non-object answer")
                answer = candidate
            events.append({"event": "codex_native_event", "monotonic_ns": captured, "payload": payload})
        remaining = max(0.01, deadline - time.monotonic())
        return_code = process.wait(timeout=remaining)
        stdin_thread.join(timeout=1)
        stdout_thread.join(timeout=1)
        stderr_thread.join(timeout=1)
        if stdin_thread.is_alive():
            raise RuntimeError("credential writer did not terminate")
        if writer_errors:
            raise RuntimeError("failed to supply credential to the isolated runtime") from writer_errors[0]
    except BaseException:
        _kill_container(command, process)
        raise
    expected_prefixes = [
        "supervisor.credential_unlinked",
        "supervisor.external_auth_ready",
    ]
    expected_tail = [
        "thread.started",
        "turn.started",
        "turn.completed",
        "supervisor.answer",
    ]
    stderr = _redact(b"".join(stderr_chunks).decode("utf-8", errors="replace"), secrets)
    ended = time.monotonic_ns()
    events.append({"event": "run_end", "monotonic_ns": ended, "exit_code": return_code})
    sequence_valid = (
        len(sequence) == 5 and sequence[0] in expected_prefixes and sequence[1:] == expected_tail and answer is not None
    )
    trace = {
        "events": events,
        "stderr": stderr,
        "duration_ns": ended - started,
        "credential_boundary_ready_before_turn": sequence_valid,
        "credential_removed_before_turn": sequence_valid and sequence[0] == "supervisor.credential_unlinked",
        "fileless_external_auth_before_turn": sequence_valid and sequence[0] == "supervisor.external_auth_ready",
        "answer": answer,
        "supervisor_sequence_valid": sequence_valid,
    }
    write_new_text(trace_path, json.dumps(trace, indent=2))
    if not sequence_valid:
        raise RuntimeError(
            f"invalid supervisor event sequence: {sequence}; "
            f"redacted stderr: {stderr[-2000:]}; redacted trace: {trace_path}"
        )
    return trace


def run_and_capture(command: list[str], trace_path: Path, *, timeout_seconds: int = 120) -> dict[str, Any]:
    """Timestamp JSONL when each line is received; never timestamp it post hoc."""
    started = time.monotonic_ns()
    events: list[dict[str, Any]] = [{"event": "run_start", "monotonic_ns": started}]
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    assert process.stdout is not None
    assert process.stderr is not None
    lines: queue.Queue[tuple[int, bytes] | None] = queue.Queue()
    stderr_chunks: list[bytes] = []

    def read_stdout() -> None:
        assert process.stdout is not None
        for line in process.stdout:
            lines.put((time.monotonic_ns(), line))
        lines.put(None)

    def read_stderr() -> None:
        assert process.stderr is not None
        captured = 0
        limit = 2 * 1024 * 1024
        while True:
            chunk = process.stderr.read(65536)
            if not chunk:
                break
            if captured < limit:
                retained = chunk[: limit - captured]
                stderr_chunks.append(retained)
                captured += len(retained)

    stdout_thread = threading.Thread(target=read_stdout, daemon=True)
    stderr_thread = threading.Thread(target=read_stderr, daemon=True)
    stdout_thread.start()
    stderr_thread.start()
    deadline = time.monotonic() + timeout_seconds
    try:
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise subprocess.TimeoutExpired(command, timeout_seconds)
            try:
                item = lines.get(timeout=remaining)
            except queue.Empty as error:
                raise subprocess.TimeoutExpired(command, timeout_seconds) from error
            if item is None:
                break
            captured, raw_line = item
            if len(raw_line) > 1024 * 1024:
                raise RuntimeError("runtime line exceeded its size limit")
            line = raw_line.decode("utf-8", errors="replace")
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                payload = {"type": "unparsed_stdout", "text": line.rstrip("\n")}
            events.append({"event": "runtime_jsonl", "monotonic_ns": captured, "payload": payload})
        return_code = process.wait(timeout=max(0.01, deadline - time.monotonic()))
        stdout_thread.join(timeout=1)
        stderr_thread.join(timeout=1)
    except BaseException:
        _kill_container(command, process)
        raise
    stderr = b"".join(stderr_chunks).decode("utf-8", errors="replace")
    ended = time.monotonic_ns()
    events.append({"event": "run_end", "monotonic_ns": ended, "exit_code": return_code})
    trace = {"events": events, "stderr": stderr, "duration_ns": ended - started}
    write_new_text(trace_path, json.dumps(trace, indent=2))
    return trace


def summarize_codex_trace(trace: dict[str, Any], *, authoritative_urls: Iterable[str] = ()) -> dict[str, Any]:
    """Extract only observable timings from captured Codex JSONL events."""
    tool_events: list[dict[str, Any]] = []
    evidence_times: list[int] = []
    final_answer_ns: int | None = None
    usage: dict[str, Any] | None = None
    hspr_lookup_start_ns: int | None = None
    hspr_lookup_end_ns: int | None = None
    accepted_urls = tuple(url.rstrip("/") for url in authoritative_urls if url)
    for captured in trace.get("events", []):
        if captured.get("event") != "codex_native_event":
            continue
        monotonic_ns = captured.get("monotonic_ns")
        payload = captured.get("payload", {})
        native_type = payload.get("type")
        item = payload.get("item", {})
        item_type = item.get("type")
        phase = "start" if native_type == "item.started" else "end"
        serialized = json.dumps(payload, sort_keys=True)
        if item_type == "web_search" and native_type in {"item.started", "item.completed"}:
            action_text = serialized.lower()
            operation = next(
                (candidate for candidate in ("search", "open", "click", "download") if candidate in action_text),
                "web",
            )
            tool_events.append(
                {
                    "event": f"observable_web_{operation}_{phase}",
                    "monotonic_ns": monotonic_ns,
                    "item_id": item.get("id"),
                }
            )
        elif item_type == "command_execution" and native_type in {
            "item.started",
            "item.completed",
        }:
            command_text = str(item.get("command", ""))
            reads_input = "/input/" in command_text
            if "hspr-identity.json" in command_text and phase == "start":
                hspr_lookup_start_ns = monotonic_ns
            if "hspr-identity.json" in command_text and phase == "end":
                hspr_lookup_end_ns = monotonic_ns
            tool_events.append(
                {
                    "event": f"observable_local_command_{phase}",
                    "monotonic_ns": monotonic_ns,
                    "item_id": item.get("id"),
                    "reads_allowlisted_input": reads_input,
                }
            )
        if native_type == "item.completed" and item_type == "agent_message":
            final_answer_ns = monotonic_ns
        if native_type == "turn.completed":
            usage = payload.get("usage")
        if monotonic_ns is not None and any(url in serialized for url in accepted_urls):
            evidence_times.append(monotonic_ns)
    return {
        "run_start_monotonic_ns": trace.get("events", [{}])[0].get("monotonic_ns"),
        "run_end_monotonic_ns": trace.get("events", [{}])[-1].get("monotonic_ns"),
        "hspr_lookup_start_monotonic_ns": hspr_lookup_start_ns,
        "hspr_lookup_end_monotonic_ns": hspr_lookup_end_ns,
        "observable_tool_events": tool_events,
        "first_authoritative_evidence_monotonic_ns": min(evidence_times) if evidence_times else None,
        "last_authoritative_evidence_monotonic_ns": max(evidence_times) if evidence_times else None,
        "final_answer_monotonic_ns": final_answer_ns,
        "usage": usage,
        "cost": None,
    }


def prove_isolation(
    *, repo_path: Path, sealed_path: Path, runtime_dir: Path, image: str = PINNED_NODE_IMAGE
) -> dict[str, Any]:
    """Run a hostile probe that must see its packet but not host-sensitive paths."""
    with tempfile.TemporaryDirectory(prefix="hspr-isolation-") as temp_name:
        root = Path(temp_name)
        packet = root / "packet"
        output = root / "output"
        staged_runtime = stage_runtime(runtime_dir, root / "runtime")
        packet.mkdir()
        output.mkdir(mode=0o777)
        output.chmod(0o777)
        (packet / "question.json").write_text('{"question_id":"probe"}', encoding="utf-8")
        (packet / "question.json").chmod(0o444)
        packet.chmod(0o555)
        command = docker_command(
            image=image,
            packet_dir=packet,
            output_dir=output,
            runtime_dir=staged_runtime,
            runtime_command=("node", "/runtime/isolation-probe.mjs"),
            network="none",
            environment={"PROBE_REPO_PATH": str(repo_path.resolve()), "PROBE_SEALED_PATH": str(sealed_path.resolve())},
        )
        trace = run_and_capture(command, root / "trace.json")
        completed = [
            item["payload"]
            for item in trace["events"]
            if item["event"] == "runtime_jsonl" and item["payload"].get("event") == "probe_result"
        ]
        if len(completed) != 1 or not completed[0].get("passed"):
            raise RuntimeError(f"isolation probe failed: {completed}")
        return {"passed": True, "probe": completed[0], "mount_count": 3, "network": "none"}


def prove_credential_supervisor(*, runtime_dir: Path, image: str = PINNED_NODE_IMAGE) -> dict[str, Any]:
    """Exercise the production supervisor protocol with a fake credential and child."""
    with tempfile.TemporaryDirectory(prefix="hspr-supervisor-") as temp_name:
        root = Path(temp_name)
        packet = root / "packet"
        packet.mkdir()
        (packet / "question.json").write_text('{"question_id":"mock"}', encoding="utf-8")
        (packet / "question.json").chmod(0o444)
        packet.chmod(0o555)
        runtime = stage_runtime(runtime_dir, root / "runtime", include_test_mock=True)
        command = docker_command(
            image=image,
            packet_dir=packet,
            output_dir=root / "not-mounted",
            runtime_dir=runtime,
            runtime_command=(),
            network="none",
            environment={"CODEX_HOME": "/auth"},
            output_tmpfs=True,
        )
        container_image = command.pop()
        command.insert(2, "--interactive")
        command.extend(["--tmpfs", "/auth:rw,noexec,nosuid,nodev,size=64m,mode=0700,uid=65534,gid=65534"])
        command.extend(
            [
                container_image,
                "node",
                "/runtime/credential-supervisor.mjs",
                "--",
                "node",
                "/runtime/mock-codex.mjs",
            ]
        )
        trace = run_codex_and_capture(
            command,
            root / "trace.json",
            b'{"fake":"not-a-live-secret"}',
            timeout_seconds=30,
        )
        command_outputs = [
            event["payload"]["item"].get("aggregated_output")
            for event in trace["events"]
            if event.get("event") == "codex_native_event"
            and event["payload"].get("type") == "item.completed"
            and event["payload"].get("item", {}).get("type") == "command_execution"
        ]
        passed = (
            trace["events"][-1].get("exit_code") == 0
            and trace["credential_removed_before_turn"]
            and command_outputs == ["CREDENTIAL_INACCESSIBLE"]
            and trace["answer"].get("question_id") == "mock"
        )
        if not passed:
            raise RuntimeError("credential supervisor proof failed")
        return {
            "passed": True,
            "credential_removed_before_turn": True,
            "command_output": "CREDENTIAL_INACCESSIBLE",
            "network": "none",
        }


def prove_live_subscription_boundary(
    *,
    runtime_dir: Path,
    codex_package_dir: Path,
    credential_json: bytes,
    output_dir: Path,
    model: str = "gpt-5.6-luna",
    reasoning: str = "medium",
) -> dict[str, Any]:
    """Run one non-financial real-Codex smoke through the production supervisor."""
    output_dir.mkdir(mode=0o700, parents=True, exist_ok=False)
    with tempfile.TemporaryDirectory(prefix="hspr-live-boundary-") as temp_name:
        root = Path(temp_name)
        packet = root / "packet"
        packet.mkdir()
        (packet / "question.json").write_text(
            json.dumps({"smoke_id": "subscription-boundary-v2", "financial_question": False}),
            encoding="utf-8",
        )
        schema = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "additionalProperties": False,
            "required": ["probe_marker", "source_url", "web_fact"],
            "properties": {
                "probe_marker": {"type": "string", "const": "REAL_CONTEXT_CREDENTIAL_INACCESSIBLE"},
                "source_url": {"type": "string", "pattern": "^https://"},
                "web_fact": {"type": "string", "minLength": 1},
            },
        }
        (packet / "response-schema.json").write_text(json.dumps(schema), encoding="utf-8")
        for path in packet.iterdir():
            path.chmod(0o444)
        packet.chmod(0o555)
        runtime = stage_runtime(runtime_dir, root / "runtime")
        prompt = (
            "This is a non-financial access-boundary smoke. First run exactly "
            "`/usr/local/bin/node /runtime/live-boundary-probe.mjs` with the shell and require its success marker. "
            "Then use native web search and open an official OpenAI page to confirm one harmless current "
            "fact about Codex CLI. Return only the schema object with the probe marker, official HTTPS URL, "
            "and concise fact. Do not access any repository or benchmark material."
        )
        command = codex_docker_command(
            packet_dir=packet,
            output_dir=output_dir,
            runtime_dir=runtime,
            codex_package_dir=codex_package_dir,
            model=model,
            reasoning=reasoning,
            prompt=prompt,
        )
        trace = run_codex_and_capture(command, output_dir / "native-trace.json", credential_json)
    command_outputs = [
        str(event["payload"]["item"].get("aggregated_output", ""))
        for event in trace["events"]
        if event.get("event") == "codex_native_event"
        and event["payload"].get("type") == "item.completed"
        and event["payload"].get("item", {}).get("type") == "command_execution"
    ]
    web_events = [
        event
        for event in trace["events"]
        if event.get("event") == "codex_native_event" and event["payload"].get("item", {}).get("type") == "web_search"
    ]
    passed = (
        trace["credential_boundary_ready_before_turn"]
        and trace["fileless_external_auth_before_turn"]
        and any("REAL_CONTEXT_CREDENTIAL_INACCESSIBLE" in output for output in command_outputs)
        and bool(web_events)
        and trace["answer"].get("probe_marker") == "REAL_CONTEXT_CREDENTIAL_INACCESSIBLE"
        and str(trace["answer"].get("source_url", "")).startswith("https://")
    )
    result = {
        "schema_version": 1,
        "official": False,
        "financial_question": False,
        "model": model,
        "reasoning": reasoning,
        "passed": passed,
        "credential_boundary_ready_before_turn": trace["credential_boundary_ready_before_turn"],
        "fileless_external_auth_before_turn": trace["fileless_external_auth_before_turn"],
        "real_tool_probe_marker_seen": any(
            "REAL_CONTEXT_CREDENTIAL_INACCESSIBLE" in output for output in command_outputs
        ),
        "native_web_events_seen": len(web_events),
    }
    write_new_text(output_dir / "result.json", json.dumps(result, indent=2) + "\n")
    if not passed:
        raise RuntimeError("real subscription-boundary smoke failed; preserved redacted trace")
    return result
