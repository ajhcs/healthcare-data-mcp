from __future__ import annotations

from pathlib import Path

import pytest

from shared.cache_manager import core


def test_cache_status_reports_manifest_backed_states(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SEC_USER_AGENT", raising=False)

    report = core.cache_status_payload(tmp_path)
    by_id = {entry["dataset_id"]: entry for entry in report["datasets"]}

    assert report["readiness_model"] == "manifest_backed_source_readiness"
    assert by_id["cms_hospital_quality"]["validation_status"] == "not_validated"
    assert by_id["cms_hospital_quality"]["report_eligible"] is False
    assert by_id["financial_intelligence" if False else "ahrq_hfmd"]["missing_env"] == ["SEC_USER_AGENT"]
    assert by_id["pa_hospital_reports"]["readiness_status"] == "state_limited"
    assert by_id["state_health_data"]["readiness_status"] == "manual_import_required"
    assert "Missing cache data is an unknown" in by_id["cms_hospital_quality"]["next_action"]


def test_plan_cache_refresh_for_flagship_workflow_has_ordered_blockers(tmp_path: Path) -> None:
    plan = core.plan_cache_refresh(workflow_id="hospital_competitive_profile", cache_root=tmp_path)

    ids = [item["dataset_id"] for item in plan["ordered_plan"]]
    assert ids[:4] == [
        "cms_hospital_general_info",
        "cms_hospital_quality",
        "cms_cost_report",
        "ahrq_health_system_compendium",
    ]
    assert plan["blockers"]
    assert all("next_action" in blocker for blocker in plan["blockers"])


def test_profile_evidence_pack_cache_preflight_includes_core_sources(tmp_path: Path) -> None:
    readiness = core.list_cache_sources(workflow="profile_evidence_pack", cache_root=tmp_path)
    source_ids = {row["dataset_id"] for row in readiness["sources"]}

    assert {
        "cms_provider_of_services",
        "cms_hospital_general_info",
        "ahrq_health_system_compendium",
        "cms_cost_report",
        "cms_pecos_public_provider_enrollment",
        "cms_pecos_hospital_chow",
    } <= source_ids
    assert readiness["summary"]


def test_start_cache_refresh_rejects_unknown_and_oversized_requests(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="Unknown dataset_id"):
        core.start_cache_refresh(["not_a_dataset"], cache_root=tmp_path)

    with pytest.raises(ValueError, match="max_bytes"):
        core.start_cache_refresh(
            ["cms_hospital_general_info"],
            cache_root=tmp_path,
            dry_run=False,
            max_bytes=core.DEFAULT_MAX_BYTES + 1,
        )


def test_mutating_paths_are_confined_to_cache_root(tmp_path: Path) -> None:
    outside = tmp_path.parent / "outside.csv"
    outside.write_text("id\n1\n", encoding="utf-8")

    with pytest.raises(ValueError, match="escapes"):
        core.validate_cache_source("cms_hospital_general_info", cache_root=tmp_path, staged_path=outside)


def test_public_url_policy_rejects_private_and_unregistered_targets() -> None:
    with pytest.raises(ValueError, match="exact allowlisted"):
        core._validate_public_url(  # noqa: SLF001 - policy-level regression test
            "https://example.com/file.csv",
            allowed_urls=("https://data.cms.gov/file.csv",),
        )

    with pytest.raises(ValueError, match="exact allowlisted"):
        core._validate_public_url(  # noqa: SLF001 - policy-level regression test
            "https://data.cms.gov/other.csv",
            allowed_urls=("https://data.cms.gov/file.csv",),
        )

    with pytest.raises(ValueError, match="Private"):
        core._validate_public_url(  # noqa: SLF001 - policy-level regression test
            "http://127.0.0.1/file.csv",
            allowed_urls=("http://127.0.0.1/file.csv",),
        )


def test_promote_validated_artifact_writes_manifest_and_rollback_metadata(tmp_path: Path) -> None:
    staged = tmp_path / "bronze" / "cms_hospital_general_info" / "run-1" / "source.csv"
    staged.parent.mkdir(parents=True)
    staged.write_text(_general_info_csv(), encoding="utf-8")

    manifest = core.promote_cache_artifact(
        "cms_hospital_general_info",
        staged,
        cache_root=tmp_path,
        run_id="run-1",
        relative_path="hospital_general_info.csv",
    )

    assert manifest["cache_status"] == "partial"
    assert manifest["validation_status"] == "pass"
    assert manifest["row_count"] == 1
    current = core.get_cache_manifest("cms_hospital_general_info", cache_root=tmp_path)
    assert current["artifact_id"] == manifest["artifact_id"]
    assert Path(current["path"]).exists()


def test_wrong_schema_fails_validation_and_promotion(tmp_path: Path) -> None:
    staged = tmp_path / "bronze" / "cms_hospital_general_info" / "run-1" / "source.csv"
    staged.parent.mkdir(parents=True)
    staged.write_text("foo\nbar\n", encoding="utf-8")

    validation = core.validate_cache_source("cms_hospital_general_info", cache_root=tmp_path, staged_path=staged)

    assert validation["validation"]["status"] == "fail"
    assert validation["validation"]["report_eligible"] is False
    with pytest.raises(ValueError, match="failed validation"):
        core.promote_cache_artifact(
            "cms_hospital_general_info",
            staged,
            cache_root=tmp_path,
            run_id="run-1",
            relative_path="hospital_general_info.csv",
        )


def test_real_cms_general_info_header_validates_with_aliases(tmp_path: Path) -> None:
    staged = tmp_path / "bronze" / "cms_hospital_general_info" / "run-1" / "source.csv"
    staged.parent.mkdir(parents=True)
    staged.write_text(_real_cms_general_info_csv(), encoding="utf-8")

    validation = core.validate_cache_source(
        "cms_hospital_general_info",
        cache_root=tmp_path,
        staged_path=staged,
        relative_path="hospital_general_info.csv",
    )

    assert validation["validation"]["status"] == "pass"
    assert "City/Town" in validation["validation"]["metrics"]["raw_columns"]
    assert "city" in validation["validation"]["metrics"]["columns"]


def test_exact_measure_dataset_requires_measure_id(tmp_path: Path) -> None:
    staged = tmp_path / "bronze" / "cms_hospital_quality" / "run-1" / "source.csv"
    staged.parent.mkdir(parents=True)
    staged.write_text(
        "facility_id,facility_name,measure_name,score,compared_to_national,number_of_discharges,payment_reduction\n"
        "390223,Example Hospital,CLABSI,0.72,Better,10,0\n",
        encoding="utf-8",
    )

    validation = core.validate_cache_source(
        "cms_hospital_quality",
        cache_root=tmp_path,
        staged_path=staged,
        relative_path="hospital_quality_hcahps.csv",
    )

    assert validation["validation"]["status"] == "fail"
    assert any(defect["expected"] == "measure_id" for defect in validation["validation"]["defects"])


def test_multi_artifact_dataset_is_partial_until_all_required_artifacts_are_promoted(tmp_path: Path) -> None:
    staged = tmp_path / "bronze" / "cms_hospital_quality" / "run-1" / "source.csv"
    staged.parent.mkdir(parents=True)
    staged.write_text(_quality_csv(), encoding="utf-8")

    manifest = core.promote_cache_artifact(
        "cms_hospital_quality",
        staged,
        cache_root=tmp_path,
        run_id="run-1",
        relative_path="hospital_quality_hrrp.csv",
    )
    status = core.inspect_cache_source("cms_hospital_quality", cache_root=tmp_path)["status"]

    assert manifest["cache_status"] == "partial"
    assert status["readiness_status"] == "partial"
    assert status["report_eligible"] is False


def test_ahrq_artifacts_validate_against_artifact_specific_identity(tmp_path: Path) -> None:
    system_path = tmp_path / "bronze" / "ahrq" / "system.csv"
    linkage_path = tmp_path / "bronze" / "ahrq" / "linkage.csv"
    system_path.parent.mkdir(parents=True)
    system_path.write_text(
        "health_sys_id,health_sys_name,health_sys_city,health_sys_state,hosp_cnt,sys_beds\n"
        "1001,Example Health,Philadelphia,PA,2,500\n",
        encoding="utf-8",
    )
    linkage_path.write_text(
        "compendium_hospital_id,ccn,hospital_name,health_sys_id,health_sys_name,hos_beds\n"
        "H1,390001,Example Hospital,1001,Example Health,250\n",
        encoding="utf-8",
    )

    system_validation = core.validate_cache_source(
        "ahrq_health_system_compendium",
        cache_root=tmp_path,
        staged_path=system_path,
        relative_path="ahrq_system_2023.csv",
    )["validation"]
    linkage_validation = core.validate_cache_source(
        "ahrq_health_system_compendium",
        cache_root=tmp_path,
        staged_path=linkage_path,
        relative_path="ahrq_hospital_linkage_2023.csv",
    )["validation"]

    assert system_validation["status"] == "pass"
    assert linkage_validation["status"] == "pass"


def test_multi_artifact_dataset_requires_all_artifacts_for_ready(tmp_path: Path) -> None:
    spec = core.get_dataset_spec("cms_hospital_quality")
    staged_artifacts = []
    for index, relative_path in enumerate(spec.expected_artifacts):
        staged = tmp_path / "bronze" / "cms_hospital_quality" / "run-all" / f"source-{index}.csv"
        staged.parent.mkdir(parents=True, exist_ok=True)
        staged.write_text(_quality_csv(), encoding="utf-8")
        validation = core.validate_cache_source(
            "cms_hospital_quality",
            cache_root=tmp_path,
            staged_path=staged,
            relative_path=relative_path,
        )["validation"]
        assert validation["status"] == "pass"
        staged_artifacts.append((relative_path, staged, spec.source_urls[index], core.CacheValidationResult(**validation)))

    manifest = core._promote_many(spec, tmp_path, staged_artifacts, "run-all")  # noqa: SLF001
    status = core.inspect_cache_source("cms_hospital_quality", cache_root=tmp_path)["status"]

    assert manifest.cache_status == "ready"
    assert status["readiness_status"] == "ready"
    assert status["report_eligible"] is True


def test_promote_requires_relative_path_for_multi_artifact_dataset(tmp_path: Path) -> None:
    staged = tmp_path / "bronze" / "cms_hospital_quality" / "run-1" / "source.csv"
    staged.parent.mkdir(parents=True)
    staged.write_text(_quality_csv(), encoding="utf-8")

    with pytest.raises(ValueError, match="relative_path is required"):
        core.promote_cache_artifact("cms_hospital_quality", staged, cache_root=tmp_path, run_id="run-1")


def test_each_quality_artifact_can_be_promoted_by_explicit_relative_path(tmp_path: Path) -> None:
    spec = core.get_dataset_spec("cms_hospital_quality")
    for index, relative_path in enumerate(spec.expected_artifacts):
        staged = tmp_path / "bronze" / "cms_hospital_quality" / f"run-{index}" / "source.csv"
        staged.parent.mkdir(parents=True)
        staged.write_text(_quality_csv(), encoding="utf-8")

        manifest = core.promote_cache_artifact(
            "cms_hospital_quality",
            staged,
            cache_root=tmp_path,
            run_id=f"run-{index}",
            relative_path=relative_path,
            source_url=spec.source_urls[index],
        )

        assert any(artifact["relative_path"] == relative_path for artifact in manifest["artifacts"])


def test_relative_path_and_source_url_mismatches_are_rejected(tmp_path: Path) -> None:
    staged = tmp_path / "bronze" / "cms_hospital_quality" / "run-1" / "source.csv"
    staged.parent.mkdir(parents=True)
    staged.write_text(_quality_csv(), encoding="utf-8")

    with pytest.raises(ValueError, match="expected artifact"):
        core.promote_cache_artifact(
            "cms_hospital_quality",
            staged,
            cache_root=tmp_path,
            run_id="run-1",
            relative_path="../escape.csv",
        )

    with pytest.raises(ValueError, match="source_url does not match"):
        core.promote_cache_artifact(
            "cms_hospital_quality",
            staged,
            cache_root=tmp_path,
            run_id="run-1",
            relative_path="hospital_quality_hac.csv",
            source_url=core.get_dataset_spec("cms_hospital_quality").source_urls[0],
        )


def test_checksum_mismatch_marks_ready_manifest_corrupt(tmp_path: Path) -> None:
    spec = core.get_dataset_spec("cms_hospital_quality")
    staged_artifacts = []
    for index, relative_path in enumerate(spec.expected_artifacts):
        staged = tmp_path / "bronze" / "cms_hospital_quality" / "run-all" / f"source-{index}.csv"
        staged.parent.mkdir(parents=True, exist_ok=True)
        staged.write_text(_quality_csv(), encoding="utf-8")
        validation = core.validate_cache_source(
            "cms_hospital_quality",
            cache_root=tmp_path,
            staged_path=staged,
            relative_path=relative_path,
        )["validation"]
        staged_artifacts.append((relative_path, staged, spec.source_urls[index], core.CacheValidationResult(**validation)))
    manifest = core._promote_many(spec, tmp_path, staged_artifacts, "run-all")  # noqa: SLF001
    Path(manifest.artifacts[0]["path"]).write_text(_quality_csv(score="9.99"), encoding="utf-8")

    status = core.inspect_cache_source("cms_hospital_quality", cache_root=tmp_path)["status"]

    assert status["readiness_status"] == "corrupt"
    assert status["validation_status"] == "fail"
    assert status["report_eligible"] is False
    assert status["integrity_defects"][0]["field"] == "checksum_sha256"


def test_compatibility_copy_tamper_marks_ready_manifest_corrupt(tmp_path: Path) -> None:
    _promote_all_general_info_artifacts(tmp_path)
    assert core.inspect_cache_source("cms_hospital_general_info", cache_root=tmp_path)["status"][
        "readiness_status"
    ] == "ready"

    (tmp_path / "hospital_general_info.csv").write_text(_general_info_csv("Tampered Hospital"), encoding="utf-8")

    status = core.inspect_cache_source("cms_hospital_general_info", cache_root=tmp_path)["status"]

    assert status["readiness_status"] == "corrupt"
    assert status["validation_status"] == "fail"
    assert status["report_eligible"] is False
    assert any(defect["field"].startswith("compatibility_copy") for defect in status["integrity_defects"])


def test_missing_published_compatibility_copy_marks_required_artifact_corrupt(tmp_path: Path) -> None:
    _promote_all_general_info_artifacts(tmp_path)

    (tmp_path / "hospital_quality_hospital_info.csv").unlink()

    status = core.inspect_cache_source("cms_hospital_general_info", cache_root=tmp_path)["status"]

    assert status["readiness_status"] == "corrupt"
    assert status["validation_status"] == "fail"
    assert status["report_eligible"] is False
    assert any(
        defect["field"] == "compatibility_copy"
        and defect["relative_path"] == "hospital_quality_hospital_info.csv"
        for defect in status["integrity_defects"]
    )


def test_quarantine_updates_current_manifest_and_removes_published_copies(tmp_path: Path) -> None:
    _promote_all_general_info_artifacts(tmp_path)
    assert core.get_cache_manifest("cms_hospital_general_info", cache_root=tmp_path)["cache_status"] == "ready"
    assert (tmp_path / "hospital_general_info.csv").exists()
    assert (tmp_path / "hospital_quality_hospital_info.csv").exists()

    quarantine = core.quarantine_cache_artifact(
        "cms_hospital_general_info",
        cache_root=tmp_path,
        reason="regression_test",
    )
    current = core.get_cache_manifest("cms_hospital_general_info", cache_root=tmp_path)
    status = core.inspect_cache_source("cms_hospital_general_info", cache_root=tmp_path)["status"]

    assert quarantine["status"] == "quarantined"
    assert current["cache_status"] == "corrupt"
    assert current["validation_status"] == "fail"
    assert status["readiness_status"] == "corrupt"
    assert not (tmp_path / "hospital_general_info.csv").exists()
    assert not (tmp_path / "hospital_quality_hospital_info.csv").exists()
    assert all(Path(path).resolve().is_relative_to((tmp_path / "quarantine").resolve()) for path in quarantine["paths"])


def test_promotion_and_rollback_paths_remain_under_cache_root(tmp_path: Path) -> None:
    first = tmp_path / "bronze" / "cms_hospital_general_info" / "run-1" / "nested" / "source.csv"
    first.parent.mkdir(parents=True)
    first.write_text(_general_info_csv("First Hospital"), encoding="utf-8")
    first_manifest = core.promote_cache_artifact(
        "cms_hospital_general_info",
        first,
        cache_root=tmp_path,
        run_id="run-1",
        relative_path="hospital_general_info.csv",
    )

    second = tmp_path / "bronze" / "cms_hospital_general_info" / "run-2" / "nested" / "source.csv"
    second.parent.mkdir(parents=True)
    second.write_text(_general_info_csv("Second Hospital"), encoding="utf-8")
    second_manifest = core.promote_cache_artifact(
        "cms_hospital_general_info",
        second,
        cache_root=tmp_path,
        run_id="run-2",
        relative_path="hospital_general_info.csv",
    )

    rollback = core.rollback_cache_artifact("cms_hospital_general_info", cache_root=tmp_path)

    for path in [first_manifest["path"], second_manifest["path"], *rollback["paths"]]:
        Path(path).resolve().relative_to(tmp_path.resolve())


def test_workflow_cache_status_uses_severity_not_lexical_order() -> None:
    from shared.utils.workflows import build_workflow_plan

    plan = build_workflow_plan(
        "quality_profile",
        inputs={"ccn": "390223"},
        cache_status={
            "datasets": [
                {"dataset_id": "cms_hospital_general_info", "readiness_status": "ready", "report_eligible": True},
                {"dataset_id": "cms_hospital_quality", "readiness_status": "ready", "report_eligible": True},
                {"dataset_id": "cms_hospital_quality", "readiness_status": "stale", "report_eligible": False},
            ],
            "entries": [],
        },
    )

    by_source = {check["source_id"]: check for check in plan["cache_readiness"]["checks"]}
    assert by_source["cms_hospital_quality"]["status"] == "stale"
    assert plan["cache_readiness"]["status"] == "blocked"


def test_workflow_cache_readiness_distinguishes_missing_without_negative_claim(tmp_path: Path) -> None:
    from shared.utils.workflows import build_workflow_plan

    plan = build_workflow_plan(
        "hospital_competitive_profile",
        inputs={"ccn": "390223", "measure": "clabsi_sir"},
        cache_status=core.cache_status_payload(tmp_path),
    )

    assert plan["cache_readiness"]["status"] == "blocked"
    assert "Missing cache data is an unknown" in plan["cache_readiness"]["missing_data_policy"]
    by_tool = {step["tool"]: step for step in plan["steps"]}
    assert by_tool["get_quality_measure_rows"]["mcp_call"]["arguments_template"]["measure"] == "clabsi_sir"
    assert by_tool["get_quality_measure_rows"]["identity_contract"]["match_policy"] == (
        "exact_identifier_required_for_report_fact"
    )


def test_workflow_cache_readiness_blocks_when_cache_status_not_checked(monkeypatch: pytest.MonkeyPatch) -> None:
    from shared.utils.workflows import build_workflow_plan

    monkeypatch.setenv("SEC_USER_AGENT", "test@example.com")

    plan = build_workflow_plan(
        "hospital_competitive_profile",
        inputs={"ccn": "390223", "measure": "clabsi_sir"},
    )

    assert plan["cache_readiness"]["status"] == "blocked"
    assert plan["cache_readiness"]["status_counts"]["not_checked"] == 4
    assert {blocker["status"] for blocker in plan["cache_readiness"]["blockers"]} == {"not_checked"}


@pytest.mark.asyncio
async def test_promote_cache_artifact_mcp_schema_exposes_relative_path() -> None:
    from servers.cache_manager import server

    tools = await server.mcp.list_tools()
    promote = next(tool for tool in tools if tool.name == "promote_cache_artifact")

    assert "relative_path" in promote.inputSchema["properties"]


def _promote_all_general_info_artifacts(cache_root: Path) -> None:
    spec = core.get_dataset_spec("cms_hospital_general_info")
    for index, relative_path in enumerate(spec.expected_artifacts):
        staged = cache_root / "bronze" / "cms_hospital_general_info" / f"run-{index}" / "source.csv"
        staged.parent.mkdir(parents=True, exist_ok=True)
        staged.write_text(_general_info_csv(f"Example Hospital {index}"), encoding="utf-8")
        core.promote_cache_artifact(
            "cms_hospital_general_info",
            staged,
            cache_root=cache_root,
            run_id=f"run-{index}",
            relative_path=relative_path,
        )


def _general_info_csv(name: str = "Example Hospital") -> str:
    return (
        "facility_id,facility_name,address,city,state,zip_code,hospital_type,hospital_ownership,"
        "emergency_services,hospital_overall_rating\n"
        f"390223,{name},1 Main St,Philadelphia,PA,19104,Acute Care,Voluntary,Yes,4\n"
    )


def _real_cms_general_info_csv() -> str:
    return (
        '"Facility ID","Facility Name",Address,City/Town,State,"ZIP Code",County/Parish,'
        '"Telephone Number","Hospital Type","Hospital Ownership","Emergency Services",'
        '"Hospital overall rating"\n'
        "390223,Example Hospital,1 Main St,Philadelphia,PA,19104,Philadelphia,555-0100,"
        "Acute Care,Voluntary,Yes,4\n"
    )


def _quality_csv(score: str = "0.72") -> str:
    return (
        "facility_id,measure_id,facility_name,measure_name,score,compared_to_national,"
        "number_of_discharges,payment_reduction\n"
        f"390223,clabsi_sir,Example Hospital,CLABSI SIR,{score},Better,10,0\n"
    )
