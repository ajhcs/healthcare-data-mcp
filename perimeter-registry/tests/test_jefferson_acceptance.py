from perimeter_registry import Registry


def test_jefferson_revenue_never_returns_a_naked_scalar() -> None:
    result = Registry.jefferson().resolve("Jefferson revenue")

    assert result.status == "menu"
    assert result.question == "Which Jefferson reporting scope do you mean?"
    assert [option.label for option in result.options] == [
        "Audited consolidated enterprise",
        "System excluding insurance",
        "Insurance component",
    ]
    assert all(option.scope_id for option in result.options)


def test_fy25_enterprise_answer_labels_all_material_inclusions() -> None:
    result = Registry.jefferson().resolve("Jefferson FY25 enterprise revenue")

    assert result.status == "resolved"
    assert len(result.options) == 1
    enterprise = result.options[0]
    assert enterprise.amount_usd == 15_755_857_000
    assert enterprise.period == "FY ended 2025-06-30"
    assert enterprise.includes == (
        "university education and research",
        "care delivery",
        "insurance",
        "LVHN actual results from 2024-08-01 (11 months)",
    )
    assert "not hospital-only" in enterprise.flags


def test_fy25_excluding_insurance_still_flags_university_inclusion() -> None:
    result = Registry.jefferson().resolve("Jefferson Health FY25 revenue excluding insurance")

    assert result.status == "resolved"
    system = result.options[0]
    assert system.scope_id == "system_excluding_insurance_fy2025"
    assert system.amount_usd == 13_564_443_000
    assert system.includes == ("university education and research", "care delivery")
    assert system.excludes == ("insurance",)
    assert "not healthcare-delivery-only" in system.flags


def test_jefferson_health_plans_maps_to_hpp_insurance_not_tjuh() -> None:
    result = Registry.jefferson().resolve("Jefferson Health Plans")

    assert result.status == "resolved"
    assert result.entity_ids == ("hpp",)
    assert "tjuh" not in result.entity_ids
    assert result.options[0].scope_id == "insurance_fy2025"
    assert result.flags == ("Jefferson Health Plans is a marketing/reporting component",)


def test_tjuh_maps_to_its_exact_ein_and_ccn_not_tju_ein() -> None:
    result = Registry.jefferson().resolve("Thomas Jefferson University Hospitals")

    assert result.status == "resolved"
    assert result.entity_ids == ("tjuh",)
    assert result.identifiers == (("EIN", "23-2829095"), ("CCN", "390174"))
    assert ("EIN", "23-1352651") not in result.identifiers
    assert result.flags == ("individual filer; not Jefferson enterprise",)


def test_pre_august_2024_jefferson_excludes_lvhn() -> None:
    result = Registry.jefferson().resolve("Jefferson revenue", as_of="2024-07-31")

    assert result.status == "menu"
    assert result.as_of == "2024-07-31"
    assert all("LVHN" in option.excludes for option in result.options)
    assert result.flags == ("LVHN entered the perimeter on 2024-08-01",)


def test_fy25_reported_actual_and_lvhn_pro_forma_remain_separate() -> None:
    result = Registry.jefferson().resolve("Jefferson FY25 trend")

    assert result.status == "menu"
    actual, pro_forma = result.options
    assert actual.amount_usd == 15_755_857_000
    assert actual.basis == "audited actual; 11 months of LVHN"
    assert pro_forma.amount_usd == 16_148_416_000
    assert pro_forma.basis == "unaudited pro forma; 12 months of LVHN"
    assert actual.scope_id != pro_forma.scope_id
    assert result.flags == ("reported actual and pro forma are not interchangeable",)


def test_debt_resolves_to_obligated_group_and_excludes_insurance() -> None:
    result = Registry.jefferson().resolve("Jefferson debt")

    assert result.status == "resolved"
    debt = result.options[0]
    assert debt.scope_id == "obligated_group_2025"
    assert debt.amount_usd is None
    assert "HPP and health-plan entities" in debt.excludes
    assert result.entity_ids == ("tju", "jhc", "tjuh")
    assert result.flags == ("debt perimeter; not a legal entity or GAAP consolidation",)


def test_separate_990_totals_cannot_be_summed_as_jefferson_revenue() -> None:
    result = Registry.jefferson().resolve("Sum Jefferson separate Form 990 totals")

    assert result.status == "rejected"
    assert result.options == ()
    assert result.flags == ("separate Form 990 filers are non-additive tax perimeters",)


def test_lvhn_historical_node_remains_queryable_after_merger() -> None:
    entity = Registry.jefferson().lookup_entity("LVHN", as_of="2025-07-01")

    assert entity.entity_id == "lvhn"
    assert entity.name == "Lehigh Valley Health Network"
    assert entity.identifiers == (("EIN", "22-2458317"),)
    assert entity.lifecycle_status == "merged"
    assert entity.effective_end == "2025-06-30"
