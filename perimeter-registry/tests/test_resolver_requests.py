from perimeter_registry import Registry


def test_jefferson_health_revenue_discloses_care_intent_and_university_inclusion() -> None:
    result = Registry.jefferson().resolve("Jefferson Health revenue")

    assert result.status == "resolved"
    assert result.options[0].scope_id == "system_excluding_insurance_fy2025"
    assert result.flags == (
        "care-delivery intent inferred from Jefferson Health",
        "published System amount still includes university activity",
    )


def test_generic_form_990_question_asks_one_short_question() -> None:
    result = Registry.jefferson().resolve("Jefferson Form 990")

    assert result.status == "needs_clarification"
    assert result.question == "Which legal filer and tax year?"
    assert len(result.options) == 5
    assert all(option.scope_id.startswith("form990:") for option in result.options)


def test_unmatched_request_asks_for_scope_instead_of_returning_a_scalar() -> None:
    result = Registry.jefferson().resolve("Jefferson operating margin")

    assert result.status == "needs_clarification"
    assert result.question == "Which Jefferson legal entity or reporting scope do you mean?"
    assert result.options


def test_invalid_as_of_date_is_rejected_deterministically() -> None:
    try:
        Registry.jefferson().resolve("Jefferson revenue", as_of="July 2024")
    except ValueError as error:
        assert str(error) == "as_of must be an ISO date (YYYY-MM-DD)"
    else:
        raise AssertionError("non-ISO as_of date was accepted")


def test_bare_jefferson_health_uses_care_delivery_intent() -> None:
    result = Registry.jefferson().resolve("Jefferson Health")

    assert result.status == "resolved"
    assert result.options[0].scope_id == "system_excluding_insurance_fy2025"
    assert "published System amount still includes university activity" in result.flags


def test_tjuh_acronym_form_990_does_not_match_tju_prefix() -> None:
    result = Registry.jefferson().resolve("TJUH Form 990 2023")

    assert result.entity_ids == ("tjuh",)
    assert result.identifiers[0] == ("EIN", "23-2829095")
    assert result.tax_period_year == 2023


def test_resolved_pre_august_scope_carries_lvhn_exclusion() -> None:
    result = Registry.jefferson().resolve("Jefferson Health", as_of="2024-07-31")

    assert result.options[0].excludes == ("insurance", "LVHN")
    assert result.options[0].amount_usd is None
    assert result.options[0].basis == "no compatible pre-LVHN measurement in fixture"
    assert result.flags[-1] == "LVHN entered the perimeter on 2024-08-01"


def test_health_plans_resolution_carries_hpp_ein() -> None:
    result = Registry.jefferson().resolve("Jefferson Health Plans")

    assert result.identifiers == (("EIN", "23-2379751"),)


def test_form_990_exact_ein_and_year_selects_one_filer_without_fetching() -> None:
    result = Registry.jefferson().resolve("Form 990 23-2829095 2023")

    assert result.status == "resolved"
    assert result.entity_ids == ("tjuh",)
    assert result.tax_period_year == 2023
    assert result.flags == ("exact filer and tax-period year selected; no facts fetched",)


def test_form_990_named_filer_without_year_asks_one_question() -> None:
    result = Registry.jefferson().resolve("TJUH Form 990")

    assert result.status == "needs_clarification"
    assert result.question == "Which tax year?"
    assert result.entity_ids == ("tjuh",)


def test_pre_august_debt_does_not_expose_2025_lvhn_membership() -> None:
    result = Registry.jefferson().resolve("Jefferson debt", as_of="2024-07-31")

    assert result.status == "needs_clarification"
    assert result.entity_ids == ()
    assert "LVHN" in result.options[0].excludes
    assert result.options[0].amount_usd is None


def test_form_990_multiple_eins_or_years_require_clarification() -> None:
    result = Registry.jefferson().resolve("Form 990 23-2829095 23-1352651 2022 2023")

    assert result.status == "needs_clarification"
    assert result.question == "Which one EIN and tax year?"
    assert result.entity_ids == ()


def test_form_990_year_2100_matches_adapter_contract() -> None:
    result = Registry.jefferson().resolve("Form 990 23-2829095 2100")

    assert result.status == "resolved"
    assert result.tax_period_year == 2100
