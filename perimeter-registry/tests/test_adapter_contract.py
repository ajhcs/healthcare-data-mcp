import pytest

from perimeter_registry import Form990Query


def test_future_990_query_requires_exact_ein_and_year() -> None:
    query = Form990Query(ein="23-2829095", tax_period_year=2024)

    assert query.ein == "23-2829095"
    assert query.tax_period_year == 2024


@pytest.mark.parametrize("ein", ["232829095", "Jefferson", "23-282909"])
def test_future_990_query_rejects_non_exact_ein(ein: str) -> None:
    with pytest.raises(ValueError, match="NN-NNNNNNN"):
        Form990Query(ein=ein, tax_period_year=2024)
