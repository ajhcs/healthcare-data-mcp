from shared.utils.source_status import SOURCE_STATUS_FIELDS, UNAVAILABLE, normalize_source_status


def test_normalize_source_status_preserves_known_fields_and_infers_cache_retrieval() -> None:
    status = normalize_source_status(
        {
            "source_url": "https://example.test/source.csv",
            "source_period": "2026 Q1",
            "cache_status": "ready",
            "cache_freshness": "fresh",
            "caveat": "Public file refreshes quarterly.",
        }
    )

    assert status == {
        "source_url": "https://example.test/source.csv",
        "source_period": "2026 Q1",
        "cache_status": "ready",
        "cache_freshness": "fresh",
        "retrieval_method": "cache",
        "caveat": "Public file refreshes quarterly.",
    }


def test_normalize_source_status_makes_missing_fields_explicit() -> None:
    status = normalize_source_status({"cache_status": "missing"})

    assert set(status) == set(SOURCE_STATUS_FIELDS)
    assert status["source_url"] == UNAVAILABLE
    assert status["source_period"] == UNAVAILABLE
    assert status["cache_status"] == "missing"
    assert status["cache_freshness"] == UNAVAILABLE
    assert status["retrieval_method"] == "cache"
    assert status["caveat"] == UNAVAILABLE
