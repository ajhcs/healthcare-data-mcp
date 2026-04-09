"""Tests for MRF download guardrails."""

import pytest

from servers.price_transparency import mrf_processor


class _FakeStreamResponse:
    def __init__(self, *, headers: dict[str, str], chunks: list[bytes]):
        self.headers = headers
        self._chunks = chunks

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def raise_for_status(self):
        return None

    async def aiter_bytes(self, chunk_size: int = 1024 * 1024):
        for chunk in self._chunks:
            yield chunk


class _FakeClient:
    def __init__(self, response: _FakeStreamResponse):
        self._response = response

    def stream(self, *args, **kwargs):
        return self._response


@pytest.mark.asyncio
async def test_download_mrf_rejects_content_length_above_limit(monkeypatch, tmp_path):
    monkeypatch.setenv("MRF_MAX_DOWNLOAD_BYTES", "10")
    monkeypatch.setenv("MRF_MIN_FREE_BYTES", "0")
    monkeypatch.setattr(mrf_processor, "_hospital_cache_dir", lambda hospital_id: tmp_path / hospital_id)

    from shared.utils import http_client

    response = _FakeStreamResponse(headers={"Content-Length": "11"}, chunks=[])
    monkeypatch.setattr(http_client, "get_client", lambda: _FakeClient(response))

    with pytest.raises(ValueError, match="configured max size"):
        await mrf_processor.download_mrf("https://example.com/test.json", "hospital-a")

    assert not (tmp_path / "hospital-a" / "test.json").exists()
    assert not (tmp_path / "hospital-a" / "test.json.part").exists()


@pytest.mark.asyncio
async def test_download_mrf_rejects_stream_when_size_grows_past_limit(monkeypatch, tmp_path):
    monkeypatch.setenv("MRF_MAX_DOWNLOAD_BYTES", "10")
    monkeypatch.setenv("MRF_MIN_FREE_BYTES", "0")
    monkeypatch.setattr(mrf_processor, "_hospital_cache_dir", lambda hospital_id: tmp_path / hospital_id)

    from shared.utils import http_client

    response = _FakeStreamResponse(headers={}, chunks=[b"12345", b"67890", b"X"])
    monkeypatch.setattr(http_client, "get_client", lambda: _FakeClient(response))

    with pytest.raises(ValueError, match="while streaming"):
        await mrf_processor.download_mrf("https://example.com/test.csv", "hospital-b")

    assert not (tmp_path / "hospital-b" / "test.csv").exists()
    assert not (tmp_path / "hospital-b" / "test.csv.part").exists()
