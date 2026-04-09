"""
Unit tests (no database required) + integration tests (require INTEGRATION=true env var).

Run unit tests only:
    pytest tests/ -m "not integration"

Run all tests (requires live database):
    INTEGRATION=true pytest tests/
"""
from __future__ import annotations

import os
import sys

import pytest
import pytest_asyncio

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from retrieval import minmax_normalize, fuse_scores, build_filter_clause

pytestmark = pytest.mark.anyio

# ─── Unit Tests ────────────────────────────────────────────────────────────────

def test_minmax_normalize_normal():
    result = minmax_normalize([0.2, 0.5, 0.8, 1.0, 0.1])
    expected = [0.111, 0.444, 0.778, 1.0, 0.0]
    assert len(result) == len(expected)
    for r, e in zip(result, expected):
        assert abs(r - e) < 0.01, f"Expected {e}, got {r}"


def test_minmax_normalize_all_equal():
    result = minmax_normalize([0.5, 0.5, 0.5])
    assert result == [1.0, 1.0, 1.0]


def test_minmax_normalize_single_value():
    result = minmax_normalize([0.73])
    assert result == [1.0]


def test_fuse_scores_alpha_one():
    assert abs(fuse_scores(0.9, 0.1, 1.0) - 0.9) < 1e-9


def test_fuse_scores_alpha_zero():
    assert abs(fuse_scores(0.9, 0.1, 0.0) - 0.1) < 1e-9


def test_fuse_scores_default_alpha():
    assert abs(fuse_scores(1.0, 1.0, 0.7) - 1.0) < 1e-9


def test_build_filter_clause_no_filters():
    where, params = build_filter_clause(None, None, None)
    assert where == ""
    assert params == {}


def test_build_filter_clause_single_filter():
    where, params = build_filter_clause("financials", None, None)
    assert "sector = :sector" in where
    assert params == {"sector": "financials"}


def test_build_filter_clause_all_filters():
    where, params = build_filter_clause("banking", "JPM", "10-K")
    assert "sector = :sector" in where
    assert "ticker = :company" in where
    assert "filing_type = :filing_type" in where
    assert params == {"sector": "banking", "company": "JPM", "filing_type": "10-K"}


# ─── Integration Tests ─────────────────────────────────────────────────────────

INTEGRATION = os.getenv("INTEGRATION", "").lower() == "true"
integration = pytest.mark.skipif(not INTEGRATION, reason="Set INTEGRATION=true to run")


@integration
async def test_health_endpoint(real_client):
    resp = await real_client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


@integration
async def test_retrieve_returns_k_chunks(real_client):
    resp = await real_client.post(
        "/retrieve", json={"query": "net interest margin", "k": 5}
    )
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["chunks"]) == 5


@integration
async def test_retrieve_sector_filter(real_client):
    resp = await real_client.post(
        "/retrieve", json={"query": "revenue growth", "k": 5, "sector": "banking"}
    )
    assert resp.status_code == 200
    for chunk in resp.json()["chunks"]:
        assert chunk["sector"] == "banking"


@integration
async def test_retrieve_company_filter(real_client):
    resp = await real_client.post(
        "/retrieve", json={"query": "revenue growth", "k": 5, "company": "JPM"}
    )
    assert resp.status_code == 200
    for chunk in resp.json()["chunks"]:
        assert chunk["company"] == "JPM"


@integration
async def test_retrieve_filing_type_filter(real_client):
    resp = await real_client.post(
        "/retrieve", json={"query": "risk factors", "k": 5, "filing_type": "10-K"}
    )
    assert resp.status_code == 200
    for chunk in resp.json()["chunks"]:
        assert chunk["filing_type"] == "10-K"


@integration
async def test_retrieve_combined_filters(real_client):
    resp = await real_client.post(
        "/retrieve",
        json={"query": "net interest income", "k": 5, "company": "JPM", "filing_type": "10-K"},
    )
    assert resp.status_code == 200
    for chunk in resp.json()["chunks"]:
        assert chunk["company"] == "JPM"
        assert chunk["filing_type"] == "10-K"


@integration
async def test_retrieve_scores_descending(real_client):
    resp = await real_client.post(
        "/retrieve", json={"query": "earnings per share", "k": 5}
    )
    assert resp.status_code == 200
    scores = [c["score"] for c in resp.json()["chunks"]]
    assert scores == sorted(scores, reverse=True)


@integration
async def test_retrieve_alpha_extremes_differ(real_client):
    payload = {"query": "operating expenses technology", "k": 5}
    resp_vec = await real_client.post("/retrieve", json={**payload, "alpha": 1.0})
    resp_bm25 = await real_client.post("/retrieve", json={**payload, "alpha": 0.0})
    assert resp_vec.status_code == 200
    assert resp_bm25.status_code == 200
    ids_vec = [c["chunk_id"] for c in resp_vec.json()["chunks"]]
    ids_bm25 = [c["chunk_id"] for c in resp_bm25.json()["chunks"]]
    assert ids_vec != ids_bm25, "alpha=1.0 and alpha=0.0 should produce different orderings"


@integration
async def test_retrieve_chunk_metadata_fields_present(real_client):
    resp = await real_client.post(
        "/retrieve", json={"query": "cash flow from operations", "k": 3}
    )
    assert resp.status_code == 200
    required = {"chunk_id", "text", "score", "company", "sector", "filing_type",
                "filed_date", "source_url", "article_title", "page_num"}
    for chunk in resp.json()["chunks"]:
        assert required.issubset(chunk.keys()), f"Missing fields in chunk: {chunk.keys()}"


@integration
async def test_retrieve_empty_query_string(real_client):
    resp = await real_client.post("/retrieve", json={"query": ""})
    assert resp.status_code == 422


@integration
async def test_retrieve_k_exceeds_corpus(real_client):
    resp = await real_client.post(
        "/retrieve",
        json={"query": "net income", "k": 9999, "company": "JPM", "filing_type": "10-K"},
    )
    assert resp.status_code == 200
    # Should return however many chunks exist, not crash
    assert isinstance(resp.json()["chunks"], list)
