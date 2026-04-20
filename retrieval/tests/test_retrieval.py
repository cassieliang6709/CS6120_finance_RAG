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

import retrieval as retrieval_module
from retrieval import (
    build_filter_clause,
    detect_company_in_query,
    detect_filing_type_in_query,
    detect_filing_type_hint_in_query,
    detect_years_in_query,
    fuse_scores,
    minmax_normalize,
    query_prefers_explanatory_chunks,
    query_prefers_quantitative_chunks,
    resolve_company_filter,
    sanitize_bm25_query,
)

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
    where, params = build_filter_clause(None, None, None, None)
    assert where == ""
    assert params == {}


def test_build_filter_clause_single_filter():
    where, params = build_filter_clause("financials", None, None, None)
    assert "sector = :sector" in where
    assert params == {"sector": "financials"}


def test_build_filter_clause_all_filters():
    where, params = build_filter_clause("banking", "JPMorgan Chase & Co.", "10-K", 2023)
    assert "sector = :sector" in where
    assert "company_name" in where
    assert "filing_type = :filing_type" in where
    assert "fiscal_year = :fiscal_year" in where
    assert params == {
        "sector": "banking",
        "company": "JPMorgan Chase & Co.",
        "filing_type": "10-K",
        "fiscal_year": 2023,
    }


def test_query_prefers_quantitative_chunks_detects_metric_queries():
    assert query_prefers_quantitative_chunks("AMD revenue in 2023")
    assert query_prefers_quantitative_chunks("What was gross margin?")


def test_query_prefers_quantitative_chunks_skips_narrative_queries():
    assert not query_prefers_quantitative_chunks("Summarize the risk factors discussion")


def test_query_prefers_explanatory_chunks_detects_narrative_queries():
    assert query_prefers_explanatory_chunks("Why did management lower outlook?")
    assert query_prefers_explanatory_chunks("Summarize the risk factors discussion")


def test_query_prefers_explanatory_chunks_skips_metric_queries():
    assert not query_prefers_explanatory_chunks("AMD revenue in 2023")


def test_sanitize_bm25_query_removes_explicit_ticker_when_company_filter_scopes_corpus(monkeypatch):
    monkeypatch.setattr(retrieval_module, "_known_tickers", {"AMZN"})
    monkeypatch.setattr(retrieval_module, "_company_name_to_ticker", {"amazon.com, inc.": "AMZN"})
    monkeypatch.setattr(
        retrieval_module,
        "_ticker_to_company_names",
        {"AMZN": {"Amazon.com, Inc."}},
    )
    assert sanitize_bm25_query("AMZN revenue 2019 vs 2018", "AMZN") == "revenue 2019 vs 2018"


def test_sanitize_bm25_query_falls_back_when_cleanup_would_empty_query(monkeypatch):
    monkeypatch.setattr(retrieval_module, "_known_tickers", {"AMZN"})
    monkeypatch.setattr(retrieval_module, "_company_name_to_ticker", {})
    monkeypatch.setattr(retrieval_module, "_ticker_to_company_aliases", {})
    monkeypatch.setattr(retrieval_module, "_ticker_to_company_names", {})
    assert sanitize_bm25_query("AMZN", "AMZN") == "AMZN"


def test_sanitize_bm25_query_removes_company_aliases_when_company_is_resolved(monkeypatch):
    monkeypatch.setattr(retrieval_module, "_known_tickers", {"AMZN"})
    monkeypatch.setattr(retrieval_module, "_company_name_to_ticker", {"amazon.com, inc.": "AMZN"})
    monkeypatch.setattr(
        retrieval_module,
        "_ticker_to_company_aliases",
        {"AMZN": {"amazon", "amazoncom"}},
    )
    monkeypatch.setattr(
        retrieval_module,
        "_ticker_to_company_names",
        {"AMZN": {"Amazon.com, Inc."}},
    )
    assert sanitize_bm25_query("Amazon risk factors", "AMZN") == "risk factors"


def test_detect_company_in_query_supports_short_company_alias(monkeypatch):
    monkeypatch.setattr(retrieval_module, "_known_tickers", {"AMZN"})
    monkeypatch.setattr(retrieval_module, "_company_alias_to_ticker", {"amazon": "AMZN"})
    assert detect_company_in_query("What is Amazon trend in operating income?") == "AMZN"


def test_detect_company_in_query_supports_compact_alias(monkeypatch):
    monkeypatch.setattr(retrieval_module, "_known_tickers", {"XOM"})
    monkeypatch.setattr(retrieval_module, "_company_alias_to_ticker", {"exxonmobil": "XOM"})
    assert detect_company_in_query("How did ExxonMobil describe its upstream plan?") == "XOM"


def test_resolve_company_filter_maps_alias_to_ticker(monkeypatch):
    monkeypatch.setattr(retrieval_module, "_known_tickers", {"AMZN"})
    monkeypatch.setattr(retrieval_module, "_company_name_to_ticker", {"amazon.com, inc.": "AMZN"})
    monkeypatch.setattr(
        retrieval_module,
        "_company_alias_to_ticker",
        {"amazon": "AMZN", "amazoncom": "AMZN"},
    )
    assert resolve_company_filter("Amazon") == "AMZN"


def test_detect_filing_type_in_query_explicit_10q():
    assert detect_filing_type_in_query("Use the latest 10-Q for Amazon revenue.") == "10-Q"


def test_detect_filing_type_in_query_supports_unhyphenated_10k():
    assert detect_filing_type_in_query("Use the latest 10k for Amazon revenue.") == "10-K"


def test_detect_filing_type_in_query_supports_form_alias():
    assert detect_filing_type_in_query("Summarize Form 10-K risk factors for Amazon.") == "10-K"


def test_detect_filing_type_in_query_supports_year_end_alias():
    assert detect_filing_type_in_query("What did the year-end report say about liquidity?") == "10-K"


def test_detect_filing_type_in_query_supports_q_report_alias():
    assert detect_filing_type_in_query("What did the Q1 report say about margins?") == "10-Q"


def test_detect_filing_type_in_query_fy_is_not_treated_as_explicit():
    query = "What is Amazon's FY2019 net income attributable to shareholders?"
    assert detect_filing_type_in_query(query) is None


def test_detect_filing_type_in_query_quarter_hint_is_not_treated_as_explicit():
    query = "What was Amazon's Q2 2019 net income?"
    assert detect_filing_type_in_query(query) is None


def test_detect_filing_type_in_query_returns_none_for_ambiguous_explicit_mentions():
    query = "Compare Amazon's 10-K and 10-Q disclosures for 2023."
    assert detect_filing_type_in_query(query) is None


def test_detect_filing_type_hint_in_query_fy_defaults_to_10k():
    query = "What is Amazon's FY2019 net income attributable to shareholders?"
    assert detect_filing_type_hint_in_query(query) == "10-K"


def test_detect_filing_type_hint_in_query_quarter_defaults_to_10q():
    query = "What was Amazon's Q2 2019 net income?"
    assert detect_filing_type_hint_in_query(query) == "10-Q"


def test_detect_filing_type_hint_in_query_returns_none_for_ambiguous_hints():
    query = "Compare annual and quarterly trends for Amazon."
    assert detect_filing_type_hint_in_query(query) is None


def test_detect_filing_type_hint_in_query_does_not_treat_bare_year_as_10k():
    query = "What changed for Amazon in 2023?"
    assert detect_filing_type_hint_in_query(query) is None


def test_detect_years_in_query_single_year():
    assert detect_years_in_query("What changed for Amazon in 2023?") == ["2023"]


def test_detect_years_in_query_expands_ranges():
    query = "Compare Amazon revenue from 2020-2022."
    assert detect_years_in_query(query) == ["2020", "2021", "2022"]


def test_detect_years_in_query_supports_to_ranges():
    query = "Compare Amazon revenue from 2020 to 2022."
    assert detect_years_in_query(query) == ["2020", "2021", "2022"]


def test_detect_years_in_query_deduplicates_overlapping_mentions():
    query = "Compare 2020-2022 and 2021 results."
    assert detect_years_in_query(query) == ["2020", "2021", "2022"]


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
    required = {
        "chunk_id",
        "text",
        "score",
        "company",
        "sector",
        "filing_type",
        "filed_date",
        "source_url",
        "article_title",
        "page_num",
        "source_type",
        "content_kind",
        "chunk_strategy",
        "display_title",
    }
    for chunk in resp.json()["chunks"]:
        assert required.issubset(chunk.keys()), f"Missing fields in chunk: {chunk.keys()}"


@integration
async def test_numeric_query_surfaces_table_chunks(real_client):
    resp = await real_client.post(
        "/retrieve", json={"query": "AMZN revenue 2019 vs 2018", "k": 3, "company": "AMZN"}
    )
    assert resp.status_code == 200
    chunks = resp.json()["chunks"]
    assert chunks
    assert any(chunk["content_kind"] == "table" for chunk in chunks)


@integration
async def test_explanatory_query_prefers_narrative_chunks(real_client):
    resp = await real_client.post(
        "/retrieve", json={"query": "Summarize the risk factors discussion", "k": 3}
    )
    assert resp.status_code == 200
    chunks = resp.json()["chunks"]
    assert chunks
    assert chunks[0]["content_kind"] == "narrative"


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
