from __future__ import annotations

import asyncio
import re
from typing import Optional

import asyncpg
from sentence_transformers import SentenceTransformer

from config import COMPANY_BOOST, EMBEDDING_MODEL, FILING_TYPE_BOOST, FISCAL_YEAR_BOOST
from models import ChunkResult

_model: SentenceTransformer | None = None
_known_tickers: set[str] = set()
_company_name_to_ticker: dict[str, str] = {}

_TICKER_RE = re.compile(r"\b([A-Z]{2,5})\b")
_YEAR_RE = re.compile(r"(?:FY|fiscal\s+year\s*)?(20\d{2})", re.IGNORECASE)
_FILING_TYPE_PATTERNS: dict[str, re.Pattern] = {
    "10-K": re.compile(r"\b10-k\b|\bannual report\b|\bannual filing\b", re.IGNORECASE),
    "10-Q": re.compile(r"\b10-q\b|\bquarterly report\b|\bquarterly filing\b", re.IGNORECASE),
}


def get_model() -> SentenceTransformer:
    global _model
    if _model is None:
        _model = SentenceTransformer(EMBEDDING_MODEL)
    return _model


def embed_query(query: str) -> list[float]:
    return get_model().encode(query, normalize_embeddings=True).tolist()


async def load_known_tickers(pool: asyncpg.Pool) -> None:
    """Populate ticker and company-name caches from the database."""
    global _known_tickers, _company_name_to_ticker
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT DISTINCT ticker, company_name FROM v_chunk_search WHERE ticker IS NOT NULL"
        )
    _known_tickers = {r["ticker"].upper() for r in rows}
    _company_name_to_ticker = {
        r["company_name"].lower(): r["ticker"].upper()
        for r in rows
        if r["company_name"]
    }


def detect_company_in_query(query: str) -> Optional[str]:
    """Return a ticker matched from the query — by ticker symbol first, then full company name.

    Scans the original query (not uppercased) so only tokens already written in
    ALL-CAPS match — avoids false positives like 'are' → 'ARE' (a real ticker).
    """
    for match in _TICKER_RE.finditer(query):
        if match.group(1) in _known_tickers:
            return match.group(1)
    query_lower = query.lower()
    for name, ticker in _company_name_to_ticker.items():
        if re.search(r'\b' + re.escape(name) + r'\b', query_lower):
            return ticker
    return None


def detect_filing_type_in_query(query: str) -> Optional[str]:
    """Return '10-K' or '10-Q' if the query mentions a specific filing type."""
    for filing_type, pattern in _FILING_TYPE_PATTERNS.items():
        if pattern.search(query):
            return filing_type
    return None


def detect_year_in_query(query: str) -> Optional[str]:
    """Return the first 20xx year string found in the query, or None."""
    match = _YEAR_RE.search(query)
    return match.group(1) if match else None


def build_boost_expression(
    boost_ticker: Optional[str],
    boost_filing_type: Optional[str],
    boost_year: Optional[str],
    start_idx: int,
) -> tuple[str, list, int]:
    """Return a SQL multiplicative boost expression, its positional param values, and next index.

    Boost values are inlined as float literals (server-controlled config); only the
    comparison values (ticker, filing_type, year) are parameterised for safety.
    """
    parts: list[str] = []
    values: list = []
    idx = start_idx
    if boost_ticker:
        parts.append(f"CASE WHEN ticker = ${idx} THEN {COMPANY_BOOST}::float ELSE 1.0 END")
        values.append(boost_ticker)
        idx += 1
    if boost_filing_type:
        parts.append(f"CASE WHEN filing_type = ${idx} THEN {FILING_TYPE_BOOST}::float ELSE 1.0 END")
        values.append(boost_filing_type)
        idx += 1
    if boost_year:
        parts.append(f"CASE WHEN fiscal_year::text = ${idx} THEN {FISCAL_YEAR_BOOST}::float ELSE 1.0 END")
        values.append(boost_year)
        idx += 1
    expr = " * ".join(parts) if parts else "1.0"
    return expr, values, idx


def minmax_normalize(scores: list[float]) -> list[float]:
    if not scores:
        return []
    mn = min(scores)
    mx = max(scores)
    if mx == mn:
        return [1.0] * len(scores)
    return [(s - mn) / (mx - mn) for s in scores]


def fuse_scores(score_v: float, score_b: float, alpha: float) -> float:
    return alpha * score_v + (1.0 - alpha) * score_b


def build_filter_clause(
    sector: Optional[str],
    company: Optional[str],
    filing_type: Optional[str],
) -> tuple[str, dict]:
    """Return a SQL WHERE fragment using :name placeholders, and a params dict."""
    conditions: list[str] = []
    params: dict[str, str] = {}
    if sector is not None:
        conditions.append("sector = :sector")
        params["sector"] = sector
    if company is not None:
        conditions.append("ticker = :company")
        params["company"] = company
    if filing_type is not None:
        conditions.append("filing_type = :filing_type")
        params["filing_type"] = filing_type
    return " AND ".join(conditions), params


def _apply_filter(
    base_sql: str,
    where: str,
    params: dict,
    next_idx: int,
) -> tuple[str, list, int]:
    """Replace :name placeholders with $N positional params for asyncpg."""
    positional_where = where
    values: list = []
    idx = next_idx
    for key, val in params.items():
        positional_where = positional_where.replace(f":{key}", f"${idx}")
        values.append(val)
        idx += 1
    replacement = f"AND {positional_where}" if positional_where else ""
    return base_sql.replace("__FILTER__", replacement), values, idx


async def _vector_search(
    pool: asyncpg.Pool,
    query_vec: list[float],
    limit: int,
    filter_where: str,
    filter_params: dict,
    boost_ticker: Optional[str] = None,
    boost_filing_type: Optional[str] = None,
    boost_year: Optional[str] = None,
) -> list[dict]:
    boost_expr, boost_vals, _ = build_boost_expression(
        boost_ticker, boost_filing_type, boost_year, start_idx=3 + len(filter_params)
    )
    sql = f"""
        SELECT
            id::text                                                      AS chunk_id,
            ticker                                                        AS company,
            sector,
            filing_type,
            filed_date,
            source_url,
            content                                                       AS text,
            company_name,
            fiscal_year,
            (1 - (embedding <=> $1::vector)) * {boost_expr}              AS score_v
        FROM v_chunk_search
        WHERE embedding IS NOT NULL
        __FILTER__
        ORDER BY (1 - (embedding <=> $1::vector)) * {boost_expr} DESC
        LIMIT $2
    """
    sql, filter_vals, _ = _apply_filter(sql, filter_where, filter_params, next_idx=3)
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, query_vec, limit, *filter_vals, *boost_vals)
    return [dict(r) for r in rows]


async def _bm25_search(
    pool: asyncpg.Pool,
    query: str,
    limit: int,
    filter_where: str,
    filter_params: dict,
    boost_ticker: Optional[str] = None,
    boost_filing_type: Optional[str] = None,
    boost_year: Optional[str] = None,
) -> list[dict]:
    boost_expr, boost_vals, _ = build_boost_expression(
        boost_ticker, boost_filing_type, boost_year, start_idx=3 + len(filter_params)
    )
    sql = f"""
        SELECT
            id::text                                                              AS chunk_id,
            ticker                                                                AS company,
            sector,
            filing_type,
            filed_date,
            source_url,
            content                                                               AS text,
            company_name,
            fiscal_year,
            ts_rank(content_tsv, plainto_tsquery('english', $1)) * {boost_expr}  AS score_b
        FROM v_chunk_search
        WHERE content_tsv @@ plainto_tsquery('english', $1)
        __FILTER__
        ORDER BY score_b DESC
        LIMIT $2
    """
    sql, filter_vals, _ = _apply_filter(sql, filter_where, filter_params, next_idx=3)
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, query, limit, *filter_vals, *boost_vals)
    return [dict(r) for r in rows]


async def retrieve(
    pool: asyncpg.Pool,
    query: str,
    k: int,
    alpha: float,
    sector: Optional[str],
    company: Optional[str],
    filing_type: Optional[str],
) -> list[ChunkResult]:
    overfetch = k * 3
    query_vec = embed_query(query)
    filter_where, filter_params = build_filter_clause(sector, company, filing_type)

    # Detect signals before retrieval so boosts are embedded in SQL ORDER BY,
    # affecting which chunks the DB returns rather than just reordering them after.
    boost_ticker = None if company else detect_company_in_query(query)
    boost_filing_type = None if filing_type else detect_filing_type_in_query(query)
    boost_year = detect_year_in_query(query)

    vec_rows, bm25_rows = await asyncio.gather(
        _vector_search(pool, query_vec, overfetch, filter_where, filter_params,
                       boost_ticker, boost_filing_type, boost_year),
        _bm25_search(pool, query, overfetch, filter_where, filter_params,
                     boost_ticker, boost_filing_type, boost_year),
    )

    vec_map: dict[str, dict] = {r["chunk_id"]: r for r in vec_rows}
    bm25_map: dict[str, dict] = {r["chunk_id"]: r for r in bm25_rows}
    all_ids = list({**vec_map, **bm25_map}.keys())

    raw_v = [vec_map[cid]["score_v"] if cid in vec_map else 0.0 for cid in all_ids]
    raw_b = [bm25_map[cid]["score_b"] if cid in bm25_map else 0.0 for cid in all_ids]

    norm_v = minmax_normalize(raw_v) if vec_rows else [0.0] * len(all_ids)
    norm_b = minmax_normalize(raw_b) if bm25_rows else [0.0] * len(all_ids)

    scored = sorted(
        [(fuse_scores(norm_v[i], norm_b[i], alpha), cid) for i, cid in enumerate(all_ids)],
        reverse=True,
    )

    results: list[ChunkResult] = []
    for score, cid in scored[:k]:
        row = vec_map.get(cid) or bm25_map[cid]
        fiscal_year = row.get("fiscal_year")
        company_name = row.get("company_name") or ""
        ft = row.get("filing_type") or ""
        article_title = (
            f"{company_name} {ft} {fiscal_year}" if fiscal_year else f"{company_name} {ft}"
        ).strip()
        results.append(
            ChunkResult(
                chunk_id=cid,
                text=row["text"],
                score=round(score, 6),
                company=row["company"],
                sector=row["sector"],
                filing_type=ft,
                filed_date=row.get("filed_date"),
                source_url=row.get("source_url"),
                article_title=article_title or None,
                page_num=None,
            )
        )
    return results
