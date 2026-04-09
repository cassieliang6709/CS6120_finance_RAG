from __future__ import annotations

import asyncio
from typing import Optional

import asyncpg
from sentence_transformers import SentenceTransformer

from config import EMBEDDING_MODEL
from models import ChunkResult

_model: SentenceTransformer | None = None


def get_model() -> SentenceTransformer:
    global _model
    if _model is None:
        _model = SentenceTransformer(EMBEDDING_MODEL)
    return _model


def embed_query(query: str) -> list[float]:
    return get_model().encode(query, normalize_embeddings=True).tolist()


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
) -> list[dict]:
    sql = """
        SELECT
            id::text                                              AS chunk_id,
            ticker                                                AS company,
            sector,
            filing_type,
            filed_date,
            source_url,
            content                                               AS text,
            company_name,
            fiscal_year,
            1 - (embedding <=> $1::vector)                        AS score_v
        FROM v_chunk_search
        WHERE embedding IS NOT NULL
        __FILTER__
        ORDER BY embedding <=> $1::vector
        LIMIT $2
    """
    sql, extra, _ = _apply_filter(sql, filter_where, filter_params, next_idx=3)
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, query_vec, limit, *extra)
    return [dict(r) for r in rows]


async def _bm25_search(
    pool: asyncpg.Pool,
    query: str,
    limit: int,
    filter_where: str,
    filter_params: dict,
) -> list[dict]:
    sql = """
        SELECT
            id::text                                              AS chunk_id,
            ticker                                                AS company,
            sector,
            filing_type,
            filed_date,
            source_url,
            content                                               AS text,
            company_name,
            fiscal_year,
            ts_rank(content_tsv, plainto_tsquery('english', $1))  AS score_b
        FROM v_chunk_search
        WHERE content_tsv @@ plainto_tsquery('english', $1)
        __FILTER__
        ORDER BY score_b DESC
        LIMIT $2
    """
    sql, extra, _ = _apply_filter(sql, filter_where, filter_params, next_idx=3)
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, query, limit, *extra)
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

    vec_rows, bm25_rows = await asyncio.gather(
        _vector_search(pool, query_vec, overfetch, filter_where, filter_params),
        _bm25_search(pool, query, overfetch, filter_where, filter_params),
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
