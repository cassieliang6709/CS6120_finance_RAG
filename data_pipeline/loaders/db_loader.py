"""
PostgreSQL / pgvector bulk loader
====================================
Loads all pipeline outputs into PostgreSQL using psycopg2 with the pgvector
extension registered.  Every public ``load_*`` function performs batched
UPSERT (INSERT … ON CONFLICT DO UPDATE) with a configurable batch size.

Connection management
---------------------
``DBLoader`` holds a single persistent connection that is re-established on
failure.  All write operations are committed per batch so interrupted runs
can be resumed without re-processing already-loaded data.

pgvector
--------
Embedding vectors (numpy float32 arrays) are serialised using the
``pgvector.psycopg2`` adapter so they are stored as native ``vector`` columns
understood by the IVFFlat index.
"""

from __future__ import annotations

import logging
from typing import Any, Sequence

import numpy as np
import psycopg2
import psycopg2.extras
from pgvector.psycopg2 import register_vector

from data_pipeline.config import DATABASE_URL, DB_BATCH_SIZE

logger = logging.getLogger(__name__)

Row = dict[str, Any]


# ---------------------------------------------------------------------------
# Connection helper
# ---------------------------------------------------------------------------

def _connect(dsn: str) -> psycopg2.extensions.connection:
    """Open a psycopg2 connection and register the pgvector type adapter."""
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    register_vector(conn)
    return conn


# ---------------------------------------------------------------------------
# DBLoader
# ---------------------------------------------------------------------------

class DBLoader:
    """
    Bulk-load financial pipeline data into PostgreSQL.

    Parameters
    ----------
    dsn:
        PostgreSQL connection string (defaults to ``DATABASE_URL`` from config).
    batch_size:
        Number of rows per executemany call.
    """

    def __init__(
        self,
        dsn: str = DATABASE_URL,
        batch_size: int = DB_BATCH_SIZE,
    ) -> None:
        self.dsn = dsn
        self.batch_size = batch_size
        self._conn: psycopg2.extensions.connection | None = None

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    @property
    def conn(self) -> psycopg2.extensions.connection:
        """Return the active connection, re-opening if necessary."""
        if self._conn is None or self._conn.closed:
            logger.info("Opening database connection")
            self._conn = _connect(self.dsn)
        return self._conn

    def close(self) -> None:
        """Close the database connection."""
        if self._conn and not self._conn.closed:
            self._conn.close()
            logger.info("Database connection closed")

    def __enter__(self) -> "DBLoader":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Schema initialisation
    # ------------------------------------------------------------------

    def init_schema(self, schema_path: str = "./data_pipeline/schema.sql") -> None:
        """
        Execute the SQL schema file to create tables, indexes, and
        triggers if they don't already exist.
        """
        try:
            with open(schema_path) as f:
                sql = f.read()
            with self.conn.cursor() as cur:
                cur.execute(sql)
            self.conn.commit()
            logger.info("Schema initialised from %s", schema_path)
        except Exception as exc:
            self.conn.rollback()
            logger.error("Schema initialisation failed: %s", exc)
            raise

    # ------------------------------------------------------------------
    # Internal batch execute
    # ------------------------------------------------------------------

    def _execute_batch(
        self,
        sql: str,
        rows: Sequence[Row],
        key_fn: Any = None,
    ) -> int:
        """
        Execute *sql* for each row in *rows* in batches of ``self.batch_size``.
        Returns the total number of rows processed.
        """
        if not rows:
            return 0

        total = 0
        cur = self.conn.cursor()
        try:
            for batch_start in range(0, len(rows), self.batch_size):
                batch = rows[batch_start : batch_start + self.batch_size]
                psycopg2.extras.execute_batch(cur, sql, batch, page_size=self.batch_size)
                self.conn.commit()
                total += len(batch)
                logger.debug("Loaded batch %d-%d / %d", batch_start, batch_start + len(batch), len(rows))
        except Exception as exc:
            self.conn.rollback()
            logger.error("Batch load failed: %s", exc)
            raise
        finally:
            cur.close()

        return total

    # ------------------------------------------------------------------
    # 1. companies
    # ------------------------------------------------------------------

    def load_company(self, rows: Sequence[Row]) -> int:
        """
        Upsert rows into ``companies``.

        Expected keys: ticker, name, sector, industry, market_cap, description.
        """
        sql = """
            INSERT INTO companies (ticker, name, sector, industry, market_cap, description, updated_at)
            VALUES (%(ticker)s, %(name)s, %(sector)s, %(industry)s, %(market_cap)s, %(description)s, NOW())
            ON CONFLICT (ticker) DO UPDATE SET
                name        = EXCLUDED.name,
                sector      = EXCLUDED.sector,
                industry    = EXCLUDED.industry,
                market_cap  = EXCLUDED.market_cap,
                description = EXCLUDED.description,
                updated_at  = NOW()
        """
        n = self._execute_batch(sql, rows)
        logger.info("Loaded %d companies", n)
        return n

    # ------------------------------------------------------------------
    # 2. filings
    # ------------------------------------------------------------------

    def load_filing(self, rows: Sequence[Row]) -> dict[tuple, int]:
        """
        Upsert rows into ``filings`` and return a mapping of
        (ticker, filing_type, fiscal_year, period) -> filing_id.

        Expected keys: ticker, filing_type, fiscal_year, period,
                       filed_date, source_url, local_path.
        """
        sql = """
            INSERT INTO filings (ticker, filing_type, fiscal_year, period, filed_date, source_url, local_path)
            VALUES (%(ticker)s, %(filing_type)s, %(fiscal_year)s, %(period)s,
                    %(filed_date)s, %(source_url)s, %(local_path)s)
            ON CONFLICT (ticker, filing_type, fiscal_year, period) DO UPDATE SET
                filed_date  = EXCLUDED.filed_date,
                source_url  = EXCLUDED.source_url,
                local_path  = EXCLUDED.local_path
        """
        self._execute_batch(sql, rows)

        # Fetch back the IDs
        id_map: dict[tuple, int] = {}
        if not rows:
            return id_map

        with self.conn.cursor() as cur:
            for row in rows:
                cur.execute(
                    """
                    SELECT id FROM filings
                    WHERE ticker = %s AND filing_type = %s
                      AND fiscal_year = %s AND period = %s
                    """,
                    (row["ticker"], row["filing_type"], row["fiscal_year"], row["period"]),
                )
                result = cur.fetchone()
                if result:
                    key = (row["ticker"], row["filing_type"], row["fiscal_year"], row["period"])
                    id_map[key] = result[0]

        logger.info("Loaded %d filings", len(rows))
        return id_map

    # ------------------------------------------------------------------
    # 3. chunks
    # ------------------------------------------------------------------

    def load_chunks(self, rows: Sequence[Row]) -> int:
        """
        Upsert filing text chunks into ``chunks``.

        Expected keys: filing_id, ticker, sector, filing_type, fiscal_year,
                       period, section_name, content, char_count, token_count,
                       embedding (numpy array or list), source_url.

        The ``content_tsv`` column is populated automatically by a DB trigger.
        """
        sql = """
            INSERT INTO chunks (
                filing_id, ticker, sector, filing_type, fiscal_year, period,
                section_name, content, char_count, token_count, embedding, source_url
            )
            VALUES (
                %(filing_id)s, %(ticker)s, %(sector)s, %(filing_type)s,
                %(fiscal_year)s, %(period)s, %(section_name)s, %(content)s,
                %(char_count)s, %(token_count)s, %(embedding)s, %(source_url)s
            )
            ON CONFLICT DO NOTHING
        """
        # Ensure embeddings are numpy arrays
        normalised_rows = []
        for row in rows:
            r = dict(row)
            if r.get("embedding") is not None:
                if not isinstance(r["embedding"], np.ndarray):
                    r["embedding"] = np.array(r["embedding"], dtype=np.float32)
                else:
                    r["embedding"] = r["embedding"].astype(np.float32)
            normalised_rows.append(r)

        n = self._execute_batch(sql, normalised_rows)
        logger.info("Loaded %d chunks", n)
        return n

    # ------------------------------------------------------------------
    # 4. market_data
    # ------------------------------------------------------------------

    def load_market_data(self, rows: Sequence[Row]) -> int:
        """
        Upsert daily market data into ``market_data``.

        Expected keys: ticker, date, open, high, low, close, adj_close,
                       volume, market_cap, pe_ratio, pb_ratio, ps_ratio,
                       dividend_yield, beta.
        """
        sql = """
            INSERT INTO market_data (
                ticker, date, open, high, low, close, adj_close, volume,
                market_cap, pe_ratio, pb_ratio, ps_ratio, dividend_yield, beta
            )
            VALUES (
                %(ticker)s, %(date)s, %(open)s, %(high)s, %(low)s, %(close)s,
                %(adj_close)s, %(volume)s, %(market_cap)s, %(pe_ratio)s,
                %(pb_ratio)s, %(ps_ratio)s, %(dividend_yield)s, %(beta)s
            )
            ON CONFLICT (ticker, date) DO UPDATE SET
                open           = EXCLUDED.open,
                high           = EXCLUDED.high,
                low            = EXCLUDED.low,
                close          = EXCLUDED.close,
                adj_close      = EXCLUDED.adj_close,
                volume         = EXCLUDED.volume,
                market_cap     = EXCLUDED.market_cap,
                pe_ratio       = EXCLUDED.pe_ratio,
                pb_ratio       = EXCLUDED.pb_ratio,
                ps_ratio       = EXCLUDED.ps_ratio,
                dividend_yield = EXCLUDED.dividend_yield,
                beta           = EXCLUDED.beta
        """
        n = self._execute_batch(sql, rows)
        logger.info("Loaded %d market_data rows", n)
        return n

    # ------------------------------------------------------------------
    # 5. financials
    # ------------------------------------------------------------------

    def load_financials(self, rows: Sequence[Row]) -> int:
        """
        Upsert quarterly financials into ``financials``.

        Expected keys: ticker, fiscal_year, period, period_end_date,
                       revenue, gross_profit, operating_income, net_income,
                       eps_basic, eps_diluted, ebitda, total_assets,
                       total_liabilities, total_debt, shareholders_equity,
                       cash_and_equivalents, operating_cash_flow, capex,
                       free_cash_flow.
        """
        sql = """
            INSERT INTO financials (
                ticker, fiscal_year, period, period_end_date,
                revenue, gross_profit, operating_income, net_income,
                eps_basic, eps_diluted, ebitda,
                total_assets, total_liabilities, total_debt,
                shareholders_equity, cash_and_equivalents,
                operating_cash_flow, capex, free_cash_flow
            )
            VALUES (
                %(ticker)s, %(fiscal_year)s, %(period)s, %(period_end_date)s,
                %(revenue)s, %(gross_profit)s, %(operating_income)s, %(net_income)s,
                %(eps_basic)s, %(eps_diluted)s, %(ebitda)s,
                %(total_assets)s, %(total_liabilities)s, %(total_debt)s,
                %(shareholders_equity)s, %(cash_and_equivalents)s,
                %(operating_cash_flow)s, %(capex)s, %(free_cash_flow)s
            )
            ON CONFLICT (ticker, fiscal_year, period) DO UPDATE SET
                period_end_date     = EXCLUDED.period_end_date,
                revenue             = EXCLUDED.revenue,
                gross_profit        = EXCLUDED.gross_profit,
                operating_income    = EXCLUDED.operating_income,
                net_income          = EXCLUDED.net_income,
                eps_basic           = EXCLUDED.eps_basic,
                eps_diluted         = EXCLUDED.eps_diluted,
                ebitda              = EXCLUDED.ebitda,
                total_assets        = EXCLUDED.total_assets,
                total_liabilities   = EXCLUDED.total_liabilities,
                total_debt          = EXCLUDED.total_debt,
                shareholders_equity = EXCLUDED.shareholders_equity,
                cash_and_equivalents = EXCLUDED.cash_and_equivalents,
                operating_cash_flow = EXCLUDED.operating_cash_flow,
                capex               = EXCLUDED.capex,
                free_cash_flow      = EXCLUDED.free_cash_flow
        """
        n = self._execute_batch(sql, rows)
        logger.info("Loaded %d financials rows", n)
        return n

    # ------------------------------------------------------------------
    # 6. macro_indicators
    # ------------------------------------------------------------------

    def load_macro(self, rows: Sequence[Row]) -> int:
        """
        Upsert FRED macro data into ``macro_indicators``.

        Expected keys: date, indicator_id, series_name, value.
        """
        sql = """
            INSERT INTO macro_indicators (date, indicator_id, series_name, value)
            VALUES (%(date)s, %(indicator_id)s, %(series_name)s, %(value)s)
            ON CONFLICT (date, indicator_id) DO UPDATE SET
                value       = EXCLUDED.value,
                series_name = EXCLUDED.series_name
        """
        n = self._execute_batch(sql, rows)
        logger.info("Loaded %d macro_indicators rows", n)
        return n

    # ------------------------------------------------------------------
    # 7. news_articles
    # ------------------------------------------------------------------

    def load_news(self, rows: Sequence[Row]) -> int:
        """
        Upsert news articles into ``news_articles``.

        Expected keys: ticker, title, content, summary, author,
                       published_date, source_url, source,
                       embedding (numpy array or None).

        The ``content_tsv`` column is populated automatically by a DB trigger.
        """
        sql = """
            INSERT INTO news_articles (
                ticker, title, content, summary, author,
                published_date, source_url, source, embedding
            )
            VALUES (
                %(ticker)s, %(title)s, %(content)s, %(summary)s, %(author)s,
                %(published_date)s, %(source_url)s, %(source)s, %(embedding)s
            )
            ON CONFLICT (source_url) DO UPDATE SET
                title          = EXCLUDED.title,
                content        = EXCLUDED.content,
                summary        = EXCLUDED.summary,
                embedding      = EXCLUDED.embedding
        """
        normalised_rows = []
        for row in rows:
            r = dict(row)
            if r.get("embedding") is not None:
                if not isinstance(r["embedding"], np.ndarray):
                    r["embedding"] = np.array(r["embedding"], dtype=np.float32)
                else:
                    r["embedding"] = r["embedding"].astype(np.float32)
            normalised_rows.append(r)

        n = self._execute_batch(sql, normalised_rows)
        logger.info("Loaded %d news_articles rows", n)
        return n

    # ------------------------------------------------------------------
    # 8. earnings_transcripts
    # ------------------------------------------------------------------

    def load_transcript(self, rows: Sequence[Row]) -> dict[tuple, int]:
        """
        Upsert earnings transcripts and return a map of
        (ticker, fiscal_year, quarter) -> transcript_id.

        Expected keys: ticker, fiscal_year, quarter, content,
                       published_date, source_url.
        """
        sql = """
            INSERT INTO earnings_transcripts (
                ticker, fiscal_year, quarter, content, published_date, source_url
            )
            VALUES (
                %(ticker)s, %(fiscal_year)s, %(quarter)s, %(content)s,
                %(published_date)s, %(source_url)s
            )
            ON CONFLICT (ticker, fiscal_year, quarter) DO UPDATE SET
                content        = EXCLUDED.content,
                published_date = EXCLUDED.published_date,
                source_url     = EXCLUDED.source_url
        """
        self._execute_batch(sql, rows)

        id_map: dict[tuple, int] = {}
        if not rows:
            return id_map

        with self.conn.cursor() as cur:
            for row in rows:
                cur.execute(
                    """
                    SELECT id FROM earnings_transcripts
                    WHERE ticker = %s AND fiscal_year = %s AND quarter = %s
                    """,
                    (row["ticker"], row["fiscal_year"], row["quarter"]),
                )
                result = cur.fetchone()
                if result:
                    key = (row["ticker"], row["fiscal_year"], row["quarter"])
                    id_map[key] = result[0]

        logger.info("Loaded %d earnings_transcripts", len(rows))
        return id_map

    # ------------------------------------------------------------------
    # 9. transcript_chunks
    # ------------------------------------------------------------------

    def load_transcript_chunks(self, rows: Sequence[Row]) -> int:
        """
        Upsert transcript chunks into ``transcript_chunks``.

        Expected keys: transcript_id, ticker, fiscal_year, quarter,
                       section, chunk_index, content, token_count,
                       embedding (numpy array or None).
        """
        sql = """
            INSERT INTO transcript_chunks (
                transcript_id, ticker, fiscal_year, quarter,
                section, chunk_index, content, token_count, embedding
            )
            VALUES (
                %(transcript_id)s, %(ticker)s, %(fiscal_year)s, %(quarter)s,
                %(section)s, %(chunk_index)s, %(content)s, %(token_count)s,
                %(embedding)s
            )
            ON CONFLICT DO NOTHING
        """
        normalised_rows = []
        for row in rows:
            r = dict(row)
            if r.get("embedding") is not None:
                if not isinstance(r["embedding"], np.ndarray):
                    r["embedding"] = np.array(r["embedding"], dtype=np.float32)
                else:
                    r["embedding"] = r["embedding"].astype(np.float32)
            normalised_rows.append(r)

        n = self._execute_batch(sql, normalised_rows)
        logger.info("Loaded %d transcript_chunks", n)
        return n

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------

    def get_stats(self) -> dict[str, int]:
        """
        Return row counts for all pipeline tables.
        """
        tables = [
            "companies",
            "filings",
            "chunks",
            "market_data",
            "financials",
            "macro_indicators",
            "news_articles",
            "earnings_transcripts",
            "transcript_chunks",
        ]
        stats: dict[str, int] = {}
        with self.conn.cursor() as cur:
            for table in tables:
                try:
                    cur.execute(f"SELECT COUNT(*) FROM {table}")
                    row = cur.fetchone()
                    stats[table] = row[0] if row else 0
                except Exception as exc:
                    logger.warning("Could not count %s: %s", table, exc)
                    stats[table] = -1
        return stats

    def get_db_size(self) -> str:
        """Return the database size as a human-readable string."""
        with self.conn.cursor() as cur:
            cur.execute("SELECT pg_size_pretty(pg_database_size(current_database()))")
            row = cur.fetchone()
            return row[0] if row else "unknown"


# ---------------------------------------------------------------------------
# Module-level convenience functions
# ---------------------------------------------------------------------------

def get_loader(dsn: str = DATABASE_URL) -> DBLoader:
    """Return a new :class:`DBLoader` connected to *dsn*."""
    return DBLoader(dsn=dsn)
