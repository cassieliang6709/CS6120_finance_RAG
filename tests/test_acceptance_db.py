import os
import unittest
from pathlib import Path

import psycopg2

from data_pipeline.config import SEC_DOWNLOAD_DIR
from data_pipeline.downloaders.sec_downloader import SECDownloader


TEST_DATABASE_URL = os.getenv(
    "TEST_DATABASE_URL",
    os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/financial_rag"),
)

SEC_TICKER_ALIASES = {
    "0001038357": "PXD",
}


class DatabaseAcceptanceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.conn = psycopg2.connect(TEST_DATABASE_URL)
        cls.conn.autocommit = True

    @classmethod
    def tearDownClass(cls) -> None:
        cls.conn.close()

    def fetchone(self, sql: str, params: tuple = ()) -> tuple:
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
        assert row is not None
        return row

    def test_database_meets_10k_volume_requirements(self) -> None:
        total_rows = self.fetchone(
            """
            SELECT
                (SELECT COUNT(*) FROM companies) +
                (SELECT COUNT(*) FROM filings) +
                (SELECT COUNT(*) FROM chunks) +
                (SELECT COUNT(*) FROM market_data) +
                (SELECT COUNT(*) FROM financials) +
                (SELECT COUNT(*) FROM macro_indicators) +
                (SELECT COUNT(*) FROM news_articles) +
                (SELECT COUNT(*) FROM earnings_transcripts) +
                (SELECT COUNT(*) FROM transcript_chunks)
            """
        )[0]
        chunk_rows = self.fetchone("SELECT COUNT(*) FROM chunks")[0]

        self.assertGreaterEqual(total_rows, 10_000)
        self.assertGreaterEqual(chunk_rows, 10_000)

    def test_all_chunk_embeddings_are_present(self) -> None:
        total_chunks, embedded_chunks = self.fetchone(
            "SELECT COUNT(*), COUNT(embedding) FROM chunks"
        )
        self.assertEqual(embedded_chunks, total_chunks)

    def test_no_duplicate_chunk_groups_exist(self) -> None:
        duplicate_groups = self.fetchone(
            """
            WITH duplicate_groups AS (
                SELECT filing_id, chunk_index
                FROM chunks
                GROUP BY filing_id, chunk_index
                HAVING COUNT(*) > 1
            )
            SELECT COUNT(*) FROM duplicate_groups
            """
        )[0]
        self.assertEqual(duplicate_groups, 0)

    def test_company_metadata_is_backfilled(self) -> None:
        ticker_like_names = self.fetchone(
            "SELECT COUNT(*) FROM v_chunk_search WHERE company_name = ticker"
        )[0]
        unresolved_company_sectors = self.fetchone(
            "SELECT COUNT(*) FROM companies WHERE lower(coalesce(sector, '')) IN ('', 'unknown')"
        )[0]
        unresolved_chunk_sectors = self.fetchone(
            "SELECT COUNT(*) FROM chunks WHERE lower(coalesce(sector, '')) IN ('', 'unknown')"
        )[0]
        missing_company_links = self.fetchone(
            """
            SELECT COUNT(DISTINCT c.ticker)
            FROM chunks c
            LEFT JOIN companies co ON co.ticker = c.ticker
            WHERE co.ticker IS NULL
            """
        )[0]

        self.assertEqual(ticker_like_names, 0)
        self.assertEqual(unresolved_company_sectors, 0)
        self.assertEqual(unresolved_chunk_sectors, 0)
        self.assertEqual(missing_company_links, 0)

    def test_validated_sec_subset_is_intact(self) -> None:
        validated_tickers = ("AAPL", "JPM", "UNH", "XOM")

        filing_count, filing_dates, filing_urls = self.fetchone(
            """
            SELECT COUNT(*), COUNT(filed_date), COUNT(source_url)
            FROM filings
            WHERE ticker = ANY(%s)
              AND fiscal_year = 2023
              AND filing_type IN ('10-K', '10-Q')
            """,
            (list(validated_tickers),),
        )
        period_rows = self.fetchone(
            """
            SELECT COUNT(*)
            FROM filings
            WHERE ticker = ANY(%s)
              AND fiscal_year = 2023
              AND (
                    (filing_type = '10-K' AND period = 'annual') OR
                    (filing_type = '10-Q' AND period IN ('Q1', 'Q2', 'Q3'))
                  )
            """,
            (list(validated_tickers),),
        )[0]
        sec_chunk_count, sec_embedded_count = self.fetchone(
            """
            SELECT COUNT(*), COUNT(embedding)
            FROM chunks
            WHERE ticker = ANY(%s)
              AND fiscal_year = 2023
              AND filing_type IN ('10-K', '10-Q')
            """,
            (list(validated_tickers),),
        )

        self.assertEqual(filing_count, 16)
        self.assertEqual(filing_dates, 16)
        self.assertEqual(filing_urls, 16)
        self.assertEqual(period_rows, 16)
        self.assertEqual(sec_chunk_count, 2285)
        self.assertEqual(sec_embedded_count, 2285)

    def test_local_sec_disk_coverage_is_complete(self) -> None:
        local_root = Path(SEC_DOWNLOAD_DIR) / "sec-edgar-filings"
        local_dirs = sorted(path.name for path in local_root.iterdir() if path.is_dir())
        years = list(range(2018, 2026))
        local_metas = SECDownloader()._collect_metadata(local_dirs, ["10-K", "10-Q"], years)
        local_keys = {
            (SEC_TICKER_ALIASES.get(ticker, ticker), filing_type, fiscal_year, period)
            for ticker, filing_type, fiscal_year, period, *_ in local_metas
        }
        local_tickers = sorted({SEC_TICKER_ALIASES.get(ticker, ticker) for ticker in local_dirs})

        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT ticker, filing_type, fiscal_year, period
                FROM filings
                WHERE ticker = ANY(%s)
                """,
                (local_tickers,),
            )
            db_keys = set(cur.fetchall())

        self.assertGreater(len(local_keys), 0)
        self.assertEqual(local_keys - db_keys, set())
        self.assertEqual(db_keys - local_keys, set())


if __name__ == "__main__":
    unittest.main()
