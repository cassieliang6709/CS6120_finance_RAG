SELECT
    'companies' AS table_name,
    COUNT(*) AS row_count
FROM companies
UNION ALL
SELECT 'filings', COUNT(*) FROM filings
UNION ALL
SELECT 'chunks', COUNT(*) FROM chunks
UNION ALL
SELECT 'market_data', COUNT(*) FROM market_data
UNION ALL
SELECT 'financials', COUNT(*) FROM financials
UNION ALL
SELECT 'macro_indicators', COUNT(*) FROM macro_indicators
UNION ALL
SELECT 'news_articles', COUNT(*) FROM news_articles
UNION ALL
SELECT 'earnings_transcripts', COUNT(*) FROM earnings_transcripts
UNION ALL
SELECT 'transcript_chunks', COUNT(*) FROM transcript_chunks
ORDER BY table_name;

SELECT
    (SELECT COUNT(*) FROM companies) +
    (SELECT COUNT(*) FROM filings) +
    (SELECT COUNT(*) FROM chunks) +
    (SELECT COUNT(*) FROM market_data) +
    (SELECT COUNT(*) FROM financials) +
    (SELECT COUNT(*) FROM macro_indicators) +
    (SELECT COUNT(*) FROM news_articles) +
    (SELECT COUNT(*) FROM earnings_transcripts) +
    (SELECT COUNT(*) FROM transcript_chunks) AS total_rows;

SELECT
    CASE
        WHEN (
            (SELECT COUNT(*) FROM companies) +
            (SELECT COUNT(*) FROM filings) +
            (SELECT COUNT(*) FROM chunks) +
            (SELECT COUNT(*) FROM market_data) +
            (SELECT COUNT(*) FROM financials) +
            (SELECT COUNT(*) FROM macro_indicators) +
            (SELECT COUNT(*) FROM news_articles) +
            (SELECT COUNT(*) FROM earnings_transcripts) +
            (SELECT COUNT(*) FROM transcript_chunks)
        ) >= 10000 THEN 'PASS'
        ELSE 'FAIL'
    END AS meets_10k_requirement;

SELECT
    COUNT(*) AS chunk_count,
    CASE
        WHEN COUNT(*) >= 10000 THEN 'PASS'
        ELSE 'FAIL'
    END AS meets_10k_chunks_requirement
FROM chunks;

SELECT COUNT(*) AS ticker_like_company_names
FROM v_chunk_search
WHERE company_name = ticker;

SELECT COUNT(*) AS unresolved_company_sectors
FROM companies
WHERE lower(coalesce(sector, '')) IN ('', 'unknown');

SELECT COUNT(*) AS unresolved_chunk_sectors
FROM chunks
WHERE lower(coalesce(sector, '')) IN ('', 'unknown');

SELECT COUNT(DISTINCT c.ticker) AS chunk_tickers_missing_companies
FROM chunks c
LEFT JOIN companies co ON co.ticker = c.ticker
WHERE co.ticker IS NULL;
