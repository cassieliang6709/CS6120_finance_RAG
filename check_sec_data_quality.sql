-- Quick manual validation bundle for SEC filings + chunks quality.
-- Usage:
--   psql -d financial_rag -f check_sec_data_quality.sql

SELECT
    'sec_metadata_completeness' AS check_name,
    COUNT(*) FILTER (
        WHERE filing_type IN ('10-K', '10-Q')
          AND (
                filed_date IS NULL OR
                period_of_report IS NULL OR
                nullif(accession_number, '') IS NULL OR
                nullif(cik, '') IS NULL OR
                nullif(source_url, '') IS NULL
              )
    ) AS bad_rows,
    CASE
        WHEN COUNT(*) FILTER (
            WHERE filing_type IN ('10-K', '10-Q')
              AND (
                    filed_date IS NULL OR
                    period_of_report IS NULL OR
                    nullif(accession_number, '') IS NULL OR
                    nullif(cik, '') IS NULL OR
                    nullif(source_url, '') IS NULL
                  )
        ) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM filings;

SELECT
    'ten_k_fiscal_year_matches_period_of_report' AS check_name,
    COUNT(*) AS bad_rows,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM filings
WHERE filing_type = '10-K'
  AND period_of_report IS NOT NULL
  AND EXTRACT(YEAR FROM period_of_report) <> fiscal_year;

SELECT
    'ten_q_period_labels_are_q1_q2_q3' AS check_name,
    COUNT(*) AS bad_rows,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM filings
WHERE filing_type = '10-Q'
  AND period NOT IN ('Q1', 'Q2', 'Q3');

SELECT
    'chunk_parent_metadata_consistency' AS check_name,
    COUNT(*) AS bad_rows,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM chunks c
JOIN filings f ON f.id = c.filing_id
WHERE c.ticker IS DISTINCT FROM f.ticker
   OR c.filing_type IS DISTINCT FROM f.filing_type
   OR c.fiscal_year IS DISTINCT FROM f.fiscal_year
   OR c.period IS DISTINCT FROM f.period
   OR c.filed_date IS DISTINCT FROM f.filed_date
   OR c.source_url IS DISTINCT FROM f.source_url;

SELECT
    'chunk_search_columns_populated' AS check_name,
    COUNT(*) AS bad_rows,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM chunks
WHERE content_tsv IS NULL
   OR embedding IS NULL
   OR numeric_token_count IS NULL
   OR number_density IS NULL
   OR data_signal_score IS NULL
   OR is_quantitative IS NULL;

SELECT
    'structured_chunk_metadata_populated' AS check_name,
    COUNT(*) AS bad_rows,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM v_retrieval_chunks
WHERE content_kind IS NULL
   OR nullif(chunk_strategy, '') IS NULL
   OR structure_meta IS NULL;

SELECT
    'sec_table_chunks_exist' AS check_name,
    COUNT(*) AS table_chunk_rows,
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM chunks
WHERE content_kind = 'table';

SELECT
    'financial_statements_table_chunks_exist' AS check_name,
    COUNT(*) AS table_chunk_rows,
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM chunks
WHERE content_kind = 'table'
  AND section_name = 'Financial Statements';

WITH source_counts AS (
    SELECT source_type, COUNT(*) AS row_count
    FROM v_retrieval_chunks
    GROUP BY source_type
)
SELECT
    'retrieval_view_source_coverage' AS check_name,
    jsonb_object_agg(source_type, row_count ORDER BY source_type) AS source_counts,
    CASE
        WHEN COUNT(*) FILTER (WHERE source_type = 'sec') > 0
         AND COUNT(*) FILTER (WHERE source_type = 'news') > 0
        THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM source_counts;

SELECT
    'exact_company_name_filterable_for_amd_2023_10k' AS check_name,
    COUNT(*) AS matching_rows,
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM v_chunk_search
WHERE lower(company_name) = lower('Advanced Micro Devices, Inc.')
  AND ticker = 'AMD'
  AND filing_type = '10-K'
  AND fiscal_year = 2023;

WITH section_scores AS (
    SELECT section_name, AVG(data_signal_score) AS avg_score
    FROM chunks
    WHERE section_name IN (
        'Financial Statements',
        'Selected Financial Data',
        'Business Description',
        'Risk Factors'
    )
    GROUP BY section_name
)
SELECT
    'quantitative_sections_outscore_narrative_sections' AS check_name,
    ROUND(MAX(CASE WHEN section_name = 'Financial Statements' THEN avg_score END)::numeric, 2) AS financial_statements_avg,
    ROUND(MAX(CASE WHEN section_name = 'Selected Financial Data' THEN avg_score END)::numeric, 2) AS selected_financial_data_avg,
    ROUND(MAX(CASE WHEN section_name = 'Business Description' THEN avg_score END)::numeric, 2) AS business_description_avg,
    ROUND(MAX(CASE WHEN section_name = 'Risk Factors' THEN avg_score END)::numeric, 2) AS risk_factors_avg,
    CASE
        WHEN MAX(CASE WHEN section_name = 'Financial Statements' THEN avg_score END)
             > MAX(CASE WHEN section_name = 'Business Description' THEN avg_score END)
         AND MAX(CASE WHEN section_name = 'Selected Financial Data' THEN avg_score END)
             > MAX(CASE WHEN section_name = 'Risk Factors' THEN avg_score END)
        THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM section_scores;
