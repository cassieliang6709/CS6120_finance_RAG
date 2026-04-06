-- =============================================================================
-- Financial Research RAG Pipeline – PostgreSQL Schema
-- Requires: pgvector extension  (CREATE EXTENSION IF NOT EXISTS vector)
--           pg_trgm  (for trigram GIN indexes on text search)
-- =============================================================================

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS btree_gin;

-- =============================================================================
-- 1. companies
-- =============================================================================
CREATE TABLE IF NOT EXISTS companies (
    ticker          TEXT        PRIMARY KEY,
    name            TEXT        NOT NULL,
    sector          TEXT        NOT NULL,
    industry        TEXT,
    market_cap      NUMERIC,
    description     TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_companies_sector
    ON companies (sector);

-- =============================================================================
-- 2. filings
-- =============================================================================
CREATE TABLE IF NOT EXISTS filings (
    id              BIGSERIAL   PRIMARY KEY,
    ticker          TEXT        NOT NULL REFERENCES companies (ticker) ON DELETE CASCADE,
    filing_type     TEXT        NOT NULL,   -- '10-K', '10-Q', '8-K'
    fiscal_year     SMALLINT,
    period          TEXT,                   -- e.g. 'Q1', 'Q2', 'annual'
    filed_date      DATE,
    source_url      TEXT,
    local_path      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (ticker, filing_type, fiscal_year, period)
);

CREATE INDEX IF NOT EXISTS idx_filings_ticker
    ON filings (ticker);
CREATE INDEX IF NOT EXISTS idx_filings_type_year
    ON filings (filing_type, fiscal_year);
CREATE INDEX IF NOT EXISTS idx_filings_filed_date
    ON filings (filed_date);

-- =============================================================================
-- 3. chunks  (SEC filing text chunks with vector + FTS)
-- =============================================================================
CREATE TABLE IF NOT EXISTS chunks (
    id              BIGSERIAL   PRIMARY KEY,
    filing_id       BIGINT      NOT NULL REFERENCES filings (id) ON DELETE CASCADE,
    ticker          TEXT        NOT NULL,
    sector          TEXT        NOT NULL,
    filing_type     TEXT        NOT NULL,
    fiscal_year     SMALLINT,
    period          TEXT,
    filed_date      DATE,
    section_name    TEXT,                   -- 'MD&A', 'Risk Factors', etc.
    chunk_index     INTEGER,
    content         TEXT        NOT NULL,
    char_count      INTEGER     NOT NULL,
    token_count     INTEGER     NOT NULL,
    embedding       vector(384),
    content_tsv     TSVECTOR,
    source_url      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE chunks
    ADD COLUMN IF NOT EXISTS filed_date DATE;

ALTER TABLE chunks
    ADD COLUMN IF NOT EXISTS chunk_index INTEGER;

UPDATE chunks c
SET filed_date = f.filed_date
FROM filings f
WHERE c.filing_id = f.id
  AND c.filed_date IS NULL;

WITH chunk_positions AS (
    SELECT
        id,
        ROW_NUMBER() OVER (
            PARTITION BY filing_id
            ORDER BY id
        ) - 1 AS inferred_chunk_index
    FROM chunks
    WHERE chunk_index IS NULL
)
UPDATE chunks c
SET chunk_index = cp.inferred_chunk_index
FROM chunk_positions cp
WHERE c.id = cp.id;

ALTER TABLE chunks
    ALTER COLUMN chunk_index SET NOT NULL;

-- Vector similarity index (IVFFlat – good for recall at scale)
-- lists = sqrt(row_count) is a reasonable heuristic; for the expected corpus
-- size, 256 is a better default than 100.
DO $$
DECLARE
    index_reloptions TEXT[];
BEGIN
    SELECT c.reloptions
    INTO index_reloptions
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname = 'idx_chunks_embedding_ivfflat'
      AND n.nspname = current_schema();

    IF index_reloptions IS NULL THEN
        EXECUTE '
            CREATE INDEX idx_chunks_embedding_ivfflat
            ON chunks USING ivfflat (embedding vector_cosine_ops)
            WITH (lists = 256)
        ';
    ELSIF NOT ('lists=256' = ANY(index_reloptions)) THEN
        EXECUTE 'DROP INDEX idx_chunks_embedding_ivfflat';
        EXECUTE '
            CREATE INDEX idx_chunks_embedding_ivfflat
            ON chunks USING ivfflat (embedding vector_cosine_ops)
            WITH (lists = 256)
        ';
    END IF;
END
$$;

-- Full-text search
CREATE INDEX IF NOT EXISTS idx_chunks_content_tsv
    ON chunks USING GIN (content_tsv);

-- Btree lookups
CREATE INDEX IF NOT EXISTS idx_chunks_ticker
    ON chunks (ticker);
CREATE INDEX IF NOT EXISTS idx_chunks_filing_id
    ON chunks (filing_id);
CREATE INDEX IF NOT EXISTS idx_chunks_filing_chunk_index
    ON chunks (filing_id, chunk_index);
CREATE INDEX IF NOT EXISTS idx_chunks_filed_date
    ON chunks (filed_date);
CREATE INDEX IF NOT EXISTS idx_chunks_sector_type
    ON chunks (sector, filing_type);
CREATE INDEX IF NOT EXISTS idx_chunks_fiscal_year
    ON chunks (fiscal_year);
CREATE INDEX IF NOT EXISTS idx_chunks_section
    ON chunks (section_name);

-- Auto-populate tsvector on insert/update
CREATE OR REPLACE FUNCTION chunks_tsv_trigger() RETURNS TRIGGER AS $$
BEGIN
    NEW.content_tsv := to_tsvector(
        'english',
        COALESCE(NEW.section_name, '') || ' ' || COALESCE(NEW.content, '')
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trig_chunks_tsv ON chunks;
CREATE TRIGGER trig_chunks_tsv
    BEFORE INSERT OR UPDATE OF section_name, content ON chunks
    FOR EACH ROW EXECUTE FUNCTION chunks_tsv_trigger();

-- =============================================================================
-- 4. market_data  (daily OHLCV + fundamentals from yfinance)
-- =============================================================================
CREATE TABLE IF NOT EXISTS market_data (
    ticker          TEXT        NOT NULL REFERENCES companies (ticker) ON DELETE CASCADE,
    date            DATE        NOT NULL,
    open            NUMERIC,
    high            NUMERIC,
    low             NUMERIC,
    close           NUMERIC,
    adj_close       NUMERIC,
    volume          BIGINT,
    market_cap      NUMERIC,
    pe_ratio        NUMERIC,
    pb_ratio        NUMERIC,
    ps_ratio        NUMERIC,
    dividend_yield  NUMERIC,
    beta            NUMERIC,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (ticker, date)
);

CREATE INDEX IF NOT EXISTS idx_market_data_date
    ON market_data (date);
CREATE INDEX IF NOT EXISTS idx_market_data_ticker_date
    ON market_data (ticker, date DESC);

-- =============================================================================
-- 5. financials  (quarterly income statement / balance sheet / cash flow)
-- =============================================================================
CREATE TABLE IF NOT EXISTS financials (
    id                      BIGSERIAL   PRIMARY KEY,
    ticker                  TEXT        NOT NULL REFERENCES companies (ticker) ON DELETE CASCADE,
    fiscal_year             SMALLINT    NOT NULL,
    period                  TEXT        NOT NULL,   -- 'Q1','Q2','Q3','Q4','annual'
    period_end_date         DATE,
    -- Income statement
    revenue                 NUMERIC,
    gross_profit            NUMERIC,
    operating_income        NUMERIC,
    net_income              NUMERIC,
    eps_basic               NUMERIC,
    eps_diluted             NUMERIC,
    ebitda                  NUMERIC,
    -- Balance sheet
    total_assets            NUMERIC,
    total_liabilities       NUMERIC,
    total_debt              NUMERIC,
    shareholders_equity     NUMERIC,
    cash_and_equivalents    NUMERIC,
    -- Cash flow
    operating_cash_flow     NUMERIC,
    capex                   NUMERIC,
    free_cash_flow          NUMERIC,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (ticker, fiscal_year, period)
);

CREATE INDEX IF NOT EXISTS idx_financials_ticker
    ON financials (ticker);
CREATE INDEX IF NOT EXISTS idx_financials_year_period
    ON financials (fiscal_year, period);

-- =============================================================================
-- 6. macro_indicators  (FRED series)
-- =============================================================================
CREATE TABLE IF NOT EXISTS macro_indicators (
    date            DATE        NOT NULL,
    indicator_id    TEXT        NOT NULL,   -- FRED series ID e.g. 'DFF'
    series_name     TEXT        NOT NULL,
    value           NUMERIC,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (date, indicator_id)
);

CREATE INDEX IF NOT EXISTS idx_macro_indicator_id
    ON macro_indicators (indicator_id, date);
CREATE INDEX IF NOT EXISTS idx_macro_date
    ON macro_indicators (date);

-- =============================================================================
-- 7. news_articles
-- =============================================================================
CREATE TABLE IF NOT EXISTS news_articles (
    id              BIGSERIAL   PRIMARY KEY,
    ticker          TEXT        REFERENCES companies (ticker) ON DELETE SET NULL,
    title           TEXT        NOT NULL,
    content         TEXT,
    summary         TEXT,
    author          TEXT,
    published_date  TIMESTAMPTZ,
    source_url      TEXT        UNIQUE,
    source          TEXT,                   -- 'yahoo_rss', 'reuters_rss', etc.
    embedding       vector(384),
    content_tsv     TSVECTOR,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_news_embedding_ivfflat
    ON news_articles USING ivfflat (embedding vector_cosine_ops)
    WITH (lists = 50);

CREATE INDEX IF NOT EXISTS idx_news_content_tsv
    ON news_articles USING GIN (content_tsv);

CREATE INDEX IF NOT EXISTS idx_news_ticker
    ON news_articles (ticker);
CREATE INDEX IF NOT EXISTS idx_news_published_date
    ON news_articles (published_date DESC);

CREATE OR REPLACE FUNCTION news_tsv_trigger() RETURNS TRIGGER AS $$
BEGIN
    NEW.content_tsv := to_tsvector(
        'english',
        COALESCE(NEW.title, '') || ' ' || COALESCE(NEW.content, '') || ' ' || COALESCE(NEW.summary, '')
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trig_news_tsv ON news_articles;
CREATE TRIGGER trig_news_tsv
    BEFORE INSERT OR UPDATE OF title, content, summary ON news_articles
    FOR EACH ROW EXECUTE FUNCTION news_tsv_trigger();

-- =============================================================================
-- 8. earnings_transcripts
-- =============================================================================
CREATE TABLE IF NOT EXISTS earnings_transcripts (
    id              BIGSERIAL   PRIMARY KEY,
    ticker          TEXT        NOT NULL REFERENCES companies (ticker) ON DELETE CASCADE,
    fiscal_year     SMALLINT    NOT NULL,
    quarter         SMALLINT    NOT NULL,   -- 1..4
    content         TEXT        NOT NULL,
    published_date  DATE,
    source_url      TEXT        UNIQUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (ticker, fiscal_year, quarter)
);

CREATE INDEX IF NOT EXISTS idx_transcripts_ticker
    ON earnings_transcripts (ticker);
CREATE INDEX IF NOT EXISTS idx_transcripts_year_quarter
    ON earnings_transcripts (fiscal_year, quarter);

-- =============================================================================
-- 9. transcript_chunks
-- =============================================================================
CREATE TABLE IF NOT EXISTS transcript_chunks (
    id              BIGSERIAL   PRIMARY KEY,
    transcript_id   BIGINT      NOT NULL REFERENCES earnings_transcripts (id) ON DELETE CASCADE,
    ticker          TEXT        NOT NULL,
    fiscal_year     SMALLINT    NOT NULL,
    quarter         SMALLINT    NOT NULL,
    section         TEXT,                   -- 'prepared_remarks', 'qa', 'closing'
    chunk_index     INTEGER     NOT NULL,
    content         TEXT        NOT NULL,
    token_count     INTEGER,
    embedding       vector(384),
    content_tsv     TSVECTOR,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tc_embedding_ivfflat
    ON transcript_chunks USING ivfflat (embedding vector_cosine_ops)
    WITH (lists = 50);

CREATE INDEX IF NOT EXISTS idx_tc_content_tsv
    ON transcript_chunks USING GIN (content_tsv);

CREATE INDEX IF NOT EXISTS idx_tc_ticker
    ON transcript_chunks (ticker);
CREATE INDEX IF NOT EXISTS idx_tc_transcript_id
    ON transcript_chunks (transcript_id);
CREATE INDEX IF NOT EXISTS idx_tc_section
    ON transcript_chunks (section);

CREATE OR REPLACE FUNCTION transcript_chunks_tsv_trigger() RETURNS TRIGGER AS $$
BEGIN
    NEW.content_tsv := to_tsvector('english', COALESCE(NEW.content, ''));
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trig_tc_tsv ON transcript_chunks;
CREATE TRIGGER trig_tc_tsv
    BEFORE INSERT OR UPDATE OF content ON transcript_chunks
    FOR EACH ROW EXECUTE FUNCTION transcript_chunks_tsv_trigger();

-- =============================================================================
-- Helper view: chunk search results with filing metadata
-- =============================================================================
CREATE OR REPLACE VIEW v_chunk_search AS
SELECT
    c.id,
    c.ticker,
    co.name            AS company_name,
    c.sector,
    c.filing_type,
    c.fiscal_year,
    c.period,
    c.section_name,
    c.content,
    c.token_count,
    c.embedding,
    c.content_tsv,
    c.source_url,
    c.filed_date
FROM chunks c
JOIN companies co ON co.ticker = c.ticker;
