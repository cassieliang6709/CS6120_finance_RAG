# SEC Runbook

This runbook is for the SEC-only ingestion path used by the RAG project.

## 1. Environment

PostgreSQL is expected to run through Homebrew `postgresql@17`.

Your shell should include:

```bash
export PATH="/opt/homebrew/opt/postgresql@17/bin:$PATH"
```

Python setup:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install psycopg2-binary pgvector python-dotenv sec-edgar-downloader sentence-transformers transformers beautifulsoup4 lxml tqdm yfinance fredapi feedparser requests pandas numpy
```

## 2. Database setup

Start PostgreSQL and create the project database:

```bash
brew services start postgresql@17
createdb financial_rag
psql -d financial_rag -c "CREATE EXTENSION IF NOT EXISTS vector;"
psql -d financial_rag -f data_pipeline/schema.sql
```

## 3. `.env`

Create `.env` with at least:

```bash
DATABASE_URL=postgresql:///financial_rag
SEC_USER_AGENT=YourName your-email@example.com
SEC_DOWNLOAD_DIR=./data/sec_filings
LOG_LEVEL=INFO
LOG_FILE=./pipeline.log
```

Use a real name and email for `SEC_USER_AGENT`.

## 4. Minimal SEC-only test

Run one filing first:

```bash
source .venv/bin/activate
python3 -m data_pipeline.pipeline \
  --tickers AAPL \
  --years 2023 \
  --filing-types 10-K \
  --skip-market \
  --skip-macro \
  --skip-news \
  --skip-transcripts
```

For faster metadata and cleaning checks, skip embeddings:

```bash
python3 -m data_pipeline.pipeline \
  --tickers AAPL JPM UNH XOM \
  --years 2023 \
  --filing-types 10-K 10-Q \
  --skip-market \
  --skip-macro \
  --skip-news \
  --skip-transcripts \
  --skip-embed
```

## 5. Validation queries

Check filing metadata:

```bash
psql -d financial_rag -c "
SELECT ticker, filing_type, fiscal_year, period, filed_date, source_url
FROM filings
ORDER BY ticker, filing_type, period;
"
```

Check per-filing chunk counts:

```bash
psql -d financial_rag -c "
SELECT ticker, filing_type, fiscal_year, period, filed_date, COUNT(*) AS chunk_count
FROM chunks
GROUP BY ticker, filing_type, fiscal_year, period, filed_date
ORDER BY ticker, filing_type, period;
"
```

Check section distribution:

```bash
psql -d financial_rag -c "
SELECT ticker, filing_type, period, section_name, COUNT(*) AS n
FROM chunks
GROUP BY ticker, filing_type, period, section_name
ORDER BY ticker, filing_type, period, n DESC;
"
```

Spot-check content:

```bash
psql -d financial_rag -c "
SELECT ticker, filing_type, period, section_name, left(content, 200) AS preview
FROM chunks
ORDER BY random()
LIMIT 10;
"
```

## 6. Current known issues

- Some filings still collapse into a single chunk because the cleaner does not robustly handle every SEC HTML layout.
- `Exhibits` can dominate chunk volume for some issuers, especially banks.
- SEC URLs are now structurally correct, but a browser request may still need an acceptable SEC user agent to avoid `403`.

## 7. Recommended handoff to retrieval

Before retrieval work starts, the retrieval owner should receive:

- the latest `filings` row count
- the latest `chunks` row count
- a sample of valid `source_url` values
- a note on which tickers have cleaner quality issues

