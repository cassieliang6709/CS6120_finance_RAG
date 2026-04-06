# SEC Setup Todo

Goal for today: get the SEC-only path working end to end.

## Current local status

- `psql` is not installed.
- PostgreSQL server is not installed.
- `.env` does not exist yet.
- Python packages for this pipeline are not installed yet.

## 3-hour plan

### 0:00-0:45 Database install and startup

Install PostgreSQL:

```bash
brew install postgresql@16
brew services start postgresql@16
echo 'export PATH="/opt/homebrew/opt/postgresql@16/bin:$PATH"' >> ~/.zshrc
export PATH="/opt/homebrew/opt/postgresql@16/bin:$PATH"
psql --version
```

Install `pgvector`:

```bash
brew install pgvector
```

Create the database:

```bash
createdb financial_rag
psql -d financial_rag -c "CREATE EXTENSION IF NOT EXISTS vector;"
psql -d financial_rag -c "SELECT extname FROM pg_extension;"
```

Run the project schema:

```bash
psql -d financial_rag -f data_pipeline/schema.sql
```

### 0:45-1:15 Python environment

Create a virtual environment and install the minimum dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install psycopg2-binary pgvector python-dotenv sec-edgar-downloader sentence-transformers transformers beautifulsoup4 lxml tqdm
```

Create `.env` from the example:

```bash
cp .env.example .env
```

Update `SEC_USER_AGENT` in `.env` to your real name/email before hitting EDGAR.

### 1:15-1:45 Schema and DB validation

Check that tables exist:

```bash
psql -d financial_rag -c "\dt"
psql -d financial_rag -c "\d filings"
psql -d financial_rag -c "\d chunks"
```

Check that triggers and indexes exist:

```bash
psql -d financial_rag -c "\d+ chunks"
psql -d financial_rag -c "\d+ news_articles"
```

### 1:45-2:20 SEC-only minimum pipeline test

Start with one ticker and one year:

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

If that works, try one `10-Q` sample:

```bash
python3 -m data_pipeline.pipeline \
  --tickers AAPL \
  --years 2023 \
  --filing-types 10-Q \
  --skip-market \
  --skip-macro \
  --skip-news \
  --skip-transcripts
```

### 2:20-2:45 Data validation queries

Check filings:

```bash
psql -d financial_rag -c "SELECT ticker, filing_type, fiscal_year, period, source_url FROM filings ORDER BY id DESC LIMIT 10;"
```

Check chunks and trigger-generated `content_tsv`:

```bash
psql -d financial_rag -c "SELECT ticker, filing_type, fiscal_year, section_name, token_count FROM chunks ORDER BY id DESC LIMIT 10;"
psql -d financial_rag -c "SELECT id, content_tsv IS NOT NULL AS has_tsv FROM chunks ORDER BY id DESC LIMIT 10;"
```

Spot-check actual content:

```bash
psql -d financial_rag -c "SELECT ticker, section_name, left(content, 200) FROM chunks ORDER BY random() LIMIT 5;"
```

### 2:45-3:00 Log bugs and next fixes

Today, explicitly record these two issues:

1. `10-Q` period is too coarse.
   `pipeline.py` currently maps all `10-Q` rows to `quarterly`, which can collide for multiple quarters in one fiscal year.

2. SEC `source_url` construction is unreliable.
   `sec_downloader.py` currently uses `ticker` in the archive URL path where a `CIK`-based path is needed for stable citations.

## Definition of done for today

- PostgreSQL is installed and running.
- `pgvector` extension is enabled.
- `financial_rag` database exists.
- `schema.sql` runs successfully.
- `.env` exists and has valid local values.
- One SEC `10-K` sample loads into `filings` and `chunks`.
- One SEC `10-Q` sample is attempted and its metadata issue is documented.
- You have SQL screenshots or copy-paste results proving the DB contents.
