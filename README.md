# CS 6120 Financial RAG Pipeline

SEC filing retrieval-augmented generation system for a 50-ticker project universe. The current live snapshot includes local SEC 10-K/10-Q coverage for 29 tickers across 2018-2025, chunks and embeds the text, and loads everything into PostgreSQL with pgvector for hybrid (vector + full-text) retrieval.

## Quick start (Docker)

```bash
docker compose up --build
```

This starts two services:

- **db** — `pgvector/pgvector:pg16`, restores `financial_rag.dump` on first boot
- **pipeline** — runs the full pipeline against the database

To start only the database (e.g. for local development):

```bash
docker compose up db
```

The `db` service uses a named Docker volume, `pgdata`, to persist PostgreSQL
data across container restarts. Because of that, `financial_rag.dump` is only
restored the first time that volume is initialized.

If you already ran Docker once and want the database to be recreated from a
newer `financial_rag.dump`, remove the existing volume first:

```bash
docker compose down -v
docker compose up --build
```

## Environment variables

`.env` is checked into the repo with the project defaults. Edit before running if needed:

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | `postgresql:///financial_rag` | Local Postgres URL (overridden by docker-compose) |
| `SEC_USER_AGENT` | `YueLiang liangyue3666@gmail.com` | Required by EDGAR fair-use policy |
| `SEC_DOWNLOAD_DIR` | `./data/sec_filings` | Where raw filings are saved |
| `LOG_LEVEL` | `INFO` | `DEBUG` / `INFO` / `WARNING` |
| `LOG_FILE` | `./pipeline.log` | Log output path |

When running via Docker, `DATABASE_URL` is automatically set to `postgresql://postgres:postgres@db:5432/financial_rag` by `docker-compose.yml` and the `.env` value is ignored.

## Database dump

`financial_rag.dump` is a PostgreSQL custom-format dump (`pg_restore`-compatible).
It is intended to mirror the current live `financial_rag` database snapshot used
for acceptance checks.

Current snapshot highlights:

- Total rows across project tables: `155,536`
- `filings` rows: `860`
- `chunks` rows: `89,848`
- Chunk embeddings: included for all `chunks` rows (`384` dimensions, `all-MiniLM-L6-v2`)
- Company metadata in `v_chunk_search.company_name`: backfilled with display names instead of raw tickers
- Validated SEC subset retained for `AAPL`, `JPM`, `UNH`, and `XOM` in 2023 (`10-K`, `10-Q`)
- Full local SEC disk coverage is present for 29 tickers:
  `AAPL`, `ABBV`, `AMZN`, `BAC`, `C`, `COP`, `COST`, `CVX`, `EOG`, `GOOGL`, `HD`, `JNJ`, `JPM`, `MCD`, `META`, `MRK`, `MSFT`, `NKE`, `NVDA`, `PFE`, `PNC`, `SBUX`, `SCHW`, `SLB`, `TFC`, `UNH`, `USB`, `WMT`, `XOM`
- Local SEC year coverage currently spans `2018-2025` for those 29 tickers

Docker automatically restores it via `init-db.sh` on first boot. To restore manually:

```bash
pg_restore -U postgres -d financial_rag financial_rag.dump
```

To verify embedding coverage after restore:

```sql
SELECT COUNT(*) AS total_chunks, COUNT(embedding) AS chunks_with_embedding
FROM chunks;
```

For retrieval and query-time metadata boosts, prefer `v_chunk_search.company_name`
instead of reconstructing company names from tickers in application code.

## Backfilling embeddings for an existing database

If you already restored an older dump or loaded chunks without embeddings, you
can backfill the missing vectors in place:

```bash
.venv/bin/python -m data_pipeline.backfill_chunk_embeddings \
  --dsn postgresql:///financial_rag \
  --batch-size 64
```

The script runs in offline Hugging Face mode by default and reuses the locally
cached `sentence-transformers/all-MiniLM-L6-v2` model when available.

If a dump contains ticker-only company names or unresolved sectors, repair the
metadata in place:

```bash
.venv/bin/python data_pipeline/backfill_company_names.py
psql -d financial_rag -f check_db_entries.sql
```

To backfill every local SEC 10-K / 10-Q filing already present on disk:

```bash
.venv/bin/python -m data_pipeline.backfill_local_sec_filings \
  --db-url postgresql:///financial_rag
```

## Running the pipeline locally

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# SEC only, one ticker, skip embeddings (fast)
python -m data_pipeline.pipeline \
  --tickers AAPL \
  --years 2023 \
  --filing-types 10-K \
  --skip-market --skip-macro --skip-news --skip-transcripts \
  --skip-embed
```

See `SEC_RUNBOOK.md` for full validation queries and known issues.

## Pipeline stages

| Flag | Stage |
|---|---|
| `--skip-sec` | SEC filing download |
| `--skip-market` | yfinance price/financials |
| `--skip-macro` | FRED macro indicators |
| `--skip-news` | RSS news articles |
| `--skip-transcripts` | Earnings call transcripts |
| `--skip-embed` | Sentence-transformer embeddings |
| `--skip-load` | DB writes (dry run) |
| `--skip-download` | All download stages at once |
