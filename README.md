# CS 6120 Financial RAG Pipeline

SEC filing retrieval-augmented generation system. Downloads 10-K/10-Q/8-K filings for 50 tickers (2019–2023), chunks and embeds the text, and loads everything into PostgreSQL with pgvector for hybrid (vector + full-text) retrieval.

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

`financial_rag.dump` is a PostgreSQL custom-format dump (`pg_restore`-compatible). It contains the validated SEC corpus:

- Tickers: `AAPL`, `JPM`, `UNH`, `XOM`
- Year: 2023
- Filing types: 10-K, 10-Q
- Embeddings: **not included** (run pipeline with embeddings to populate)

Docker automatically restores it via `init-db.sh` on first boot. To restore manually:

```bash
pg_restore -U postgres -d financial_rag financial_rag.dump
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
