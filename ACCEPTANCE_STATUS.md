# Acceptance Status

Validation date: April 17, 2026

Environment checked:

- local PostgreSQL database: `financial_rag`
- validation query bundle: `check_db_entries.sql`
- acceptance tests: `tests/test_metadata.py`, `tests/test_acceptance_db.py`

## What currently passes

- The live `financial_rag` database clears the project-wide volume gate.
  - total rows: `155,536`
  - `filings` rows: `860`
  - `chunks` rows: `89,848`
- All `chunks` rows now have embeddings.
  - `chunks` rows with embeddings: `89,848 / 89,848`
  - missing chunk embeddings: `0`
- The live database has no duplicate SEC chunk keys.
  - duplicate `(filing_id, chunk_index)` groups: `0`
- `check_db_entries.sql` returns `PASS` for:
  - total rows >= `10,000`
  - `chunks` >= `10,000`
  - no ticker-like `company_name` values in `v_chunk_search`
  - no unresolved sectors in `companies` or `chunks`
  - no `chunks` rows whose ticker is missing from `companies`
- Company-name backfill is already complete in the live database.
  - rerunning `data_pipeline/backfill_company_names.py` on April 16, 2026 returned:
    `Inserted 0 companies, updated 0 companies, updated 0 chunk rows.`
- The validated SEC subset is intact.
  - tickers: `AAPL`, `JPM`, `UNH`, `XOM`
  - year: `2023`
  - filing types: `10-K`, `10-Q`
  - filings present: `16 / 16`
  - filings with `filed_date`: `16 / 16`
  - filings with `source_url`: `16 / 16`
  - SEC validated chunks: `2,285`
  - SEC validated chunks with embeddings: `2,285`
- Full local SEC disk coverage is complete.
  - local SEC tickers covered in `filings`: `AAPL`, `ABBV`, `AMZN`, `BAC`, `C`, `COP`, `COST`, `CVX`, `EOG`, `GOOGL`, `HD`, `JNJ`, `JPM`, `MCD`, `META`, `MRK`, `MSFT`, `NKE`, `NVDA`, `PFE`, `PNC`, `SBUX`, `SCHW`, `SLB`, `TFC`, `UNH`, `USB`, `WMT`, `XOM`
  - local SEC filing keys discovered on disk: `860`
  - local SEC filing keys present in DB: `860`
  - missing local SEC filing keys: `0`
  - local SEC year coverage spans `2018-2025`

## Remaining caveats

- SEC filing coverage is not the full 50-ticker universe.
  - `filings` currently cover `29` tickers total
  - the full local SEC disk snapshot is complete for those 29 tickers
  - it is still not a complete 50-ticker SEC corpus
- Some optional data domains are still empty.
  - `macro_indicators`: `0`
  - `earnings_transcripts`: `0`
  - `transcript_chunks`: `0`
- `financial_rag.dump` has been refreshed from the live database.
  - archive timestamp: `2026-04-17 12:29:13 PDT`
  - dump row counts now match the live DB snapshot used for validation

## Interpretation

If the acceptance target is:

- "Does the current live database satisfy the project's 10k+ data and metadata requirements?"
  - Yes.
- "Is every chunk in the live database embedded and is the repo dump fully synchronized with the live database?"
  - Yes.

## Recommended next steps

1. Keep `financial_rag.dump` aligned with the live database whenever the DB changes again.
2. Decide whether the final submission must also include non-empty macro and transcript tables, or whether the current acceptance target is only the `10k+` database requirement plus the validated SEC subset.

## Commands used for rerun

```bash
psql -d financial_rag -f check_db_entries.sql
.venv/bin/python -m unittest tests/test_metadata.py tests/test_acceptance_db.py
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/financial_rag \
  .venv/bin/python data_pipeline/backfill_company_names.py
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/financial_rag \
  .venv/bin/python -m data_pipeline.backfill_chunk_embeddings --batch-size 64
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/financial_rag \
  .venv/bin/python -m data_pipeline.backfill_local_sec_filings
HUGGINGFACE_HUB_OFFLINE=1 HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1 \
  .venv/bin/python -m data_pipeline.pipeline \
  --tickers MSFT GOOGL AMZN NVDA META JNJ PFE ABBV MRK CVX COP SLB EOG WMT COST HD MCD NKE SBUX BAC \
  --years 2018 2019 2020 2021 2022 2023 2024 2025 \
  --filing-types 10-K 10-Q \
  --skip-market --skip-macro --skip-news --skip-transcripts \
  --db-url postgresql:///financial_rag
```
