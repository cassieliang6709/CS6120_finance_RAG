# SEC Validation Notes

Validation window:

- tickers: `AAPL`, `JPM`, `UNH`, `XOM`
- year: `2023`
- filing types: `10-K`, `10-Q`
- mode: `SEC-only`, `--skip-embed`

## What passed

- PostgreSQL + `pgvector` setup is working.
- SEC filing metadata now records:
  - correct `CIK`-based `source_url`
  - `filed_date`
  - correct `10-Q` periods (`Q1`, `Q2`, `Q3`)
- `AAPL` and `JPM` produced reasonable multi-section filings.
- `MD&A` and `Risk Factors` are present for `AAPL` and `JPM`.

## What changed during validation

- Fixed SEC metadata extraction:
  - `10-Q` now stores `Q1/Q2/Q3`
  - `source_url` now uses `CIK` instead of ticker
  - `filed_date` now loads from SEC headers
- Fixed cleaner truncation:
  - early `SIGNATURES` / `EXHIBIT INDEX` matches from the table of contents no longer truncate the whole filing
- Added cleaner-side filtering for low-value appendix sections:
  - `Exhibits`
  - very large `Item 16` / `Item 6` appendix-like sections

## Current per-filing chunk counts

- `AAPL 10-K`: 190
- `AAPL 10-Q`: 62 / 68 / 64
- `JPM 10-K`: 606
- `JPM 10-Q`: 299 / 381 / 389
- `UNH 10-K`: 132
- `UNH 10-Q`: 33 / 40 / 40
- `XOM 10-K`: 55
- `XOM 10-Q`: 31 / 42 / 45

## What still needs follow-up

- `JPM` is still much larger than the others and should be spot-checked manually.
- Some issuers still produce duplicate or awkward section labels when the filing layout is unusual.
- `10-Q` filings for `UNH` and `XOM` are now usable, but `MD&A` often appears as only a single chunk and may still need better section-boundary logic.

## Interpretation

- The SEC metadata layer is now in good shape for retrieval handoff.
- The cleaner is much more stable after the early-truncation fix.
- Filtering appendix material dramatically reduced chunk explosion for `UNH` and `XOM`.
- The main remaining risk is section quality, not database setup or citation metadata.

## Recommended next fixes

1. Spot-check 2 to 3 random filings per sector in the database, not just by SQL counts.
2. Improve section-boundary quality for `MD&A` in some `10-Q` layouts.
3. Decide whether to exclude `Selected Financial Data` or other appendix-style sections from retrieval.
