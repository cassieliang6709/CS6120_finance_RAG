# SEC Handoff

This document is the final handoff note for the SEC ingestion layer.

## Status

The SEC data pipeline is ready for retrieval integration.

What is already fixed:

- `10-Q` filings now store correct `period` values like `Q1`, `Q2`, `Q3`
- `filed_date` is parsed from SEC filing headers
- `source_url` now uses the correct SEC `CIK` path for citation links
- the SEC cleaner no longer truncates filings early because of table-of-contents matches
- low-value appendix sections like `Exhibits` are filtered out by default

## What Retrieval Should Use First

For the first retrieval pass, use these section types:

- `MD&A`
- `Risk Factors`
- `Financial Statements`

For now, do not use:

- `Exhibits`
- `Selected Financial Data`

Reason:

- the current project goal is explanation-oriented financial QA
- these three sections carry most of the useful answerable content
- appendix-heavy sections add noise and inflate retrieval volume

## Current Validation Snapshot

Validated tickers:

- `AAPL`
- `JPM`
- `UNH`
- `XOM`

Validated year:

- `2023`

Current per-filing chunk counts:

- `AAPL 10-K`: `190`
- `AAPL 10-Q`: `62 / 68 / 64`
- `JPM 10-K`: `606`
- `JPM 10-Q`: `299 / 381 / 389`
- `UNH 10-K`: `132`
- `UNH 10-Q`: `33 / 40 / 40`
- `XOM 10-K`: `55`
- `XOM 10-Q`: `31 / 42 / 45`

These counts are now in a usable range for retrieval work.

## Suggested SQL Entry Point

If you want a clean first-pass retrieval corpus:

```sql
SELECT
    ticker,
    filing_type,
    fiscal_year,
    period,
    section_name,
    content,
    source_url,
    filed_date
FROM chunks
WHERE section_name IN ('MD&A', 'Risk Factors', 'Financial Statements');
```

If you want to inspect filing metadata first:

```sql
SELECT
    ticker,
    filing_type,
    fiscal_year,
    period,
    filed_date,
    source_url
FROM filings
ORDER BY ticker, filing_type, period;
```

## Files You May Want To Read

- `SEC_RUNBOOK.md`
- `SEC_VALIDATION_NOTES.md`
- `data_pipeline/processors/html_cleaner.py`
- `data_pipeline/downloaders/sec_downloader.py`

## If Something Looks Wrong

The first thing to check is not the database schema.

Check these in order:

1. whether the section label is sensible
2. whether the chunk preview is actual body text instead of a table of contents
3. whether the filing should be excluded from retrieval because of section quality

## Recommended Next Step

Pull `main`, use the filtered SEC corpus above, and build the first hybrid retrieval pass on top of `chunks`.
