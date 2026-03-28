"""
SEC EDGAR downloader
=====================
Downloads 10-K, 10-Q, and 8-K filings for a list of tickers using the
sec-edgar-downloader library, then returns metadata tuples for downstream
processing.

Rate limiting is enforced via an asyncio Semaphore (max 10 req/s as required
by EDGAR's fair-use policy).
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from pathlib import Path
from typing import Generator

from sec_edgar_downloader import Downloader

from data_pipeline.config import (
    SEC_DOWNLOAD_DIR,
    SEC_MAX_REQUESTS_PER_SECOND,
    SEC_USER_AGENT,
    TICKER_TO_SECTOR,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Type alias for filing metadata tuples returned to the pipeline
# ---------------------------------------------------------------------------
FilingMeta = tuple[str, str, int, Path, str]
#              ticker  type  year  local_path  source_url


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _make_downloader(download_dir: str) -> Downloader:
    """Create an EDGAR Downloader pointed at *download_dir*."""
    # sec-edgar-downloader >= 0.5 accepts a company name + email for user-agent
    parts = SEC_USER_AGENT.split()
    company = parts[0] if parts else "financial-rag"
    email = parts[1] if len(parts) > 1 else "user@example.com"
    return Downloader(company, email, download_dir)


def _filing_root(download_dir: str, ticker: str, filing_type: str) -> Path:
    """Return the directory where sec-edgar-downloader stores filings."""
    return Path(download_dir) / "sec-edgar-filings" / ticker / filing_type


def _iter_filing_paths(
    root: Path,
) -> Generator[tuple[Path, str], None, None]:
    """
    Walk *root* and yield (document_path, accession_number) for every
    primary document (htm/html/txt) inside each accession sub-directory.
    """
    if not root.exists():
        return
    for accession_dir in sorted(root.iterdir()):
        if not accession_dir.is_dir():
            continue
        accession = accession_dir.name
        # sec-edgar-downloader places the primary doc as 'full-submission.txt'
        # or individual htm files.  We prefer the largest .htm/.html file.
        candidates = sorted(
            list(accession_dir.glob("*.htm")) + list(accession_dir.glob("*.html")),
            key=lambda p: p.stat().st_size,
            reverse=True,
        )
        if not candidates:
            # Fall back to the full-submission text file
            full = accession_dir / "full-submission.txt"
            if full.exists():
                candidates = [full]
        if candidates:
            yield candidates[0], accession


def _build_source_url(ticker: str, filing_type: str, accession: str) -> str:
    """Construct the EDGAR viewer URL for a given accession number."""
    accession_clean = accession.replace("-", "")
    return (
        f"https://www.sec.gov/Archives/edgar/data/"
        f"{ticker}/{accession_clean}/{accession}-index.htm"
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

class SECDownloader:
    """
    Download SEC filings for the specified tickers / filing types / years.

    Parameters
    ----------
    download_dir:
        Root directory for downloaded filings (defaults to config value).
    max_rps:
        Maximum EDGAR requests per second.
    """

    def __init__(
        self,
        download_dir: str = SEC_DOWNLOAD_DIR,
        max_rps: int = SEC_MAX_REQUESTS_PER_SECOND,
    ) -> None:
        self.download_dir = download_dir
        self.max_rps = max_rps
        os.makedirs(download_dir, exist_ok=True)
        self._downloader = _make_downloader(download_dir)
        # Semaphore used in async context; also track wall-clock time for sync
        self._last_request_times: list[float] = []

    # ------------------------------------------------------------------
    # Rate limiting (synchronous token-bucket style)
    # ------------------------------------------------------------------

    def _rate_limit(self) -> None:
        """Block until we are within the allowed requests-per-second budget."""
        now = time.monotonic()
        # Keep only requests within the last 1 second
        self._last_request_times = [
            t for t in self._last_request_times if now - t < 1.0
        ]
        if len(self._last_request_times) >= self.max_rps:
            sleep_for = 1.0 - (now - self._last_request_times[0])
            if sleep_for > 0:
                time.sleep(sleep_for)
        self._last_request_times.append(time.monotonic())

    # ------------------------------------------------------------------
    # Core download logic
    # ------------------------------------------------------------------

    def _download_one(
        self,
        ticker: str,
        filing_type: str,
        after_date: str,
        before_date: str,
        limit: int = 20,
    ) -> None:
        """
        Download filings for a single ticker/type combination.
        Already-downloaded filings are skipped automatically by the library.
        """
        self._rate_limit()
        try:
            self._downloader.get(
                filing_type,
                ticker,
                limit=limit,
                after=after_date,
                before=before_date,
            )
            logger.debug("Downloaded %s %s (%s – %s)", ticker, filing_type, after_date, before_date)
        except Exception as exc:
            logger.warning(
                "Failed to download %s %s: %s", ticker, filing_type, exc
            )

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def download(
        self,
        tickers: list[str],
        filing_types: list[str],
        years: list[int],
    ) -> list[FilingMeta]:
        """
        Download filings for *tickers* / *filing_types* / *years* and return
        a list of ``FilingMeta`` tuples for all documents on disk (including
        previously downloaded ones).

        Returns
        -------
        list[FilingMeta]
            Each element: (ticker, filing_type, year, local_path, source_url)
        """
        after_date = f"{min(years) - 1}-07-01"
        before_date = f"{max(years) + 1}-06-30"

        total = len(tickers) * len(filing_types)
        processed = 0

        for ticker in tickers:
            for filing_type in filing_types:
                processed += 1
                logger.info(
                    "[%d/%d] Downloading %s %s", processed, total, ticker, filing_type
                )
                self._download_one(ticker, filing_type, after_date, before_date)

        # Collect metadata for everything on disk (current run + prior runs)
        return self._collect_metadata(tickers, filing_types, years)

    def _collect_metadata(
        self,
        tickers: list[str],
        filing_types: list[str],
        years: list[int],
    ) -> list[FilingMeta]:
        """
        Walk the download directory and build FilingMeta tuples.
        Year filtering is approximate (based on directory modification time
        and filing naming conventions).
        """
        results: list[FilingMeta] = []
        year_set = set(years)

        for ticker in tickers:
            for filing_type in filing_types:
                root = _filing_root(self.download_dir, ticker, filing_type)
                for doc_path, accession in _iter_filing_paths(root):
                    # Attempt to extract fiscal year from accession dir name
                    # or fall back to file mtime
                    year = _guess_year_from_path(doc_path, accession)
                    if year not in year_set:
                        continue
                    source_url = _build_source_url(ticker, filing_type, accession)
                    results.append(
                        (ticker, filing_type, year, doc_path, source_url)
                    )

        logger.info("Collected %d filing documents from disk", len(results))
        return results


# ---------------------------------------------------------------------------
# Async wrapper (for use in async orchestration contexts)
# ---------------------------------------------------------------------------

async def download_async(
    tickers: list[str],
    filing_types: list[str],
    years: list[int],
    download_dir: str = SEC_DOWNLOAD_DIR,
    max_rps: int = SEC_MAX_REQUESTS_PER_SECOND,
) -> list[FilingMeta]:
    """
    Async wrapper around :class:`SECDownloader`.  Downloads each
    ticker/type pair concurrently while honouring the rate limit via a
    semaphore.

    Because sec-edgar-downloader's ``get()`` is synchronous, we run it in
    a thread-pool executor.
    """
    semaphore = asyncio.Semaphore(max_rps)
    dl = SECDownloader(download_dir=download_dir, max_rps=max_rps)
    loop = asyncio.get_event_loop()

    after_date = f"{min(years) - 1}-07-01"
    before_date = f"{max(years) + 1}-06-30"

    async def _task(ticker: str, filing_type: str) -> None:
        async with semaphore:
            await loop.run_in_executor(
                None,
                dl._download_one,
                ticker,
                filing_type,
                after_date,
                before_date,
            )

    tasks = [
        _task(ticker, filing_type)
        for ticker in tickers
        for filing_type in filing_types
    ]
    await asyncio.gather(*tasks, return_exceptions=True)
    return dl._collect_metadata(tickers, filing_types, years)


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def _guess_year_from_path(doc_path: Path, accession: str) -> int:
    """
    Try to determine the fiscal year from the accession number (which
    encodes the filing date as YYYYMMDD) or fall back to mtime year.
    """
    # Accession numbers have format: XXXXXXXXXX-YY-NNNNNN
    # where YY is the 2-digit year of filing
    parts = accession.split("-")
    if len(parts) >= 2:
        yy = parts[1]
        if yy.isdigit():
            year = int(yy)
            return 2000 + year if year < 100 else year
    # Fall back to file modification time
    try:
        mtime = doc_path.stat().st_mtime
        import datetime
        return datetime.datetime.fromtimestamp(mtime).year
    except OSError:
        return 0


# ---------------------------------------------------------------------------
# CLI entry point for standalone testing
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    tickers = sys.argv[1:] or ["AAPL", "MSFT"]
    downloader = SECDownloader()
    metas = downloader.download(tickers, ["10-K"], [2022, 2023])
    for m in metas:
        print(m[0], m[1], m[2], m[3])
