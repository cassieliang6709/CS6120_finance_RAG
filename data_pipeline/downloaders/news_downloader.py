"""
News downloader (RSS feeds)
=============================
Fetches financial news for each ticker from:
  - Yahoo Finance RSS: https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}
  - Reuters RSS (market news): https://feeds.reuters.com/reuters/businessNews

Articles are deduplicated by URL.  Content and title are combined for embedding.
"""

from __future__ import annotations

import hashlib
import logging
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any

import feedparser
import requests
from bs4 import BeautifulSoup

from data_pipeline.config import ALL_TICKERS, REQUEST_DELAY_SECONDS, REQUEST_TIMEOUT

logger = logging.getLogger(__name__)

NewsRow = dict[str, Any]

# ---------------------------------------------------------------------------
# RSS feed templates
# ---------------------------------------------------------------------------

YAHOO_RSS_URL = "https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&region=US&lang=en-US"

REUTERS_RSS_FEEDS = [
    "https://feeds.reuters.com/reuters/businessNews",
    "https://feeds.reuters.com/reuters/companyNews",
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_published(entry: Any) -> datetime | None:
    """Parse the published date from a feedparser entry."""
    for field in ("published", "updated", "created"):
        raw = getattr(entry, field, None)
        if raw:
            try:
                return parsedate_to_datetime(raw)
            except Exception:
                pass
    if hasattr(entry, "published_parsed") and entry.published_parsed:
        try:
            return datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
        except Exception:
            pass
    return None


def _clean_html(html_text: str) -> str:
    """Strip HTML tags from a string."""
    try:
        soup = BeautifulSoup(html_text, "lxml")
        return soup.get_text(separator=" ", strip=True)
    except Exception:
        return html_text


def _entry_to_row(entry: Any, ticker: str | None, source: str) -> NewsRow | None:
    """
    Convert a feedparser entry dict into a ``NewsRow`` dict.
    Returns None if the entry has no URL (cannot deduplicate).
    """
    url: str | None = getattr(entry, "link", None)
    if not url:
        return None

    title: str = getattr(entry, "title", "") or ""
    summary: str = getattr(entry, "summary", "") or ""
    content_raw: str = ""

    # feedparser sometimes puts full content in entry.content list
    if hasattr(entry, "content") and entry.content:
        content_raw = entry.content[0].get("value", "") or ""

    # Prefer content over summary; fall back to summary
    content = _clean_html(content_raw) if content_raw else _clean_html(summary)

    published = _parse_published(entry)
    author = getattr(entry, "author", None)

    return {
        "ticker": ticker,
        "title": title.strip(),
        "content": content.strip(),
        "summary": _clean_html(summary).strip(),
        "author": author,
        "published_date": published,
        "source_url": url.strip(),
        "source": source,
        # embedding filled in later by embedder
        "embedding": None,
    }


# ---------------------------------------------------------------------------
# Downloader class
# ---------------------------------------------------------------------------

class NewsDownloader:
    """
    Download financial news articles from Yahoo Finance and Reuters RSS feeds.

    Parameters
    ----------
    tickers:
        List of tickers to fetch Yahoo Finance news for.
    request_delay:
        Seconds to pause between requests.
    timeout:
        HTTP request timeout in seconds.
    include_reuters:
        If True, also fetch Reuters RSS feeds (not ticker-specific).
    """

    def __init__(
        self,
        tickers: list[str] | None = None,
        request_delay: float = REQUEST_DELAY_SECONDS,
        timeout: int = REQUEST_TIMEOUT,
        include_reuters: bool = True,
    ) -> None:
        self.tickers = tickers or ALL_TICKERS
        self.request_delay = request_delay
        self.timeout = timeout
        self.include_reuters = include_reuters
        self._seen_urls: set[str] = set()

    # ------------------------------------------------------------------
    # Internal fetch
    # ------------------------------------------------------------------

    def _fetch_feed(self, url: str) -> feedparser.FeedParserDict:
        """Fetch and parse an RSS feed URL, returning a feedparser dict."""
        try:
            resp = requests.get(url, timeout=self.timeout)
            resp.raise_for_status()
            return feedparser.parse(resp.content)
        except requests.RequestException as exc:
            logger.warning("Failed to fetch RSS feed %s: %s", url, exc)
            return feedparser.FeedParserDict()

    def _process_feed(
        self,
        feed: feedparser.FeedParserDict,
        ticker: str | None,
        source: str,
    ) -> list[NewsRow]:
        """Convert feedparser entries to NewsRow dicts, deduplicating by URL."""
        rows: list[NewsRow] = []
        for entry in getattr(feed, "entries", []):
            row = _entry_to_row(entry, ticker, source)
            if row is None:
                continue
            url = row["source_url"]
            if url in self._seen_urls:
                continue
            self._seen_urls.add(url)
            rows.append(row)
        return rows

    # ------------------------------------------------------------------
    # Per-ticker Yahoo Finance
    # ------------------------------------------------------------------

    def fetch_yahoo_news(self, ticker: str) -> list[NewsRow]:
        """Fetch Yahoo Finance RSS for a single ticker."""
        url = YAHOO_RSS_URL.format(ticker=ticker)
        feed = self._fetch_feed(url)
        rows = self._process_feed(feed, ticker, "yahoo_rss")
        logger.debug("Yahoo RSS for %s: %d new articles", ticker, len(rows))
        return rows

    # ------------------------------------------------------------------
    # Reuters feeds (general market news)
    # ------------------------------------------------------------------

    def fetch_reuters_news(self) -> list[NewsRow]:
        """Fetch Reuters business/company RSS feeds (not ticker-specific)."""
        rows: list[NewsRow] = []
        for feed_url in REUTERS_RSS_FEEDS:
            feed = self._fetch_feed(feed_url)
            batch = self._process_feed(feed, None, "reuters_rss")
            rows.extend(batch)
            logger.debug("Reuters feed %s: %d new articles", feed_url, len(batch))
            time.sleep(self.request_delay)
        return rows

    # ------------------------------------------------------------------
    # Full download
    # ------------------------------------------------------------------

    def download_all(self) -> list[NewsRow]:
        """
        Download all news articles for every configured ticker (Yahoo) and
        all Reuters feeds.

        Returns
        -------
        list[NewsRow]
            Deduplicated list of news article dicts.
        """
        all_rows: list[NewsRow] = []
        self._seen_urls.clear()

        # Ticker-specific Yahoo Finance news
        for i, ticker in enumerate(self.tickers, 1):
            logger.info("[%d/%d] Fetching Yahoo news for %s", i, len(self.tickers), ticker)
            rows = self.fetch_yahoo_news(ticker)
            all_rows.extend(rows)
            time.sleep(self.request_delay)

        # Reuters
        if self.include_reuters:
            logger.info("Fetching Reuters RSS feeds")
            rows = self.fetch_reuters_news()
            all_rows.extend(rows)

        logger.info(
            "News download complete: %d articles (%d unique URLs)",
            len(all_rows),
            len(self._seen_urls),
        )
        return all_rows


# ---------------------------------------------------------------------------
# Module-level convenience function
# ---------------------------------------------------------------------------

def download_news(
    tickers: list[str] | None = None,
    include_reuters: bool = True,
) -> list[NewsRow]:
    """Shorthand for ``NewsDownloader().download_all()``."""
    dl = NewsDownloader(tickers=tickers, include_reuters=include_reuters)
    return dl.download_all()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    articles = download_news(tickers=["AAPL", "MSFT"])
    print(f"Downloaded {len(articles)} articles")
    for a in articles[:3]:
        print(f"  [{a['ticker']}] {a['title']} ({a['published_date']})")
