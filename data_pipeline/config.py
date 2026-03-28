"""
Pipeline configuration: tickers, years, FRED series, DB settings,
and model hyperparameters.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Company universe
# ---------------------------------------------------------------------------

SECTORS: dict[str, list[str]] = {
    "banking": ["JPM", "BAC", "WFC", "GS", "MS"],
    "tech": [
        "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA",
        "META", "TSLA", "AMD", "INTC", "CRM",
    ],
    "healthcare": [
        "JNJ", "UNH", "PFE", "ABBV", "MRK",
        "LLY", "BMY", "AMGN", "GILD", "CVS",
    ],
    "energy": [
        "XOM", "CVX", "COP", "SLB", "EOG",
        "PXD", "VLO", "MPC", "PSX", "OXY",
    ],
    "consumer": [
        "WMT", "HD", "MCD", "NKE", "SBUX",
        "TGT", "COST", "LOW", "TJX", "DG",
    ],
}

# Flat list of all 50 tickers
ALL_TICKERS: list[str] = [t for tickers in SECTORS.values() for t in tickers]

# Reverse lookup: ticker -> sector
TICKER_TO_SECTOR: dict[str, str] = {
    ticker: sector
    for sector, tickers in SECTORS.items()
    for ticker in tickers
}

# ---------------------------------------------------------------------------
# Time range
# ---------------------------------------------------------------------------

START_YEAR: int = 2019
END_YEAR: int = 2023
YEARS: list[int] = list(range(START_YEAR, END_YEAR + 1))

# Date strings for yfinance / FRED
START_DATE: str = f"{START_YEAR}-01-01"
END_DATE: str = f"{END_YEAR}-12-31"

# ---------------------------------------------------------------------------
# SEC filing types
# ---------------------------------------------------------------------------

FILING_TYPES: list[str] = ["10-K", "10-Q", "8-K"]
DEFAULT_FILING_TYPES: list[str] = ["10-K", "10-Q"]

# ---------------------------------------------------------------------------
# FRED macroeconomic series
# ---------------------------------------------------------------------------

FRED_SERIES: dict[str, str] = {
    "DFF": "Federal Funds Effective Rate",
    "CPIAUCSL": "Consumer Price Index (All Urban Consumers)",
    "GDP": "Gross Domestic Product",
    "UNRATE": "Civilian Unemployment Rate",
    "T10Y2Y": "10-Year minus 2-Year Treasury Yield Spread",
    "DCOILWTICO": "Crude Oil Prices: West Texas Intermediate",
    "VIXCLS": "CBOE Volatility Index (VIX)",
}

FRED_API_KEY: str = os.getenv("FRED_API_KEY", "")

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

DATABASE_URL: str = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/financial_rag",
)

DB_POOL_MIN: int = 1
DB_POOL_MAX: int = 10

# ---------------------------------------------------------------------------
# Embedding model
# ---------------------------------------------------------------------------

EMBEDDING_MODEL: str = "sentence-transformers/all-MiniLM-L6-v2"
EMBEDDING_DIM: int = 384
EMBEDDING_BATCH_SIZE: int = 64

# ---------------------------------------------------------------------------
# Chunking
# ---------------------------------------------------------------------------

CHUNK_SIZE_TOKENS: int = 512
CHUNK_OVERLAP_TOKENS: int = 64

# ---------------------------------------------------------------------------
# Download settings
# ---------------------------------------------------------------------------

SEC_MAX_REQUESTS_PER_SECOND: int = 10
SEC_USER_AGENT: str = os.getenv(
    "SEC_USER_AGENT",
    "financial-rag-pipeline research@example.com",
)
SEC_DOWNLOAD_DIR: str = os.getenv("SEC_DOWNLOAD_DIR", "./data/sec_filings")

REQUEST_TIMEOUT: int = 30          # seconds
REQUEST_DELAY_SECONDS: float = 1.0  # polite delay between web requests

# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------

DB_BATCH_SIZE: int = 1_000

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
LOG_FILE: str = os.getenv("LOG_FILE", "./pipeline.log")
