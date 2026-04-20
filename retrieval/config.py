import os
from dotenv import load_dotenv

load_dotenv()  # loads retrieval/.env if present, shell env vars always take precedence

EMBEDDING_MODEL: str = os.getenv("EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2")

DB_HOST: str = os.getenv("DB_HOST", "localhost")
DB_PORT: int = int(os.getenv("DB_PORT", "5432"))
DB_NAME: str = os.getenv("DB_NAME", "financial_rag")
DB_USER: str = os.getenv("DB_USER", "postgres")
DB_PASSWORD: str = os.getenv("DB_PASSWORD", "postgres")

DEFAULT_K: int = int(os.getenv("DEFAULT_K", "5"))
DEFAULT_ALPHA: float = float(os.getenv("DEFAULT_ALPHA", "0.7"))

DB_POOL_MIN: int = 1
DB_POOL_MAX: int = 10

# LLM (OpenAI-compatible SGLang endpoint, backend-only — never exposed to frontend)
SGLANG_BASE_URL: str = os.getenv("SGLANG_BASE_URL", "")
SGLANG_MODEL: str = os.getenv("SGLANG_MODEL", "Qwen/Qwen3.5-397B-A17B-FP8")
SGLANG_API_KEY: str = os.getenv("SGLANG_API_KEY", "")
SGLANG_MAX_TOKENS: int = int(os.getenv("SGLANG_MAX_TOKENS", "32768"))

# Public /chat endpoint auth — empty string disables auth entirely
API_KEY: str = os.getenv("API_KEY", "")

# CORS — "*" or comma-separated origins
CORS_ORIGINS: list[str] = [
    o.strip() for o in os.getenv("CORS_ORIGINS", "*").split(",") if o.strip()
]

# Boost multiplier applied to chunks whose ticker matches a company detected in the query.
# Set to 1.0 to disable.
COMPANY_BOOST: float = float(os.getenv("COMPANY_BOOST", "1.5"))
FILING_TYPE_BOOST: float = float(os.getenv("FILING_TYPE_BOOST", "1.3"))
FISCAL_YEAR_BOOST: float = float(os.getenv("FISCAL_YEAR_BOOST", "1.3"))
