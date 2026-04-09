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
