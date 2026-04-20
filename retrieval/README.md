# Retrieval Service

## 1. Introduction

This service implements the `/retrieve` endpoint for the financial RAG pipeline. It accepts a natural-language query, searches the PostgreSQL + pgvector database of SEC filing chunks, and returns the top-k most relevant chunks with metadata and scores. Generate service calls this endpoint internally to build the LLM prompt; the frontend never calls `/retrieve` directly.

The search strategy is **hybrid**: it runs vector similarity search and BM25 keyword search in parallel, normalizes both score distributions to [0, 1], and fuses them with a configurable weight `alpha`. The result is a ranked list of chunks with full provenance — company, filing type, date, and a direct SEC EDGAR URL. An optional metadata filter (sector, company, filing type) narrows the corpus before either search branch runs.

On top of hybrid search, the service applies **metadata-aware boosting**: when a company ticker, filing type, or fiscal year is detected in the query text, a configurable score multiplier is baked directly into the SQL `ORDER BY` of both search branches. This ensures that relevant chunks from the mentioned company/period rank higher in the retrieved candidate pool — not just in post-retrieval reordering.

**Where this fits:**
```
Frontend (T4)
    └── POST /generate  →  Generate service (T3)
                               └── POST /retrieve  →  THIS SERVICE (T2)
                                                          └── PostgreSQL (pgvector + tsvector)
```

**Critical constraint:** The embedding model is fixed at `sentence-transformers/all-MiniLM-L6-v2`. This must match exactly what T1 used at ingestion time. Changing it would make all cosine similarity scores meaningless without a full re-index.

---

## 2. Quick Start

### Start database and retrieval service

```bash
# From the repo root
docker compose up --build
```

The retrieval API will be available at `http://localhost:8000` once both containers are healthy.

### Health check

```bash
curl http://localhost:8000/health
```

Expected response:
```json
{"status": "ok"}
```

### Sample retrieve request

Basic query (no filters):
```bash
curl -s -X POST http://localhost:8000/retrieve \
  -H "Content-Type: application/json" \
  -d '{"query": "net interest margin", "k": 3}' | python3 -m json.tool
```

With company and filing type filters:
```bash
curl -s -X POST http://localhost:8000/retrieve \
  -H "Content-Type: application/json" \
  -d '{
    "query": "revenue growth and operating expenses",
    "k": 5,
    "alpha": 0.7,
    "company": "JPM",
    "filing_type": "10-K"
  }' | python3 -m json.tool
```

Pure BM25 keyword search (`alpha=0.0`):
```bash
curl -s -X POST http://localhost:8000/retrieve \
  -H "Content-Type: application/json" \
  -d '{"query": "risk factors interest rate", "k": 5, "alpha": 0.0}' | python3 -m json.tool
```

### Testing metadata-aware boosting

These queries exercise each boost signal. Results should be dominated by the expected company, filing type, or fiscal year.

Ticker detection — `AMD` in all-caps triggers company boost:
```bash
curl -s -X POST http://localhost:8000/retrieve \
  -H "Content-Type: application/json" \
  -d '{"query":"What are AMD risk factors?","k":5}' | python3 -m json.tool
```

Filing type detection — `annual report` triggers 10-K boost:
```bash
curl -s -X POST http://localhost:8000/retrieve \
  -H "Content-Type: application/json" \
  -d '{"query":"What does the annual report say about JPM revenue?","k":5}' | python3 -m json.tool
```

Fiscal year detection — `FY2022` triggers year boost:
```bash
curl -s -X POST http://localhost:8000/retrieve \
  -H "Content-Type: application/json" \
  -d '{"query":"What were MSFT earnings in FY2022?","k":5}' | python3 -m json.tool
```

### Running tests locally

```bash
# From the retrieval/ directory
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Unit tests only (no database required)
pytest tests/ -m "not integration" -v

# Integration tests (requires docker compose up db retrieval to be running)
INTEGRATION=true pytest tests/ -v
```

---

## 3. Key Technical Details

### Hybrid search and score fusion

Every query runs two searches concurrently via `asyncio.gather`:

1. **Vector search** — encodes the query with `sentence-transformers/all-MiniLM-L6-v2` and computes cosine similarity against `chunks.embedding` using pgvector's `<=>` operator. Returns `1 - (embedding <=> query_vec)` as `score_v`.
2. **BM25 search** — converts the query to a `tsquery` with `plainto_tsquery('english', ...)` and scores against the pre-computed `chunks.content_tsv` tsvector using PostgreSQL `ts_rank`. Returns `ts_rank(...)` as `score_b`.

Each branch over-fetches `k × 3` rows so that a chunk ranking #8 on vector but #1 on BM25 can still surface after fusion.

After fetching, scores are merged into a unified candidate pool and each distribution is independently **min-max normalized** to [0, 1]. Normalized scores are fused:

```
hybrid_score = α × score_v + (1 − α) × score_b
```

The pool is sorted descending by `hybrid_score` and the top-k are returned. Default `alpha = 0.7` favors semantic similarity; set `alpha = 1.0` for pure vector, `alpha = 0.0` for pure BM25.

### Metadata-aware boosting

SEC filings contain large amounts of boilerplate text (e.g. "risk factors" language) that is semantically similar across all companies. A query like *"What are AMD risk factors?"* would otherwise surface chunks from unrelated companies because the embedding model weights the generic phrase "risk factors" more heavily than the company name token.

To counteract this, the service detects metadata signals in the query before retrieval and embeds score multipliers directly into the SQL `ORDER BY`:

| Signal | How detected | Boost applied to |
|---|---|---|
| Company ticker | All-caps 2–5 letter tokens matched against known tickers (e.g. `AMD`, `JPM`) | Chunks where `ticker = detected_ticker` |
| Filing type | Keywords: `10-K`, `annual report`, `annual filing`, `10-Q`, `quarterly report` | Chunks where `filing_type` matches |
| Fiscal year | Pattern `20XX` or `FY20XX` or `fiscal year 20XX` | Chunks where `fiscal_year` matches |

Detection fires on the original query text (not uppercased) to avoid false positives — common words like "are" would otherwise match the real ticker `ARE`. If the caller already supplies an explicit filter for a field (e.g. `company="AMD"`), detection is skipped for that field since the filter already restricts the corpus.

Multiple signals stack multiplicatively. A query like *"What does AMD's 2022 annual report say about risk?"* hits all three signals: `COMPANY_BOOST × FILING_TYPE_BOOST × FISCAL_YEAR_BOOST`.

The three boost multipliers are configurable via environment variables — set to `1.0` to disable any of them:

| Variable | Default | Effect |
|---|---|---|
| `COMPANY_BOOST` | `1.5` | Multiplier for chunks matching the detected company ticker |
| `FILING_TYPE_BOOST` | `1.3` | Multiplier for chunks matching the detected filing type |
| `FISCAL_YEAR_BOOST` | `1.3` | Multiplier for chunks matching the detected fiscal year |

> **Note on scores:** Because boosts are applied before score normalization, the final hybrid scores can exceed 1.0 when boosted chunks dominate the candidate pool.

### Metadata filters

`sector`, `company`, and `filing_type` filters are applied as SQL `WHERE` conditions **before** both search branches execute, so they restrict the candidate corpus, not just the final results. Filters are independently optional and can be combined freely. Enum values are case-sensitive and must match what T1 stored (e.g., `"banking"` not `"Banking"`, `"10-K"` not `"10k"`).

The `build_filter_clause` function returns a SQL fragment using `:name` placeholders and a params dict. The internal `_apply_filter` helper converts these to asyncpg's `$N` positional params before query execution.

### Database view used for queries

Both search branches query `v_chunk_search`, a pre-existing view defined in T1's schema that joins the `chunks`, `filings`, and `companies` tables. This avoids repeating the join logic in application code and provides `filed_date` (from `filings`) and `company_name` (from `companies`) in a single query.

### Schema mapping

The actual database schema uses different column names than the API response. This service maps them transparently:

| API field | DB column | Notes |
|---|---|---|
| `chunk_id` | `id::text` | Cast BIGSERIAL to string |
| `text` | `content` | The chunk text |
| `company` | `ticker` | e.g. `"JPM"` |
| `article_title` | _(constructed)_ | `"{company_name} {filing_type} {fiscal_year}"` |
| `page_num` | _(absent)_ | Always `null` — not in schema |

### Score ordering guarantee

The `/retrieve` response always returns chunks sorted by `hybrid_score` descending. Scores are rounded to 6 decimal places. If two chunks have the same hybrid score, their relative order is not guaranteed.

### Edge cases

- **All BM25 scores zero** (query terms match nothing): `norm_b` is set to all zeros rather than all ones, so the BM25 component does not artificially inflate the hybrid score. The result degrades gracefully to pure vector search.
- **`k` exceeds corpus size**: returns however many chunks exist without crashing.
- **Empty query string**: FastAPI's Pydantic validation (`min_length=1`) rejects it with HTTP 422 before it reaches any database code.
- **All vector scores equal** (degenerate embedding): `minmax_normalize` returns all ones rather than dividing by zero.

---

## 4. For Teammates (T3 / T4)

### What T3 (Generate service) needs to know

**Endpoint:** `POST http://<RETRIEVE_URL>/retrieve`

During development with Docker: `http://localhost:8000/retrieve`

#### Request body

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `query` | string | **Yes** | — | Natural-language question (must be non-empty, else HTTP 422) |
| `k` | integer | No | `5` | Number of chunks to return (≥ 1) |
| `alpha` | float | No | `0.7` | Hybrid weight: `1.0` = pure vector, `0.0` = pure BM25 |
| `sector` | string | No | `null` | Filter by sector (e.g. `"Financial Services"`) |
| `company` | string | No | `null` | Filter by ticker (e.g. `"JPM"`, `"AAPL"`) |
| `filing_type` | string | No | `null` | Filter by filing type: `"10-K"` or `"10-Q"` |

All filter fields are case-sensitive and must match exactly what is stored in the database.

#### Example request

```json
{
  "query": "What did JPM say about net interest margin in 2023?",
  "k": 5,
  "alpha": 0.7,
  "company": "JPM",
  "filing_type": "10-K"
}
```

#### Response

```json
{
  "chunks": [
    {
      "chunk_id": "1042",
      "text": "Net interest margin declined 12 basis points to 2.68% in 2023...",
      "score": 0.874321,
      "company": "JPM",
      "sector": "Financial Services",
      "filing_type": "10-K",
      "filed_date": "2024-02-09",
      "source_url": "https://www.sec.gov/Archives/edgar/data/19617/...",
      "article_title": "JPMorgan Chase & Co. 10-K 2023",
      "page_num": null
    }
  ]
}
```

#### Response field notes

| Field | Notes |
|---|---|
| `chunk_id` | Stable string ID — use for deduplication if calling `/retrieve` multiple times |
| `score` | Hybrid score in [0, 1], higher is more relevant. Chunks are always sorted descending. |
| `source_url` | Direct SEC EDGAR URL — use for citations |
| `article_title` | Pre-formatted as `"{company_name} {filing_type} {fiscal_year}"` — ready to display |
| `page_num` | Always `null` — not available in the current schema |

#### Error responses

| Status | Cause |
|---|---|
| `422` | `query` is empty or request body is malformed |
| `500` | Database connection failure |

#### Integration tips

- Over-fetching and re-ranking on the T3 side is unnecessary — `/retrieve` already returns the top-k by hybrid score.
- Use `chunks[i].source_url` and `chunks[i].article_title` to build citations in the prompt.
- During development without Docker, point at the stub: `uvicorn stub_retrieve:app --reload --port 8000` — same interface, no DB needed.

### What T1 (Data pipeline) needs to know

- The embedding model is locked to `sentence-transformers/all-MiniLM-L6-v2`. Any change requires a full re-index.
- This service reads from `v_chunk_search` (the view in `schema.sql`). Do not drop or rename it.
- The `content_tsv` tsvector column must be populated at ingestion time (T1's trigger already handles this).
- `sector` values in the response come directly from `chunks.sector` — make sure these are lowercase strings (e.g., `"banking"`, `"tech"`) so T4's filter UI works predictably.

### Environment variables reference

| Variable | Required | Default | Description |
|---|---|---|---|
| `DB_HOST` | Yes | `localhost` | PostgreSQL host (T1's GCP instance) |
| `DB_PORT` | No | `5432` | PostgreSQL port |
| `DB_NAME` | No | `financial_rag` | Database name |
| `DB_USER` | No | `postgres` | Database username |
| `DB_PASSWORD` | No | `postgres` | Database password (default matches Docker Compose) |
| `EMBEDDING_MODEL` | No | `sentence-transformers/all-MiniLM-L6-v2` | Must match T1's ingestion model |
| `DEFAULT_K` | No | `5` | Default number of chunks returned |
| `DEFAULT_ALPHA` | No | `0.7` | Default hybrid weight |
| `COMPANY_BOOST` | No | `1.5` | Score multiplier for chunks matching a company ticker detected in the query. Set to `1.0` to disable. |
| `FILING_TYPE_BOOST` | No | `1.3` | Score multiplier for chunks matching a filing type detected in the query. Set to `1.0` to disable. |
| `FISCAL_YEAR_BOOST` | No | `1.3` | Score multiplier for chunks matching a fiscal year detected in the query. Set to `1.0` to disable. |

---

## 5. File Structure

```
retrieval/
├── main.py              # FastAPI app, route definitions
├── retrieval.py         # Core logic: embed, search, normalize, fuse
├── db.py                # Database connection pool
├── config.py            # Constants and environment variable loading
├── models.py            # Pydantic request/response schemas
├── stub_retrieve.py     # Hardcoded fake server for parallel development
├── tests/
│   ├── __init__.py
│   ├── conftest.py      # Pytest fixtures
│   └── test_retrieval.py
├── infra/               # Terraform — all AWS infrastructure
│   ├── main.tf
│   ├── variables.tf
│   ├── outputs.tf
│   ├── ecr.tf
│   ├── ecs.tf
│   ├── iam.tf
│   ├── networking.tf
│   ├── secrets.tf
│   └── terraform.tfvars.example
├── Dockerfile
├── .dockerignore
├── requirements.txt
└── plan.md
```

### `config.py`
Loads all configuration from environment variables. Defines `EMBEDDING_MODEL` (must never change after T1 indexes data), database connection parameters, and retrieval defaults (`DEFAULT_K=5`, `DEFAULT_ALPHA=0.7`). In Docker, env vars are injected via `docker-compose.yml` and override defaults automatically.

### `models.py`
Defines the three Pydantic models that form the public contract with T3. `RetrieveRequest` validates the incoming JSON — `query` is required and non-empty; all other fields are optional. `ChunkResult` maps DB column names to API field names (`content` → `text`, `ticker` → `company`). `RetrieveResponse` wraps a list of `ChunkResult` objects.

### `db.py`
Manages a singleton asyncpg connection pool. `get_pool()` creates the pool on first call and reuses it thereafter. `_init_connection` registers the pgvector type codec on every connection. `close_pool()` is called on FastAPI shutdown.

### `retrieval.py`
Core logic. `embed_query` encodes a string to a 384-dim float list. `minmax_normalize` scales scores to [0, 1]. `fuse_scores` combines vector and BM25 scores by `alpha`. `load_known_tickers` populates an in-process cache of all tickers and company names from the DB at startup. `detect_company_in_query`, `detect_filing_type_in_query`, and `detect_year_in_query` extract metadata signals from the query text. `build_boost_expression` converts detected signals into a SQL `CASE WHEN` multiplier expression embedded in both search queries. `retrieve` detects signals, runs both searches in parallel with boosts applied, merges, normalizes, fuses, and returns sorted `ChunkResult` objects.

### `main.py`
FastAPI entry point. `GET /health` returns `{"status": "ok"}`. `POST /retrieve` accepts a `RetrieveRequest` and returns a `RetrieveResponse`. The lifespan context manager creates and closes the DB pool.

### `stub_retrieve.py`
Drop-in fake server with the same endpoints but hardcoded responses — no DB needed. Start with `uvicorn stub_retrieve:app --reload --port 8000` for T3/T4 to develop against.

### `Dockerfile`
Builds from `python:3.11-slim`. Pre-downloads the sentence-transformers model during build so the container does not fetch it on first startup. Listens on port 8000.

### `infra/` (Terraform)
All AWS infrastructure as code: ECR repository, ECS Fargate service, internal ALB, IAM roles, security groups, and Secrets Manager entry for `DB_PASSWORD`.
