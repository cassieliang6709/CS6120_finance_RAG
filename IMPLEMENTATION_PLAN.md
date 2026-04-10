# Plan: Public-facing RAG chat service (/chat endpoint + Cloudflare Tunnel)

## Context

**What prompted this:** We have an LLM hosted on HPC (exposed via ngrok, rotating URL) and a local RAG retrieval service (`POST /retrieve` on `localhost:8000`). A teammate is building a frontend and graders/users need public access. Goal: **make the whole RAG + LLM stack publicly accessible** via one HTTP endpoint, without spinning up AWS infra yet.

**The missing pieces:**
1. A `/chat` endpoint that does retrieval + LLM generation in one call (frontend can't glue these together from scratch).
2. A public tunnel for the retrieval service — using **Cloudflare Tunnel** (free, persistent hostname, no ngrok rate limits).
3. Optional API-key auth so a public URL isn't wide-open abuse bait (starts disabled via empty env var — `API_KEY=""`).

**Architecture after this change:**
```
Public users ──▶ https://<name>.trycloudflare.com ──▶ localhost:8000
                                                            │
                                                    retrieval container
                                                    (/health, /retrieve, /chat)
                                                            │
                                      ┌─────────────────────┼─────────────────────┐
                                      ▼                                           ▼
                              asyncpg → Postgres+pgvector         AsyncOpenAI ── SGLANG_BASE_URL
                              (local Docker volume)               (HPC ngrok, backend-only secret)
```
**Key insight:** the HPC ngrok URL is a backend-only env var (`SGLANG_BASE_URL`) inside the retrieval container — users/frontend never see it, never call it.

## Recommended approach

### Files created / modified (9 files + 1 test)

| # | File | Change |
|---|---|---|
| 1 | `retrieval/requirements.txt` | add `openai>=1.40.0`, `sse-starlette>=2.1.0` |
| 2 | `retrieval/config.py` | add `SGLANG_BASE_URL`, `SGLANG_MODEL`, `SGLANG_API_KEY`, `SGLANG_MAX_TOKENS`, `API_KEY`, `CORS_ORIGINS` |
| 3 | `retrieval/models.py` | add `ChatRequest`, `ChatResponse` |
| 4 | `retrieval/chat.py` | **NEW** — context formatter, lazy `AsyncOpenAI` client, `stream_chat`, `chat_once` |
| 5 | `retrieval/main.py` | add `CORSMiddleware`, `require_api_key` dep, `POST /chat` (SSE + JSON) |
| 6 | `docker-compose.yml` | pass new env vars via `${VAR:-default}` substitution |
| 7 | `.env` (repo root) | append new vars as empty/sane defaults |
| 8 | `rag_chat.py` (repo root, NEW) | thin CLI: hybrid one-shot/REPL, parses SSE |
| 9 | `README.md` | append "Chat endpoint" API docs + "Public access with Cloudflare Tunnel" section |
| +1 | `retrieval/tests/test_chat.py` | **NEW** — unit tests for `format_context` and `build_messages` (no DB/LLM) |

### 1. `retrieval/requirements.txt`
Added:
```
openai>=1.40.0
sse-starlette>=2.1.0
```

### 2. `retrieval/config.py`
Appended to the end (matches existing `os.getenv` + `load_dotenv` pattern):
```python
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
```

### 3. `retrieval/models.py`
Appended after `RetrieveResponse`:
```python
class ChatRequest(BaseModel):
    query: str = Field(..., min_length=1)
    k: int = Field(default=5, ge=1, le=20)
    alpha: float = Field(default=0.7, ge=0.0, le=1.0)
    sector: Optional[str] = None
    company: Optional[str] = None
    filing_type: Optional[str] = None
    stream: bool = True
    system_prompt: Optional[str] = None

class ChatResponse(BaseModel):
    answer: str
    thinking: Optional[str] = None
    chunks: list[ChunkResult]
```

### 4. `retrieval/chat.py` (NEW)

Flat imports matching the existing codebase (`from config import ...`, not `from retrieval.config`).

- `DEFAULT_SYSTEM_PROMPT` — "answer using ONLY provided SEC excerpts, cite [1], [2], ..."
- `get_llm_client()` — lazy-init `AsyncOpenAI` behind an `asyncio.Lock` (double-check after lock) so the container can start with `SGLANG_BASE_URL=""`. Raises `RuntimeError` if unconfigured. Uses `api_key=SGLANG_API_KEY or "EMPTY"` because the OpenAI SDK rejects empty strings at construction. Timeout is `httpx.Timeout(connect=10, read=120, write=10, pool=10)` — the `read` timeout is **per-chunk**, not wall-clock, so 32k-token answers work as long as tokens keep flowing.
- `format_context(chunks)` — numbered blocks `[1] AAPL 10-K (filed YYYY-MM-DD)\n<text>`, separated by `---`.
- `build_messages(query, context, system_prompt)` — returns `[system, user]` messages list.
- `async def stream_chat(...)` — async generator yielding `{"event": "thinking"|"answer"|"error"|"done", "data": json_str}`. Handles both `delta.reasoning_content` (Qwen thinking mode) and `delta.content`. Errors are **yielded** as events, not raised, because HTTP headers are already flushed by the time we're iterating the upstream stream. Always yields `done` in a `finally`.
- `async def chat_once(...)` — non-streaming one-shot. Handles `reasoning_content` on `message` (not `delta`).

### 5. `retrieval/main.py`
Added:
- `CORSMiddleware` with `allow_origins=config.CORS_ORIGINS`, `allow_credentials=False` (browser rejects `*` + credentials)
- `require_api_key` dependency using `hmac.compare_digest` (timing-safe), no-op when `config.API_KEY` is empty
- `POST /chat` with:
  - Returns HTTP 503 if `SGLANG_BASE_URL` is empty
  - Calls `retrieve(pool, ...)` directly (no internal HTTP hop)
  - `stream=True` → `EventSourceResponse(event_gen(), ping=15)` — the `ping=15` keeps the Cloudflare tunnel warm (idle connections get killed around 100s)
  - First SSE event is `chunks` with `model_dump(mode="json")` so `filed_date` becomes an ISO string
  - Drops out of the loop if `await request.is_disconnected()` — releases the upstream LLM stream on frontend disconnect
  - `stream=False` → returns `ChatResponse`

### 6. `docker-compose.yml`
Extended the `retrieval` service's `environment:` block with 6 new vars using `${VAR:-default}` substitution so Docker Compose reads them from the repo-root `.env` at startup.

### 7. `.env` (repo root)
Appended commented sections for `SGLANG_*`, `API_KEY`, `CORS_ORIGINS`, `RAG_URL` (CLI). All empty/sane defaults so the container starts without crashing; `/chat` returns 503 until `SGLANG_BASE_URL` is set.

### 8. `rag_chat.py` (NEW, repo root)

Synchronous CLI that hits `POST /chat` (streaming) and pretty-prints the events.

**Note:** Uses `requests` instead of `httpx` — `requests` is already in repo-root `requirements.txt` (`httpx` is not), so no new dependency at the repo root. `requests` + `iter_lines()` handles SSE parsing perfectly well; keeping the CLI synchronous also simplifies the code (no asyncio, no async generator).

- `argparse` for positional query + `--company`, `--filing-type`, `--sector`, `-k` flags
- Reads `RAG_URL` (default `http://localhost:8000`) and `API_KEY` from `.env`
- One-shot mode when a query is given as positional args; REPL loop (`/quit` to exit) otherwise
- Parses SSE events with `iter_lines(decode_unicode=True)`, recognizes `event:`, `data:`, comment lines (keep-alive `:` prefix), and blank-line event boundaries
- Handles `chunks`, `thinking`, `answer`, `error`, `done` events — prints section headers lazily so output stays clean

### 9. `README.md`
Appended two sections:

**Chat endpoint (LLM + RAG)** — API contract for the frontend teammate:
- Required env vars table
- Request schema
- Streaming response shape (SSE events)
- Non-streaming response shape (JSON)
- Browser SSE snippet using `fetch` + `ReadableStream` (since `EventSource` doesn't support POST)
- Quick local `curl` examples (streaming + non-streaming)
- CLI helper docs

**Public access with Cloudflare Tunnel** — end-to-end setup:
```bash
brew install cloudflared
docker compose up -d
cloudflared tunnel --url http://localhost:8000
# → https://<random>.trycloudflare.com
```
Plus instructions for when the HPC ngrok URL rotates (`docker compose up -d retrieval`) and an architecture summary diagram.

### +1. `retrieval/tests/test_chat.py` (NEW)

Pure-function unit tests for `format_context` and `build_messages`. No DB, no LLM, no network, no `@integration` marker:
- `test_format_context_numbers_blocks_sequentially`
- `test_format_context_includes_filed_date_when_present`
- `test_format_context_omits_filed_date_when_missing`
- `test_format_context_empty_list`
- `test_build_messages_uses_default_system_prompt`
- `test_build_messages_custom_system_prompt_override`
- `test_build_messages_user_content_contains_context_and_question`

## Reused existing code (no reinvention)

- **`retrieve()`** at `retrieval/retrieval.py:140` — called directly from `/chat`, no internal HTTP hop
- **`get_pool()`** at `retrieval/db.py:13` — asyncpg pool singleton, managed by existing FastAPI lifespan
- **`ChunkResult`** in `retrieval/models.py` — reused for context formatting and SSE `chunks` event via `model_dump(mode="json")`
- **Config pattern** in `retrieval/config.py` — `os.getenv()` + `load_dotenv()`, followed exactly for new vars
- **Flat imports** — Dockerfile sets `WORKDIR /app` + `COPY . .`, so all retrieval-internal imports are flat (`from chat import ...`, not `from retrieval.chat import ...`)

## Verification

### Unit-level (no LLM, no tunnel)
```bash
cd retrieval && pip install -r requirements.txt
pytest tests/test_chat.py -v
```
All 7 tests should pass in under a second.

### Integration (local Docker, LLM reachable)
1. Edit `.env` → set `SGLANG_BASE_URL=https://<your-hpc-ngrok>.ngrok.app/v1`
2. Rebuild the retrieval container (picks up new deps):
   ```bash
   docker compose up -d --build retrieval
   docker compose logs -f retrieval   # confirm startup, no import errors
   ```
3. Smoke tests:
   ```bash
   curl http://localhost:8000/health
   # {"status":"ok"}

   curl -X POST http://localhost:8000/chat \
     -H "Content-Type: application/json" \
     -d '{"query":"What are Apple risk factors?","company":"AAPL","stream":false}'
   # JSON with answer, thinking, chunks

   curl -N -X POST http://localhost:8000/chat \
     -H "Content-Type: application/json" \
     -d '{"query":"What are Apple risk factors?","company":"AAPL","stream":true}'
   # event: chunks → event: thinking → event: answer (many deltas) → event: done
   ```
4. CLI helper:
   ```bash
   python rag_chat.py "What are Apple risk factors?"
   python rag_chat.py --company AAPL --filing-type 10-K "Summarize risks"
   python rag_chat.py   # REPL mode
   ```
5. Negative test (LLM unconfigured):
   - Comment out `SGLANG_BASE_URL=...` in `.env`, `docker compose up -d retrieval`
   - Hit `/chat` → expect HTTP 503 `{"detail":"LLM not configured (SGLANG_BASE_URL is empty)"}`

### Public access
```bash
cloudflared tunnel --url http://localhost:8000
# copy the https://<random>.trycloudflare.com URL
```
From another device / phone:
```bash
curl https://<random>.trycloudflare.com/health
curl -X POST https://<random>.trycloudflare.com/chat \
  -H 'Content-Type: application/json' \
  -d '{"query":"test","stream":false}'
```

### API key on (optional)
1. Set `API_KEY=some-secret` in `.env`
2. `docker compose up -d retrieval`
3. Without header → HTTP 401
4. With `-H "X-API-Key: some-secret"` → HTTP 200

### Ngrok URL rotation (LLM side)
```bash
# Edit .env → SGLANG_BASE_URL=https://newurl.ngrok.app/v1
docker compose up -d retrieval   # picks up env change, keeps db running
```

## Out of scope (intentionally)

- AWS deployment via `retrieval/infra/` Terraform stack (future work; ngrok + Cloudflare Tunnel is enough for class demo)
- Named Cloudflare tunnels with persistent hostnames (requires CF account + DNS; quick tunnel is enough to start)
- Rate limiting (add `slowapi` later only if abuse observed)
- Conversation history / multi-turn in the CLI REPL (each query is independent)
- Re-embedding / re-ingesting more tickers
- Tightening `/retrieve` to internal-only (frontend doesn't need it directly since `/chat` returns chunks in the first SSE event)
