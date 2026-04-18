# CS 6120 Financial RAG Pipeline

SEC filing retrieval-augmented generation system for a 50-ticker project universe. The current live snapshot includes local SEC 10-K/10-Q coverage for all 50 project tickers across 2018-2025, chunks and embeds the text, and loads everything into PostgreSQL with pgvector for hybrid (vector + full-text) retrieval.

## Quick start (Docker)

```bash
docker compose up --build
```

This starts two services:

- **db** — `pgvector/pgvector:pg17`, restores `financial_rag.dump` on first boot
- **retrieval** — hybrid search API (`/retrieve`) + LLM-powered chat (`/chat`), available at `http://localhost:8000`

> **Note:** `/retrieve` works immediately with just the database. `/chat` additionally requires a running SGLang LLM endpoint — see [Chat endpoint](#chat-endpoint-llm--rag) below.

If already build once:

```bash
docker compose up
```

## Environment variables

`.env` is checked into the repo with the project defaults. Edit before running if needed:

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | `postgresql:///financial_rag` | Local Postgres URL (overridden by docker-compose) |
| `SEC_USER_AGENT` | `YueLiang liangyue3666@gmail.com` | Required by EDGAR fair-use policy |
| `SEC_DOWNLOAD_DIR` | `./data/sec_filings` | Where raw filings are saved |
| `LOG_LEVEL` | `INFO` | `DEBUG` / `INFO` / `WARNING` |
| `LOG_FILE` | `./pipeline.log` | Log output path |

When running via Docker, `DATABASE_URL` is automatically set to `postgresql://postgres:postgres@db:5432/financial_rag` by `docker-compose.yml` and the `.env` value is ignored.

The `pipeline` service is commented out in `docker-compose.yml`. Run the pipeline directly against the database (see **Running the pipeline locally** below).

## Database dump

`financial_rag.dump` is a PostgreSQL custom-format dump (`pg_restore`-compatible).
It is intended to mirror the current live `financial_rag` database snapshot used
for acceptance checks.

The repository version of `financial_rag.dump` is tracked with Git LFS, since
the final snapshot is too large for normal Git storage. For class handoff, the
same dump can also be shared separately through Google Drive when direct file
download is more convenient than cloning the repository with LFS enabled.

Current snapshot highlights:

- Total rows across project tables: `228,573`
- `companies` rows: `50`
- `filings` rows: `1,511`
- `chunks` rows: `162,234`
- Chunk embeddings: included for all `chunks` rows (`384` dimensions, `all-MiniLM-L6-v2`)
- Chunk embeddings missing: `0`
- Company metadata in `v_chunk_search.company_name`: backfilled with display names instead of raw tickers
- Unresolved company sectors: `0`
- Unresolved chunk sectors: `0`
- Ticker-like company names in `v_chunk_search`: `0`
- Validated SEC subset retained for `AAPL`, `JPM`, `UNH`, and `XOM` in 2023 (`10-K`, `10-Q`)
- Full local SEC disk coverage is present for all 50 project tickers
- Local SEC year coverage spans `2018-2025`

Docker automatically restores it via `init-db.sh` on first boot. To restore manually:

```bash
createdb financial_rag
pg_restore --clean --if-exists --no-owner -d financial_rag financial_rag.dump
```

To verify embedding coverage after restore:

```sql
SELECT COUNT(*) AS total_chunks, COUNT(embedding) AS chunks_with_embedding
FROM chunks;
```

For retrieval and query-time metadata boosts, prefer `v_chunk_search.company_name`
instead of reconstructing company names from tickers in application code.

## Backfilling embeddings for an existing database

If you already restored an older dump or loaded chunks without embeddings, you
can backfill the missing vectors in place:

```bash
.venv/bin/python -m data_pipeline.backfill_chunk_embeddings \
  --dsn postgresql:///financial_rag \
  --batch-size 64
```

The script runs in offline Hugging Face mode by default and reuses the locally
cached `sentence-transformers/all-MiniLM-L6-v2` model when available.

If a dump contains ticker-only company names or unresolved sectors, repair the
metadata in place:

```bash
.venv/bin/python data_pipeline/backfill_company_names.py
psql -d financial_rag -f check_db_entries.sql
```

To backfill every local SEC 10-K / 10-Q filing already present on disk:

```bash
.venv/bin/python -m data_pipeline.backfill_local_sec_filings \
  --db-url postgresql:///financial_rag
```

## Running the pipeline locally

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# SEC only, one ticker, skip embeddings (fast)
python -m data_pipeline.pipeline \
  --tickers AAPL \
  --years 2023 \
  --filing-types 10-K \
  --skip-market --skip-macro --skip-news --skip-transcripts \
  --skip-embed
```

See `SEC_RUNBOOK.md` for full validation queries and known issues.

## Pipeline stages

| Flag | Stage |
|---|---|
| `--skip-sec` | SEC filing download |
| `--skip-market` | yfinance price/financials |
| `--skip-macro` | FRED macro indicators |
| `--skip-news` | RSS news articles |
| `--skip-transcripts` | Earnings call transcripts |
| `--skip-embed` | Sentence-transformer embeddings |
| `--skip-load` | DB writes (dry run) |
| `--skip-download` | All download stages at once |

## Chat endpoint (LLM + RAG)

The retrieval service also exposes a `POST /chat` endpoint that runs retrieval
and an LLM call in a single request, streaming back the answer as Server-Sent
Events. The LLM is an OpenAI-compatible SGLang endpoint (e.g. Qwen hosted on
HPC) configured via `SGLANG_BASE_URL`.

### Required environment variables

Set these in `.env` (read by `docker compose` at startup):

| Variable | Required | Description |
|---|---|---|
| `SGLANG_BASE_URL` | yes | OpenAI-compatible base URL for the LLM, e.g. `https://xxx.ngrok.app/v1` |
| `SGLANG_MODEL` | no | Model name (default `Qwen/Qwen3.5-397B-A17B-FP8`) |
| `SGLANG_API_KEY` | no | API key if the LLM endpoint requires one |
| `SGLANG_MAX_TOKENS` | no | Max output tokens (default `32768`) |
| `API_KEY` | no | Public `/chat` auth key. Empty = auth disabled |
| `CORS_ORIGINS` | no | `*` or comma-separated origins for the frontend |

When `SGLANG_BASE_URL` is empty, `/chat` returns HTTP 503. `/health` and
`/retrieve` continue to work without the LLM.

### Request schema

```
POST /chat
Content-Type: application/json
X-API-Key: <optional, only when API_KEY is set>

{
  "query":       "What are Apple's main risk factors?",  // required
  "k":           5,                 // number of chunks to retrieve (1..20)
  "alpha":       0.7,               // hybrid weight (0=FTS only, 1=vector only)
  "sector":      null,              // optional filter
  "company":     "AAPL",            // optional filter
  "filing_type": "10-K",            // optional filter
  "stream":      true,              // true = SSE, false = JSON
  "system_prompt": null             // optional prompt override
}
```

### Streaming response (`stream: true`, default)

Content-Type: `text/event-stream`. Events arrive in this order:

```
event: chunks
data: [{"chunk_id":"...","text":"...","company":"AAPL","filing_type":"10-K",...}, ...]

event: thinking                      ← only if the model exposes reasoning
data: {"delta":"Let me analyze..."}

event: answer
data: {"delta":"Apple's main risks"}
data: {"delta":" include supply-chain"}
...

event: done
data: {}
```

Frontend pattern (browser `EventSource`):

```js
// EventSource only supports GET, so use fetch + ReadableStream for POST+SSE.
const res = await fetch("https://your-tunnel.trycloudflare.com/chat", {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({ query, k: 5, stream: true }),
});
const reader = res.body.getReader();
const decoder = new TextDecoder();
let buf = "";
while (true) {
  const { value, done } = await reader.read();
  if (done) break;
  buf += decoder.decode(value, { stream: true });
  // parse SSE events out of `buf` (split on \n\n, read `event:` and `data:` lines)
}
```

### Non-streaming response (`stream: false`)

```json
{
  "answer":   "Apple's main risks include supply-chain concentration...",
  "thinking": "Let me analyze the 10-K sections...",
  "chunks":   [ {chunk_id, text, company, filing_type, score, ...}, ... ]
}
```

### Quick local test

```bash
# Non-streaming
curl -X POST http://localhost:8000/chat \
  -H "Content-Type: application/json" \
  -d '{"query":"What are Apple risk factors?","company":"AAPL","stream":false}'

# Streaming (use curl -N to disable buffering)
curl -N -X POST http://localhost:8000/chat \
  -H "Content-Type: application/json" \
  -d '{"query":"What are Apple risk factors?","company":"AAPL","stream":true}'
```

### Debugging CLI

`rag_chat.py` at the repo root is a thin client for `/chat` with pretty-printed
streaming output. It reads `RAG_URL` and `API_KEY` from `.env`.

```bash
# Install the few deps it needs (already in requirements.txt)
pip install -r requirements.txt

# One-shot
python rag_chat.py "What are Apple's main risk factors?"
python rag_chat.py --company AAPL --filing-type 10-K "Summarize risks"

# REPL mode (no positional query)
python rag_chat.py
> What are Apple's main risk factors?
> /quit
```

## Public access with Cloudflare Tunnel

Use Cloudflare Tunnel to expose the `retrieval` service to the public
internet for a demo or a remote frontend teammate — it's free, requires no
account for ephemeral URLs, and doesn't have ngrok's rate limits.

```bash
# 1. Install cloudflared
brew install cloudflared                    # macOS
# sudo apt install cloudflared              # Debian/Ubuntu
# see https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/downloads/

# 2. Make sure .env has a valid SGLANG_BASE_URL (HPC tunnel) and start the stack
docker compose up -d

# 3. Expose http://localhost:8000 via an ephemeral trycloudflare URL
cloudflared tunnel --url http://localhost:8000
#   → logs print:  https://<random-name>.trycloudflare.com
#   → share that URL with the frontend team / graders
```

The `trycloudflare.com` URL rotates each time `cloudflared` restarts. For a
persistent hostname, create a free Cloudflare account and use a
[named tunnel](https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/get-started/create-remote-tunnel/).

### When the HPC ngrok URL rotates

The LLM lives behind its own ngrok tunnel on HPC, which rotates on restart.
Update the backend-only env var and bounce the retrieval container:

```bash
# 1. Edit .env → SGLANG_BASE_URL=https://newurl.ngrok.app/v1
# 2. Recreate the retrieval container to pick up the new env (db keeps running)
docker compose up -d retrieval
```

### Architecture summary

```
Public users / frontend
    │
    ▼  https://<name>.trycloudflare.com
cloudflared tunnel
    │
    ▼  localhost:8000
retrieval container  ──┬──▶ asyncpg → Postgres + pgvector  (local volume)
                       └──▶ AsyncOpenAI → SGLANG_BASE_URL  (HPC ngrok, backend-only)
```

The HPC ngrok URL is **never exposed to end users** — only the retrieval
container reads it, as a backend env var.
