# `/chat` API Reference — Frontend Integration Guide

## Endpoint

```
POST /chat
Content-Type: application/json
```

## Request body

```jsonc
{
  "query":         "What are Apple's main risk factors?",  // required, min 1 char
  "k":             5,          // number of sources to retrieve (1–20, default 5)
  "alpha":         0.7,        // hybrid weight: 0.0 = keyword only, 1.0 = vector only
  "company":       "AAPL",     // optional filter: "AAPL" | "JPM" | "UNH" | "XOM"
  "filing_type":   "10-K",     // optional filter: "10-K" | "10-Q"
  "sector":        null,       // optional filter
  "stream":        true,       // true = SSE streaming, false = single JSON response
  "system_prompt":  null       // optional override for the LLM system prompt
}
```

---

## Response mode 1: Streaming (`"stream": true`, default)

**Content-Type:** `text/event-stream` (Server-Sent Events)

Events arrive in this order:

### Event 1: `chunks` (always first, arrives immediately)

The retrieved SEC filing sources. Render these right away while the LLM is thinking.

```
event: chunks
data: [
  {
    "chunk_id": "abc123",
    "text": "The Company obtains components from single or limited sources...",
    "score": 0.847,
    "company": "AAPL",
    "sector": "technology",
    "filing_type": "10-K",
    "filed_date": "2023-11-03",
    "source_url": "https://...",
    "article_title": "AAPL 10-K 2023",
    "page_num": null
  },
  { ... },
  { ... }
]
```

`data` is a JSON array of chunk objects. Number of items = `k` from the request.

### Event 2: `thinking` (zero or many, only if the model reasons)

Qwen's chain-of-thought reasoning tokens. Optional — the model may skip straight to answering.

```
event: thinking
data: {"delta": "Let me analyze "}

event: thinking
data: {"delta": "the 10-K risk factors section..."}
```

Each event has a `delta` field — a **partial string** (one or a few tokens). **Concatenate all deltas** to build the full thinking text.

### Event 3: `answer` (many, the actual response)

The LLM's answer with `[N]` citations referencing the chunks from event 1.

```
event: answer
data: {"delta": "Apple's"}

event: answer
data: {"delta": " main risks"}

event: answer
data: {"delta": " include supply-chain [1]"}

event: answer
data: {"delta": " and competition [2]..."}
```

Same pattern — each `delta` is a partial string. **Concatenate all deltas** to build the full answer. Citations like `[1]`, `[2]` refer to the chunk at that index (1-based) from the `chunks` event.

### Event 4: `error` (zero or one, only on failure)

If the LLM connection drops mid-stream:

```
event: error
data: {"message": "Connection refused"}
```

### Event 5: `done` (always last)

Terminal event. Stop reading the stream.

```
event: done
data: {}
```

### Complete SSE lifecycle

```
event: chunks       ← 1 event, JSON array of sources
data: [...]

event: thinking     ← 0+ events, concatenate deltas
data: {"delta":"..."}

event: answer       ← 1+ events, concatenate deltas
data: {"delta":"..."}

event: done         ← 1 event, stop reading
data: {}
```

### Keep-alive pings

The backend sends SSE comments every 15 seconds to keep the connection alive through Cloudflare/proxies:

```
: ping
```

**Ignore any line starting with `:`** — they're not events.

---

## Response mode 2: Non-streaming (`"stream": false`)

**Content-Type:** `application/json`

Returns everything at once after the LLM finishes generating:

```json
{
  "answer": "Apple's main risks include supply-chain concentration [1] and intense competition [2]...",
  "thinking": "Let me analyze the 10-K sections for risk factors...",
  "chunks": [
    {
      "chunk_id": "abc123",
      "text": "The Company obtains components from single or limited sources...",
      "score": 0.847,
      "company": "AAPL",
      "sector": "technology",
      "filing_type": "10-K",
      "filed_date": "2023-11-03",
      "source_url": "https://...",
      "article_title": "AAPL 10-K 2023",
      "page_num": null
    }
  ]
}
```

- `answer` — full answer string with `[N]` citations
- `thinking` — full reasoning text, or `null` if the model didn't reason
- `chunks` — same schema as the streaming `chunks` event

---

## Citation format

The LLM cites sources as `[1]`, `[2]`, etc. These are **1-based indices** into the `chunks` array:

```
chunks[0]  →  [1] in the answer
chunks[1]  →  [2] in the answer
chunks[2]  →  [3] in the answer
```

**Frontend should**: parse `[N]` patterns in the answer text (regex: `\[(\d+)\]`) and render them as interactive elements (tooltip, popover, expandable card, etc.) that show the corresponding `chunks[N-1]` content.

---

## Chunk object schema

Every chunk (in both streaming and non-streaming) has this shape:

| Field | Type | Description |
|---|---|---|
| `chunk_id` | string | Unique ID |
| `text` | string | The actual SEC filing text (can be long) |
| `score` | float | Relevance score (0–1, higher = more relevant) |
| `company` | string | Ticker: `"AAPL"`, `"JPM"`, `"UNH"`, `"XOM"` |
| `sector` | string | Sector name |
| `filing_type` | string | `"10-K"` or `"10-Q"` |
| `filed_date` | string \| null | ISO date (`"2023-11-03"`) or null |
| `source_url` | string \| null | EDGAR URL or null |
| `article_title` | string \| null | e.g. `"AAPL 10-K 2023"` |
| `page_num` | int \| null | Always null currently |

---

## Error responses

| Status | When | Body |
|---|---|---|
| 422 | Invalid request (empty query, k out of range) | `{"detail": [...validation errors...]}` |
| 401 | `API_KEY` is set but `X-API-Key` header is wrong/missing | `{"detail": "invalid or missing X-API-Key"}` |
| 503 | `SGLANG_BASE_URL` not configured | `{"detail": "LLM not configured (SGLANG_BASE_URL is empty)"}` |

---

## Frontend parsing example (JavaScript)

```js
const res = await fetch("https://<backend-url>/chat", {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({ query: "...", stream: true, k: 5 }),
});

const reader = res.body.getReader();
const decoder = new TextDecoder();
let buf = "";
let chunks = [];
let thinking = "";
let answer = "";

while (true) {
  const { value, done } = await reader.read();
  if (done) break;
  buf += decoder.decode(value, { stream: true });

  // Split on double newline (SSE event boundary)
  const events = buf.split("\n\n");
  buf = events.pop(); // keep incomplete last chunk in buffer

  for (const raw of events) {
    let eventType = null;
    let data = null;

    for (const line of raw.split("\n")) {
      if (line.startsWith("event:")) eventType = line.slice(6).trim();
      if (line.startsWith("data:"))  data = line.slice(5).trim();
    }

    if (!data || !eventType) continue;
    const parsed = JSON.parse(data);

    switch (eventType) {
      case "chunks":
        chunks = parsed;             // array of source objects
        renderSources(chunks);        // show sources immediately
        break;
      case "thinking":
        thinking += parsed.delta;     // concatenate
        renderThinking(thinking);     // update thinking display
        break;
      case "answer":
        answer += parsed.delta;       // concatenate
        renderAnswer(answer, chunks); // update answer + parse [N] citations
        break;
      case "error":
        showError(parsed.message);
        break;
      case "done":
        finalizeUI();
        return;
    }
  }
}
```

---

## Quick test commands

```bash
# Health check
curl https://<backend-url>/health

# Non-streaming (returns full JSON)
curl -X POST https://<backend-url>/chat \
  -H "Content-Type: application/json" \
  -d '{"query":"What are Apple risk factors?","company":"AAPL","stream":false}'

# Streaming (use -N to disable curl buffering)
curl -N -X POST https://<backend-url>/chat \
  -H "Content-Type: application/json" \
  -d '{"query":"What are Apple risk factors?","company":"AAPL","stream":true}'
```

---

## Available data

The database currently contains:

- **Companies:** AAPL, JPM, UNH, XOM
- **Year:** 2023
- **Filing types:** 10-K, 10-Q
- **Embeddings:** 384-dim vectors (all-MiniLM-L6-v2)

Questions about other tickers or years will return "I don't know" style answers.
