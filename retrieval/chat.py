"""LLM glue for the /chat endpoint.

Formats retrieved chunks into a citation-friendly context block and calls the
configured OpenAI-compatible SGLang endpoint (Qwen) either in streaming or
one-shot mode.

The client is lazy-initialized so the container can start successfully even
with SGLANG_BASE_URL empty — failure surfaces at request time as an HTTP 503.
"""

import asyncio
import json
from typing import AsyncIterator, Optional

import httpx
from openai import AsyncOpenAI

import config
from models import ChunkResult


DEFAULT_SYSTEM_PROMPT = (
    "You are a financial analyst assistant. Answer the user's question using "
    "ONLY the provided SEC filing excerpts as source material. Cite sources "
    "inline as [1], [2], etc., matching the numbered context blocks. If the "
    "answer is not in the provided context, say you don't know — do not "
    "fabricate figures or facts."
)


_client: Optional[AsyncOpenAI] = None
_client_lock = asyncio.Lock()


async def get_llm_client() -> AsyncOpenAI:
    """Lazy-init AsyncOpenAI so the container starts without SGLANG_BASE_URL set."""
    global _client
    if _client is None:
        async with _client_lock:
            if _client is None:  # re-check after acquiring the lock
                if not config.SGLANG_BASE_URL:
                    raise RuntimeError("SGLANG_BASE_URL not configured")
                _client = AsyncOpenAI(
                    base_url=config.SGLANG_BASE_URL,
                    # OpenAI SDK rejects empty string at construction — use placeholder
                    api_key=config.SGLANG_API_KEY or "EMPTY",
                    timeout=httpx.Timeout(
                        connect=10.0, read=120.0, write=10.0, pool=10.0
                    ),
                )
    return _client


def format_context(chunks: list[ChunkResult]) -> str:
    """Format chunks as numbered blocks with company/filing metadata for citation."""
    blocks = []
    for i, c in enumerate(chunks, 1):
        header = f"[{i}] {c.company} {c.filing_type}"
        if c.filed_date:
            header += f" (filed {c.filed_date.isoformat()})"
        blocks.append(f"{header}\n{c.text}")
    return "\n\n---\n\n".join(blocks)


def build_messages(
    query: str, context: str, system_prompt: Optional[str] = None
) -> list[dict]:
    """Build the OpenAI chat messages list: one system turn + one user turn."""
    sp = system_prompt or DEFAULT_SYSTEM_PROMPT
    user = f"Context from SEC filings:\n\n{context}\n\nQuestion: {query}"
    return [
        {"role": "system", "content": sp},
        {"role": "user", "content": user},
    ]


async def stream_chat(
    query: str,
    chunks: list[ChunkResult],
    system_prompt: Optional[str] = None,
) -> AsyncIterator[dict]:
    """Async generator yielding SSE event dicts.

    Events shape: {"event": "thinking"|"answer"|"error"|"done", "data": json_str}.
    Caller wraps in sse_starlette.EventSourceResponse.
    """
    client = await get_llm_client()
    messages = build_messages(query, format_context(chunks), system_prompt)

    try:
        stream = await client.chat.completions.create(
            model=config.SGLANG_MODEL,
            messages=messages,
            max_tokens=config.SGLANG_MAX_TOKENS,
            stream=True,
        )
        async for chunk in stream:
            if not chunk.choices:
                continue
            delta = chunk.choices[0].delta
            # Qwen thinking mode surfaces reasoning on delta.reasoning_content
            reasoning = getattr(delta, "reasoning_content", None)
            if reasoning:
                yield {
                    "event": "thinking",
                    "data": json.dumps({"delta": reasoning}),
                }
            if delta.content:
                yield {
                    "event": "answer",
                    "data": json.dumps({"delta": delta.content}),
                }
    except asyncio.CancelledError:
        # Frontend disconnected — re-raise so FastAPI cleans up the response
        raise
    except Exception as e:
        # Once stream headers are flushed we can't change HTTP status,
        # so surface the error as an in-band event instead of raising.
        yield {
            "event": "error",
            "data": json.dumps({"message": str(e)}),
        }
    finally:
        yield {"event": "done", "data": "{}"}


async def chat_once(
    query: str,
    chunks: list[ChunkResult],
    system_prompt: Optional[str] = None,
) -> dict:
    """Non-streaming one-shot call. Returns {'answer': str, 'thinking': str|None}."""
    client = await get_llm_client()
    messages = build_messages(query, format_context(chunks), system_prompt)

    resp = await client.chat.completions.create(
        model=config.SGLANG_MODEL,
        messages=messages,
        max_tokens=config.SGLANG_MAX_TOKENS,
        stream=False,
    )
    msg = resp.choices[0].message
    return {
        "answer": msg.content or "",
        "thinking": getattr(msg, "reasoning_content", None),
    }
