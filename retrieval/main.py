import hmac
import json
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from sse_starlette.sse import EventSourceResponse

import config
from chat import chat_once, stream_chat
from db import close_pool, get_pool
from models import ChatRequest, ChatResponse, RetrieveRequest, RetrieveResponse
from retrieval import load_known_tickers, retrieve


@asynccontextmanager
async def lifespan(app: FastAPI):
    pool = await get_pool()
    await load_known_tickers(pool)
    yield
    await close_pool()


app = FastAPI(title="RAG Retrieve Service", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=config.CORS_ORIGINS,
    # `*` origins + credentials is rejected by browsers; keep credentials off
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


async def require_api_key(
    x_api_key: Optional[str] = Header(None, alias="X-API-Key"),
) -> None:
    """No-op when API_KEY env var is empty; otherwise require a matching header."""
    if not config.API_KEY:
        return
    if not x_api_key or not hmac.compare_digest(x_api_key, config.API_KEY):
        raise HTTPException(status_code=401, detail="invalid or missing X-API-Key")


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/retrieve", response_model=RetrieveResponse)
async def retrieve_endpoint(req: RetrieveRequest):
    pool = await get_pool()
    chunks = await retrieve(
        pool=pool,
        query=req.query,
        k=req.k,
        alpha=req.alpha,
        sector=req.sector,
        company=req.company,
        filing_type=req.filing_type,
    )
    return RetrieveResponse(chunks=chunks)


@app.post("/chat", dependencies=[Depends(require_api_key)])
async def chat_endpoint(req: ChatRequest, request: Request):
    if not config.SGLANG_BASE_URL:
        raise HTTPException(
            status_code=503,
            detail="LLM not configured (SGLANG_BASE_URL is empty)",
        )

    pool = await get_pool()
    chunks = await retrieve(
        pool=pool,
        query=req.query,
        k=req.k,
        alpha=req.alpha,
        sector=req.sector,
        company=req.company,
        filing_type=req.filing_type,
    )

    if req.stream:

        async def event_gen():
            # First event: the retrieved sources so the frontend can render them
            # immediately while the LLM is still generating. model_dump(mode="json")
            # ensures date fields become ISO strings for safe json.dumps.
            yield {
                "event": "chunks",
                "data": json.dumps(
                    [c.model_dump(mode="json") for c in chunks]
                ),
            }
            async for ev in stream_chat(req.query, chunks, req.system_prompt):
                if await request.is_disconnected():
                    break
                yield ev

        # ping keeps the Cloudflare tunnel warm (idle connections get killed ~100s)
        return EventSourceResponse(event_gen(), ping=15)

    result = await chat_once(req.query, chunks, req.system_prompt)
    return ChatResponse(
        answer=result["answer"],
        thinking=result["thinking"],
        chunks=chunks,
    )
