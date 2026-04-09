from contextlib import asynccontextmanager
from fastapi import FastAPI
from db import get_pool, close_pool
from models import RetrieveRequest, RetrieveResponse
from retrieval import retrieve


@asynccontextmanager
async def lifespan(app: FastAPI):
    await get_pool()
    yield
    await close_pool()


app = FastAPI(title="RAG Retrieve Service", lifespan=lifespan)


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
