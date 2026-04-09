"""
Stub implementation — returns hardcoded fake responses.
Run this so T3/T4 can develop against /retrieve before the real DB is wired up.

    uvicorn stub_retrieve:app --reload --port 8000
"""
from datetime import date
from fastapi import FastAPI
from models import RetrieveRequest, RetrieveResponse, ChunkResult

app = FastAPI(title="RAG Retrieve Service (Stub)")

_FAKE_CHUNKS = [
    ChunkResult(
        chunk_id="jpm_10k_2023_chunk_042",
        text=(
            "Net interest margin declined 12 basis points to 2.68% in fiscal year 2023, "
            "driven by higher funding costs as the Federal Reserve continued its rate-hiking cycle."
        ),
        score=0.87,
        company="JPM",
        sector="banking",
        filing_type="10-K",
        filed_date=date(2024, 2, 9),
        source_url="https://www.sec.gov/Archives/edgar/data/19617/000001961724000057/jpm-20231231.htm",
        article_title="JPMorgan Chase 10-K 2023",
        page_num=47,
    ),
    ChunkResult(
        chunk_id="jpm_10k_2023_chunk_043",
        text=(
            "Total revenue increased 23% year over year, reflecting strong net interest income "
            "growth partially offset by lower noninterest revenue across trading and advisory segments."
        ),
        score=0.81,
        company="JPM",
        sector="banking",
        filing_type="10-K",
        filed_date=date(2024, 2, 9),
        source_url="https://www.sec.gov/Archives/edgar/data/19617/000001961724000057/jpm-20231231.htm",
        article_title="JPMorgan Chase 10-K 2023",
        page_num=48,
    ),
    ChunkResult(
        chunk_id="bac_10k_2023_chunk_021",
        text=(
            "Net interest margin for the full year 2023 was 2.11%, compared with 1.98% in 2022, "
            "as rising rates increased asset yields faster than funding costs."
        ),
        score=0.74,
        company="BAC",
        sector="banking",
        filing_type="10-K",
        filed_date=date(2024, 2, 16),
        source_url="https://www.sec.gov/Archives/edgar/data/70858/000007085824000044/bac-20231231.htm",
        article_title="Bank of America 10-K 2023",
        page_num=32,
    ),
    ChunkResult(
        chunk_id="gs_10k_2023_chunk_018",
        text=(
            "Investment banking revenues decreased 12% to $5.8 billion, primarily reflecting "
            "lower advisory and underwriting activity amid challenging market conditions."
        ),
        score=0.68,
        company="GS",
        sector="banking",
        filing_type="10-K",
        filed_date=date(2024, 2, 26),
        source_url="https://www.sec.gov/Archives/edgar/data/886982/000088698224000010/gs-20231231.htm",
        article_title="Goldman Sachs 10-K 2023",
        page_num=22,
    ),
    ChunkResult(
        chunk_id="wfc_10k_2023_chunk_055",
        text=(
            "Net interest income increased $8.0 billion, or 16%, compared with 2022, driven by "
            "the impact of higher interest rates on earning assets, partially offset by higher "
            "rates on interest-bearing liabilities."
        ),
        score=0.62,
        company="WFC",
        sector="banking",
        filing_type="10-K",
        filed_date=date(2024, 2, 14),
        source_url="https://www.sec.gov/Archives/edgar/data/72971/000007297124000044/wfc-20231231.htm",
        article_title="Wells Fargo 10-K 2023",
        page_num=61,
    ),
]


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/retrieve", response_model=RetrieveResponse)
async def retrieve_endpoint(req: RetrieveRequest):
    chunks = _FAKE_CHUNKS[: req.k]
    return RetrieveResponse(chunks=chunks)
