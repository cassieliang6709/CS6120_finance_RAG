from datetime import date
from typing import Optional
from pydantic import BaseModel, Field


class RetrieveRequest(BaseModel):
    query: str = Field(..., min_length=1)
    k: int = Field(default=5, ge=1)
    alpha: float = Field(default=0.7, ge=0.0, le=1.0)
    sector: Optional[str] = None
    company: Optional[str] = None
    filing_type: Optional[str] = None


class ChunkResult(BaseModel):
    chunk_id: str
    text: str
    score: float
    company: str
    sector: str
    filing_type: str
    filed_date: Optional[date] = None
    source_url: Optional[str] = None
    article_title: Optional[str] = None
    page_num: Optional[int] = None


class RetrieveResponse(BaseModel):
    chunks: list[ChunkResult]
