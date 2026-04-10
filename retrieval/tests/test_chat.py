"""Unit tests for chat.py helpers (no DB, no LLM, no network)."""
from __future__ import annotations

import os
import sys
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from chat import DEFAULT_SYSTEM_PROMPT, build_messages, format_context
from models import ChunkResult


def _chunk(chunk_id: str, text: str, company: str = "AAPL", **kw) -> ChunkResult:
    defaults = dict(
        chunk_id=chunk_id,
        text=text,
        score=0.9,
        company=company,
        sector="technology",
        filing_type="10-K",
        filed_date=None,
        source_url=None,
        article_title=None,
        page_num=None,
    )
    defaults.update(kw)
    return ChunkResult(**defaults)


def test_format_context_numbers_blocks_sequentially():
    chunks = [
        _chunk("a", "Apple revenue grew 8%.", company="AAPL"),
        _chunk("b", "JPM net interest income rose.", company="JPM"),
    ]
    out = format_context(chunks)
    assert "[1] AAPL 10-K" in out
    assert "[2] JPM 10-K" in out
    assert "Apple revenue grew 8%." in out
    assert "JPM net interest income rose." in out
    # Blocks should be separated by the delimiter
    assert "---" in out


def test_format_context_includes_filed_date_when_present():
    chunks = [_chunk("a", "text", filed_date=date(2023, 11, 3))]
    out = format_context(chunks)
    assert "2023-11-03" in out


def test_format_context_omits_filed_date_when_missing():
    chunks = [_chunk("a", "text", filed_date=None)]
    out = format_context(chunks)
    assert "(filed" not in out


def test_format_context_empty_list():
    assert format_context([]) == ""


def test_build_messages_uses_default_system_prompt():
    msgs = build_messages("What is ROE?", "[1] AAPL 10-K\nsome text", None)
    assert len(msgs) == 2
    assert msgs[0]["role"] == "system"
    assert msgs[0]["content"] == DEFAULT_SYSTEM_PROMPT
    assert msgs[1]["role"] == "user"
    assert "What is ROE?" in msgs[1]["content"]
    assert "[1] AAPL 10-K" in msgs[1]["content"]


def test_build_messages_custom_system_prompt_override():
    custom = "You are a lawyer. Be precise."
    msgs = build_messages("Q", "ctx", custom)
    assert msgs[0]["content"] == custom


def test_build_messages_user_content_contains_context_and_question():
    msgs = build_messages("Why did revenue drop?", "CONTEXT_BLOCK", None)
    user_content = msgs[1]["content"]
    assert "CONTEXT_BLOCK" in user_content
    assert "Why did revenue drop?" in user_content
    # Question should appear after the context
    assert user_content.index("CONTEXT_BLOCK") < user_content.index("Why did revenue drop?")
