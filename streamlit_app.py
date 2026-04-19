#!/usr/bin/env python3
"""Streamlit UI for the financial RAG `/chat` endpoint.

Run from the repo root (with .venv activated):

    streamlit run streamlit_app.py

Connection (server-side only; the browser never calls the RAG API directly):

- ``RAG_URL`` — retrieval service root (default ``http://localhost:8000``).
- ``API_KEY`` — optional; sent as ``X-API-Key`` when set.

Configure ``RAG_URL`` and optional ``API_KEY`` in the host environment (``.env``,
Streamlit Cloud secrets, etc.). They are not shown or editable in the UI.

Requires ``CORS_ORIGINS`` on the retrieval service when the API is on another
host.

Citations: inline ``[N]`` in the model answer jump to the matching retrieved
database chunk under Sources, where the original source article link is also
shown when available.

Chat-style UI: conversation history in ``st.session_state["messages"]``; model
reasoning is shown as Markdown inside an expander (not raw monospace).
"""

from __future__ import annotations

import html
import json
import os
import re
from pathlib import Path
from typing import Any

import markdown
import requests
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(
    page_title="SEC Filing Q&A",
    layout="wide",
    initial_sidebar_state="expanded",
)

DEFAULT_RAG = os.getenv("RAG_URL", "http://localhost:8000").rstrip("/")
DEFAULT_COMPANIES = ("",)
DEFAULT_FILINGS = ("",)
STATIC_CORPUS_COMPANIES = (
    "AAPL", "ABBV", "AMD", "AMGN", "AMZN", "BAC", "BMY", "C", "COP", "COST",
    "CRM", "CVS", "CVX", "DG", "EOG", "GILD", "GOOGL", "GS", "HD", "INTC",
    "JNJ", "JPM", "LLY", "LOW", "MCD", "META", "MPC", "MRK", "MS", "MSFT",
    "NKE", "NVDA", "OXY", "PFE", "PNC", "PSX", "PXD", "SBUX", "SCHW", "SLB",
    "TFC", "TGT", "TJX", "TSLA", "UNH", "USB", "VLO", "WFC", "WMT", "XOM",
)
STATIC_CORPUS_FILINGS = ("10-K", "10-Q")
STATIC_CORPUS_YEARS = tuple(range(2018, 2026))

_CITE_BRACKET_RE = re.compile(r"\[(\d+)\]")
# Model reasoning: lines like "* **Risk factors:**" → markdown headings
_REASON_HEAD_LINE = re.compile(
    r"^\s*[\*\-]\s+\*\*(.+?)\*\*\s*:\s*(.*)$",
)
_REASON_HEAD_ONLY = re.compile(r"^\s*[\*\-]\s+\*\*(.+?)\*\*\s*:?\s*$")
_REASON_BOLD_ONLY = re.compile(r"^\s*\*\*(.+?)\*\*\s*:?\s*$")

_FIN_CSS_VERSION = "11"

# Northeastern wordmark (Wikimedia Commons; trademark of Northeastern University).
_NU_LOGO = Path(__file__).resolve().parent / "assets" / "northeastern_wordmark.svg"


class ChatUserError(Exception):
    """Shown in the UI instead of a traceback."""


@st.cache_data(ttl=60)
def _load_filter_options(rag_url: str) -> dict[str, Any]:
    try:
        r = requests.get(f"{rag_url}/filters", timeout=(5.0, 20.0))
        r.raise_for_status()
        data = r.json()
        return {
            "companies": list(data.get("companies") or []),
            "filing_types": list(data.get("filing_types") or []),
            "fiscal_years": list(data.get("fiscal_years") or []),
            "metadata_source": "backend",
        }
    except (requests.RequestException, ValueError, json.JSONDecodeError):
        return {
            "companies": list(dict.fromkeys(STATIC_CORPUS_COMPANIES)),
            "filing_types": list(STATIC_CORPUS_FILINGS),
            "fiscal_years": list(STATIC_CORPUS_YEARS),
            "metadata_source": "static",
        }


def _corpus_summary_text(filter_options: dict[str, Any]) -> dict[str, str]:
    companies = [str(x) for x in filter_options.get("companies") or []]
    filing_types = [str(x) for x in filter_options.get("filing_types") or []]
    fiscal_years = [int(x) for x in filter_options.get("fiscal_years") or []]
    metadata_source = str(filter_options.get("metadata_source") or "backend")
    metadata_available = metadata_source == "backend"
    metadata_static = metadata_source == "static"

    company_count = len(companies)
    if company_count == 0:
        universe_label = "Unavailable" if not (metadata_available or metadata_static) else "0 tickers"
        company_help = (
            "Static demo corpus snapshot unavailable"
            if metadata_static
            else (
                "Live backend corpus metadata unavailable"
                if not metadata_available
                else "No tickers returned by backend metadata"
            )
        )
    elif company_count <= 6:
        universe_label = f"{company_count} tickers"
        company_help = ", ".join(companies)
    else:
        universe_label = f"{company_count} tickers"
        company_help = ", ".join(companies[:6]) + ", …"

    if fiscal_years:
        years_sorted = sorted(fiscal_years)
        if len(years_sorted) == 1:
            years_label = str(years_sorted[0])
        else:
            years_label = f"{years_sorted[0]}-{years_sorted[-1]}"
        years_help = ", ".join(str(y) for y in years_sorted)
    else:
        years_label = "Unavailable" if not (metadata_available or metadata_static) else "Unknown"
        years_help = (
            "Static demo corpus snapshot unavailable"
            if metadata_static
            else (
                "Live backend corpus metadata unavailable"
                if not metadata_available
                else "No fiscal-year metadata returned by backend"
            )
        )

    filing_label = ", ".join(filing_types) if filing_types else ("Unavailable" if not (metadata_available or metadata_static) else "Unknown")
    filing_help = (
        ", ".join(filing_types)
        if filing_types
        else (
            "Static demo corpus snapshot unavailable"
            if metadata_static
            else (
                "Live backend corpus metadata unavailable"
                if not metadata_available
                else "No filing-type metadata returned by backend"
            )
        )
    )
    if metadata_available:
        coverage_text = (
            f"The live backend currently exposes <strong>{universe_label}</strong> "
            f"for <strong>{years_label}</strong>."
        )
    elif metadata_static:
        coverage_text = (
            f"The landing page shows a <strong>static corpus snapshot</strong>: "
            f"<strong>{universe_label}</strong>, <strong>{filing_label}</strong>, and "
            f"<strong>{years_label}</strong>. Retrieval itself still comes from the backend."
        )
    else:
        coverage_text = (
            "Live backend corpus metadata is currently unavailable, so the UI omits the "
            "snapshot instead of showing stale placeholder values."
        )

    return {
        "universe_label": universe_label,
        "company_help": company_help,
        "filing_label": filing_label,
        "filing_help": filing_help,
        "years_label": years_label,
        "years_help": years_help,
        "coverage_text": coverage_text,
    }


def _prettify_reasoning_markdown(text: str) -> str:
    """Normalize chain-of-thought blobs (e.g. '* **Topic:** body') into readable markdown."""
    if not text or not text.strip():
        return text
    out: list[str] = []
    for line in text.splitlines():
        m = _REASON_HEAD_LINE.match(line)
        if m:
            title, rest = m.group(1).strip(), m.group(2).strip()
            out.append(f"#### {title}")
            if rest:
                out.append(rest)
            out.append("")
            continue
        m = _REASON_HEAD_ONLY.match(line)
        if m:
            out.append(f"#### {m.group(1).strip()}")
            out.append("")
            continue
        m = _REASON_BOLD_ONLY.match(line)
        if m:
            out.append(f"#### {m.group(1).strip()}")
            out.append("")
            continue
        out.append(line)
    return "\n".join(out).rstrip() + ("\n" if out else "")


def _inject_northeastern_theme() -> None:
    """One-time global CSS: Northeastern University brand colors (red #C8102E, black, gold accent)."""
    # Inject on every rerun. Streamlit rebuilds the DOM on rerun, so session-level
    # gating can leave pages unstyled after sidebar interactions (e.g., SSE toggle).
    st.markdown(
        """
<style>
@import url("https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600&family=Newsreader:opsz,wght@6..72,500;6..72,600&display=swap");

:root {
  --nu-red: #C8102E;
  --nu-red-dim: #9c0c25;
  --nu-black: #000000;
  --nu-gold: #A4804A;
  --nu-bg: #f5f5f5;
  --nu-paper: #ffffff;
  --nu-border: #d4d4d4;
  --nu-muted: #555555;
}

[data-testid="stAppViewContainer"],
[data-testid="stAppViewContainer"] > .main {
  background: var(--nu-bg) !important;
}

[data-testid="stHeader"] {
  background: var(--nu-black) !important;
  border-bottom: 3px solid var(--nu-red) !important;
  color: #ffffff !important;
}

[data-testid="stSidebar"] {
  background: var(--nu-paper) !important;
  border-right: 1px solid var(--nu-border) !important;
}

[data-testid="stSidebar"] .block-container {
  padding-top: 1.25rem;
}

[data-testid="stSidebar"] h1,
[data-testid="stSidebar"] h2,
[data-testid="stSidebar"] h3,
[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] p {
  color: var(--nu-black) !important;
}

[data-testid="stSidebar"] label,
[data-testid="stSidebar"] span[data-baseweb="typo"] {
  color: #333333 !important;
}

[data-testid="stSidebar"] .stSlider [data-baseweb="slider"] [role="slider"] {
  background-color: var(--nu-red) !important;
}

.fin-hero {
  background: var(--nu-paper);
  padding: 1.5rem 1.75rem 1.65rem;
  border-radius: 10px;
  margin-bottom: 1.25rem;
  border: 1px solid var(--nu-border);
  border-left: 5px solid var(--nu-red);
  box-shadow: 0 4px 24px rgba(0, 0, 0, 0.06);
}
.fin-hero-title {
  font-family: "Newsreader", Georgia, serif;
  font-size: clamp(1.75rem, 3vw, 2.2rem);
  font-weight: 600;
  margin: 0;
  line-height: 1.12;
  letter-spacing: -0.02em;
  color: var(--nu-black);
}
.fin-hero-lede {
  font-family: "IBM Plex Sans", sans-serif;
  font-size: clamp(0.98rem, 1.9vw, 1.08rem);
  font-weight: 500;
  margin: 0.55rem 0 0;
  line-height: 1.5;
  max-width: 64rem;
  color: #2a2a2a;
}
.fin-hero-body {
  font-family: "IBM Plex Sans", sans-serif;
  margin: 0.9rem 0 0;
  max-width: 64rem;
  line-height: 1.6;
  font-size: 0.94rem;
  color: #444444;
}
.fin-hero-motivation {
  font-family: "IBM Plex Sans", sans-serif;
  margin: 0.85rem 0 0;
  padding: 0.65rem 0.75rem 0.65rem 0.9rem;
  border-left: 3px solid var(--nu-red);
  max-width: 64rem;
  font-size: 0.88rem;
  line-height: 1.58;
  color: #333333;
  background: rgba(200, 16, 46, 0.06);
  border-radius: 0 8px 8px 0;
}
.fin-hero-motivation strong {
  color: var(--nu-red);
  font-weight: 600;
}
.fin-hero-rule {
  height: 3px;
  width: 72px;
  background: linear-gradient(90deg, var(--nu-red), rgba(200, 16, 46, 0.15));
  border-radius: 2px;
  margin-top: 1rem;
}

.fin-h2 {
  font-family: "Newsreader", Georgia, serif;
  font-size: 1.35rem;
  font-weight: 600;
  color: var(--nu-black);
  margin: 1.35rem 0 0.65rem;
  padding-bottom: 0.35rem;
  border-bottom: 2px solid var(--nu-red);
  letter-spacing: -0.01em;
}

.rag-answer-md {
  background: var(--nu-paper);
  border: 1px solid var(--nu-border);
  border-left: 4px solid var(--nu-red);
  padding: 1.2rem 1.45rem;
  border-radius: 0 10px 10px 0;
  line-height: 1.65;
  color: #1a1a1a;
  font-size: 1.02rem;
  box-shadow: 0 2px 12px rgba(0, 0, 0, 0.04);
}
.rag-answer-md a {
  color: var(--nu-red-dim);
  font-weight: 600;
  text-decoration: none;
  border-bottom: 1px dotted rgba(200, 16, 46, 0.45);
}
.rag-answer-md a:hover {
  color: var(--nu-black);
  border-bottom-color: var(--nu-black);
}
.rag-answer-md p { margin: 0.45rem 0; line-height: 1.6; }
.rag-answer-md p:first-child { margin-top: 0; }
.rag-answer-md p:last-child { margin-bottom: 0; }
.rag-answer-md h1, .rag-answer-md h2, .rag-answer-md h3, .rag-answer-md h4 {
  font-family: "IBM Plex Sans", sans-serif;
  color: var(--nu-black);
  font-weight: 600;
  margin: 0.85rem 0 0.4rem;
  line-height: 1.25;
}
.rag-answer-md h1 { font-size: 1.35rem; }
.rag-answer-md h2 { font-size: 1.2rem; border-bottom: 1px solid var(--nu-border); padding-bottom: 0.25rem; }
.rag-answer-md h3 { font-size: 1.08rem; color: #222; }
.rag-answer-md h4 { font-size: 1rem; color: #333; }
.rag-answer-md ul, .rag-answer-md ol { margin: 0.4rem 0 0.55rem 1.35rem; }
.rag-answer-md li { margin: 0.22rem 0; line-height: 1.55; }
.rag-answer-md strong { font-weight: 600; color: var(--nu-black); }
.rag-answer-md code {
  background: #f0f0f0;
  padding: 0.1rem 0.35rem;
  border-radius: 4px;
  font-size: 0.9em;
}
.rag-answer-md pre {
  background: #f5f5f5;
  border: 1px solid var(--nu-border);
  border-radius: 6px;
  padding: 0.65rem 0.85rem;
  overflow-x: auto;
  font-size: 0.88rem;
}
.rag-answer-md pre code { background: none; padding: 0; }
.rag-answer-md blockquote {
  border-left: 3px solid var(--nu-red);
  margin: 0.5rem 0;
  padding: 0.15rem 0 0.15rem 0.85rem;
  color: #333;
}
.rag-answer-md table { border-collapse: collapse; width: 100%; margin: 0.5rem 0; font-size: 0.92rem; }
.rag-answer-md th, .rag-answer-md td { border: 1px solid var(--nu-border); padding: 0.35rem 0.5rem; text-align: left; }
.rag-answer-md th { background: #f0f0f0; font-weight: 600; }
.cite-no-url, .cite-oob {
  color: var(--nu-muted);
  font-weight: 600;
}

[data-testid="stExpander"] {
  background: #ffffff !important;
  border: 1px solid var(--nu-border) !important;
  border-radius: 8px !important;
  margin-bottom: 0.45rem !important;
  box-shadow: 0 1px 4px rgba(0, 0, 0, 0.04);
}

.fin-chunk-meta-grid {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 0.4rem 0.9rem;
  margin: 0.2rem 0 0.75rem;
}
.fin-chunk-meta-item {
  font-size: 1rem;
  line-height: 1.45;
}
.fin-chunk-meta-label {
  color: var(--nu-red);
  font-weight: 700;
  margin-right: 0.35rem;
}
.fin-chunk-meta-value {
  color: #111;
  font-weight: 500;
  word-break: break-word;
}
.fin-chunk-doc-row {
  margin: 0.15rem 0 0.75rem;
  font-size: 1rem;
  line-height: 1.45;
}
.fin-chunk-doc-link {
  display: inline-block;
  background: var(--nu-red);
  color: #fff !important;
  border: 1px solid var(--nu-red);
  border-radius: 7px;
  padding: 0.36rem 0.68rem;
  font-weight: 600;
  text-decoration: none !important;
  font-size: 0.92rem;
  vertical-align: middle;
}
.fin-chunk-doc-link:hover {
  background: var(--nu-red-dim);
  border-color: var(--nu-red-dim);
  color: #fff !important;
}
.fin-chunk-text-wrap {
  border: 1px solid var(--nu-border);
  border-left: 4px solid var(--nu-red);
  border-radius: 0 8px 8px 0;
  background: #fcfcfc;
  padding: 0.7rem 0.8rem;
  margin: 0.35rem 0 0.75rem;
}
.fin-chunk-text-wrap pre {
  margin: 0;
  white-space: pre-wrap;
  word-break: break-word;
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
  font-size: 0.88rem;
  line-height: 1.5;
  color: #1f1f1f;
  background: transparent;
}
.fin-source-link {
  display: inline-block;
  background: var(--nu-red);
  color: #fff !important;
  border: 1px solid var(--nu-red);
  border-radius: 7px;
  padding: 0.42rem 0.7rem;
  font-weight: 600;
  text-decoration: none !important;
  font-size: 0.88rem;
}
.fin-source-link:hover {
  background: var(--nu-red-dim);
  border-color: var(--nu-red-dim);
  color: #fff !important;
}

.fin-footnote {
  font-size: 0.78rem;
  color: var(--nu-muted);
  margin-top: 2rem;
  padding-top: 1rem;
  border-top: 1px solid var(--nu-border);
  font-family: "IBM Plex Sans", sans-serif;
}


.fin-sidebar-brand {
  font-family: "Newsreader", Georgia, serif;
  font-size: 1.08rem;
  font-weight: 600;
  color: var(--nu-black) !important;
  letter-spacing: -0.02em;
  margin: 0.35rem 0 0.15rem;
}
.fin-sb-section {
  font-family: "IBM Plex Sans", sans-serif;
  font-size: 0.7rem;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.1em;
  color: var(--nu-red) !important;
  margin: 1.1rem 0 0.5rem !important;
}

.main .stForm {
  background: #ffffff;
  border: 1px solid var(--nu-border);
  border-radius: 10px;
  padding: 1.15rem 1.25rem 1.35rem;
  box-shadow: 0 4px 24px rgba(0, 0, 0, 0.05);
}
.main .stForm label p {
  font-family: "IBM Plex Sans", sans-serif;
  font-weight: 500;
  color: var(--nu-black);
}

.fin-empty-hint {
  font-family: "IBM Plex Sans", sans-serif;
  font-size: 0.92rem;
  color: var(--nu-muted);
  line-height: 1.55;
}

[data-testid="stMetricValue"] {
  font-size: 1.05rem !important;
  line-height: 1.25 !important;
  white-space: normal !important;
  overflow: visible !important;
  text-overflow: clip !important;
}

/* Chat: rely on Streamlit’s default chat UI; only gentle vertical rhythm */
[data-testid="stChatMessage"] {
  margin-bottom: 0.5rem !important;
}

.fin-thinking-wrap {
  background: linear-gradient(180deg, #fafafa 0%, #f0f0f0 100%);
  border: 1px solid var(--nu-border);
  border-radius: 8px;
  padding: 0.85rem 1rem;
  margin: 0.5rem 0 0.75rem;
  font-size: 0.92rem;
  line-height: 1.55;
  color: #444444;
}
.fin-thinking-wrap strong {
  color: var(--nu-black);
  font-weight: 600;
}
.fin-thinking-wrap p {
  margin: 0.35rem 0;
}
.fin-thinking-kicker {
  font-family: "IBM Plex Sans", sans-serif;
  font-size: 0.68rem;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.1em;
  color: var(--nu-red);
  margin: 0 0 0.5rem;
}
</style>
        """,
        unsafe_allow_html=True,
    )


def _render_sidebar_brand() -> None:
    if _NU_LOGO.is_file():
        st.image(str(_NU_LOGO), width="stretch")
    st.markdown(
        '<p class="fin-sidebar-brand">SEC Filing Q&amp;A</p>',
        unsafe_allow_html=True,
    )
    st.caption("CS 6120 NLP · Khoury College")
    st.caption("RAG · Citations · static corpus snapshot")


def _render_hero(summary: dict[str, str]) -> None:
    st.markdown(
        """
<div class="fin-hero">
  <p class="fin-hero-title">SEC Filing Q&amp;A</p>
  <p class="fin-hero-lede">Ask a question in plain English and the system searches real <strong>10-K</strong> and
  <strong>10-Q</strong> filings from EDGAR, answers from the retrieved evidence, and shows exactly where each claim came
  from with <strong>[1], [2], …</strong> citations.</p>
  <p class="fin-hero-motivation"><strong>Why this matters:</strong> SEC filings change every quarter, but a static LLM does
  not automatically absorb the latest disclosures. <strong>RAG</strong> closes that gap by pulling the right filing
  passages at query time, grounding the response in source text, and keeping the evidence trail visible from start to
  finish. It works the way a strong analyst does: go to the filing, isolate the relevant lines, and answer from the record.</p>
</div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown('<p class="fin-h2">Static Corpus Snapshot</p>', unsafe_allow_html=True)
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Universe", summary["universe_label"], help=summary["company_help"])
    with c2:
        st.metric("Filing types", summary["filing_label"], help=summary["filing_help"])
    with c3:
        st.metric("Fiscal data", summary["years_label"], help=summary["years_help"])
    with c4:
        st.metric(
            "Retrieval",
            "Hybrid vector + full-text",
            help="Alpha-weighted fusion of vector similarity and BM25/full-text search in the backend",
        )
    st.info(
        "**Try it** — Ask anything in the chat box. Each reply lists **Sources** you can expand; **[n]** in the answer "
        "jumps to the retrieved database chunk used for that citation, with a link back to the original source article.",
        icon="💡",
    )


def _render_empty_state(summary: dict[str, str]) -> None:
    st.markdown('<p class="fin-h2">Static Corpus Snapshot</p>', unsafe_allow_html=True)
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Universe", summary["universe_label"], help=summary["company_help"])
    with c2:
        st.metric("Filing types", summary["filing_label"], help=summary["filing_help"])
    with c3:
        st.metric("Fiscal data", summary["years_label"], help=summary["years_help"])
    with c4:
        st.metric(
            "Retrieval",
            "Hybrid vector + full-text",
            help="Alpha-weighted fusion of vector similarity and BM25/full-text search in the backend",
        )
    st.info(
        "**Try it** — Ask anything in the chat box. Each reply lists **Sources** you can expand. Click on **[n]** in the answer "
        "jumps to the retrieved database chunk used for that citation, with a link back to the original source article.",
        icon="💡",
    )


def _parse_detail(detail: Any) -> str:
    if isinstance(detail, str):
        return detail
    if isinstance(detail, list):
        parts = []
        for item in detail:
            if isinstance(item, dict) and "msg" in item:
                parts.append(str(item["msg"]))
            else:
                parts.append(json.dumps(item))
        return "; ".join(parts)
    return json.dumps(detail)


def _chunk_anchor_id(msg_id: int, chunk_index: int) -> str:
    return f"chunk-{msg_id}-{chunk_index}"


def _chunk_expander_key_suffix(
    msg_id: int,
    chunk_view_target: tuple[int, int] | None,
) -> str:
    if chunk_view_target is None:
        return "base"
    forced_msg_id, forced_chunk_index = chunk_view_target
    if msg_id != forced_msg_id:
        return "base"
    return f"cite-{forced_chunk_index}"


def _chunk_focus_href(msg_id: int, chunk_index: int) -> str:
    return f"#{_chunk_anchor_id(msg_id, chunk_index)}"


def _chunk_view_target_from_url() -> tuple[int, int] | None:
    """
    Parse URL params for:
      ?view=chunk&msg_id=<id>&chunk=<n>
    """
    if st.query_params.get("view") != "chunk":
        return None
    try:
        msg_id = int(str(st.query_params.get("msg_id", "")))
        chunk_index = int(str(st.query_params.get("chunk", "")))
    except (TypeError, ValueError):
        return None
    if msg_id < 0 or chunk_index < 1:
        return None
    return msg_id, chunk_index


def _render_chunk_view_url_cleanup(target_view_present: bool) -> None:
    """Remove view params from the URL via history.replaceState (no rerun)."""
    if not target_view_present:
        return
    st.components.v1.html(
        """
        <script>
          (() => {
            const url = new URL(window.location.href);
            if (url.searchParams.get('view') !== 'chunk') return;
            url.searchParams.delete('view');
            url.searchParams.delete('msg_id');
            url.searchParams.delete('chunk');
            const qs = url.searchParams.toString();
            const next = url.pathname + (qs ? ('?' + qs) : '');
            window.history.replaceState({}, document.title, next);
          })();
        </script>
        """,
        height=0,
        width=1,
    )


def _render_chunk_focus_bridge() -> None:
    # Invisible bridge component to execute JavaScript in the parent context.
    # Note: st.components.v1.html is used because it allows parent document access (if same-origin).
    st.components.v1.html(
        """
        <script>
        (() => {
          const root = window.top || window.parent || window;
          const doc = root.document;

          const setOpen = (details, open) => {
            if (!details) return;
            const wantOpen = !!open;
            if (!!details.open === wantOpen) return;

            // Toggle the native <details> state directly rather than dispatching a
            // synthetic click on the summary/button. A click bubbles into Streamlit's
            // React tree and can trigger a partial rerun that restores the scroll
            // position immediately after we scrolled — which is what causes the
            // "scrolls up then snaps back" feel on the first citation click.
            details.open = wantOpen;

            // Keep any aria-expanded attribute consistent so screen readers and our
            // own click listener's state-flip detector agree with the new open state.
            const btn = details.querySelector(":scope > summary button[aria-expanded], :scope > summary [aria-expanded]");
            if (btn) btn.setAttribute("aria-expanded", wantOpen ? "true" : "false");
            const summaryEl = details.querySelector(":scope > summary");
            if (summaryEl && summaryEl.hasAttribute("aria-expanded")) {
              summaryEl.setAttribute("aria-expanded", wantOpen ? "true" : "false");
            }
          };

          const getScrollParent = (el) => {
            if (!el) return null;
            let node = el.parentElement;
            while (node) {
              const style = root.getComputedStyle(node);
              const y = style.overflowY;
              if ((y === "auto" || y === "scroll") && node.scrollHeight > node.clientHeight) {
                return node;
              }
              node = node.parentElement;
            }
            return doc.scrollingElement || doc.documentElement;
          };

          const scrollChunkToTop = (el) => {
            if (!el) return;
            const doScroll = (instant) => {
              const scroller = getScrollParent(el);
              if (!scroller) return;
              const rect = el.getBoundingClientRect();
              const scrollerRect = scroller.getBoundingClientRect();
              const deltaTop = rect.top - scrollerRect.top;
              const top = scroller.scrollTop + deltaTop - 8;
              scroller.scrollTo({
                top: Math.max(top, 0),
                behavior: instant ? "auto" : "smooth",
              });
            };
            // Retry a few times because Streamlit may reflow after expander toggles.
            // Final attempt uses an instant jump so any late scroll-restore is
            // overridden with a precise landing at the chunk's top.
            doScroll(false);
            setTimeout(() => doScroll(false), 120);
            setTimeout(() => doScroll(false), 320);
            setTimeout(() => doScroll(true), 600);
          };

          // Auto-scroll when a chunk card is manually expanded by the user.
          // Guard against re-binding on every Streamlit rerun (the bridge re-injects).
          if (!root.__finChunkAutoScrollBound) {
            root.__finChunkAutoScrollBound = true;
            doc.addEventListener(
              "click",
              (ev) => {
                const t = ev.target;
                if (!t || !t.closest) return;
                // Streamlit expanders render as native <details><summary>…</summary>.
                // Some variants also expose a button[aria-expanded]. Match either.
                const toggler = t.closest("summary, button[aria-expanded]");
                if (!toggler) return;
                const details = toggler.closest("details");
                if (!details) return;
                // Only act on the OUTER chunk card (holds a .fin-chunk-marker inside).
                // The nested "Retrieved fields" details has no marker descendant.
                const marker = details.querySelector(
                  ".fin-chunk-marker[data-msg-id][data-chunk-index]"
                );
                if (!marker) return;
                // Capture state before the native toggle fires, then poll for the flip.
                const wasOpen = !!details.open;
                const tryScroll = (attempt) => {
                  if (details.open && !wasOpen) {
                    scrollChunkToTop(details);
                    return;
                  }
                  if (!details.open && wasOpen) return; // user collapsed, do nothing
                  if (attempt < 8) setTimeout(() => tryScroll(attempt + 1), 70);
                };
                setTimeout(() => tryScroll(0), 0);
              },
              true
            );
          }

          root.__finFocusChunk = (msgId, chunkIndex, anchorId) => {
            console.log(`[Bridge] Focusing chunk ${chunkIndex} for msg ${msgId}`);
            const collectDetails = () => {
              const markers = Array.from(
                doc.querySelectorAll(`.fin-chunk-marker[data-msg-id="${msgId}"][data-chunk-index]`)
              );
              let target = null;
              const others = new Set();
              markers.forEach((marker) => {
                const mIdx = Number(marker.getAttribute("data-chunk-index"));
                // Marker is inside the outer chunk expander, so the nearest <details> is
                // the one Streamlit uses for that chunk (not the nested "Retrieved fields").
                const details = marker.closest("details");
                if (!details) return;
                if (mIdx === chunkIndex) target = details;
                else others.add(details);
              });
              return { target, others };
            };

            let { target: targetDetails, others: toClose } = collectDetails();

            if (!targetDetails) {
              const anchor = doc.getElementById(anchorId);
              if (anchor) scrollChunkToTop(anchor);
              return false;
            }

            // Apply all layout changes synchronously BEFORE any scroll so the
            // first visible paint already has final geometry:
            //   1) Open the target chunk (native <details> toggles synchronously).
            //   2) Close every other chunk so their heights collapse.
            // Doing these in sequence (open first, then close others) avoids the
            // "scroll back up" flicker caused by collapsing siblings above the
            // target before the target had a chance to open and anchor the view.
            setOpen(targetDetails, true);
            toClose.forEach((d) => setOpen(d, false));

            // Scroll once the browser has laid out the new open/close state.
            requestAnimationFrame(() => scrollChunkToTop(targetDetails));

            // Retry after React / Streamlit's own state update may nudge the
            // layout once more (chunk content, inner expanders, etc.).
            setTimeout(() => {
              const refreshed = collectDetails();
              targetDetails = refreshed.target || targetDetails;
              refreshed.others.forEach((d) => setOpen(d, false));
              scrollChunkToTop(targetDetails);
            }, 220);

            return false;
          };

        })();
        </script>
        """,
        height=0,
        width=1,
    )


def _render_chunk_focus_request(chunk_view_target: tuple[int, int] | None) -> None:
    """After a citation-driven rerun, re-focus the target chunk in the browser."""
    if chunk_view_target is None:
        return
    msg_id, chunk_index = chunk_view_target
    anchor_id = _chunk_anchor_id(msg_id, chunk_index)
    st.components.v1.html(
        f"""
        <script>
        (() => {{
          const root = window.top || window.parent || window;
          const run = () => {{
            if (typeof root.__finFocusChunk !== "function") return;
            root.__finFocusChunk({msg_id}, {chunk_index}, {json.dumps(anchor_id)});
          }};
          setTimeout(run, 0);
          setTimeout(run, 150);
          setTimeout(run, 400);
        }})();
        </script>
        """,
        height=0,
        width=1,
    )


def _citation_link_html(msg_id: int, n: int, label: str, chunks: list[dict[str, Any]]) -> str:
    """Single ``[n]`` citation as a safe HTML link to the retrieved chunk."""
    esc_label = html.escape(label, quote=False)
    if 1 <= n <= len(chunks):
        chunk_id = str(chunks[n - 1].get("chunk_id") or "").strip()
        title = f"Jump to retrieved chunk {n}"
        if chunk_id:
            title += f" ({chunk_id})"
        anchor_id = _chunk_anchor_id(msg_id, n)
        href = _chunk_focus_href(msg_id, n)
        onclick = (
            "event.preventDefault();"
            "event.stopPropagation();"
            "const root=window.top||window.parent||window;"
            f"if(typeof root.__finFocusChunk==='function'){{root.__finFocusChunk({msg_id},{n},{json.dumps(anchor_id)});}}"
            "const url=new URL(root.location.href);"
            f"url.hash={json.dumps(anchor_id)};"
            "root.history.replaceState({}, root.document.title, url.toString());"
            "return false;"
        )
        return (
            f'<a href="{href}" '
            f'title="{html.escape(title, quote=True)}" '
            f'onclick="{html.escape(onclick, quote=True)}">{esc_label}</a>'
        )
    return f'<span class="cite-oob" title="Citation out of range">{esc_label}</span>'


def _markdown_with_citations(msg_id: int, text: str, chunks: list[dict[str, Any]]) -> str:
    """Render Markdown and inject chunk links for ``[n]`` citations."""
    if not text.strip():
        return ""
    slots: list[str] = []

    def _repl(m: re.Match[str]) -> str:
        idx = len(slots)
        n = int(m.group(1))
        slots.append(_citation_link_html(msg_id, n, m.group(0), chunks))
        return f"<!--CITE_SLOT_{idx}-->"

    masked = _CITE_BRACKET_RE.sub(_repl, text)
    body = markdown.markdown(
        masked,
        extensions=["extra", "nl2br", "sane_lists"],
        output_format="html5",
    )
    for i, fragment in enumerate(slots):
        body = body.replace(f"<!--CITE_SLOT_{i}-->", fragment)
    return body


def _chunk_record_for_display(chunk: dict[str, Any]) -> dict[str, Any]:
    return {
        "chunk_id": chunk.get("chunk_id"),
        "text": chunk.get("text"),
        "score": chunk.get("score"),
        "company": chunk.get("company"),
        "sector": chunk.get("sector"),
        "filing_type": chunk.get("filing_type"),
        "filed_date": chunk.get("filed_date"),
        "source_url": chunk.get("source_url"),
        "article_title": chunk.get("article_title"),
        "page_num": chunk.get("page_num"),
    }


def _render_chunk_card(
    msg_id: int,
    chunk_index: int,
    chunk: dict[str, Any],
    *,
    expanded: bool,
    chunk_view_target: tuple[int, int] | None,
) -> None:
    exp_key_suffix = _chunk_expander_key_suffix(msg_id, chunk_view_target)
    exp_key = f"fin-chunk-expander-{msg_id}-{chunk_index}-{exp_key_suffix}"
    fields_key = f"fin-chunk-fields-expander-{msg_id}-{chunk_index}"

    if chunk_view_target is not None:
        forced_msg_id, forced_chunk_index = chunk_view_target
        expanded = msg_id == forced_msg_id and chunk_index == forced_chunk_index

    score = float(chunk.get("score", 0))
    title = f"[{chunk_index}] {chunk.get('company', '')} · {chunk.get('filing_type', '')}  ·  relevance {score:.3f}"
    chunk_id = str(chunk.get("chunk_id") or "").strip() or "n/a"
    company = str(chunk.get("company") or "").strip() or "n/a"
    sector = str(chunk.get("sector") or "").strip() or "n/a"
    filing_type = str(chunk.get("filing_type") or "").strip() or "n/a"
    filed_date = str(chunk.get("filed_date") or "").strip() or "n/a"
    article_title = str(chunk.get("article_title") or "").strip() or "n/a"
    relevance = f"{score:.3f}"
    url = str(chunk.get("source_url") or "").strip()
    with st.expander(title, expanded=expanded, key=exp_key):
        st.markdown(
            (
                f'<span id="{html.escape(_chunk_anchor_id(msg_id, chunk_index), quote=True)}"></span>'
                f'<span id="chunk-marker-{msg_id}-{chunk_index}" class="fin-chunk-marker" '
                f'data-msg-id="{msg_id}" data-chunk-index="{chunk_index}" hidden></span>'
            ),
            unsafe_allow_html=True,
        )
        st.markdown(
            f"""
<div class="fin-chunk-meta-grid">
  <div class="fin-chunk-meta-item"><span class="fin-chunk-meta-label">Chunk ID:</span><span class="fin-chunk-meta-value">{html.escape(chunk_id)}</span></div>
  <div class="fin-chunk-meta-item"><span class="fin-chunk-meta-label">Company:</span><span class="fin-chunk-meta-value">{html.escape(company)}</span></div>
  <div class="fin-chunk-meta-item"><span class="fin-chunk-meta-label">Sector:</span><span class="fin-chunk-meta-value">{html.escape(sector)}</span></div>
  <div class="fin-chunk-meta-item"><span class="fin-chunk-meta-label">Filing type:</span><span class="fin-chunk-meta-value">{html.escape(filing_type)}</span></div>
  <div class="fin-chunk-meta-item"><span class="fin-chunk-meta-label">Filed date:</span><span class="fin-chunk-meta-value">{html.escape(filed_date)}</span></div>
  <div class="fin-chunk-meta-item"><span class="fin-chunk-meta-label">Relevance:</span><span class="fin-chunk-meta-value">{relevance}</span></div>
</div>
            """,
            unsafe_allow_html=True,
        )

        if url:
            st.markdown(
                f"""
<div class="fin-chunk-doc-row">
  <span class="fin-chunk-meta-label">Document:</span>
  <a class="fin-chunk-doc-link" href="{html.escape(url, quote=True)}" target="_blank" rel="noopener noreferrer">{html.escape(article_title)}</a>
</div>
                """,
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                f"""
<div class="fin-chunk-doc-row">
  <span class="fin-chunk-meta-label">Document:</span>
  <span class="fin-chunk-meta-value">{html.escape(article_title)}</span>
</div>
                """,
                unsafe_allow_html=True,
            )

        st.markdown("**Retrieved chunk text**")
        chunk_text = html.escape(str(chunk.get("text") or ""))
        st.markdown(
            f'<div class="fin-chunk-text-wrap"><pre>{chunk_text}</pre></div>',
            unsafe_allow_html=True,
        )

        with st.expander("Retrieved fields", expanded=False, key=fields_key):
            st.json(_chunk_record_for_display(chunk))


def _render_chunks(
    msg_id: int,
    chunks: list[dict[str, Any]],
    *,
    expanded_index: int | None,
    heading: str = "Retrieved chunks",
    chunk_view_target: tuple[int, int] | None,
) -> None:
    st.markdown(f'<p class="fin-h2">{html.escape(heading)}</p>', unsafe_allow_html=True)
    if not chunks:
        st.caption("No passages returned for this query.")
        return
    for i, c in enumerate(chunks, 1):
        _render_chunk_card(
            msg_id,
            i,
            c,
            expanded=expanded_index is not None and expanded_index == i,
            chunk_view_target=chunk_view_target,
        )


def _render_answer_body(msg_id: int, answer: str, chunks: list[dict[str, Any]]) -> None:
    if not answer.strip():
        st.caption("_No answer text returned._")
        return
    st.markdown(_markdown_with_citations(msg_id, answer, chunks), unsafe_allow_html=True)


def _render_reasoning_body(msg_id: int, thinking: str, chunks: list[dict[str, Any]]) -> None:
    st.markdown(_markdown_with_citations(msg_id, thinking, chunks), unsafe_allow_html=True)


def _render_assistant_turn(
    msg: dict[str, Any],
    chunk_view_target: tuple[int, int] | None,
) -> None:
    """One assistant message: optional reasoning, answer, sources."""
    msg_id = int(msg["id"])
    chunks: list[dict[str, Any]] = msg.get("chunks") or []
    answer = msg.get("answer") or ""
    thinking = msg.get("thinking") or ""
    err = msg.get("error")

    if err:
        st.error(err)
        return

    expanded_index = None
    if chunk_view_target is not None and chunk_view_target[0] == msg_id:
        expanded_index = chunk_view_target[1]

    _render_chunks(
        msg_id,
        chunks,
        expanded_index=expanded_index,
        heading="Retrieved chunks for this answer",
        chunk_view_target=chunk_view_target,
    )
    st.divider()
    _render_answer_body(msg_id, answer, chunks)
    if thinking.strip():
        with st.expander("Model reasoning", expanded=False):
            _render_reasoning_body(
                msg_id,
                _prettify_reasoning_markdown(thinking),
                chunks,
            )


def _next_message_id() -> int:
    # Keep ids strictly monotonic even if session hot-reloads or _msg_seq is missing.
    # Reused ids can cause Streamlit widget-key collisions and duplicate-looking renders.
    existing_max = 0
    for m in st.session_state.get("messages", []):
        try:
            existing_max = max(existing_max, int(m.get("id", 0)))
        except (TypeError, ValueError):
            continue
    st.session_state["_msg_seq"] = max(int(st.session_state.get("_msg_seq", 0)), existing_max) + 1
    return int(st.session_state["_msg_seq"])


def _sanitize_messages(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Drop duplicate ids, keeping the latest occurrence for each id."""
    out_reversed: list[dict[str, Any]] = []
    seen_ids: set[int] = set()
    for msg in reversed(messages):
        try:
            mid = int(msg.get("id", 0))
        except (TypeError, ValueError):
            mid = 0
        if mid in seen_ids:
            continue
        seen_ids.add(mid)
        out_reversed.append(msg)
    out_reversed.reverse()
    # Enforce chronological render order in case session list order drifted.
    return sorted(
        out_reversed,
        key=lambda m: int(m.get("id", 0)) if str(m.get("id", "")).lstrip("-").isdigit() else 0,
    )


def main() -> None:
    _inject_northeastern_theme()
    _render_chunk_focus_bridge()

    if "messages" not in st.session_state:
        st.session_state.messages = []

    chunk_view_target = _chunk_view_target_from_url()
    _render_chunk_view_url_cleanup(chunk_view_target is not None)

    rag_url = DEFAULT_RAG
    api_key = os.getenv("API_KEY", "").strip()
    filter_options = _load_filter_options(rag_url)
    summary = _corpus_summary_text(filter_options)
    companies = ("", *tuple(str(x) for x in filter_options.get("companies") or []))
    filings = ("", *tuple(str(x) for x in filter_options.get("filing_types") or []))

    with st.sidebar:
        _render_sidebar_brand()
        st.divider()
        st.markdown('<p class="fin-sb-section">Filters</p>', unsafe_allow_html=True)
        company = st.selectbox("Issuer", companies, format_func=lambda x: "All issuers" if x == "" else x)
        filing_type = st.selectbox("Filing", filings, format_func=lambda x: "All types" if x == "" else x)
        use_stream = st.toggle(
            "Stream answer (SSE)",
            value=True,
            help="Show reasoning and answer tokens live in the assistant bubble as they arrive.",
        )
        st.caption("Retrieval settings such as top-k and hybrid weighting are controlled by the backend.")

        st.divider()
        if st.button("Clear conversation", width="stretch"):
            st.session_state.messages = []
            st.session_state.pop("_msg_seq", None)
            st.session_state.pop("_pending_rag", None)
            st.query_params.clear()
            st.rerun()

        st.session_state["_fin_debug"] = st.toggle(
            "Debug: show message state",
            value=bool(st.session_state.get("_fin_debug", False)),
            help="Shows the raw session_state.messages and pending info, to diagnose duplicate-render bugs.",
        )

    prompt = st.chat_input(
        "Ask about 10-K or 10-Q filings…",
        disabled=st.session_state.get("_pending_rag") is not None,
    )
    if prompt:
        if st.session_state.get("_pending_rag") is not None:
            st.info("A response is still generating. Please wait for it to finish.")
            st.stop()
        q = prompt.strip()
        if not q:
            st.warning("Enter a question.")
        else:
            uid = _next_message_id()
            st.session_state.messages.append({"id": uid, "role": "user", "content": q})

            headers = {"Content-Type": "application/json"}
            if api_key.strip():
                headers["X-API-Key"] = api_key.strip()

            payload: dict[str, Any] = {
                "query": q,
                "stream": use_stream,
                "sector": None,
                "system_prompt": None,
            }
            if company:
                payload["company"] = company
            if filing_type:
                payload["filing_type"] = filing_type

            aid = _next_message_id()
            st.session_state["_pending_rag"] = {
                "rag_url": rag_url,
                "headers": headers,
                "payload": payload,
                "aid": aid,
                "use_stream": use_stream,
            }
            st.rerun()

    messages: list[dict[str, Any]] = _sanitize_messages(st.session_state.messages)
    if len(messages) != len(st.session_state.messages):
        st.session_state.messages = messages
    pending: dict[str, Any] | None = st.session_state.get("_pending_rag")

    # While a run is pending, render ONLY history BEFORE the current user question.
    # The current user question and the streaming assistant bubble are rendered inside the
    # pending block itself so they cannot be confused/reconciled with any prior turn.
    pending_user_id = 0
    pending_aid = 0
    if pending is not None:
        try:
            pending_aid = int(pending.get("aid", 0))
        except (TypeError, ValueError):
            pending_aid = 0
        if pending_aid > 0:
            user_ids = [
                int(m.get("id", 0))
                for m in messages
                if m.get("role") == "user" and int(m.get("id", 0)) < pending_aid
            ]
            pending_user_id = max(user_ids) if user_ids else 0
            messages = [m for m in messages if int(m.get("id", 0)) < pending_user_id]

    landing_placeholder = st.empty()

    if not messages and pending is None:
        with landing_placeholder.container():
            _render_hero(summary)
    elif messages and pending is None:
        landing_placeholder.empty()
        st.caption("Ask a follow-up below. Sidebar filters apply to your **next** message.")
    else:
        landing_placeholder.empty()

    # Render each historical message inside its own keyed container so Streamlit's
    # reconciler cannot merge/replay it against the pending streaming block. A keyed
    # container produces a stable, globally-unique widget path per message id.
    for msg in messages:
        role = msg.get("role", "assistant")
        try:
            mid = int(msg.get("id", 0))
        except (TypeError, ValueError):
            mid = 0
        msg_container = st.container(key=f"fin-msg-{role}-{mid}")
        with msg_container:
            with st.chat_message(role):
                if role == "user":
                    st.markdown(msg.get("content", ""))
                else:
                    _render_assistant_turn(msg, chunk_view_target)

    _render_chunk_focus_request(chunk_view_target)

    # --- Temporary diagnostic (remove once duplicate-A1 bug is confirmed fixed).
    if st.session_state.get("_fin_debug") or os.getenv("FIN_DEBUG") == "1":
        with st.expander("DEBUG: message state", expanded=False):
            st.json(
                {
                    "session_messages": [
                        {"id": m.get("id"), "role": m.get("role")}
                        for m in st.session_state.get("messages", [])
                    ],
                    "filtered_for_history": [
                        {"id": m.get("id"), "role": m.get("role")} for m in messages
                    ],
                    "pending": {
                        "aid": pending.get("aid") if pending else None,
                        "use_stream": pending.get("use_stream") if pending else None,
                    }
                    if pending
                    else None,
                    "pending_user_id": pending_user_id,
                    "pending_aid": pending_aid,
                }
            )

    if pending is not None:
        # Render the current user question here (removed from history loop above) so the
        # pending group is visually a single cohesive unit.
        current_user_msg = next(
            (m for m in st.session_state.messages if int(m.get("id", 0)) == pending_user_id),
            None,
        )
        if current_user_msg is not None:
            with st.chat_message("user"):
                st.markdown(current_user_msg.get("content", ""))

        # Be tolerant of stale/partial session payloads during sidebar-triggered reruns.
        rag_url_pending = str(pending.get("rag_url") or rag_url)
        headers_pending = pending.get("headers")
        if not isinstance(headers_pending, dict):
            headers_pending = {"Content-Type": "application/json"}
        payload_pending = pending.get("payload")
        if not isinstance(payload_pending, dict):
            payload_pending = {}
        aid = int(pending.get("aid") or _next_message_id())
        use_stream_pending = bool(pending.get("use_stream", use_stream))

        # IMPORTANT: do NOT use st.chat_message("assistant") here.
        # Streamlit reconciles successive same-type blocks across reruns by script
        # position, so the streaming bubble can briefly reuse DOM from the prior
        # assistant message (A1) — you see A1's chunks/answer flash under Q2 until
        # the first A2 token arrives. A keyed container with unique HTML wrappers
        # creates a distinct widget path that cannot reconcile against A1's.
        streaming_container = st.container(key=f"fin-streaming-{aid}")
        with streaming_container:
            chunks_ph = st.empty()
            think_ph = st.empty()
            ans_ph = st.empty()
            chunks_ph.markdown(
                '<p class="fin-h2">Retrieving passages for this answer…</p>',
                unsafe_allow_html=True,
            )
        try:
            if use_stream_pending:
                chunks, thinking, answer = _run_streaming_collect(
                    rag_url_pending,
                    headers_pending,
                    payload_pending,
                    message_id=aid,
                    thinking_placeholder=think_ph,
                    answer_placeholder=ans_ph,
                    chunks_placeholder=chunks_ph,
                )
            else:
                with st.spinner("Retrieving passages and generating an answer…"):
                    chunks, thinking, answer = _run_json_collect(
                        rag_url_pending,
                        headers_pending,
                        payload_pending,
                    )
                chunks_ph.empty()
                think_ph.empty()
                ans_ph.empty()
            st.session_state.messages.append(
                {
                    "id": aid,
                    "role": "assistant",
                    "answer": answer,
                    "thinking": thinking,
                    "chunks": chunks,
                    "error": None,
                }
            )
        except ChatUserError as e:
            st.session_state.messages.append(
                {
                    "id": aid,
                    "role": "assistant",
                    "answer": "",
                    "thinking": "",
                    "chunks": [],
                    "error": str(e),
                }
            )
        st.session_state.pop("_pending_rag", None)
        st.rerun()

    if not messages and st.session_state.get("_pending_rag") is None:
        st.markdown(
            '<p class="fin-footnote">For academic / research use. Not investment advice. '
            "Verify material facts against official SEC filings.</p>",
            unsafe_allow_html=True,
        )


def _run_json_collect(
    rag_url: str,
    headers: dict[str, str],
    payload: dict[str, Any],
) -> tuple[list[dict[str, Any]], str, str]:
    body = {**payload, "stream": False}
    try:
        r = requests.post(f"{rag_url}/chat", headers=headers, json=body, timeout=(15.0, 300.0))
    except requests.RequestException as e:
        raise ChatUserError(f"Request failed: {e}") from e
    if r.status_code != 200:
        try:
            raise ChatUserError(_parse_detail(r.json().get("detail", r.text)))
        except json.JSONDecodeError:
            raise ChatUserError(r.text or f"HTTP {r.status_code}") from None
    data = r.json()
    chunks = data.get("chunks") or []
    if not isinstance(chunks, list):
        chunks = []
    thinking = data.get("thinking") or ""
    if thinking is None:
        thinking = ""
    answer = data.get("answer", "") or ""
    return chunks, str(thinking), str(answer)


def _run_streaming_collect(
    rag_url: str,
    headers: dict[str, str],
    payload: dict[str, Any],
    *,
    message_id: int,
    thinking_placeholder: Any | None = None,
    answer_placeholder: Any | None = None,
    chunks_placeholder: Any | None = None,
) -> tuple[list[dict[str, Any]], str, str]:
    headers = {**headers, "Accept": "text/event-stream"}
    thinking_text = ""
    answer_text = ""
    chunks_out: list[dict[str, Any]] = []
    current_event: str | None = None
    live_ui = answer_placeholder is not None
    status = None if live_ui else st.status("Running retrieval & generation…", expanded=True)

    def _note(msg: str) -> None:
        if status is not None:
            status.write(msg)

    try:
        with requests.post(
            f"{rag_url}/chat",
            headers=headers,
            json=payload,
            stream=True,
            timeout=(15.0, 300.0),
        ) as r:
            if r.status_code != 200:
                try:
                    raise ChatUserError(_parse_detail(r.json().get("detail", r.text)))
                except json.JSONDecodeError:
                    raise ChatUserError(r.text or f"HTTP {r.status_code}") from None

            for raw_line in r.iter_lines(decode_unicode=True):
                if raw_line is None:
                    continue
                if raw_line == "":
                    current_event = None
                    continue
                if raw_line.startswith(":"):
                    continue
                if raw_line.startswith("event:"):
                    current_event = raw_line[6:].strip()
                    continue
                if not raw_line.startswith("data:"):
                    continue
                data_str = raw_line[5:].strip()
                if not data_str:
                    continue
                try:
                    data = json.loads(data_str)
                except json.JSONDecodeError:
                    continue

                if current_event == "chunks":
                    chunks_out = data if isinstance(data, list) else []
                    n = len(chunks_out)
                    _note(f"**{n}** passages retrieved from filing store.")
                    if chunks_placeholder is not None:
                        with chunks_placeholder.container():
                            _render_chunks(
                                message_id,
                                chunks_out,
                                expanded_index=None,
                                heading="Retrieved chunks for this answer",
                                chunk_view_target=None,
                            )
                elif current_event == "thinking":
                    delta = data.get("delta", "") if isinstance(data, dict) else ""
                    thinking_text += delta
                    if thinking_placeholder is not None and thinking_text.strip():
                        thinking_placeholder.markdown(
                            "<strong>Reasoning ... </strong>"
                            f'{_markdown_with_citations(message_id, _prettify_reasoning_markdown(thinking_text), chunks_out)}',
                            unsafe_allow_html=True,
                        )
                elif current_event == "answer":
                    delta = data.get("delta", "") if isinstance(data, dict) else ""
                    answer_text += delta
                    if answer_placeholder is not None:
                        answer_placeholder.markdown(
                            "<strong>Answering ... </strong>"
                            f'{_markdown_with_citations(message_id, answer_text, chunks_out)}',
                            unsafe_allow_html=True,
                        )
                elif current_event == "error":
                    msg = data.get("message", "stream error") if isinstance(data, dict) else "stream error"
                    raise ChatUserError(msg)
                elif current_event == "done":
                    break
    except requests.RequestException as e:
        raise ChatUserError(f"Request failed: {e}") from e

    if status is not None:
        if thinking_text.strip():
            status.write("Model reasoning received.")
        if answer_text.strip():
            status.write("Draft answer complete.")
        else:
            status.write("No answer text (check LLM / SGLANG_BASE_URL).")
        status.update(label="Run complete", state="complete")

    if live_ui:
        if thinking_placeholder is not None:
            if thinking_text.strip():
                thinking_placeholder.markdown(
                    "<strong>Reasoning (complete)</strong>"
                    f'{_markdown_with_citations(message_id, _prettify_reasoning_markdown(thinking_text), chunks_out)}',
                    unsafe_allow_html=True,
                )
            else:
                thinking_placeholder.empty()
        if answer_placeholder is not None:
            if answer_text.strip():
                answer_placeholder.markdown(
                    "<strong>Answer (complete)</strong>"
                    f'{_markdown_with_citations(message_id, answer_text, chunks_out)}',
                    unsafe_allow_html=True,
                )
            else:
                answer_placeholder.caption("_No answer text returned._")

    return chunks_out, thinking_text, answer_text


if __name__ == "__main__":
    main()
