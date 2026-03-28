"""
Text chunker
=============
Splits section text into overlapping token-window chunks while trying not
to break sentences mid-way.  Uses the HuggingFace AutoTokenizer for the
all-MiniLM-L6-v2 model so token counts are consistent with the embedder.

Chunk strategy
--------------
1. Tokenise the full section text.
2. Walk through tokens in steps of (CHUNK_SIZE - OVERLAP), collecting
   CHUNK_SIZE-token windows.
3. For each window, decode to text and then *trim to the nearest sentence
   boundary* so chunks don't end mid-sentence.
4. Return (section_name, chunk_text, token_count) tuples.
"""

from __future__ import annotations

import logging
import re
from typing import Sequence

from transformers import AutoTokenizer, PreTrainedTokenizerBase

from data_pipeline.config import (
    CHUNK_OVERLAP_TOKENS,
    CHUNK_SIZE_TOKENS,
    EMBEDDING_MODEL,
)

logger = logging.getLogger(__name__)

# Sentence-boundary regex: split on . ! ? followed by whitespace + capital
_SENTENCE_END_RE = re.compile(r"(?<=[.!?])\s+(?=[A-Z\"\'])")

# Minimum chunk length (characters) to bother embedding
MIN_CHUNK_CHARS = 50

# Type alias
ChunkTuple = tuple[str, str, int]  # (section_name, text, token_count)


# ---------------------------------------------------------------------------
# Tokeniser singleton (load once per process)
# ---------------------------------------------------------------------------

_tokenizer: PreTrainedTokenizerBase | None = None


def get_tokenizer(model_name: str = EMBEDDING_MODEL) -> PreTrainedTokenizerBase:
    """Return a cached tokenizer for *model_name*."""
    global _tokenizer
    if _tokenizer is None:
        logger.info("Loading tokenizer: %s", model_name)
        _tokenizer = AutoTokenizer.from_pretrained(model_name)
    return _tokenizer


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _trim_to_sentence(text: str) -> str:
    """
    Trim *text* so it ends at a sentence boundary (. ! ?).
    If no sentence boundary is found, return *text* unchanged.
    """
    # Find the last sentence-ending punctuation
    for m in reversed(list(re.finditer(r"[.!?]['\"]?", text))):
        candidate = text[: m.end()].strip()
        if len(candidate) >= MIN_CHUNK_CHARS:
            return candidate
    return text.strip()


def _start_at_sentence(text: str) -> str:
    """
    Drop leading text that is a fragment (doesn't start with a capital
    letter or quotation mark).  Best-effort only.
    """
    text = text.lstrip()
    if not text:
        return text
    # If it already starts with a capital, return as-is
    if text[0].isupper() or text[0] in ('"', "'", "("):
        return text
    # Otherwise, find the first sentence start
    m = _SENTENCE_END_RE.search(text)
    if m:
        return text[m.end():].lstrip()
    return text


# ---------------------------------------------------------------------------
# Chunker
# ---------------------------------------------------------------------------

class Chunker:
    """
    Split text into overlapping token windows, trimming at sentence boundaries.

    Parameters
    ----------
    chunk_size:
        Target number of tokens per chunk.
    overlap:
        Number of tokens of overlap between consecutive chunks.
    model_name:
        HuggingFace model name used to load the tokenizer.
    """

    def __init__(
        self,
        chunk_size: int = CHUNK_SIZE_TOKENS,
        overlap: int = CHUNK_OVERLAP_TOKENS,
        model_name: str = EMBEDDING_MODEL,
    ) -> None:
        if overlap >= chunk_size:
            raise ValueError("overlap must be less than chunk_size")
        self.chunk_size = chunk_size
        self.overlap = overlap
        self.tokenizer = get_tokenizer(model_name)

    # ------------------------------------------------------------------
    # Single section
    # ------------------------------------------------------------------

    def chunk_section(
        self,
        section_name: str,
        text: str,
    ) -> list[ChunkTuple]:
        """
        Chunk a single section of text.

        Parameters
        ----------
        section_name:
            Human-readable section label (e.g. 'MD&A').
        text:
            The section's plain text.

        Returns
        -------
        list[ChunkTuple]
            Each element is (section_name, chunk_text, token_count).
        """
        text = text.strip()
        if len(text) < MIN_CHUNK_CHARS:
            return []

        # Tokenise without special tokens so we can safely slice
        token_ids: list[int] = self.tokenizer.encode(
            text,
            add_special_tokens=False,
            truncation=False,
        )

        total_tokens = len(token_ids)
        if total_tokens == 0:
            return []

        # If the whole section fits in one chunk, return it directly
        if total_tokens <= self.chunk_size:
            decoded = self.tokenizer.decode(token_ids, skip_special_tokens=True)
            decoded = decoded.strip()
            if len(decoded) >= MIN_CHUNK_CHARS:
                return [(section_name, decoded, total_tokens)]
            return []

        step = self.chunk_size - self.overlap
        chunks: list[ChunkTuple] = []
        start = 0

        while start < total_tokens:
            end = min(start + self.chunk_size, total_tokens)
            window_ids = token_ids[start:end]

            raw_text = self.tokenizer.decode(window_ids, skip_special_tokens=True)
            raw_text = raw_text.strip()

            # Trim to sentence boundaries
            if end < total_tokens:
                # Not the last chunk: trim trailing fragment
                trimmed = _trim_to_sentence(raw_text)
            else:
                # Last chunk: keep as-is (it ends at the document end)
                trimmed = raw_text

            # Skip leading fragment on all but the first chunk
            if start > 0:
                trimmed = _start_at_sentence(trimmed)

            if len(trimmed) >= MIN_CHUNK_CHARS:
                # Re-tokenise the trimmed text to get the accurate token count
                actual_ids = self.tokenizer.encode(
                    trimmed,
                    add_special_tokens=False,
                    truncation=False,
                )
                chunks.append((section_name, trimmed, len(actual_ids)))

            start += step
            if end == total_tokens:
                break

        return chunks

    # ------------------------------------------------------------------
    # Multiple sections
    # ------------------------------------------------------------------

    def chunk_sections(
        self,
        sections: Sequence[tuple[str, str]],
    ) -> list[ChunkTuple]:
        """
        Chunk a list of (section_name, text) pairs.

        Parameters
        ----------
        sections:
            Typically the output of ``HTMLCleaner.clean()``.

        Returns
        -------
        list[ChunkTuple]
            Flat list of (section_name, chunk_text, token_count).
        """
        all_chunks: list[ChunkTuple] = []
        for section_name, text in sections:
            section_chunks = self.chunk_section(section_name, text)
            all_chunks.extend(section_chunks)
            logger.debug(
                "Section '%s': %d chunks from %d chars",
                section_name,
                len(section_chunks),
                len(text),
            )
        return all_chunks

    # ------------------------------------------------------------------
    # Convenience for transcript text
    # ------------------------------------------------------------------

    def chunk_transcript_section(
        self,
        section: str,
        text: str,
    ) -> list[ChunkTuple]:
        """
        Convenience wrapper for earnings call transcript sections.
        Identical to ``chunk_section`` but exposed with an explicit name.
        """
        return self.chunk_section(section, text)


# ---------------------------------------------------------------------------
# Module-level convenience
# ---------------------------------------------------------------------------

def chunk_text(
    sections: Sequence[tuple[str, str]],
    chunk_size: int = CHUNK_SIZE_TOKENS,
    overlap: int = CHUNK_OVERLAP_TOKENS,
) -> list[ChunkTuple]:
    """Chunk a list of (section_name, text) pairs with default settings."""
    chunker = Chunker(chunk_size=chunk_size, overlap=overlap)
    return chunker.chunk_sections(sections)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO)
    sample_text = """
    Item 7. Management's Discussion and Analysis of Financial Condition
    and Results of Operations.

    Overview
    --------
    Our revenue for fiscal year 2023 increased 12% year-over-year, driven
    primarily by strong growth in our cloud services segment.  Operating
    income expanded by 200 basis points as we achieved operating leverage.
    We generated $4.2 billion in free cash flow during the year.

    Segment Results
    ---------------
    Cloud Services revenue grew 34% to $18.5 billion, fueled by enterprise
    adoption of our platform.  Margins in this segment improved to 38%
    from 33% in the prior year.
    """
    chunker = Chunker()
    chunks = chunker.chunk_section("MD&A", sample_text)
    for i, (sec, text, tok) in enumerate(chunks):
        print(f"Chunk {i+1} [{sec}] ({tok} tokens): {text[:120]}...")
