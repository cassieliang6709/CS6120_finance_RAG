"""
SEC HTML/XBRL cleaner
========================
Parses SEC EDGAR filing documents (10-K, 10-Q, 8-K) and returns a list of
(section_name, clean_text) tuples.  The cleaner:

  1. Removes boilerplate: XBRL inline tags, style/script elements, cover
     pages, signature blocks, and exhibit indexes.
  2. Detects and labels sections by their Item numbers.
  3. Maps Item numbers to human-readable names.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Generator

from bs4 import BeautifulSoup, NavigableString, Tag
from lxml import etree

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Section name mappings
# ---------------------------------------------------------------------------

SECTION_MAP_10K: dict[str, str] = {
    "item 1": "Business Description",
    "item 1a": "Risk Factors",
    "item 1b": "Unresolved Staff Comments",
    "item 2": "Properties",
    "item 3": "Legal Proceedings",
    "item 4": "Mine Safety Disclosures",
    "item 5": "Market for Common Equity",
    "item 6": "Selected Financial Data",
    "item 7": "MD&A",
    "item 7a": "Quantitative and Qualitative Disclosures about Market Risk",
    "item 8": "Financial Statements",
    "item 9": "Changes in and Disagreements with Accountants",
    "item 9a": "Controls and Procedures",
    "item 9b": "Other Information",
    "item 10": "Directors and Corporate Governance",
    "item 11": "Executive Compensation",
    "item 12": "Security Ownership",
    "item 13": "Certain Relationships",
    "item 14": "Principal Accountant Fees",
    "item 15": "Exhibits",
}

SECTION_MAP_10Q: dict[str, str] = {
    "item 1": "Financial Statements",
    "item 2": "MD&A",
    "item 3": "Quantitative and Qualitative Disclosures about Market Risk",
    "item 4": "Controls and Procedures",
    "item 1 legal": "Legal Proceedings",
    "item 1a": "Risk Factors",
    "item 2 issuer": "Unregistered Sales of Equity Securities",
    "item 3": "Defaults upon Senior Securities",
    "item 4": "Mine Safety Disclosures",
    "item 5": "Other Information",
    "item 6": "Exhibits",
}

SECTION_MAP_8K: dict[str, str] = {
    "item 1.01": "Entry into a Material Definitive Agreement",
    "item 1.02": "Termination of a Material Definitive Agreement",
    "item 2.01": "Completion of Acquisition or Disposition",
    "item 2.02": "Results of Operations and Financial Condition",
    "item 2.03": "Creation of a Direct Financial Obligation",
    "item 4.01": "Changes in Registrant's Certifying Accountant",
    "item 5.02": "Departure of Directors or Officers",
    "item 7.01": "Regulation FD Disclosure",
    "item 8.01": "Other Events",
    "item 9.01": "Financial Statements and Exhibits",
}

# Patterns matching SEC cover page boilerplate
COVER_PAGE_PATTERNS = [
    re.compile(r"UNITED STATES\s+SECURITIES AND EXCHANGE COMMISSION", re.I | re.S),
    re.compile(r"Washington,\s*D\.C\.\s*20549", re.I),
    re.compile(r"FORM\s+10-[KQ]\s*\n", re.I),
]

# Patterns for signature blocks
SIGNATURE_PATTERNS = [
    re.compile(r"SIGNATURES?\s*\n", re.I),
    re.compile(r"Pursuant to the requirements of.*Securities Exchange Act", re.I | re.S),
]

# Exhibit index patterns
EXHIBIT_PATTERNS = [
    re.compile(r"EXHIBIT\s+INDEX\s*\n", re.I),
    re.compile(r"List of Exhibits\s*\n", re.I),
]

# XBRL inline tags to unwrap (keep their text content)
XBRL_TAGS_UNWRAP = {
    "ix:nonnumeric",
    "ix:nonfraction",
    "ix:continuation",
    "xbrli:period",
    "xbrli:instant",
}

# XBRL tags to remove entirely (no useful text)
XBRL_TAGS_REMOVE = {
    "ix:header",
    "ix:hidden",
    "ix:resources",
    "xbrli:xbrl",
    "xbrl:context",
    "link:linkbase",
}

# General boilerplate phrases to strip
BOILERPLATE_PHRASES = [
    re.compile(r"Table of Contents", re.I),
    re.compile(r"^\s*Page\s*$", re.I | re.M),
    re.compile(r"^\s*F-\d+\s*$", re.M),  # Financial statement page numbers
]


# ---------------------------------------------------------------------------
# Section detection
# ---------------------------------------------------------------------------

# Matches headings like "ITEM 1A.", "Item 7 —", "ITEM 1.", etc.
_ITEM_HEADER_RE = re.compile(
    r"^\s*(ITEM\s+(\d+[AB]?(?:\.\d+)?))[\s\.\-–—:]+(.{0,80})?$",
    re.I | re.M,
)


def _normalize_item(raw: str) -> str:
    """Normalize an item label to lowercase for map lookup, e.g. 'ITEM 1A' -> 'item 1a'."""
    raw = raw.replace("\xa0", " ")
    raw = re.sub(r"\s+", " ", raw).strip().lower()
    return raw


def _map_section(item_label: str, filing_type: str) -> str:
    """
    Map an item label to a human-readable section name.
    Returns the raw item label if no mapping is found.
    """
    key = _normalize_item(item_label)
    if filing_type == "10-K":
        return SECTION_MAP_10K.get(key, item_label.title())
    elif filing_type == "10-Q":
        return SECTION_MAP_10Q.get(key, item_label.title())
    elif filing_type == "8-K":
        return SECTION_MAP_8K.get(key, item_label.title())
    return item_label.title()


# ---------------------------------------------------------------------------
# HTML cleaning
# ---------------------------------------------------------------------------

class HTMLCleaner:
    """
    Parse and clean an SEC EDGAR HTML/text filing.

    Parameters
    ----------
    filing_type:
        One of '10-K', '10-Q', or '8-K'.  Affects section name mapping.
    min_section_length:
        Minimum character count for a section to be included in output.
    """

    def __init__(
        self,
        filing_type: str = "10-K",
        min_section_length: int = 200,
    ) -> None:
        self.filing_type = filing_type.upper()
        self.min_section_length = min_section_length

    # ------------------------------------------------------------------
    # Parsing
    # ------------------------------------------------------------------

    def _load_soup(self, content: str | bytes) -> BeautifulSoup:
        """Parse HTML using lxml, with BeautifulSoup as interface."""
        try:
            soup = BeautifulSoup(content, "lxml")
        except Exception:
            soup = BeautifulSoup(content, "html.parser")
        return soup

    def _extract_primary_document(self, content: str | bytes) -> str | bytes:
        """
        When SEC filings are stored as ``full-submission.txt``, extract the main
        filing document (the 10-K / 10-Q body) instead of parsing the entire
        multi-document submission with exhibits attached.
        """
        if isinstance(content, bytes):
            text = content.decode("utf-8", errors="ignore")
        else:
            text = content

        if "<DOCUMENT>" not in text.upper():
            return content

        target_type = self.filing_type.upper()
        document_blocks = re.findall(r"<DOCUMENT>(.*?)</DOCUMENT>", text, re.I | re.S)
        for block in document_blocks:
            type_match = re.search(r"<TYPE>\s*([^\n\r<]+)", block, re.I)
            if not type_match:
                continue
            doc_type = type_match.group(1).strip().upper()
            if doc_type != target_type:
                continue

            text_match = re.search(r"<TEXT>(.*)", block, re.I | re.S)
            if text_match:
                return text_match.group(1).strip()

        return content

    def _strip_xbrl(self, soup: BeautifulSoup) -> None:
        """
        Remove XBRL-specific tags.  Tags in XBRL_TAGS_REMOVE are deleted
        entirely; tags in XBRL_TAGS_UNWRAP have their tag removed but their
        text content preserved.
        """
        # Lower-case tag names for matching
        for tag in soup.find_all(True):
            name = tag.name.lower() if tag.name else ""
            if name in XBRL_TAGS_REMOVE:
                tag.decompose()
            elif name in XBRL_TAGS_UNWRAP:
                tag.unwrap()

    def _strip_noise(self, soup: BeautifulSoup) -> None:
        """Remove script, style, and other non-content tags."""
        for tag in soup.find_all(
            ["script", "style", "meta", "link", "noscript", "iframe", "img"]
        ):
            tag.decompose()

    def _extract_text(self, soup: BeautifulSoup) -> str:
        """
        Extract visible text from the cleaned soup.
        Paragraphs are separated by double newlines.
        """
        texts: list[str] = []
        for element in soup.recursiveChildGenerator():
            if isinstance(element, NavigableString):
                text = str(element).strip()
                if text:
                    texts.append(text)
            elif isinstance(element, Tag) and element.name in {
                "p", "div", "tr", "br", "h1", "h2", "h3", "h4", "h5", "li"
            }:
                texts.append("\n")

        raw = " ".join(texts)
        # Collapse excessive whitespace while preserving paragraph breaks
        raw = re.sub(r"[ \t]+", " ", raw)
        raw = re.sub(r"\n{3,}", "\n\n", raw)
        return raw.strip()

    # ------------------------------------------------------------------
    # Boilerplate removal
    # ------------------------------------------------------------------

    def _remove_cover_page(self, text: str) -> str:
        """
        Remove the SEC cover page section (everything before the first
        actual business content heading).
        """
        for pattern in COVER_PAGE_PATTERNS:
            m = pattern.search(text)
            if m:
                # Find the next Item heading after the cover page
                item_m = _ITEM_HEADER_RE.search(text, m.end())
                if item_m:
                    return text[item_m.start():]
        return text

    def _remove_signature_block(self, text: str) -> str:
        """Truncate text at the first signature block."""
        for pattern in SIGNATURE_PATTERNS:
            m = pattern.search(text)
            if m and m.start() > max(5_000, int(len(text) * 0.5)):
                return text[: m.start()].rstrip()
        return text

    def _remove_exhibit_index(self, text: str) -> str:
        """Truncate text at the exhibit index."""
        for pattern in EXHIBIT_PATTERNS:
            m = pattern.search(text)
            if m and m.start() > max(5_000, int(len(text) * 0.5)):
                return text[: m.start()].rstrip()
        return text

    def _strip_boilerplate_phrases(self, text: str) -> str:
        """Remove common boilerplate lines."""
        for pattern in BOILERPLATE_PHRASES:
            text = pattern.sub("", text)
        return text

    # ------------------------------------------------------------------
    # Section splitting
    # ------------------------------------------------------------------

    def _split_sections(self, text: str) -> list[tuple[str, str]]:
        """
        Split *text* on Item headings and return a list of
        (section_name, section_text) tuples.

        Consecutive matches to the same section label are merged.
        """
        matches = list(_ITEM_HEADER_RE.finditer(text))
        if not matches:
            # No sections found: return entire document as one chunk
            return [("Full Document", text.strip())]

        sections: list[tuple[str, str]] = []
        for i, m in enumerate(matches):
            item_label = m.group(1)
            section_name = _map_section(item_label, self.filing_type)
            start = m.end()
            end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
            section_text = text[start:end].strip()
            if len(section_text) >= self.min_section_length:
                sections.append((section_name, section_text))

        return sections if sections else [("Full Document", text.strip())]

    def _filter_sections(
        self,
        sections: list[tuple[str, str]],
    ) -> list[tuple[str, str]]:
        """
        Drop low-value appendix sections that are usually harmful for retrieval.
        """
        filtered: list[tuple[str, str]] = []
        for section_name, section_text in sections:
            normalized = _normalize_item(section_name)
            if section_name == "Exhibits":
                continue
            if normalized in {"item 16", "item 6"} and len(section_text) > 100_000:
                continue
            filtered.append((section_name, section_text))
        return filtered

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def clean(self, file_path: str | Path) -> list[tuple[str, str]]:
        """
        Load, clean, and section-split an SEC filing document.

        Parameters
        ----------
        file_path:
            Path to the downloaded HTML or text filing document.

        Returns
        -------
        list[tuple[str, str]]
            Each element is (section_name, clean_text).
        """
        path = Path(file_path)
        if not path.exists():
            logger.error("Filing not found: %s", path)
            return []

        try:
            content = path.read_bytes()
        except OSError as exc:
            logger.error("Cannot read %s: %s", path, exc)
            return []

        content = self._extract_primary_document(content)

        # Parse
        soup = self._load_soup(content)

        # Clean HTML
        self._strip_xbrl(soup)
        self._strip_noise(soup)

        # Extract text
        raw_text = self._extract_text(soup)

        # Remove boilerplate
        text = self._remove_cover_page(raw_text)
        text = self._remove_signature_block(text)
        text = self._remove_exhibit_index(text)
        text = self._strip_boilerplate_phrases(text)

        # Split into sections
        sections = self._split_sections(text)
        sections = self._filter_sections(sections)
        if not sections and len(text.strip()) >= self.min_section_length:
            sections = [("Full Document", text.strip())]

        logger.debug(
            "Cleaned %s: %d sections, %d total chars",
            path.name,
            len(sections),
            sum(len(s[1]) for s in sections),
        )
        return sections

    def clean_text(self, raw_html: str | bytes, filing_type: str | None = None) -> list[tuple[str, str]]:
        """
        Clean HTML provided as a string/bytes rather than a file path.
        Useful for in-memory processing.
        """
        if filing_type:
            self.filing_type = filing_type.upper()

        raw_html = self._extract_primary_document(raw_html)
        soup = self._load_soup(raw_html)
        self._strip_xbrl(soup)
        self._strip_noise(soup)

        raw_text = self._extract_text(soup)
        text = self._remove_cover_page(raw_text)
        text = self._remove_signature_block(text)
        text = self._remove_exhibit_index(text)
        text = self._strip_boilerplate_phrases(text)

        sections = self._filter_sections(self._split_sections(text))
        if not sections and len(text.strip()) >= self.min_section_length:
            sections = [("Full Document", text.strip())]
        return sections


# ---------------------------------------------------------------------------
# Module-level convenience function
# ---------------------------------------------------------------------------

def clean_filing(
    file_path: str | Path,
    filing_type: str = "10-K",
) -> list[tuple[str, str]]:
    """Clean a single filing and return (section_name, text) tuples."""
    cleaner = HTMLCleaner(filing_type=filing_type)
    return cleaner.clean(file_path)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.DEBUG)
    if len(sys.argv) < 2:
        print("Usage: html_cleaner.py <path_to_filing> [10-K|10-Q|8-K]")
        sys.exit(1)

    ftype = sys.argv[2] if len(sys.argv) > 2 else "10-K"
    sections = clean_filing(sys.argv[1], ftype)
    for name, text in sections:
        print(f"\n{'='*60}\n{name}\n{'='*60}")
        print(text[:500], "...")
