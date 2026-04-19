#!/usr/bin/env python3
"""
Filter FinanceBench so it matches a local corpus scope defined in a JSON spec.

Supported sources:
- Hugging Face dataset: PatronusAI/financebench
- local export: .csv
- local export: .json
- local export: .jsonl

Examples:
    python3 evaluation/filter_financebench.py \
      --output-dir evaluation/financebench_filtered

    python3 evaluation/filter_financebench.py \
      --spec evaluation/financebench_filter_spec_expanded.json \
      --output-dir evaluation/financebench_filtered

    python3 evaluation/filter_financebench.py \
      --input /path/to/financebench.csv \
      --output-dir evaluation/financebench_filtered
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter
from pathlib import Path
from typing import Iterable

try:
    from datasets import Dataset, DatasetDict, load_dataset
except ImportError:  # pragma: no cover - exercised only when optional dep is missing
    Dataset = None
    DatasetDict = None
    load_dataset = None


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_DATASET_NAME = "PatronusAI/financebench"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        help="Path to exported FinanceBench csv/json/jsonl. If omitted, load the Hugging Face dataset.",
    )
    parser.add_argument(
        "--spec",
        help="Optional path to a filter spec JSON file.",
    )
    parser.add_argument(
        "--companies",
        nargs="+",
        help="Company names or tickers to keep. Used when --spec is omitted.",
    )
    parser.add_argument(
        "--years",
        nargs="+",
        type=int,
        help="Optional doc_period years to keep.",
    )
    parser.add_argument(
        "--doc-types",
        nargs="+",
        help="Optional doc types to keep, e.g. 10k 10q.",
    )
    parser.add_argument(
        "--output-stem",
        default="financebench_filtered",
        help="Output filename stem when --spec is omitted.",
    )
    parser.add_argument(
        "--filings-path",
        help="Optional TSV of live DB filings: ticker, filing_type, fiscal_year, period.",
    )
    parser.add_argument(
        "--dataset-name",
        default=DEFAULT_DATASET_NAME,
        help="Hugging Face dataset name to load when --input is omitted.",
    )
    parser.add_argument(
        "--split",
        default=None,
        help="Optional dataset split name. Defaults to 'train' when available, otherwise the only split.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(SCRIPT_DIR / "financebench_filtered"),
        help="Directory for filtered outputs",
    )
    return parser.parse_args()


def load_spec(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def normalize_text(value: str | None) -> str:
    if value is None:
        return ""
    value = value.lower()
    value = re.sub(r"[^a-z0-9]+", " ", value)
    return " ".join(value.split())


def normalize_company_text(value: str | None) -> str:
    normalized = normalize_text(value)
    drop_tokens = {
        "inc",
        "incorporated",
        "corp",
        "corporation",
        "co",
        "company",
        "companies",
        "group",
        "holdings",
        "plc",
        "ltd",
        "the",
    }
    kept = [token for token in normalized.split() if token not in drop_tokens]
    return " ".join(kept)


def build_alias_index(spec: dict) -> dict[str, list[str]]:
    company_aliases = spec.get("company_aliases", {})
    validated_tickers = spec.get("validated_tickers", [])
    alias_index: dict[str, list[str]] = {}

    for ticker, aliases in company_aliases.items():
        values = [normalize_text(alias) for alias in aliases]
        values.append(normalize_text(ticker))
        alias_index[ticker] = sorted({value for value in values if value})

    for ticker in validated_tickers:
        alias_index.setdefault(ticker, [normalize_text(ticker)])

    return alias_index


def build_company_index(companies: list[str]) -> dict[str, list[str]]:
    return {
        company: [normalize_company_text(company), normalize_text(company)]
        for company in companies
        if company
    }


def parse_company_specs(companies: list[str]) -> tuple[list[str], dict[str, str]]:
    labels: list[str] = []
    label_to_ticker: dict[str, str] = {}
    for company in companies:
        if "=" in company:
            label, ticker = company.split("=", 1)
            label = label.strip()
            ticker = ticker.strip().upper()
        else:
            label = company.strip()
            ticker = company.strip().upper()
        if not label:
            continue
        labels.append(label)
        label_to_ticker[label] = ticker
    return labels, label_to_ticker


def get_target_periods(spec: dict) -> set[int]:
    if "doc_periods" in spec:
        return {int(value) for value in spec["doc_periods"]}
    if "doc_period" in spec:
        return {int(spec["doc_period"])}
    raise KeyError("Spec must define either 'doc_periods' or 'doc_period'.")


def get_output_stem(spec: dict) -> str:
    return str(spec.get("filter_name") or "financebench_filtered")


def load_rows(path: Path) -> list[dict]:
    suffix = path.suffix.lower()

    if suffix == ".csv":
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            return list(csv.DictReader(f))

    if suffix == ".jsonl":
        rows = []
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
        return rows

    if suffix == ".json":
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            for key in ("rows", "data", "train"):
                value = data.get(key)
                if isinstance(value, list):
                    return value
        raise ValueError("Unsupported JSON structure; expected a list of row objects")

    raise ValueError(f"Unsupported input format: {path.suffix}")


def load_filings(path: Path) -> set[tuple[str, str, int, str]]:
    filings: set[tuple[str, str, int, str]] = set()
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split("\t")
            if len(parts) != 4:
                raise ValueError(
                    f"Expected 4 tab-separated fields in filings file, got {len(parts)}: {line}"
                )
            ticker, filing_type, fiscal_year, period = parts
            filings.add((ticker.upper(), filing_type.upper(), int(fiscal_year), period.lower()))
    return filings


def load_rows_from_dataset(dataset_name: str, split: str | None) -> tuple[list[dict], str]:
    if load_dataset is None:
        raise ImportError(
            "The 'datasets' package is required to load FinanceBench directly. "
            "Install it with `pip install datasets`, or pass --input with a local export."
        )

    dataset_obj = load_dataset(dataset_name)

    if Dataset is not None and isinstance(dataset_obj, Dataset):
        chosen_split = split or "full_dataset"
        rows = [dict(row) for row in dataset_obj]
        return rows, chosen_split

    if DatasetDict is not None and isinstance(dataset_obj, DatasetDict):
        available_splits = list(dataset_obj.keys())
    elif isinstance(dataset_obj, dict):
        available_splits = list(dataset_obj.keys())
    else:
        raise TypeError(
            f"Unsupported dataset object returned by load_dataset('{dataset_name}'): "
            f"{type(dataset_obj).__name__}"
        )

    if not available_splits:
        raise ValueError(f"No splits found in dataset '{dataset_name}'.")

    if split is None:
        chosen_split = "train" if "train" in dataset_obj else available_splits[0]
    else:
        chosen_split = split

    if chosen_split not in dataset_obj:
        raise ValueError(
            f"Split '{chosen_split}' not found in dataset '{dataset_name}'. "
            f"Available splits: {', '.join(available_splits)}"
        )

    rows = [dict(row) for row in dataset_obj[chosen_split]]
    return rows, chosen_split


def matches_company(row: dict, alias_index: dict[str, list[str]]) -> tuple[bool, str | None]:
    company_norm = normalize_text(str(row.get("company", "")))
    doc_name_norm = normalize_text(str(row.get("doc_name", "")))
    searchable = f"{company_norm} {doc_name_norm}".strip()

    for ticker, aliases in alias_index.items():
        if any(alias and alias in searchable for alias in aliases):
            return True, ticker
    return False, None


def matches_company_list(
    row: dict,
    company_index: dict[str, list[str]],
) -> tuple[bool, str | None]:
    company_norm = normalize_company_text(str(row.get("company", "")))
    doc_name_norm = normalize_text(str(row.get("doc_name", "")))

    for label, aliases in company_index.items():
        for alias in aliases:
            if not alias:
                continue
            if alias == company_norm or alias in company_norm or company_norm in alias:
                return True, label
            if alias in doc_name_norm:
                return True, label
    return False, None


def infer_period(row: dict) -> str | None:
    doc_type = str(row.get("doc_type", "")).lower()
    doc_name = str(row.get("doc_name", ""))
    if doc_type == "10k":
        return "annual"
    if doc_type == "10q":
        match = re.search(r"_(\d{4})q([1-3])_10q$", doc_name, re.IGNORECASE)
        if match:
            return f"q{match.group(2)}"
    return None


def filing_exists_for_row(
    row: dict,
    matched_label: str,
    label_to_ticker: dict[str, str],
    filings_index: set[tuple[str, str, int, str]],
) -> bool:
    ticker = label_to_ticker.get(matched_label)
    if not ticker:
        return False
    doc_type = str(row.get("doc_type", "")).upper()
    try:
        doc_period = int(row.get("doc_period", ""))
    except (TypeError, ValueError):
        return False
    period = infer_period(row)
    if period is None:
        return False
    return (ticker, doc_type.replace("10K", "10-K").replace("10Q", "10-Q"), doc_period, period) in filings_index


def filter_rows(rows: Iterable[dict], spec: dict) -> tuple[list[dict], Counter, Counter, Counter]:
    alias_index = build_alias_index(spec)
    allowed_doc_types = {value.lower() for value in spec["doc_types"]}
    target_periods = get_target_periods(spec)

    kept_rows: list[dict] = []
    company_counts: Counter = Counter()
    doc_type_counts: Counter = Counter()
    period_counts: Counter = Counter()

    for row in rows:
        doc_type = str(row.get("doc_type", "")).lower()
        if doc_type not in allowed_doc_types:
            continue

        try:
            doc_period = int(row.get("doc_period", ""))
        except (TypeError, ValueError):
            continue
        if doc_period not in target_periods:
            continue

        matched, ticker = matches_company(row, alias_index)
        if not matched or ticker is None:
            continue

        row = dict(row)
        row["matched_ticker"] = ticker
        kept_rows.append(row)
        company_counts[ticker] += 1
        doc_type_counts[doc_type] += 1
        period_counts[doc_period] += 1

    return kept_rows, company_counts, doc_type_counts, period_counts


def filter_rows_by_companies(
    rows: Iterable[dict],
    companies: list[str],
    years: list[int] | None,
    doc_types: list[str] | None,
    filings_index: set[tuple[str, str, int, str]] | None,
    label_to_ticker: dict[str, str],
) -> tuple[list[dict], Counter, Counter, Counter]:
    company_index = build_company_index(companies)
    allowed_doc_types = {value.lower() for value in doc_types} if doc_types else {"10k", "10q"}
    allowed_years = set(years) if years else None

    kept_rows: list[dict] = []
    company_counts: Counter = Counter()
    doc_type_counts: Counter = Counter()
    period_counts: Counter = Counter()

    for row in rows:
        doc_type = str(row.get("doc_type", "")).lower()
        if allowed_doc_types and doc_type not in allowed_doc_types:
            continue

        try:
            doc_period = int(row.get("doc_period", ""))
        except (TypeError, ValueError):
            continue
        if allowed_years:
            if doc_period not in allowed_years:
                continue
        elif doc_period < 2018:
            continue

        matched, label = matches_company_list(row, company_index)
        if not matched or label is None:
            continue
        if filings_index is not None and not filing_exists_for_row(
            row,
            label,
            label_to_ticker,
            filings_index,
        ):
            continue

        row = dict(row)
        row["matched_company"] = label
        row["matched_ticker"] = label_to_ticker.get(label)
        kept_rows.append(row)
        company_counts[label] += 1
        doc_type_counts[doc_type] += 1
        period_counts[doc_period] += 1

    return kept_rows, company_counts, doc_type_counts, period_counts


def write_csv(rows: list[dict], path: Path) -> None:
    if not rows:
        fieldnames = []
    else:
        preferred = [
            "financebench_id",
            "matched_ticker",
            "company",
            "doc_name",
            "question",
            "answer",
            "justification",
            "doc_type",
            "doc_period",
            "doc_link",
            "question_type",
            "question_reasoning",
            "dataset_subset_label",
            "gics_sector",
            "evidence",
        ]
        fieldnames = [name for name in preferred if name in rows[0]]
        for key in rows[0]:
            if key not in fieldnames:
                fieldnames.append(key)

    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_json(rows: list[dict], path: Path) -> None:
    with path.open("w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2, ensure_ascii=False)


def write_summary(
    total_rows: int,
    filtered_rows: list[dict],
    company_counts: Counter,
    doc_type_counts: Counter,
    period_counts: Counter,
    source: str,
    spec_name: str,
    path: Path,
) -> None:
    summary = {
        "source": source,
        "spec_name": spec_name,
        "input_row_count": total_rows,
        "filtered_row_count": len(filtered_rows),
        "counts_by_ticker": dict(company_counts),
        "counts_by_doc_type": dict(doc_type_counts),
        "counts_by_doc_period": dict(sorted(period_counts.items())),
    }
    with path.open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    if args.input:
        input_path = Path(args.input).expanduser().resolve()
        rows = load_rows(input_path)
        source = str(input_path)
    else:
        rows, chosen_split = load_rows_from_dataset(args.dataset_name, args.split)
        source = f"{args.dataset_name} [{chosen_split}]"

    if args.spec:
        spec_path = Path(args.spec).expanduser().resolve()
        spec = load_spec(spec_path)
        filtered_rows, company_counts, doc_type_counts, period_counts = filter_rows(rows, spec)
        output_stem = get_output_stem(spec)
        spec_name = spec_path.name
    else:
        if not args.companies:
            raise SystemExit("Pass --companies when --spec is omitted.")
        labels, label_to_ticker = parse_company_specs(args.companies)
        filings_index = None
        if args.filings_path:
            filings_path = Path(args.filings_path).expanduser().resolve()
            filings_index = load_filings(filings_path)
        filtered_rows, company_counts, doc_type_counts, period_counts = filter_rows_by_companies(
            rows,
            labels,
            args.years,
            args.doc_types,
            filings_index,
            label_to_ticker,
        )
        output_stem = args.output_stem
        spec_name = "company_list"

    csv_path = output_dir / f"{output_stem}.csv"
    json_path = output_dir / f"{output_stem}.json"
    summary_path = output_dir / f"{output_stem}_summary.json"

    write_csv(filtered_rows, csv_path)
    write_json(filtered_rows, json_path)
    write_summary(
        len(rows),
        filtered_rows,
        company_counts,
        doc_type_counts,
        period_counts,
        source,
        spec_name,
        summary_path,
    )

    if args.spec:
        print(f"Spec: {spec_path}")
    else:
        print(f"Companies: {', '.join(args.companies)}")
    print(f"Source: {source}")
    print(f"Loaded rows: {len(rows)}")
    print(f"Filtered rows: {len(filtered_rows)}")
    print(f"Wrote CSV: {csv_path}")
    print(f"Wrote JSON: {json_path}")
    print(f"Wrote summary: {summary_path}")


if __name__ == "__main__":
    main()
