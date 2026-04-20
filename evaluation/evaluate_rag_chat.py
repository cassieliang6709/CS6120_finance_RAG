#!/usr/bin/env python3
"""Evaluate end-to-end RAG chat against a filtered FinanceBench dataset.

This script reads benchmark questions from a filtered FinanceBench JSON file,
calls the live `/chat` endpoint once per question with `stream=false`, and
writes a new JSON file that preserves the original benchmark rows while adding:

- `retrieved_chunks`
- `final_reasoning`
- `final_answer`
- `latency_ms`
- `status`
- `error`
"""

from __future__ import annotations

import argparse
import json
import socket
import time
from pathlib import Path
from typing import Any
from urllib import error, request

DEFAULT_DATASET = (
    "evaluation/financebench_filtered/"
    "financebench_filtered_overlap_10_companies_10k_10q_2018_2023.json"
)
DEFAULT_OUTPUT = (
    "evaluation/financebench_filtered/"
    "financebench_filtered_overlap_10_companies_10k_10q_2018_2023_with_rag_chat.json"
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "dataset",
        nargs="?",
        default=DEFAULT_DATASET,
        help="Path to filtered FinanceBench JSON",
    )
    p.add_argument("--base-url", default="http://localhost:8000", help="Retrieval API base URL")
    p.add_argument(
        "--output",
        default=None,
        help="Where to write the detailed per-query results JSON",
    )
    p.add_argument(
        "--timeout",
        type=float,
        default=180.0,
        help="HTTP timeout in seconds for each /chat request",
    )
    p.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional max number of rows to process, for smoke tests",
    )
    p.add_argument(
        "--write-every",
        type=int,
        default=1,
        help="Write checkpoint output every N processed rows (default: 1)",
    )
    return p.parse_args()


def build_chat_payload(
    item: dict[str, Any],
) -> dict[str, Any]:
    return {
        "query": item["question"],
        "stream": False,
    }


def evaluate_query(
    base_url: str,
    item: dict[str, Any],
    timeout: float,
) -> dict[str, Any]:
    payload = build_chat_payload(item)
    started = time.perf_counter()
    latency_ms: int

    try:
        req = request.Request(
            f"{base_url.rstrip('/')}/chat",
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            method="POST",
        )
        with request.urlopen(req, timeout=timeout) as response:
            body = json.loads(response.read().decode("utf-8"))
        latency_ms = round((time.perf_counter() - started) * 1000)
        return {
            "retrieved_chunks": body.get("chunks", []) or [],
            "final_reasoning": body.get("thinking") or None,
            "final_answer": body.get("answer") or None,
            "latency_ms": latency_ms,
            "status": "ok",
            "error": None,
        }
    except (
        error.URLError,
        error.HTTPError,
        json.JSONDecodeError,
        TimeoutError,
        socket.timeout,
    ) as exc:
        latency_ms = round((time.perf_counter() - started) * 1000)
        return {
            "retrieved_chunks": [],
            "final_reasoning": None,
            "final_answer": None,
            "latency_ms": latency_ms,
            "status": "error",
            "error": str(exc),
        }
    except Exception as exc:
        latency_ms = round((time.perf_counter() - started) * 1000)
        return {
            "retrieved_chunks": [],
            "final_reasoning": None,
            "final_answer": None,
            "latency_ms": latency_ms,
            "status": "error",
            "error": f"unexpected error: {exc}",
        }


def summarize(results: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(results)
    ok_rows = [row for row in results if row["status"] == "ok"]
    latencies = [row["latency_ms"] for row in results]

    return {
        "queries": total,
        "successes": len(ok_rows),
        "errors": total - len(ok_rows),
        "success_rate": round(len(ok_rows) / total, 6) if total else 0.0,
        "mean_latency_ms": round(sum(latencies) / total, 2) if total else 0.0,
        "mean_retrieved_chunks": round(
            sum(len(row["retrieved_chunks"]) for row in results) / total, 2
        )
        if total
        else 0.0,
    }


def write_payload(output_path: Path, payload: list[dict[str, Any]]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def default_output_path(dataset_path: Path) -> Path:
    if dataset_path.name.endswith(".json"):
        output_name = dataset_path.name[:-5] + "_with_rag_chat.json"
    else:
        output_name = dataset_path.name + "_with_rag_chat.json"
    return dataset_path.with_name(output_name)


def main() -> None:
    args = parse_args()
    dataset_path = Path(args.dataset)
    output_path = Path(args.output) if args.output else default_output_path(dataset_path)
    if args.write_every <= 0:
        raise SystemExit("--write-every must be >= 1")

    items = json.loads(dataset_path.read_text(encoding="utf-8"))
    if not isinstance(items, list):
        raise SystemExit(f"Expected a JSON list in {dataset_path}")

    if args.limit is not None:
        items = items[: args.limit]

    results: list[dict[str, Any]] = []
    payload: list[dict[str, Any]] = []

    for index, item in enumerate(items, start=1):
        result = evaluate_query(
            base_url=args.base_url,
            item=item,
            timeout=args.timeout,
        )
        results.append(result)
        payload.append({**item, **result})
        if len(payload) % args.write_every == 0:
            write_payload(output_path, payload)
        print(
            f"[{index:03d}/{len(items):03d}] "
            f"{result['status'].upper()} "
            f"{item.get('financebench_id')} "
            f"latency_ms={result['latency_ms']} "
            f"chunks={len(result['retrieved_chunks'])}"
        )

    summary = summarize(results)
    write_payload(output_path, payload)

    print()
    print(json.dumps(summary, indent=2))
    print()
    print(f"Wrote detailed results to {output_path}")


if __name__ == "__main__":
    main()
