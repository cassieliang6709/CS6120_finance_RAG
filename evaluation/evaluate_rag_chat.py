#!/usr/bin/env python3
"""Evaluate end-to-end RAG chat against the FinanceBench intersection dataset.

This script reads benchmark questions from the base FinanceBench intersection
JSON, calls the live `/chat` endpoint (which performs retrieval internally),
and writes a new JSON file that preserves the original benchmark rows while
adding:

- `retrieved_chunks`: chunks returned by `/chat` for this run
- `rag_chat`: status, error, response, and latency metadata
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
    "financebench_filtered_db_company_intersection.json"
)
DEFAULT_OUTPUT = (
    "evaluation/financebench_filtered/"
    "financebench_filtered_db_company_intersection_with_rag_chat.json"
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dataset", default=DEFAULT_DATASET, help="Path to filtered FinanceBench JSON")
    p.add_argument("--base-url", default="http://localhost:8000", help="Retrieval API base URL")
    p.add_argument(
        "--output",
        default=DEFAULT_OUTPUT,
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
    return {"query": item["question"]}


def collect_sse_response(response: Any) -> dict[str, Any]:
    current_event: str | None = None
    chunks: list[dict[str, Any]] = []
    thinking_parts: list[str] = []
    answer_parts: list[str] = []
    stream_error: str | None = None

    for raw_line in response:
        line = raw_line.decode("utf-8").strip()
        if line == "":
            current_event = None
            continue
        if line.startswith(":"):
            continue
        if line.startswith("event:"):
            current_event = line[len("event:"):].strip()
            continue
        if not line.startswith("data:"):
            continue

        data_str = line[len("data:"):].strip()
        if not data_str:
            continue
        try:
            data = json.loads(data_str)
        except json.JSONDecodeError:
            continue

        if current_event == "chunks":
            chunks = data if isinstance(data, list) else []
        elif current_event == "thinking" and isinstance(data, dict):
            thinking_parts.append(str(data.get("delta", "")))
        elif current_event == "answer" and isinstance(data, dict):
            answer_parts.append(str(data.get("delta", "")))
        elif current_event == "error" and isinstance(data, dict):
            stream_error = str(data.get("message", "stream error"))
        elif current_event == "done":
            break

    return {
        "chunks": chunks,
        "thinking": "".join(thinking_parts),
        "answer": "".join(answer_parts),
        "stream_error": stream_error,
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
                "Accept": "text/event-stream",
            },
            method="POST",
        )
        with request.urlopen(req, timeout=timeout) as response:
            body = collect_sse_response(response)
        latency_ms = round((time.perf_counter() - started) * 1000)
        status = "ok" if not body["stream_error"] else "error"
        return {
            "retrieved_chunks": body["chunks"],
            "rag_chat": {
                "status": status,
                "error": body["stream_error"],
                "response": {
                    "reasoning": body["thinking"] or None,
                    "answer": body["answer"] or None,
                },
                "timing": {
                    "latency_ms": latency_ms,
                },
            },
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
            "rag_chat": {
                "status": "error",
                "error": str(exc),
                "response": {
                    "reasoning": None,
                    "answer": None,
                },
                "timing": {
                    "latency_ms": latency_ms,
                },
            },
        }
    except Exception as exc:
        latency_ms = round((time.perf_counter() - started) * 1000)
        return {
            "retrieved_chunks": [],
            "rag_chat": {
                "status": "error",
                "error": f"unexpected error: {exc}",
                "response": {
                    "reasoning": None,
                    "answer": None,
                },
                "timing": {
                    "latency_ms": latency_ms,
                },
            },
        }


def summarize(results: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(results)
    ok_rows = [row for row in results if row["rag_chat"]["status"] == "ok"]
    latencies = [row["rag_chat"]["timing"]["latency_ms"] for row in results]

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


def main() -> None:
    args = parse_args()
    dataset_path = Path(args.dataset)
    output_path = Path(args.output)
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
        payload.append(
            {
                **item,
                "retrieved_chunks": result["retrieved_chunks"],
                "rag_chat": result["rag_chat"],
            }
        )
        if len(payload) % args.write_every == 0:
            write_payload(output_path, payload)
        print(
            f"[{index:03d}/{len(items):03d}] "
            f"{result['rag_chat']['status'].upper()} "
            f"{item.get('financebench_id')} "
            f"latency_ms={result['rag_chat']['timing']['latency_ms']} "
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
