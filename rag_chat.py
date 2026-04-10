#!/usr/bin/env python3
"""Debugging CLI for the /chat endpoint.

One-shot mode  : python rag_chat.py "What are Apple's risk factors?"
REPL mode      : python rag_chat.py           (interactive loop, /quit to exit)

Reads RAG_URL and API_KEY from the repo-root .env (or shell env). The /chat
endpoint streams back Server-Sent Events — this script parses them and
prints retrieved sources + the LLM's thinking and answer in real time.
"""

import argparse
import json
import os
import sys

import requests
from dotenv import load_dotenv

load_dotenv()

RAG_URL = os.getenv("RAG_URL", "http://localhost:8000").rstrip("/")
API_KEY = os.getenv("API_KEY", "")


def chat(
    query: str,
    *,
    k: int = 5,
    company: str | None = None,
    filing_type: str | None = None,
    sector: str | None = None,
    timeout: float = 180.0,
) -> None:
    """Send one query to /chat (streaming) and print events as they arrive."""
    headers = {"Content-Type": "application/json", "Accept": "text/event-stream"}
    if API_KEY:
        headers["X-API-Key"] = API_KEY

    payload: dict = {"query": query, "k": k, "stream": True}
    if company:
        payload["company"] = company
    if filing_type:
        payload["filing_type"] = filing_type
    if sector:
        payload["sector"] = sector

    thinking_header_printed = False
    answer_header_printed = False
    current_event: str | None = None

    try:
        with requests.post(
            f"{RAG_URL}/chat",
            headers=headers,
            json=payload,
            stream=True,
            timeout=(10.0, timeout),  # (connect, read)
        ) as r:
            if r.status_code != 200:
                print(f"[HTTP {r.status_code}] {r.text}", file=sys.stderr)
                return

            for raw_line in r.iter_lines(decode_unicode=True):
                # SSE spec: blank line terminates an event
                if raw_line is None or raw_line == "":
                    current_event = None
                    continue
                if raw_line.startswith(":"):
                    continue  # comment (sse-starlette keep-alive pings)
                if raw_line.startswith("event:"):
                    current_event = raw_line[len("event:"):].strip()
                    continue
                if not raw_line.startswith("data:"):
                    continue

                data_str = raw_line[len("data:"):].strip()
                if not data_str:
                    continue
                try:
                    data = json.loads(data_str)
                except json.JSONDecodeError:
                    continue

                if current_event == "chunks":
                    chunks = data if isinstance(data, list) else []
                    print(f"--- Retrieved {len(chunks)} chunks ---", flush=True)
                    for i, c in enumerate(chunks, 1):
                        score = c.get("score", 0.0)
                        print(
                            f"  [{i}] {c.get('company')} {c.get('filing_type')}"
                            f"  score={score:.3f}"
                        )
                elif current_event == "thinking":
                    if not thinking_header_printed:
                        print("\n=============== Thinking =================", flush=True)
                        thinking_header_printed = True
                    sys.stdout.write(data.get("delta", ""))
                    sys.stdout.flush()
                elif current_event == "answer":
                    if not answer_header_printed:
                        print("\n=============== Answer =================", flush=True)
                        answer_header_printed = True
                    sys.stdout.write(data.get("delta", ""))
                    sys.stdout.flush()
                elif current_event == "error":
                    print(
                        f"\n[stream error] {data.get('message', 'unknown')}",
                        file=sys.stderr,
                        flush=True,
                    )
                elif current_event == "done":
                    break
    except requests.exceptions.RequestException as e:
        print(f"[request failed] {e}", file=sys.stderr)
        return

    print()  # trailing newline after the streamed answer


def repl(**filters) -> None:
    print(f"RAG chat → {RAG_URL}/chat   (type /quit or Ctrl-D to exit)")
    if filters:
        print(f"filters: {filters}")
    while True:
        try:
            q = input("\n> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return
        if q in ("/quit", "/exit"):
            return
        if not q:
            continue
        chat(q, **filters)


def main() -> None:
    p = argparse.ArgumentParser(
        description="Query the RAG /chat endpoint (one-shot or REPL)."
    )
    p.add_argument("query", nargs="*", help="Question to ask. Omit to enter REPL mode.")
    p.add_argument("-k", type=int, default=5, help="Number of chunks to retrieve")
    p.add_argument("--company", help="Filter by company ticker (e.g. AAPL)")
    p.add_argument("--filing-type", help="Filter by filing type (e.g. 10-K)")
    p.add_argument("--sector", help="Filter by sector")
    args = p.parse_args()

    filters = {
        "k": args.k,
        "company": args.company,
        "filing_type": args.filing_type,
        "sector": args.sector,
    }

    if args.query:
        chat(" ".join(args.query), **filters)
    else:
        repl(**filters)


if __name__ == "__main__":
    main()
