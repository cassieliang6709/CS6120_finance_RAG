from __future__ import annotations

import html
import os
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse

import psycopg2
import psycopg2.extras


DATABASE_URL = os.getenv("DATABASE_URL", "postgresql:///financial_rag")
HOST = "127.0.0.1"
PORT = int(os.getenv("DB_DASHBOARD_PORT", "8765"))

ALLOWED_TABLES = {
    "companies",
    "filings",
    "chunks",
    "market_data",
    "financials",
    "macro_indicators",
    "news_articles",
    "news_chunks",
    "earnings_transcripts",
    "transcript_chunks",
}


def fetch_all(query: str, params: tuple | None = None) -> list[dict]:
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params or ())
            return [dict(row) for row in cur.fetchall()]


def fetch_one(query: str, params: tuple | None = None) -> dict:
    rows = fetch_all(query, params)
    return rows[0] if rows else {}


def render_bar_rows(rows: list[dict], label_key: str, value_key: str, color: str = "#0f766e") -> str:
    if not rows:
        return "<p class='empty'>No data.</p>"
    max_value = max(row[value_key] for row in rows) or 1
    parts: list[str] = ["<div class='bars'>"]
    for row in rows:
        label = html.escape(str(row[label_key]))
        value = row[value_key]
        width = max(6, int((value / max_value) * 100))
        parts.append(
            f"""
            <div class="bar-row">
              <div class="bar-label">{label}</div>
              <div class="bar-track">
                <div class="bar-fill" style="width:{width}%; background:{color};"></div>
              </div>
              <div class="bar-value">{value:,}</div>
            </div>
            """
        )
    parts.append("</div>")
    return "".join(parts)


def render_table(rows: list[dict]) -> str:
    if not rows:
        return "<p class='empty'>No rows.</p>"
    columns = list(rows[0].keys())
    head = "".join(f"<th>{html.escape(str(col))}</th>" for col in columns)
    body_rows: list[str] = []
    for row in rows:
        cells = "".join(
            f"<td>{html.escape('' if row[col] is None else str(row[col]))}</td>"
            for col in columns
        )
        body_rows.append(f"<tr>{cells}</tr>")
    return f"<div class='table-wrap'><table><thead><tr>{head}</tr></thead><tbody>{''.join(body_rows)}</tbody></table></div>"


def build_dashboard(selected_table: str | None = None) -> str:
    counts = fetch_all(
        """
        SELECT table_name, row_count
        FROM (
            SELECT 'companies' AS table_name, COUNT(*) AS row_count FROM companies
            UNION ALL SELECT 'filings', COUNT(*) FROM filings
            UNION ALL SELECT 'chunks', COUNT(*) FROM chunks
            UNION ALL SELECT 'market_data', COUNT(*) FROM market_data
            UNION ALL SELECT 'financials', COUNT(*) FROM financials
            UNION ALL SELECT 'macro_indicators', COUNT(*) FROM macro_indicators
            UNION ALL SELECT 'news_articles', COUNT(*) FROM news_articles
            UNION ALL SELECT 'news_chunks', COUNT(*) FROM news_chunks
            UNION ALL SELECT 'earnings_transcripts', COUNT(*) FROM earnings_transcripts
            UNION ALL SELECT 'transcript_chunks', COUNT(*) FROM transcript_chunks
        ) t
        ORDER BY row_count DESC, table_name
        """
    )
    total_rows = sum(row["row_count"] for row in counts)

    chunk_years = fetch_all(
        """
        SELECT fiscal_year, COUNT(*) AS chunk_count
        FROM chunks
        GROUP BY fiscal_year
        ORDER BY fiscal_year
        """
    )
    filing_mix = fetch_all(
        """
        SELECT ticker || ' ' || filing_type || ' ' || fiscal_year AS label, COUNT(*) AS chunk_count
        FROM chunks
        GROUP BY ticker, filing_type, fiscal_year
        ORDER BY chunk_count DESC, label
        LIMIT 20
        """
    )
    news_years = fetch_all(
        """
        SELECT COALESCE(EXTRACT(YEAR FROM published_date)::INT, 0) AS published_year, COUNT(*) AS article_count
        FROM news_articles
        GROUP BY published_year
        ORDER BY published_year
        """
    )
    recent_filings = fetch_all(
        """
        SELECT ticker, filing_type, fiscal_year, period, filed_date, source_url
        FROM filings
        ORDER BY filed_date DESC NULLS LAST, id DESC
        LIMIT 12
        """
    )
    recent_news = fetch_all(
        """
        SELECT ticker, published_date, title, source
        FROM news_articles
        ORDER BY published_date DESC NULLS LAST, id DESC
        LIMIT 12
        """
    )

    sample_table_html = ""
    if selected_table and selected_table in ALLOWED_TABLES:
        sample_rows = fetch_all(f"SELECT * FROM {selected_table} LIMIT 50")
        sample_table_html = f"""
        <section class="card span-2">
          <div class="card-head">
            <h2>Sample Rows: {html.escape(selected_table)}</h2>
          </div>
          {render_table(sample_rows)}
        </section>
        """

    table_links = " ".join(
        f"<a class='pill' href='/?table={name}'>{name}</a>" for name in sorted(ALLOWED_TABLES)
    )

    return f"""
    <!doctype html>
    <html lang="en">
    <head>
      <meta charset="utf-8" />
      <meta name="viewport" content="width=device-width, initial-scale=1" />
      <title>Financial RAG Database Dashboard</title>
      <style>
        :root {{
          --bg: #f4efe7;
          --card: #fffdf9;
          --ink: #1f2937;
          --muted: #6b7280;
          --border: #e5ded3;
          --accent: #0f766e;
          --accent-2: #b45309;
        }}
        * {{ box-sizing: border-box; }}
        body {{
          margin: 0;
          font-family: ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
          color: var(--ink);
          background:
            radial-gradient(circle at top left, rgba(15,118,110,.09), transparent 30%),
            radial-gradient(circle at top right, rgba(180,83,9,.08), transparent 28%),
            var(--bg);
        }}
        .page {{
          max-width: 1360px;
          margin: 0 auto;
          padding: 28px;
        }}
        .hero {{
          background: linear-gradient(135deg, #173b3a, #244f4d 55%, #7c4a18);
          color: #fff;
          border-radius: 24px;
          padding: 28px;
          box-shadow: 0 18px 40px rgba(0,0,0,.12);
        }}
        .hero h1 {{ margin: 0 0 6px; font-size: 34px; }}
        .hero p {{ margin: 0; color: rgba(255,255,255,.82); }}
        .meta {{
          margin-top: 18px;
          display: flex;
          flex-wrap: wrap;
          gap: 12px;
        }}
        .stat {{
          min-width: 180px;
          background: rgba(255,255,255,.12);
          border: 1px solid rgba(255,255,255,.16);
          border-radius: 16px;
          padding: 14px 16px;
        }}
        .stat-label {{ font-size: 12px; text-transform: uppercase; letter-spacing: .08em; opacity: .72; }}
        .stat-value {{ font-size: 28px; font-weight: 700; margin-top: 6px; }}
        .grid {{
          display: grid;
          grid-template-columns: repeat(2, minmax(0, 1fr));
          gap: 18px;
          margin-top: 20px;
        }}
        .card {{
          background: var(--card);
          border: 1px solid var(--border);
          border-radius: 20px;
          padding: 18px;
          box-shadow: 0 8px 24px rgba(31,41,55,.05);
        }}
        .span-2 {{ grid-column: span 2; }}
        .card-head {{
          display: flex;
          justify-content: space-between;
          align-items: baseline;
          gap: 12px;
          margin-bottom: 12px;
        }}
        .card h2 {{ margin: 0; font-size: 20px; }}
        .muted {{ color: var(--muted); font-size: 14px; }}
        .bars {{ display: grid; gap: 10px; }}
        .bar-row {{
          display: grid;
          grid-template-columns: 220px 1fr 84px;
          gap: 10px;
          align-items: center;
        }}
        .bar-label {{ font-size: 14px; }}
        .bar-track {{
          width: 100%;
          height: 14px;
          background: #ece5da;
          border-radius: 999px;
          overflow: hidden;
        }}
        .bar-fill {{ height: 100%; border-radius: 999px; }}
        .bar-value {{ text-align: right; font-variant-numeric: tabular-nums; font-size: 14px; }}
        .pill-row {{
          display: flex;
          flex-wrap: wrap;
          gap: 8px;
          margin-bottom: 14px;
        }}
        .pill {{
          text-decoration: none;
          color: var(--ink);
          border: 1px solid var(--border);
          border-radius: 999px;
          padding: 8px 12px;
          background: #fff;
          font-size: 13px;
        }}
        .table-wrap {{
          overflow: auto;
          border: 1px solid var(--border);
          border-radius: 14px;
        }}
        table {{
          width: 100%;
          border-collapse: collapse;
          background: #fff;
        }}
        th, td {{
          padding: 10px 12px;
          border-bottom: 1px solid var(--border);
          text-align: left;
          vertical-align: top;
          font-size: 13px;
        }}
        th {{
          position: sticky;
          top: 0;
          background: #f8f4ed;
        }}
        .empty {{ color: var(--muted); }}
        @media (max-width: 980px) {{
          .grid {{ grid-template-columns: 1fr; }}
          .span-2 {{ grid-column: span 1; }}
          .bar-row {{ grid-template-columns: 1fr; }}
          .bar-value {{ text-align: left; }}
        }}
      </style>
    </head>
    <body>
      <div class="page">
        <section class="hero">
          <h1>Financial RAG Database Dashboard</h1>
          <p>本地可视化页面。用来快速看你现在数据库里到底有什么，而不是只盯着行数。</p>
          <div class="meta">
            <div class="stat">
              <div class="stat-label">Total Rows</div>
              <div class="stat-value">{total_rows:,}</div>
            </div>
            <div class="stat">
              <div class="stat-label">SEC Chunks</div>
              <div class="stat-value">{fetch_one("SELECT COUNT(*) AS n FROM chunks").get("n", 0):,}</div>
            </div>
            <div class="stat">
              <div class="stat-label">News Articles</div>
              <div class="stat-value">{fetch_one("SELECT COUNT(*) AS n FROM news_articles").get("n", 0):,}</div>
            </div>
            <div class="stat">
              <div class="stat-label">News Chunks</div>
              <div class="stat-value">{fetch_one("SELECT COUNT(*) AS n FROM news_chunks").get("n", 0):,}</div>
            </div>
          </div>
        </section>

        <div class="grid">
          <section class="card">
            <div class="card-head">
              <h2>Table Sizes</h2>
              <span class="muted">按表看数据库规模</span>
            </div>
            {render_bar_rows(counts, "table_name", "row_count")}
          </section>

          <section class="card">
            <div class="card-head">
              <h2>Chunks By Year</h2>
              <span class="muted">SEC 文本块按年份分布</span>
            </div>
            {render_bar_rows(chunk_years, "fiscal_year", "chunk_count", "#b45309")}
          </section>

          <section class="card span-2">
            <div class="card-head">
              <h2>Top Filing Buckets</h2>
              <span class="muted">哪些公司/年份/表单贡献了最多 chunk</span>
            </div>
            {render_bar_rows(filing_mix, "label", "chunk_count", "#2563eb")}
          </section>

          <section class="card">
            <div class="card-head">
              <h2>News By Year</h2>
              <span class="muted">官网新闻的年份分布</span>
            </div>
            {render_bar_rows(news_years, "published_year", "article_count", "#7c3aed")}
          </section>

          <section class="card">
            <div class="card-head">
              <h2>Browse Tables</h2>
              <span class="muted">点击查看前 50 行样本</span>
            </div>
            <div class="pill-row">{table_links}</div>
            <p class="muted">当前查看：{html.escape(selected_table or "none")}</p>
          </section>

          <section class="card span-2">
            <div class="card-head">
              <h2>Recent Filings</h2>
              <span class="muted">最近入库的 filing 元数据</span>
            </div>
            {render_table(recent_filings)}
          </section>

          <section class="card span-2">
            <div class="card-head">
              <h2>Recent News</h2>
              <span class="muted">最近入库的新闻文档</span>
            </div>
            {render_table(recent_news)}
          </section>

          {sample_table_html}
        </div>
      </div>
    </body>
    </html>
    """


class DashboardHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)
        selected_table = params.get("table", [None])[0]
        try:
            content = build_dashboard(selected_table=selected_table)
            body = content.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except Exception as exc:
            body = f"<h1>Dashboard Error</h1><pre>{html.escape(str(exc))}</pre>".encode("utf-8")
            self.send_response(500)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    def log_message(self, format: str, *args) -> None:
        return


if __name__ == "__main__":
    server = HTTPServer((HOST, PORT), DashboardHandler)
    print(f"Dashboard running at http://{HOST}:{PORT}")
    server.serve_forever()
