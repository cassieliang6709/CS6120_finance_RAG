FROM python:3.11-slim

# System deps for psycopg2, lxml, and sec-edgar-downloader
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev \
    gcc \
    g++ \
    libxml2-dev \
    libxslt1-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY data_pipeline/ ./data_pipeline/
COPY financial_rag.dump .

ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["python", "-m", "data_pipeline.pipeline"]
