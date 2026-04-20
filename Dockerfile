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

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY data_pipeline/ ./data_pipeline/

ENTRYPOINT ["python", "-m", "data_pipeline.pipeline"]
