FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    HF_HOME=/opt/huggingface \
    SENTENCE_TRANSFORMERS_HOME=/opt/huggingface

# Install dependencies first (layer-cached unless requirements.txt changes)
COPY requirements.txt .
RUN pip install -r requirements.txt

# Pre-download the embedding model so the container doesn't fetch it on first request
RUN python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')"

COPY . .

RUN adduser --disabled-password --gecos "" appuser \
    && chown -R appuser:appuser /app /opt/huggingface

USER appuser

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
