#!/bin/bash
set -e
pg_restore \
  --username="$POSTGRES_USER" \
  --dbname="$POSTGRES_DB" \
  --no-owner \
  --role="$POSTGRES_USER" \
  /docker-entrypoint-initdb.d/financial_rag.dump
