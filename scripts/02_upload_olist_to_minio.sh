#!/usr/bin/env bash
set -e
docker compose exec -T minio sh -lc 'mc alias set local http://localhost:9000 admin minio123456'
docker compose exec -T minio sh -lc 'mc mb -p local/landing/olist || true'
for f in data/olist/raw/*.csv; do
  docker compose exec -T minio sh -lc "mkdir -p /tmp/olist"
  docker cp "$f" "$(docker compose ps -q minio):/tmp/olist/$(basename $f)"
  docker compose exec -T minio sh -lc "mc cp /tmp/olist/$(basename $f) local/landing/olist/$(basename $f)"
done
