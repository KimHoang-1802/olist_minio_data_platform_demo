#!/usr/bin/env bash
set -e
# Chạy sau khi docker compose up -d
# Nếu chưa có file sales.json, chạy scripts/00_download_nosql_sample_supplies.sh trước.
docker compose exec mongo mongoimport \
  --db sample_supplies \
  --collection sales \
  --file /datasets/sample_supplies/sales.json \
  --jsonArray || \
docker compose exec mongo mongoimport \
  --db sample_supplies \
  --collection sales \
  --file /datasets/sample_supplies/sales.json
