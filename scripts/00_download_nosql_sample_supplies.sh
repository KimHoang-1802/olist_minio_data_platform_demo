#!/usr/bin/env bash
set -e
mkdir -p data/nosql/sample_supplies
curl -L "https://raw.githubusercontent.com/neelabalan/mongodb-sample-dataset/main/sample_supplies/sales.json" \
  -o data/nosql/sample_supplies/sales.json
