# Olist + NoSQL MinIO Data Platform Demo

Demo này mô phỏng kiến trúc doanh nghiệp: nguồn quan hệ Olist CSV + nguồn NoSQL MongoDB JSON → MinIO/Lakehouse → Spark Bronze/Silver/Gold → Trino/Metabase.

## 1. Chuẩn bị
```bash
cp .env.example .env
docker compose up -d
```

MinIO Console: http://localhost:9001  
User/pass: `admin` / `minio123456`

## 2. Tải dataset NoSQL
Dataset đề xuất: MongoDB Atlas Sample Supply Store / `sample_supplies.sales`.
```bash
bash scripts/00_download_nosql_sample_supplies.sh
bash scripts/01_load_nosql_to_mongo.sh
```

## 3. Đưa Olist vào MinIO landing
```bash
bash scripts/02_upload_olist_to_minio.sh
```

## 4. Chạy Spark ETL local trong container
```bash
docker compose exec spark spark-submit spark_jobs/01_bronze_olist.py
docker compose exec spark spark-submit spark_jobs/02_silver_olist.py
docker compose exec spark spark-submit spark_jobs/03_gold_olist.py
docker compose exec spark spark-submit spark_jobs/04_nosql_sales_to_gold.py
```

## 5. Layer dữ liệu
- `data/olist/raw`: CSV gốc Olist.
- `data/nosql/sample_supplies`: JSON MongoDB sample dataset.
- `lakehouse/bronze`: dữ liệu thô có metadata.
- `lakehouse/silver`: dữ liệu cleaned/standardized.
- `lakehouse/gold`: fact/dim phục vụ BI.

## 6. Bảng Gold chính
- `fact_sales`: bán hàng.
- `fact_payment`: thanh toán.
- `fact_review`: đánh giá.
- `fact_delivery`: SLA giao hàng.
- `fact_supply_sales_item`: nguồn NoSQL, explode từ nested array `items`.

## 7. Ghi chú
File này là scaffold demo học tập. Khi triển khai thật, nên thêm Hive/Nessie catalog cho Iceberg, orchestration bằng Airflow, DQ bằng Great Expectations, và dashboard trong Metabase/Power BI.
