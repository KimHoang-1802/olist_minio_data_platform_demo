# Demo Data Platform: Olist + MongoDB NoSQL trên MinIO Lakehouse

## Kiến trúc mục tiêu
Source CSV / MongoDB JSON → Landing → Bronze → Silver → Gold → Trino → Metabase/Power BI.

## Mapping với slide ODS + Lakehouse
- ODS: dùng cho operational/near-real-time serving, API, dashboard vận hành.
- Lakehouse: dùng cho analytics/AI, medallion Bronze/Silver/Gold.
- MinIO: đóng vai trò S3-compatible object storage on-prem.
- Spark: batch/ETL processing.
- MongoDB sample_supplies: ví dụ nguồn NoSQL document, có nested array `items`.
