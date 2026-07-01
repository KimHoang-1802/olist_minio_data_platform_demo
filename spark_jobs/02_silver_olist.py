from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, current_timestamp

spark = SparkSession.builder.appName("silver-olist").getOrCreate()
bronze = "/workspace/lakehouse/bronze/olist"
silver = "/workspace/lakehouse/silver/olist"

def clean_str(c):
    return lower(trim(col(c)))

# Customers
spark.read.parquet(f"{bronze}/customers").dropDuplicates().select(
    col("customer_id").alias("ma_khach_hang_don_hang"),
    col("customer_unique_id").alias("ma_khach_hang"),
    col("customer_zip_code_prefix").cast("int").alias("ma_vung"),
    clean_str("customer_city").alias("thanh_pho"),
    clean_str("customer_state").alias("bang"),
    col("insert_date"), current_timestamp().alias("update_date")
).write.mode("overwrite").parquet(f"{silver}/silver_customers")

# Orders
spark.read.parquet(f"{bronze}/orders").dropDuplicates().select(
    col("order_id").alias("ma_don_hang"),
    col("customer_id").alias("ma_khach_hang_don_hang"),
    clean_str("order_status").alias("trang_thai_don_hang"),
    col("order_purchase_timestamp").cast("timestamp").alias("thoi_diem_dat_hang"),
    col("order_approved_at").cast("timestamp").alias("thoi_diem_duyet_don"),
    col("order_delivered_carrier_date").cast("timestamp").alias("thoi_diem_ban_giao_van_chuyen"),
    col("order_delivered_customer_date").cast("timestamp").alias("thoi_diem_giao_thanh_cong"),
    col("order_estimated_delivery_date").cast("timestamp").alias("ngay_du_kien_giao"),
    col("insert_date"), current_timestamp().alias("update_date")
).write.mode("overwrite").parquet(f"{silver}/silver_orders")

# Các bảng còn lại giữ pattern tương tự, ưu tiên rename cột sang tiếng Việt ở Gold.
for t in ["order_items", "order_payments", "order_reviews", "products", "sellers", "geolocation", "category_translation"]:
    spark.read.parquet(f"{bronze}/{t}").dropDuplicates().write.mode("overwrite").parquet(f"{silver}/silver_{t}")
