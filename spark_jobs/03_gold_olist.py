from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, to_date, datediff, when, count, sum as fsum

spark = SparkSession.builder.appName("gold-olist").getOrCreate()
silver = "/workspace/lakehouse/silver/olist"
gold = "/workspace/lakehouse/gold/olist"

customers = spark.read.parquet(f"{silver}/silver_customers")
orders = spark.read.parquet(f"{silver}/silver_orders")
items = spark.read.parquet(f"{silver}/silver_order_items")
payments = spark.read.parquet(f"{silver}/silver_order_payments")
reviews = spark.read.parquet(f"{silver}/silver_order_reviews")
products = spark.read.parquet(f"{silver}/silver_products")
sellers = spark.read.parquet(f"{silver}/silver_sellers")

orders_c = orders.join(customers, "ma_khach_hang_don_hang", "left")

# fact_sales: 1 dòng = 1 sản phẩm của seller trong 1 đơn hàng
fact_sales = items.join(orders_c, items.order_id == orders_c.ma_don_hang, "left") \
    .groupBy(
        col("ma_khach_hang"), col("order_id").alias("ma_don_hang"),
        col("product_id").alias("ma_san_pham"), col("seller_id").alias("ma_nguoi_ban"),
        col("thoi_diem_dat_hang")
    ).agg(
        count("order_item_id").alias("so_luong_san_pham"),
        fsum("price").alias("doanh_thu_san_pham"),
        fsum("freight_value").alias("phi_van_chuyen")
    )
fact_sales.write.mode("overwrite").parquet(f"{gold}/fact_sales")

# fact_payment: 1 dòng = 1 giao dịch thanh toán
fact_payment = payments.join(orders_c.select("ma_don_hang", "ma_khach_hang", "thoi_diem_dat_hang"), payments.order_id == orders_c.ma_don_hang, "left") \
    .select(
        col("order_id").alias("ma_don_hang"), "ma_khach_hang",
        col("payment_type").alias("hinh_thuc_thanh_toan"),
        col("payment_sequential").cast("int").alias("thu_tu_thanh_toan"),
        col("payment_installments").cast("int").alias("so_ky_thanh_toan"),
        col("payment_value").cast("double").alias("gia_tri_thanh_toan"),
        "thoi_diem_dat_hang"
    )
fact_payment.write.mode("overwrite").parquet(f"{gold}/fact_payment")

# fact_delivery: 1 dòng = 1 đơn hàng
fact_delivery = orders_c.select(
    "ma_don_hang", "ma_khach_hang", "trang_thai_don_hang", "thoi_diem_dat_hang",
    "thoi_diem_duyet_don", "thoi_diem_ban_giao_van_chuyen", "thoi_diem_giao_thanh_cong", "ngay_du_kien_giao",
    datediff("thoi_diem_giao_thanh_cong", "thoi_diem_dat_hang").alias("so_ngay_giao_hang"),
    datediff("thoi_diem_giao_thanh_cong", "ngay_du_kien_giao").alias("so_ngay_som_tre_so_voi_du_kien"),
    when(col("thoi_diem_giao_thanh_cong").isNull(), "Chưa giao")
    .when(col("thoi_diem_giao_thanh_cong") <= col("ngay_du_kien_giao"), "Đúng/Sớm hạn")
    .otherwise("Trễ hạn").alias("phan_loai_sla")
)
fact_delivery.write.mode("overwrite").parquet(f"{gold}/fact_delivery")

# fact_review: 1 dòng = 1 đơn hàng, có/không có review
fact_review = orders_c.select("ma_don_hang", "ma_khach_hang").join(reviews, orders_c.ma_don_hang == reviews.order_id, "left") \
    .select("ma_don_hang", "ma_khach_hang", col("review_id").alias("ma_danh_gia"), col("review_score").cast("int").alias("diem_danh_gia"))
fact_review.write.mode("overwrite").parquet(f"{gold}/fact_review")
