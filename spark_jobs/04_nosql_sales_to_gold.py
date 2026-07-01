from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, to_timestamp

spark = SparkSession.builder.appName("nosql-sales-gold").getOrCreate()
# Đọc JSON MongoDB sample_supplies sales đã tải vào data/nosql/sample_supplies/sales.json
raw = spark.read.json("/workspace/data/nosql/sample_supplies/sales.json")

# Mỗi document = 1 đơn bán hàng; items là mảng lồng nhau => explode ra fact_nosql_sales_item
fact = raw.select(
    col("_id.$oid").alias("sale_id"),
    col("storeLocation").alias("store_location"),
    col("customer.gender").alias("customer_gender"),
    col("customer.age").alias("customer_age"),
    col("purchaseMethod").alias("purchase_method"),
    explode("items").alias("item")
).select(
    "sale_id", "store_location", "customer_gender", "customer_age", "purchase_method",
    col("item.name").alias("product_name"),
    col("item.quantity").cast("int").alias("quantity"),
    col("item.price").cast("double").alias("price")
)
fact.write.mode("overwrite").parquet("/workspace/lakehouse/gold/nosql/fact_supply_sales_item")
