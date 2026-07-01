from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name

spark = SparkSession.builder.appName("bronze-olist").getOrCreate()
base = "/workspace/data/olist/raw"
out = "/workspace/lakehouse/bronze/olist"

tables = {
    "customers": "olist_customers_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
}

for name, file in tables.items():
    df = spark.read.option("header", True).option("inferSchema", True).csv(f"{base}/{file}")
    df = df.withColumn("source_file", input_file_name()).withColumn("insert_date", current_timestamp()).withColumn("update_date", current_timestamp())
    df.write.mode("overwrite").parquet(f"{out}/{name}")
    print(name, df.count())
