

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum,
    desc
)



ORDERS_PATH = "data\\orders.csv"
CUSTOMERS_PATH = "data\\customers.csv"
OUTPUT_DIR = "data\\output"

spark = (
    SparkSession.builder
    .appName("EcommerceOrderProcessing")
    .getOrCreate()
)



orders = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(str(ORDERS_PATH))
)

customers = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(str(CUSTOMERS_PATH))
)

print("\n--- ORDERS DATA ---")
orders.show(truncate=False)

print("\n--- CUSTOMERS DATA ---")
customers.show(truncate=False)



print("\n--- ORDERS SCHEMA ---")
orders.printSchema()

print("\n--- CUSTOMERS SCHEMA ---")
customers.printSchema()

required_orders = {
    "order_id", "customer_id", "product",
    "quantity", "unit_price", "status"
}

required_customers = {
    "customer_id", "customer_name", "city"
}

if not required_orders.issubset(set(orders.columns)):
    raise ValueError("orders.csv is missing required columns")

if not required_customers.issubset(set(customers.columns)):
    raise ValueError("customers.csv is missing required columns")



print("\n--- DUPLICATE ORDER IDS ---")

duplicates = (
    orders.groupBy("order_id")
    .count()
    .filter(col("count") > 1)
)

duplicates.show(truncate=False)

print("\n--- ORDERS WITH MISSING CUSTOMER IDS ---")

orders.filter(
    col("customer_id").isNull()
).show(truncate=False)

print("\n--- ORDERS WITH MISSING ORDER IDS ---")

orders.filter(
    col("order_id").isNull()
).show(truncate=False)


clean_orders = (
    orders.dropDuplicates(["order_id"])
    .filter(
        col("order_id").isNotNull()
        & col("customer_id").isNotNull()
        & col("quantity").isNotNull()
        & col("unit_price").isNotNull()
        & col("product").isNotNull()
        & col("status").isNotNull()
    )
)



clean_orders = clean_orders.withColumn(
    "order_amount",
    col("quantity").cast("double")
    * col("unit_price").cast("double")
)

print("\n--- ORDERS WITH ORDER AMOUNT ---")
clean_orders.show(truncate=False)



joined = clean_orders.join(
    customers.dropDuplicates(["customer_id"]),
    on="customer_id",
    how="left"
)

print("\n--- JOINED ORDER DATA ---")
joined.show(truncate=False)



city_product_revenue = (
    joined.groupBy("city", "product")
    .agg(
        sum("order_amount").alias("total_revenue")
    )
    .orderBy(desc("total_revenue"))
)

print("\n--- REVENUE BY CITY AND PRODUCT ---")
city_product_revenue.show(truncate=False)



status_revenue = (
    joined.groupBy("status")
    .agg(
        sum("order_amount").alias("total_revenue")
    )
    .orderBy(desc("total_revenue"))
)

print("\n--- REVENUE BY STATUS ---")
status_revenue.show(truncate=False)



delivered_orders = joined.filter(
    col("status") == "Delivered"
)

print("\n--- DELIVERED ORDERS ---")
delivered_orders.show(truncate=False)



top_customers = (
    delivered_orders
    .groupBy("customer_id", "customer_name")
    .agg(
        sum("order_amount")
        .alias("delivered_revenue")
    )
    .orderBy(desc("delivered_revenue"))
    .limit(3)
)

print("\n--- TOP THREE CUSTOMERS ---")
top_customers.show(truncate=False)



clean_orders.write.mode("overwrite").parquet(
    str(OUTPUT_DIR / "clean_orders")
)

joined.write.mode("overwrite").parquet(
    str(OUTPUT_DIR / "joined_orders")
)

city_product_revenue.write.mode("overwrite").parquet(
    str(OUTPUT_DIR / "revenue_by_city_product")
)

status_revenue.write.mode("overwrite").parquet(
    str(OUTPUT_DIR / "revenue_by_status")
)

delivered_orders.write.mode("overwrite").parquet(
    str(OUTPUT_DIR / "delivered_orders")
)

top_customers.write.mode("overwrite").parquet(
    str(OUTPUT_DIR / "top_three_customers")
)

print("\nAll reports saved successfully as Parquet files.")

spark.stop()
