

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType
)
from pyspark.sql.functions import (
    col,
    greatest,
    lit,
    when,
    sum,
    avg,
    round
)


CSV_PATH = "data\\telecom_usage.csv"
OUTPUT_DIR = "data\\output"

spark = (
    SparkSession.builder
    .appName("TelecomCustomerUsageAnalysis")
    .getOrCreate()
)

schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("data_used_gb", IntegerType(), True),
    StructField("call_minutes", IntegerType(), True),
    StructField("plan_limit_gb", IntegerType(), True)
])

df = (
    spark.read
    .option("header", True)
    .schema(schema)
    .csv(str(CSV_PATH))
)


print("\n--- DATA SCHEMA ---")
df.printSchema()

print("\n--- ALL CUSTOMER RECORDS ---")
df.show(truncate=False)

print("Total records:", df.count())


df = df.withColumn(
    "excess_gb",
    greatest(
        col("data_used_gb") - col("plan_limit_gb"),
        lit(0)
    )
)


df = df.withColumn(
    "excess_charge",
    col("excess_gb") * lit(10)
)


df = df.withColumn(
    "usage_status",
    when(
        col("data_used_gb") > col("plan_limit_gb"),
        "Exceeded"
    ).otherwise("Within Limit")
)

print("\n--- PROCESSED CUSTOMER DATA ---")

df.select(
    "customer_id",
    "customer_name",
    "city",
    "data_used_gb",
    "plan_limit_gb",
    "excess_gb",
    "excess_charge",
    "usage_status"
).show(truncate=False)


city_summary = (
    df.groupBy("city")
    .agg(
        sum("data_used_gb").alias("total_data_used_gb"),
        round(
            avg("data_used_gb"), 2
        ).alias("average_data_used_gb"),
        sum("excess_charge").alias("total_excess_charges")
    )
    .orderBy(col("total_data_used_gb").desc())
)

print("\n--- CITY-WISE USAGE SUMMARY ---")
city_summary.show(truncate=False)


top_five = (
    df.orderBy(col("data_used_gb").desc())
    .select(
        "customer_id",
        "customer_name",
        "city",
        "data_used_gb"
    )
    .limit(5)
)

print("\n--- TOP FIVE CUSTOMERS ---")
top_five.show(truncate=False)


exceeded_customers = df.filter(
    col("data_used_gb") > col("plan_limit_gb")
)

print("\n--- CUSTOMERS EXCEEDING PLAN LIMIT ---")
exceeded_customers.show(truncate=False)



df.write.mode("overwrite").parquet(
    str(OUTPUT_DIR / "processed_usage")
)

city_summary.write.mode("overwrite").parquet(
    str(OUTPUT_DIR / "city_usage_summary")
)

top_five.write.mode("overwrite").parquet(
    str(OUTPUT_DIR / "top_five_customers")
)

exceeded_customers.write.mode("overwrite").parquet(
    str(OUTPUT_DIR / "customers_exceeded")
)

print("\nAll Parquet outputs saved successfully.")

spark.stop()
