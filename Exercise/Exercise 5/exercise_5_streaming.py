
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    split,
    trim,
    to_timestamp,
    when,
    window,
    avg,
    max as spark_max
)

OUTPUT_DIR = "data\\output"
CHECKPOINT_DIR = "data\\checkpoints"

HOST = "localhost"
PORT = 9999

spark = (
    SparkSession.builder
    .appName("IoTSensorMonitoring")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

raw_stream = (
    spark.readStream
    .format("socket")
    .option("host", HOST)
    .option("port", PORT)
    .load()
)

fields = split(col("value"), ",")

parsed = raw_stream.select(
    to_timestamp(
        trim(fields.getItem(0)),
        "yyyy-MM-dd HH:mm:ss"
    ).alias("timestamp"),
    trim(fields.getItem(1)).alias("sensor_id"),
    trim(fields.getItem(2)).alias("machine_id"),
    trim(fields.getItem(3)).cast("double").alias("temperature")
)

print("--- STREAMING SCHEMA ---")
parsed.printSchema()


valid = parsed.filter(
    col("timestamp").isNotNull()
    & col("temperature").isNotNull()
    & col("sensor_id").isNotNull()
    & col("machine_id").isNotNull()
)


flagged = valid.withColumn(
    "temperature_status",
    when(col("temperature") > 90, "HIGH")
    .otherwise("NORMAL")
)


readings_query = (
    flagged.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", False)
    .option(
        "checkpointLocation",
        str(CHECKPOINT_DIR / "readings")
    )
    .start()
)


window_stats = (
    valid
    .withWatermark("timestamp", "2 minutes")
    .groupBy(
        window(col("timestamp"), "1 minute"),
        col("machine_id")
    )
    .agg(
        avg("temperature").alias("average_temperature"),
        spark_max("temperature").alias("maximum_temperature")
    )
)


high_average = window_stats.filter(
    col("average_temperature") > 85
)


summary_query = (
    high_average.writeStream
    .format("console")
    .outputMode("update")
    .option("truncate", False)
    .option(
        "checkpointLocation",
        str(CHECKPOINT_DIR / "high_average")
    )
    .start()
)

print("IoT streaming started.")
print(f"Listening for sensor data at {HOST}:{PORT}")
print("Send CSV readings from another terminal.")
print("Press Ctrl+C to stop.")

spark.streams.awaitAnyTermination()
