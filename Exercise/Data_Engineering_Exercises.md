# Data Engineering Hands-on Exercises

Five practical exercises with sample data files are included. These are student activities; solutions are not included.

## Exercise 1 — Retail Sales Analytics
**Technology:** Pandas + Matplotlib  
**Scenario:** Analyze retail transactions to understand product performance, daily revenue, and regional sales.

**Sample file:** `retail_sales.csv`

**Activities**
1. Load the CSV into a Pandas DataFrame.
2. Inspect schema, row count, data types, and missing values.
3. Create `total_sales = quantity * unit_price`.
4. Calculate total revenue.
5. Find the top three products by revenue.
6. Calculate revenue by region.
7. Calculate daily revenue.
8. Plot regional revenue as a bar chart.
9. Plot daily revenue as a line chart.
10. Identify the best-performing product and region.

## Exercise 2 — Telecom Customer Usage Analysis
**Technology:** PySpark DataFrame  
**Scenario:** Calculate data usage charges and identify customers exceeding plan limits.

**Sample file:** `telecom_usage.csv`

**Activities**
1. Create a SparkSession and load the CSV with an explicit schema.
2. Inspect the schema and records.
3. Calculate excess usage as `greatest(data_used_gb - plan_limit_gb, 0)`.
4. Calculate additional charge at ₹10 per excess GB.
5. Add `usage_status`: `Exceeded` or `Within Limit`.
6. Calculate total usage by city.
7. Find the top five customers by usage.
8. Calculate average usage and excess charges by city.
9. Filter customers exceeding their plan.
10. Save processed results as Parquet.

## Exercise 3 — E-commerce Order Processing Pipeline
**Technology:** PySpark DataFrame  
**Scenario:** Clean order data, join customer information, and build revenue reports.

**Sample files:** `orders.csv`, `customers.csv`

**Activities**
1. Load both files into Spark DataFrames.
2. Inspect data types and required columns.
3. Check duplicate order IDs and missing customer IDs.
4. Calculate `order_amount = quantity * unit_price`.
5. Join orders and customers on `customer_id`.
6. Calculate revenue by city and product.
7. Compare revenue by order status.
8. Create a report of delivered orders only.
9. Find the top three customers by delivered-order revenue.
10. Write cleaned data and aggregated reports to Parquet.

**Challenge:** Process only newly arrived orders each day.

## Exercise 4 — Server Log Analysis Using PySpark RDD
**Technology:** PySpark RDD  
**Scenario:** Analyze application logs for errors and slow endpoints.

**Sample file:** `server_logs.txt`

Format: `timestamp,server,endpoint,status_code,response_time_ms`

**Activities**
1. Load using `sc.textFile()`.
2. Split each line into fields using `map()`.
3. Convert records into structured tuples.
4. Count log records.
5. Filter status codes >= 400.
6. Count errors by status code using `map()` and `reduceByKey()`.
7. Count requests by endpoint.
8. Calculate average response time per server using key-value RDDs and `reduceByKey()`.
9. Identify servers with the most failures.
10. Save summaries with `saveAsTextFile()`.

**Challenge:** Find endpoints with average response time above 500 ms.

## Exercise 5 — Real-Time IoT Sensor Monitoring
**Technology:** PySpark Structured Streaming  
**Scenario:** Monitor machine temperatures and flag possible overheating.

**Sample file:** `iot_sample_events.txt` contains example events to type or send into the socket source.

Format: `timestamp,sensor_id,machine_id,temperature`

**Suggested local test:** Run a TCP listener such as `nc -lk 9999` in one terminal, then run a PySpark application reading from `localhost:9999`. Send new records to the listener. Netcat options vary by operating system.

**Activities**
1. Create a streaming DataFrame from the socket source.
2. Parse incoming comma-separated fields and apply appropriate data types.
3. Filter invalid timestamps and temperatures.
4. Add a status: `HIGH` if temperature > 90°C, otherwise `NORMAL`.
5. Calculate average temperature per machine in one-minute tumbling windows.
6. Calculate maximum temperature per machine per window.
7. Identify machines whose window average exceeds 85°C.
8. Display live output using `writeStream`.
9. Configure a checkpoint location and explain its purpose.
10. Send additional readings and observe changes.

**Challenge:** Replace the socket source with Kafka.

**Important:** The socket source is intended for local learning, not production ingestion. Production systems should use a durable streaming source such as Kafka. Event-time windowing, watermarks, and checkpointing should be considered for robust streaming jobs.

## Suggested duration
Allow about 2–3 hours per exercise. The streaming exercise may take longer for students new to Structured Streaming.
