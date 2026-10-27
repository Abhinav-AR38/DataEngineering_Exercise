
from pyspark.sql import SparkSession

LOG_FILE = "data\\server_logs.txt"
OUTPUT_DIR = "data\\output"

spark = (
    SparkSession.builder
    .appName("ServerLogAnalysis")
    .getOrCreate()
)

sc = spark.sparkContext
sc.setLogLevel("WARN")


lines = sc.textFile(str(LOG_FILE))


def parse_log(line):
    fields = line.strip().split(",")

    if len(fields) != 5:
        return None

    try:
        timestamp = fields[0]
        server = fields[1]
        endpoint = fields[2]
        status_code = int(fields[3])
        response_time = int(fields[4])

        return (
            timestamp,
            server,
            endpoint,
            status_code,
            response_time
        )

    except ValueError:
        return None


logs = (
    lines.map(parse_log)
    .filter(lambda record: record is not None)
    .cache()
)


total_records = logs.count()

print("\n--- TOTAL LOG RECORDS ---")
print(total_records)


errors = logs.filter(lambda record: record[3] >= 400)

print("\n--- FAILED REQUESTS ---")
for record in errors.collect():
    print(record)


errors_by_status = (
    errors
    .map(lambda record: (record[3], 1))
    .reduceByKey(lambda a, b: a + b)
)

print("\n--- ERRORS BY STATUS CODE ---")
for status, count in sorted(errors_by_status.collect()):
    print(f"HTTP {status}: {count}")


endpoint_counts = (
    logs
    .map(lambda record: (record[2], 1))
    .reduceByKey(lambda a, b: a + b)
)

print("\n--- REQUESTS BY ENDPOINT ---")
for endpoint, count in sorted(endpoint_counts.collect()):
    print(f"{endpoint}: {count}")


server_response = (
    logs
    .map(lambda record: (record[1], (record[4], 1)))
    .reduceByKey(
        lambda a, b: (
            a[0] + b[0],
            a[1] + b[1]
        )
    )
    .mapValues(lambda result: result[0] / result[1])
)

print("\n--- AVERAGE RESPONSE TIME BY SERVER ---")
for server, average in sorted(server_response.collect()):
    print(f"{server}: {average:.2f} ms")


failed_by_server = (
    errors
    .map(lambda record: (record[1], 1))
    .reduceByKey(lambda a, b: a + b)
)

failed_counts = failed_by_server.collect()

print("\n--- FAILED REQUESTS BY SERVER ---")
for server, count in sorted(failed_counts):
    print(f"{server}: {count}")

if failed_counts:
    max_failures = max(count for server, count in failed_counts)

    worst_servers = [
        (server, count)
        for server, count in failed_counts
        if count == max_failures
    ]

    print("Server(s) with most failures:", worst_servers)



error_summary = errors_by_status.map(
    lambda record:
        f"HTTP status {record[0]}: {record[1]} errors"
)

error_output_path = OUTPUT_DIR / "error_summary"

error_summary.saveAsTextFile(str(error_output_path))

print("\nError summary saved to:", error_output_path)


endpoint_average = (
    logs
    .map(lambda record: (record[2], (record[4], 1)))
    .reduceByKey(
        lambda a, b: (
            a[0] + b[0],
            a[1] + b[1]
        )
    )
    .mapValues(lambda result: result[0] / result[1])
)

slow_endpoints = (
    endpoint_average
    .filter(lambda record: record[1] > 500)
)

print("\n--- ENDPOINTS AVERAGING OVER 500 MS ---")

for endpoint, average in sorted(slow_endpoints.collect()):
    print(f"{endpoint}: {average:.2f} ms")

spark.stop()
