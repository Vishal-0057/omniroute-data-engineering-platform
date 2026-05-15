from pyspark.sql import SparkSession

print("STARTING SPARK")

spark = SparkSession.builder \
    .appName("check-stream-output") \
    .getOrCreate()

print("SPARK STARTED")

print("STARTING READ")

df = spark.read.parquet(
    "s3a://omniroute-data-lake-6600/gold/realtime_vehicle_events/"
)

print("READ COMPLETE")

print("COUNTING ROWS")

print(df.count())

print("SHOWING DATA")

df.show(truncate=False)

print("PRINTING SCHEMA")

df.printSchema()
