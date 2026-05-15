from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    col,
    count,
    when,
    lit,
    current_date,
    round,
    date_format
)

#########################################
# CONFIG
#########################################

BUCKET = "omniroute-data-lake-6600"

STREAM_PATH = f"s3a://{BUCKET}/gold/realtime_vehicle_events/"

SCD2_PATH = f"s3a://{BUCKET}/gold/asset_history_scd2/"

OUTPUT_PATH = f"s3a://{BUCKET}/gold/driver_safety_status/"

#########################################
# CREATE SPARK SESSION
#########################################

spark = SparkSession.builder \
    .appName("driver-safety-status-gold") \
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.InstanceProfileCredentialsProvider"
    ) \
    .getOrCreate()

print("Reading datasets...")

#########################################
# READ DATA
#########################################

events_df = spark.read.parquet(
    STREAM_PATH
)

scd_df = spark.read.parquet(
    SCD2_PATH
)

#########################################
# VALIDATE INPUT DATA
#########################################

print("Streaming events sample:")

events_df.show(5, truncate=False)

print("SCD2 sample:")

scd_df.show(5, truncate=False)

#########################################
# FILTER ONLY VIOLATION EVENTS
#########################################

violation_df = events_df.filter(
    (col("speed_flag") == True) |
    (col("zone_violation") == "VIOLATION")
)

print("Violation events sample:")

violation_df.show(10, truncate=False)

#########################################
# COUNT STRIKES PER DRIVER
#########################################

strike_df = violation_df.groupBy(
    "driver_id",
    "vin"
).agg(
    count("*").alias("strike_count")
)

print("Strike counts:")

strike_df.show(20, truncate=False)

#########################################
# GET ACTIVE DRIVER RECORDS FROM SCD2
#########################################

active_driver_df = scd_df.filter(
    col("status") == "IN-TRANSIT"
).select(
    "driver_id",
    "vin",
    col("daily_rate").alias("base_rate"),
    "region"
)

#########################################
# JOIN STRIKES WITH DRIVER RATES
#########################################

final_df = strike_df.join(
    active_driver_df,
    ["driver_id","vin"],
    "left"
)

#########################################
# CALCULATE ADJUSTED RATE
#########################################

final_df = final_df.withColumn(
    "current_adjusted_rate",
    round(
        col("base_rate") * (
            1 - (0.05 * col("strike_count"))
        ),
        2
    )
)

#########################################
# PAYROLL SAFE FINAL RATE
#########################################

final_df = final_df.withColumn(
    "final_payable_rate",
    when(
        col("current_adjusted_rate") < 0,
        0
    ).otherwise(
        col("current_adjusted_rate")
    )
)

#########################################
# APPLY SUSPENSION LOGIC
#########################################

final_df = final_df.withColumn(
    "status",
    when(
        col("strike_count") >= 10,
        "SUSPENDED"
    ).otherwise("ACTIVE")
)

#########################################
# ADD REPORTING MONTH
#########################################

final_df = final_df.withColumn(
    "month",
    date_format(current_date(), "yyyy-MM")
)

#########################################
# REMOVE DUPLICATES
#########################################

final_df = final_df.dropDuplicates(
    ["driver_id", "vin", "month"]
)

#########################################
# SELECT FINAL COLUMNS
#########################################

final_df = final_df.select(
    "driver_id",
    "vin",
    "region",
    "base_rate",
    "strike_count",
    "current_adjusted_rate",
    "final_payable_rate",
    "status",
    "month"
)

#########################################
# SHOW FINAL OUTPUT
#########################################

print("Final driver safety status output:")

final_df.show(50, truncate=False)

print("Schema:")

final_df.printSchema()

#########################################
# WRITE GOLD TABLE
#########################################

print("Writing driver safety status gold table...")

final_df.write \
    .mode("append") \
    .partitionBy("month") \
    .parquet(OUTPUT_PATH)

print("Driver safety status pipeline completed successfully")

spark.stop()
