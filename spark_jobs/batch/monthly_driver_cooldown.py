from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    col,
    lit,
    current_date,
    date_format,
    max as spark_max
)

#########################################
# CONFIG
#########################################

BUCKET = "omniroute-data-lake-6600"

INPUT_PATH = f"s3a://{BUCKET}/gold/driver_safety_status/"

OUTPUT_PATH = f"s3a://{BUCKET}/gold/driver_safety_status/"

#########################################
# CREATE SPARK SESSION
#########################################

spark = SparkSession.builder \
    .appName("monthly-driver-cooldown") \
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.InstanceProfileCredentialsProvider"
    ) \
    .getOrCreate()

print("Reading driver safety snapshots...")

#########################################
# READ ALL SNAPSHOTS
#########################################

df = spark.read.parquet(
    INPUT_PATH
)

#########################################
# FIND LATEST SNAPSHOT MONTH
#########################################

latest_month = df.select(
    spark_max("month")
).collect()[0][0]

print(f"Latest snapshot month: {latest_month}")

#########################################
# FILTER ONLY LATEST SNAPSHOT
#########################################

df = df.filter(
    col("month") == latest_month
)

print("Latest month snapshot:")

df.show(20, truncate=False)

#########################################
# SPLIT ACTIVE VS SUSPENDED
#########################################

active_df = df.filter(
    col("status") != "SUSPENDED"
)

suspended_df = df.filter(
    col("status") == "SUSPENDED"
)

#########################################
# RESET ELIGIBLE DRIVERS
#########################################

reset_df = active_df \
    .withColumn("strike_count", lit(0)) \
    .withColumn(
        "current_adjusted_rate",
        col("base_rate")
    ) \
    .withColumn(
        "final_payable_rate",
        col("base_rate")
    ) \
    .withColumn(
        "status",
        lit("ACTIVE")
    )

#########################################
# GENERATE CURRENT MONTH SNAPSHOT
#########################################

current_month = date_format(
    current_date(),
    "yyyy-MM"
)

reset_df = reset_df.withColumn(
    "month",
    current_month
)

suspended_df = suspended_df.withColumn(
    "month",
    current_month
)

#########################################
# UNION FINAL SNAPSHOT
#########################################

final_df = reset_df.unionByName(
    suspended_df
)

#########################################
# REMOVE DUPLICATES
#########################################

final_df = final_df.dropDuplicates(
    ["driver_id", "vin", "month"]
)

#########################################
# SHOW FINAL OUTPUT
#########################################

print("Cooldown snapshot output:")

final_df.show(50, truncate=False)

print("Schema:")

final_df.printSchema()

#########################################
# WRITE CURRENT MONTH SNAPSHOT
#########################################

print("Writing monthly cooldown snapshot...")

final_df.write \
    .mode("append") \
    .partitionBy("month") \
    .parquet(OUTPUT_PATH)

print("Monthly cooldown completed successfully")

spark.stop()
