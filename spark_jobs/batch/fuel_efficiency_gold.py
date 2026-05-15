from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lag,
    when,
    round,
    to_date,
    dayofweek,
)
from pyspark.sql.window import Window

#########################################
# CONFIG
#########################################

BUCKET = "omniroute-data-lake-6600"

FUEL_PATH = f"s3a://{BUCKET}/raw/fuel_transactions/"
VEHICLE_PATH = f"s3a://{BUCKET}/silver/vehicle_registry/"
MAINTENANCE_PATH = f"s3a://{BUCKET}/raw/maintenance_logs/"

GOLD_PATH = f"s3a://{BUCKET}/gold/fuel_efficiency_audit/"

#########################################
# CREATE SPARK SESSION
#########################################

spark = SparkSession.builder \
.appName("fuel-efficiency-gold") \
.config(
    "spark.hadoop.fs.s3a.aws.credentials.provider",
    "com.amazonaws.auth.InstanceProfileCredentialsProvider"
) \
.getOrCreate()

print("Reading datasets...")

#########################################
# READ DATA
#########################################

fuel_df = spark.read.csv(
    FUEL_PATH,
    header=True
)

vehicle_df = spark.read.parquet(
    VEHICLE_PATH
)

maintenance_df = spark.read.csv(
    MAINTENANCE_PATH,
    header=True
)

#########################################
# PREPARE FUEL DATA
#########################################

fuel_df = fuel_df \
.withColumn("fuel_liters", col("fuel_liters").cast("double")) \
.withColumn("odometer_reading", col("odometer_reading").cast("double")) \
.withColumn("transaction_date", to_date(col("timestamp")))

#########################################
# CALCULATE DISTANCE USING WINDOW
#########################################

window_spec = Window.partitionBy("vin").orderBy("transaction_date")

fuel_df = fuel_df \
.withColumn("prev_odometer", lag("odometer_reading").over(window_spec)) \
.withColumn("distance_travelled", round(col("odometer_reading") - col("prev_odometer"),2))

#########################################
# CALCULATE FUEL EFFICIENCY
#########################################

fuel_df = fuel_df \
.withColumn("km_per_liter", round(col("distance_travelled") / col("fuel_liters"),2))

#########################################
# REMOVE INVALID FIRST RECORDS
#########################################

fuel_df = fuel_df.filter(
    col("km_per_liter").isNotNull()
)
#########################################
# JOIN WITH VEHICLE DIMENSION
#########################################

fuel_df = fuel_df.join(
    vehicle_df,
    "vin",
    "left"
)

#########################################
# BASELINE CALCULATION (CORRECTED)
#########################################

fuel_df = fuel_df.withColumn(
    "baseline_kmpl",
    when(col("fuel_type") == "Diesel", 8)
    .when(col("fuel_type") == "CNG", 10)
    .when(col("fuel_type") == "LNG", 9)
    .otherwise(7)
)

#########################################
# ANOMALY DETECTION
#########################################

fuel_df = fuel_df.withColumn(
    "status",
    when(
        col("km_per_liter") < col("baseline_kmpl") * 0.88,
        "FLAGGED"
    ).otherwise("OK")
)

#########################################
# REMOVE MAINTENANCE DAYS
#########################################

maintenance_df = maintenance_df \
.withColumn("service_date", to_date(col("service_date")))

fuel_df = fuel_df.join(
    maintenance_df,
    [
        fuel_df.vin == maintenance_df.vin,
        fuel_df.transaction_date == maintenance_df.service_date
    ],
    "left_anti"
)

#########################################
# REMOVE WEEKENDS
#########################################

fuel_df = fuel_df.filter(
    ~dayofweek(col("transaction_date")).isin([1])
)

#########################################
# SELECT FINAL COLUMNS (FACT TABLE)
#########################################

final_df = fuel_df.select(
    "vin",
    "model",
    col("transaction_date").alias("audit_date"),
    "fuel_liters",
    "distance_travelled",
    "km_per_liter",
    "fuel_type",
    "baseline_kmpl",
    "status"
)

#########################################
# WRITE GOLD TABLE
#########################################

print("Writing fuel efficiency fact table...")

final_df.write \
.mode("overwrite") \
.parquet(GOLD_PATH)

#########################################
# VALIDATE OUTPUT
#########################################

print("Sample output:")

final_df.groupBy("vin").count().show()

print("Schema:")

final_df.printSchema()

spark.stop()

print("Fuel efficiency pipeline completed successfully")
