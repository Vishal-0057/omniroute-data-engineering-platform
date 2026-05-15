from pyspark.sql import SparkSession
from pyspark.sql.functions import (
col,
lead,
when
)
from pyspark.sql.window import Window

#########################################
# CONFIG
#########################################

BUCKET = "omniroute-data-lake-6600"

SILVER_PATH = f"s3a://{BUCKET}/silver/vehicle_assignment/"

GOLD_PATH = f"s3a://{BUCKET}/gold/asset_history_scd2/"

#########################################
# CREATE SPARK SESSION
#########################################

spark = SparkSession.builder \
.appName("vehicle-assignment-scd2-gold") \
.config(
"spark.hadoop.fs.s3a.aws.credentials.provider",
"com.amazonaws.auth.InstanceProfileCredentialsProvider"
) \
.getOrCreate()

print("Reading SILVER data...")

#########################################
# READ SILVER DATA
#########################################

df = spark.read.parquet(SILVER_PATH)

df.show(5)

#########################################
# WINDOW SPECIFICATION
#########################################

window_spec = Window.partitionBy(
"vin"
).orderBy(
"start_date"
)

#########################################
# CREATE SCD2 STRUCTURE
#########################################

df_scd = df \
.withColumn(
"next_start_date",
lead("start_date").over(window_spec)
) \
.withColumn(
"end_date",
col("next_start_date")
)

#########################################
# STATUS COLUMN
#########################################

df_scd = df_scd \
.withColumn(
"status",
when(
col("end_date").isNull(),
"IN-TRANSIT"
).otherwise("ARCHIVED")
)

#########################################
# CLEAN FINAL COLUMNS
#########################################

df_final = df_scd.select(

"vin",

"driver_id",

"region",

"daily_rate",

"start_date",

"end_date",

"status",

"ingestion_ts"

)

#########################################
# WRITE GOLD TABLE
#########################################

print("Writing SCD2 GOLD table...")

df_final.write \
.mode("overwrite") \
.parquet(GOLD_PATH)

#########################################
# VALIDATE OUTPUT
#########################################

print("Final SCD2 table sample")

df_final.show(20)

print("Schema")

df_final.printSchema()

#########################################

spark.stop()

print("GOLD SCD2 pipeline completed successfully")
