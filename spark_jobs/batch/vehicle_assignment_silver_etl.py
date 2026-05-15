from pyspark.sql import SparkSession
from pyspark.sql.functions import (
col,
current_timestamp,
from_unixtime
)
from pyspark.sql.types import (
StringType,
DoubleType
)

#########################################
# CONFIG
#########################################

BUCKET = "omniroute-data-lake-6600"

RAW_PATH = f"s3a://{BUCKET}/raw/vehicle_assignment/"

SILVER_PATH = f"s3a://{BUCKET}/silver/vehicle_assignment/"

#########################################
# CREATE SPARK SESSION
#########################################

spark = SparkSession.builder \
.appName("vehicle-assignment-silver-etl") \
.config(
"spark.hadoop.fs.s3a.aws.credentials.provider",
"com.amazonaws.auth.InstanceProfileCredentialsProvider"
) \
.getOrCreate()

print("Reading RAW vehicle assignment data...")

#########################################
# READ RAW CSV
#########################################

df_raw = spark.read.csv(
RAW_PATH,
header=True
)

print("RAW SAMPLE DATA")
df_raw.show(5)

#########################################
# TRANSFORMATIONS
#########################################

df_clean = df_raw \
.withColumn(
"vin",
col("vin").cast(StringType())
) \
.withColumn(
"driver_id",
col("driver_id").cast(StringType())
) \
.withColumn(
"daily_rate",
col("daily_rate").cast(DoubleType())
) \
.withColumn(
"region",
col("region").cast(StringType())
) \
.withColumn(
"start_date",
from_unixtime(
col("start_timestamp")
).cast("timestamp")
) \
.withColumn(
"end_date",
from_unixtime(
col("end_timestamp")
).cast("timestamp")
) \
.dropDuplicates() \
.withColumn(
"ingestion_ts",
current_timestamp()
)

#########################################
# WRITE TO SILVER LAYER
#########################################

print("Writing cleaned data to SILVER layer...")

df_clean.write \
.mode("overwrite") \
.parquet(SILVER_PATH)

print("SILVER layer created successfully")

#########################################
# VALIDATION
#########################################

print("Sample transformed data")

df_clean.show(10)

print("Schema")

df_clean.printSchema()

#########################################

spark.stop()
