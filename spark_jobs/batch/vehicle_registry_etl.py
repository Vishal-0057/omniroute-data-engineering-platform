from pyspark.sql import SparkSession
from pyspark.sql.functions import col,current_timestamp
from pyspark.sql.types import IntegerType,StringType

#########################################
# CONFIG
#########################################

BUCKET="omniroute-data-lake-6600"

RAW_PATH=f"s3a://{BUCKET}/raw/vehicle_registry/"

SILVER_PATH=f"s3a://{BUCKET}/silver/vehicle_registry/"

#########################################
# CREATE SPARK SESSION
#########################################

spark = SparkSession.builder \
.appName("vehicle-registry-etl") \
.config(
"spark.hadoop.fs.s3a.aws.credentials.provider",
"com.amazonaws.auth.InstanceProfileCredentialsProvider"
) \
.getOrCreate()

#########################################
# READ RAW DATA
#########################################

df = spark.read.csv(
RAW_PATH,
header=True
)

print("RAW DATA")
df.show(5)

#########################################
# TRANSFORMATIONS
#########################################

df_clean = df \
.withColumn("vin",col("vin").cast(StringType())) \
.withColumn("model",col("model").cast(StringType())) \
.withColumn("mfg_year",col("mfg_year").cast(IntegerType())) \
.withColumn("fuel_type",col("fuel_type").cast(StringType())) \
.dropDuplicates(["vin"]) \
.withColumn("ingestion_timestamp",current_timestamp())

print("CLEAN DATA")
df_clean.show(5)

#########################################
# WRITE TO SILVER LAYER
#########################################

df_clean.write \
.mode("overwrite") \
.parquet(SILVER_PATH)

print("data written to silver layer")

#########################################

spark.stop()
