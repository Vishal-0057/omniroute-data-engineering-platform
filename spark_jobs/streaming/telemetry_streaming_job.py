from pyspark.sql import SparkSession
from pyspark.sql.functions import (
 col,
 from_json,
 when,
 from_unixtime,
 coalesce,
 lit,
 broadcast
) 
from pyspark.sql.types import *

#########################################
# CONFIG
#########################################

spark = SparkSession.builder \
.appName("telemetry-streaming") \
.config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
        "org.apache.commons:commons-pool2:2.11.1"
) \
.getOrCreate()

spark.sparkContext.setLogLevel("WARN")

#########################################
# SCHEMA
#########################################

schema = StructType([
    StructField("vin", StringType()),
    StructField("timestamp", LongType()),
    StructField("lat", DoubleType()),
    StructField("long", DoubleType()),
    StructField("speed", IntegerType())
])

zone_schema = StructType([
    StructField("zone_name", StringType()),
    StructField("min_lat", DoubleType()),
    StructField("max_lat", DoubleType()),
    StructField("min_long", DoubleType()),
    StructField("max_long", DoubleType())
])

#########################################
# LOAD SCD2 TABLE
#########################################

scd_path = "s3a://omniroute-data-lake-6600/gold/asset_history_scd2/"

scd_df = spark.read.parquet(scd_path)

#########################################
# FIX NULL END DATES
#########################################

scd_df = scd_df.withColumn(
    "end_date",
    coalesce(
        col("end_date"),
        lit("9999-12-31").cast("timestamp")
    )
)


#########################################
# READ STREAM FROM KAFKA
#########################################

df = spark.readStream \
.format("kafka") \
.option("kafka.bootstrap.servers", "localhost:9092") \
.option("subscribe", "vehicle_telemetry") \
.option("startingOffsets", "latest") \
.load()

#########################################
# PARSE JSON
#########################################

df_parsed = df.selectExpr("CAST(value AS STRING)") \
.select(from_json(col("value"), schema).alias("data")) \
.select("data.*")

#########################################
# CONVERT EVENT TIME
#########################################

df_processed = df_parsed.withColumn(
    "event_time",
    from_unixtime(
        col("timestamp")
    ).cast("timestamp")
)

#########################################
# SIMPLE PROCESSING
#########################################

df_processed = df_processed.withColumn(
    "speed_flag",
    col("speed") > 80
)

#########################################
# SCD2 TEMPORAL JOIN
#########################################

join_condition = (
    (df_processed["vin"] == scd_df["vin"]) &
    (df_processed["event_time"] >= scd_df["start_date"]) &
    (df_processed["event_time"] < scd_df["end_date"])
)

df_enriched = df_processed.join(
    scd_df,
    join_condition,
    "left"
)

#########################################
# CLEAN DUPLICATE VIN COLUMN
#########################################

df_enriched = df_enriched.select(
    df_processed["vin"],
    "driver_id",
    "event_time",
    "lat",
    "long",
    "speed",
    "speed_flag",
    "region"
)

zones_df = spark.read.option("multiline", "true").json(
    "file:///home/ubuntu/omniroute-project/sample_data/restricted_zones.json"
)

#zones_df.show(truncate=False)
#zones_df.printSchema()

df_joined = df_enriched.crossJoin(broadcast(zones_df))

######
#ADD zone condition
#####

df_with_zone = df_joined.withColumn(
    "in_restricted_zone",
    (
        (col("lat") >= col("min_lat")) &
        (col("lat") <= col("max_lat")) &
        (col("long") >= col("min_long")) &
        (col("long") <= col("max_long"))
    )
)

#####add zone violation###

df_final = df_with_zone.withColumn(
    "violated_zone_name",
    when(col("in_restricted_zone") == True, col("zone_name"))
)


df_final = df_final.withColumn(
    "zone_violation",
    when(col("in_restricted_zone") == True, "VIOLATION")
    .otherwise("OUTSIDE_ZONE")
)

df_final = df_final.filter(col("in_restricted_zone") == True)
df_final = df_final.select(
    "vin",
    "driver_id",
    "event_time",
    "lat",
    "long",
    "speed",
    "speed_flag",
    "zone_name",
    "zone_violation",
    "region",
)


#########################################
# OUTPUT TO CONSOLE
#########################################


query = df_final.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option(
        "path",
        "s3a://omniroute-data-lake-6600/gold/realtime_vehicle_events/"
    ) \
    .option(
        "checkpointLocation",
        "s3a://omniroute-data-lake-6600/checkpoints/realtime_vehicle_events/"
    ) \
    .start()

query.awaitTermination()
