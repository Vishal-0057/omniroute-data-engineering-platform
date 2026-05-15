from pyspark.sql import SparkSession

BUCKET="omniroute-data-lake-6600"

PATH=f"s3a://{BUCKET}/gold/asset_history_scd2/"

spark=SparkSession.builder \
.appName("check") \
.config(
"spark.hadoop.fs.s3a.aws.credentials.provider",
"com.amazonaws.auth.InstanceProfileCredentialsProvider"
).getOrCreate()

df=spark.read.parquet(PATH)

df.show(20)
df.printSchema()

spark.stop()
