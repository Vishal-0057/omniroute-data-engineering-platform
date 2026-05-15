from pyspark.sql import SparkSession

BUCKET="omniroute-data-lake-6600"

PATH=f"s3a://{BUCKET}/silver/vehicle_registry/"

spark = SparkSession.builder \
.appName("check-silver") \
.config(
"spark.hadoop.fs.s3a.aws.credentials.provider",
"com.amazonaws.auth.InstanceProfileCredentialsProvider"
) \
.getOrCreate()

df=spark.read.parquet(PATH)
df.printSchema()
df.show(10)

spark.stop()
