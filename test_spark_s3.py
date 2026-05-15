from pyspark.sql import SparkSession

spark = SparkSession.builder \
.appName("S3-write-test") \
.config(
"spark.hadoop.fs.s3a.aws.credentials.provider",
"com.amazonaws.auth.InstanceProfileCredentialsProvider"
) \
.getOrCreate()

data = [("VIN001","Volvo",2022)]

columns=["vin","model","year"]

df = spark.createDataFrame(data,columns)

df.write.mode("overwrite").parquet(
"s3a://omniroute-data-lake-6600/silver/test_output/"
)

print("write success")

spark.stop()
