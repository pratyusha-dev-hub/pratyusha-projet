from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Clean_GCS_to_BQ") \
    .config("spark.jars", "") \
    .config("spark.driver.extraClassPath", "") \
    .config("spark.executor.extraClassPath", "") \
    .getOrCreate()

df = spark.read.csv(
    "gs://pratyusha_batch_bucket/empdetails.csv",
    header=True,
    inferSchema=True
)

df.show()

df.write \
  .format("bigquery") \
  .option("table", "project-36c16f64-83f8-4df3-b8b:emp_data.empdettable") \
  .option("temporaryGcsBucket", "pratyusha_batch_bucket") \
  .mode("overwrite") \
  .save()

spark.stop()
print("Updated version")
