from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.master("local").appName("Arithmetic Example").getOrCreate()

# Sample data
data = [(1, 10, 3), (2, 20, 5), (3, 15, 4)]
df = spark.createDataFrame(data, ["id", "value1", "value2"])

# Perform arithmetic operations
df = df.withColumn("sum", col("value1") + col("value2")) \
       .withColumn("difference", col("value1") - col("value2")) \
       .withColumn("product", col("value1") * col("value2")) \
       .withColumn("quotient", col("value1") / col("value2")) \
       .withColumn("remainder", col("value1") % col("value2"))

df.show()
