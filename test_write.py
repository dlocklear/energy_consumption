from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TestWrite").getOrCreate()
data = [(1, "Car", 300), (2, "Bus", 150), (3, "Bike", 50)]
columns = ["id", "transport_mode", "energy_consumption"]

df = spark.createDataFrame(data, columns)
df.write.mode("overwrite").parquet("hdfs://path/to/test_output")
