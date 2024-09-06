from pyspark.sql import SparkSession

def extract_data(file_path):
    spark = SparkSession.builder.appName("EnergyConsumptionETL").getOrCreate()
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    return df
