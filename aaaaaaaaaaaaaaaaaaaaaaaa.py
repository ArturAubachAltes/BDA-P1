from pyspark.sql import SparkSession

if not 'spark' in globals():
  spark = SparkSession.builder.getOrCreate()

