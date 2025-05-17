from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SharedStreamingApp") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")