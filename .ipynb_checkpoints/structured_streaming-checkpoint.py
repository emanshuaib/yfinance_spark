afrom pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, avg, stddev
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType


# 1. Create a Spark Session
spark = SparkSession.builder \
    .appName("GlobalMarketStreaming") \
    .getOrCreate()

output_dir = "global_stock_data/"

# 2. Define schema manually
schema = StructType([
    StructField("Datetime", TimestampType(), True),
    StructField("Open", DoubleType(), True),
    StructField("High", DoubleType(), True),
    StructField("Low", DoubleType(), True),
    StructField("Close", DoubleType(), True),
    StructField("Adj Close", DoubleType(), True),
    StructField("Volume", DoubleType(), True),
    StructField("Ticker", StringType(), True),
    StructField("Timestamp", TimestampType(), True)
])

# Read streaming CSV
streaming_df = spark.readStream.option("header", True).schema(schema).csv(output_dir)

# Add watermark
streaming_df = streaming_df.withWatermark("Datetime", "10 minutes")

# Group by window
metrics_df = streaming_df.groupBy(
    window(col("Datetime"), "5 minutes", "1 minute"),
    col("Ticker")
).agg(
    avg("Close").alias("avg_price"),
    stddev("Close").alias("volatility")
)

# Write stream
query = metrics_df.writeStream.outputMode("append").format("parquet") \
    .option("path", "parquet_output/") \
    .option("checkpointLocation", "checkpoint/") \
    .trigger(processingTime="1 minute") \
    .start()

query.awaitTermination()