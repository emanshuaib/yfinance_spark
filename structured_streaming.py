from shared_spark import spark
import socket
from pyspark.sql.functions import (
    col, window, avg, stddev, max as spark_max, lit
)
from pyspark.sql.types import (
    StructType, TimestampType, DoubleType,
    StringType, IntegerType
)

INPUT_PATH = "global_stock_data"
CHECKPOINT = "checkpoint/"
WATERMARK = "10 minutes"
SOCKET_HOST = "localhost"
SOCKET_PORT = 9999

schema = (
    StructType()
        .add("datetime",      TimestampType())
        .add("open",          DoubleType())
        .add("high",          DoubleType())
        .add("low",           DoubleType())
        .add("close",         DoubleType())
        .add("volume",        DoubleType())
        .add("ticker",        StringType())
        .add("fetch_time",    TimestampType())
        .add("market_status", IntegerType())   # 1=open, 0=closed
)

streaming_df = (
    spark.readStream
         .schema(schema)
         .option("header", True)
         .option("maxFilesPerTrigger", 1)
         .option("recursiveFileLookup", "true")
         .csv(INPUT_PATH)
         .withWatermark("datetime", WATERMARK)
)

agg_df = (
    streaming_df
    .groupBy(window(col("datetime"), "5 minutes", "1 minute"), col("ticker"))
    .agg(
        avg("close").alias("avg_price"),
        stddev("close").alias("volatility"),
        spark_max("market_status").alias("market_status")
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("ticker"),
        col("avg_price"),
        col("volatility"),
        col("market_status")
    )
)

# Process Batch and Send via Socket
prev_avg = {}
def send_to_socket(data, host=SOCKET_HOST, port=SOCKET_PORT):
    try:
        with socket.socket() as s:
            s.connect((host, port))
            if isinstance(data, list):
                for msg in data:
                    s.sendall(msg.encode("utf-8"))
            else:
                s.sendall(data.encode("utf-8"))
    except Exception as e:
        print("Socket send failed:", e)

def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        print(f"[batch {batch_id}] Empty batch.")
        return

    rows = batch_df.collect()
    messages = []

    for r in rows:
        tk     = r["ticker"]
        ws     = r["window_start"]
        avg_p  = r["avg_price"]
        vol    = r["volatility"]
        status = r["market_status"]
        prev   = prev_avg.get(tk)
        pct    = ((avg_p - prev) / prev * 100) if (prev is not None and avg_p is not None) else None
        prev_avg[tk] = avg_p

        # Format 
        price_str = f"{avg_p:.2f}" if avg_p is not None else "—"
        vol_str   = f"{vol:.2f}" if vol is not None else "—"
        pct_str   = f"{pct:.2f}%" if pct is not None else "—"

        msg = f"{ws},{tk},{price_str},{vol_str},{pct_str},{status}\n"
        messages.append(msg)

    send_to_socket(messages)
    print(f"[batch {batch_id}] Sent {len(messages)} rows.")

# Start Streaming 
query = (
    agg_df.writeStream
         .outputMode("complete")
         .foreachBatch(process_batch)
         .start()
)

query.awaitTermination()