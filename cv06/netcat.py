from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, regexp_replace, lower, window

from config import *

# init spark session
spark = SparkSession.builder.master(MASTER).appName("WordCountStreaming").getOrCreate()

# read data from stream
lines = spark.readStream.format("socket").option("host", "localhost").option("port", PORT).option("includeTimestamp", True).load()

# lines.printSchema()

# split lines into words and preprocess them
words = lines.select(
            "timestamp",
            explode(
                split(regexp_replace(lower(lines.value), "[^a-zA-Z\\s]", ""), "\\s+")
            ).alias("word")
        ).filter("word != ''")


# count words using windows
words_count = words \
        .withWatermark("timestamp", "1 minute") \
        .groupBy(
            window("timestamp", "30 seconds", "15 seconds"),
            "word"
        ).count()

# format output
formatted_counts = words_count \
        .select(
            "window.start",
            "window.end",
            "word",
            "count"
        ) \
        .orderBy("window.start", "count", ascending=[True, False])


query = (words_count.writeStream.outputMode("complete").format("console").option("truncate", "false").queryName("counts").start())

query.awaitTermination()

spark.stop()