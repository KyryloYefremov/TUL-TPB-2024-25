from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, regexp_replace, lower, window

from config import *

# init spark session
spark = SparkSession.builder.master(MASTER).appName("WordCountFiles").getOrCreate()

files_dir = '/files/cv07-data'

# read data from files
lines = spark.readStream.format("text").option("path", files_dir).option("maxFilesPerTrigger", 1).load()

lines.printSchema()

# split lines into words and preprocess them
words = lines.select(
            explode(
                split(regexp_replace(lower(lines.value), "[^a-zA-Z\\s]", ""), "\\s+")
            ).alias("word")
        ).filter("word != ''")


# count words
word_counts = words.groupBy("word").count()

# format
formatted_counts = word_counts.orderBy("count", ascending=False)

query = (formatted_counts.writeStream.outputMode("complete").format("console").option("truncate", "false").queryName("counts").start())

query.awaitTermination()

spark.stop()