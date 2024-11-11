from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv("/files/cv05/marvel-names.txt")

lines = spark.read.text("/files/cv05/marvel-graph.txt")

# Small tweak vs. what's shown in the video: we trim each line of whitespace as that could
# throw off the counts.
connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))

# find min conntctions (consider more than 0)
min_connections = connections.filter(func.col("connections") > 0).agg(func.min("connections")).first()[0]

# find heroes with this min connection value
obscure_heroes = connections.filter(func.col("connections") == min_connections)

obscure_heroes_with_names = obscure_heroes.join(names, "id")

obscure_heroes_with_names.show()

spark.stop()



