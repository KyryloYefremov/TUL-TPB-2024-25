from pyspark.sql import SparkSession

spark = SparkSession.builder.master("spark://fa367db42f31:7077").appName("SparkSQL").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true").csv("/files/fakefriends-header.csv")
    
print("Here is our inferred schema:")
people.printSchema()

spark.stop()

