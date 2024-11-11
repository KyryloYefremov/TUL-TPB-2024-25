from pyspark.sql import SparkSession
from config import *

spark = SparkSession.builder.master(MASTER).appName("SparkSQL").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true").csv("/files/fakefriends-header.csv")
    
print("Here is our inferred schema:")
people.printSchema()

spark.stop()

