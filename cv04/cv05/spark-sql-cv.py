from pyspark.sql import SparkSession
from config import *

spark = SparkSession.builder.master(MASTER).appName("SparkSQL").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true").csv("/files/cv05/fakefriends-header.csv")

average_friends_by_age = people.groupBy("age").avg("friends") \
    .withColumnRenamed("avg(friends)", "friends_avg")

average_friends_by_age.orderBy("age").show()

spark.stop()

