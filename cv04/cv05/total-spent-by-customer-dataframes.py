from pyspark.sql import SparkSession, Row, functions
from config import *

spark = SparkSession.builder.master(MASTER).appName("SparkSQL").getOrCreate()

# Function to convert csv row to pyspark.sql.Row
def mapper(line):
    fields = line.split(',')
    return Row(
        CustomerID=int(fields[0]),
        ItemID=int(fields[1]),
        Price=float(fields[2])
    )


lines = spark.sparkContext.textFile("/files/cv05/customer-orders.csv")
orders = lines.map(mapper)

schemaOrders = spark.createDataFrame(orders).cache()
schemaOrders.printSchema()

totalSpentByCustomer = schemaOrders.groupBy('CustomerID').sum('Price') \
    .withColumnRenamed("sum(Price)", "TotalSpent") \
    .withColumn("TotalSpent", functions.round("TotalSpent", 2)) \
    .orderBy("TotalSpent", ascending=False)

totalSpentByCustomer.show()

spark.stop()

