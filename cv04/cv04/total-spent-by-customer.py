from pyspark import SparkConf, SparkContext
from config import MASTER

conf = SparkConf().setMaster(MASTER).setAppName("RatingsHistogram")
# conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

lines = sc.textFile("/files/cv04/customer-orders.csv")


def parseInfo(line):
    fields = line.split(',')
    customerID = fields[0]
    money = float(fields[2])
    return (customerID, money)


parsedLines = lines.map(parseInfo)

# Count all customer spendings as sum
customerSpendings = parsedLines.reduceByKey(lambda x, y: x + y)
# Revert and sort in RDD
customerSpendingsSorted = customerSpendings.map(lambda x: (x[1], x[0])).sortByKey(ascending=False)

results = customerSpendingsSorted.collect()

for result in results:
    print(result)