import re
from pyspark import SparkConf, SparkContext
from config import *

conf = SparkConf().setMaster(MASTER).setAppName("WordCount")
# conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

inputText = sc.textFile("/files/book.txt")
# Remove special symbols and convert to lowcase
lines = inputText.flatMap(lambda line: re.findall(r'\b\w+\b', line.lower()))

words = lines.flatMap(lambda x: x.split())
# Change to (word, count) structure
wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
# Revert to (count, word) and sort by key
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey(ascending=False)

results = wordCountsSorted.collect()

for result in results[:20]:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if word:
        print(word.decode() + ":\t\t\t" + count)


