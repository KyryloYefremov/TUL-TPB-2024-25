from pyspark.sql import SparkSession
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
from config import *

spark = SparkSession.builder.master(MASTER).appName("LinearRegrRealEstatePredictor").getOrCreate()

real_estate_df = spark.read.option("header", "true").option("inferSchema", "true").csv("/files/realestate.csv")
# real_estate.show()

assembler = VectorAssembler(
    inputCols=["HouseAge", "DistanceToMRT", "NumberConvenienceStores"],
    outputCol="features"
)

data = assembler.transform(real_estate_df).select("PriceOfUnitArea", "features").withColumnRenamed("PriceOfUnitArea", "label")

# split data to train and test 90/10
train_data, test_data = data.randomSplit([0.9, 0.1])

# train model
dtr = DecisionTreeRegressor(featuresCol="features", labelCol="label")
model = dtr.fit(train_data)

# extract predictions
full_predictions = model.transform(test_data).cache()
predictions = full_predictions.select('prediction').rdd.map(lambda x: x[0])
labels = full_predictions.select('label').rdd.map(lambda x: x[0])

# get evaluation
evaluator = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(full_predictions)
print(f"Mean error: {rmse}")

result = predictions.zip(labels).collect()

# predicted price - right price 
for pred in result[:10]:
    print(pred)


spark.stop()