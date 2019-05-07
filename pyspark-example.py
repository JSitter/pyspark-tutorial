from pyspark import SparkContext
sc = SparkContext()
from pyspark.sql import SparkSession
pythonList = [2.3, 3.4, 4.3, 2.4, 2.3, 4.0]

parPythonData = sc.parallelize(pythonList,2)
print(parPythonData.collect())

print(parPythonData.first())

print(parPythonData.take(2))

print(parPythonData.getNumPartitions())

tempData = [59, 57.2, 53.6, 55.4, 51.8, 53.6, 55.4]

def fahrenheitToCentigrade(temperature):
  centigrade = (temperature-32)*5/9

parTempData = sc.parallelize(tempData, 2)

# RDD - Resilient Distributed DAtasets
print(parTempData)

print(parTempData.collect())

rdd = sc.textFile('text_file.txt')
words = rdd.flatMap(lambda x: x.split(' '))
result = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
print(result.collect())
print(result)

print(words.collect())
print(words.map(lambda x: (x,1)).collect())


spark = SparkSession \
  .builder \
  .appName("Python Spark regression example") \
  .config("spark.some.config.option", "some-value") \
    .getOrCreate()
regressionDataFrame = spark.read.csv('Advertising.csv', header=True, inferSchema = True)
print("Regressio Data Frame", type(regressionDataFrame))

print(regressionDataFrame.show(5))

print(regressionDataFrame.columns)

regressionDataFrame = regressionDataFrame.drop('_c0')
print(regressionDataFrame.show(5))

print(regressionDataFrame.columns)

print(regressionDataFrame.groupBy(regressionDataFrame.TV > 100).count().show(5))
print(regressionDataFrame.count())

print(regressionDataFrame.filter(regressionDataFrame.TV > 100).show(5))
print(regressionDataFrame.describe().show())
print(regressionDataFrame.describe(['TV', 'radio']).show())

from pyspark.mllib.regression import LabeledPoint
regressionDataRDD = regressionDataFrame.rdd.map(list)
# print(regressionDataFrame.crosstab('TV', 'radio').show())
regressionDataLabelPoint = regressionDataRDD.map(lambda data: LabeledPoint(data[3], data[0:3]))

from pyspark.mllib.regression import LinearRegressionWithSGD as lrSGD

print(regressionDataRDD.take(5))
regressionLabelPointSplit = regressionDataLabelPoint.randomSplit([0.7, 0.3])
regressionLabelPointTrainData = regressionLabelPointSplit[0]

